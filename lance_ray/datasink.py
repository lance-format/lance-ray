import pickle
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    Union,
)

import pyarrow as pa
from lance_namespace import DescribeTableRequest
from ray.data import DataContext
from ray.data._internal.util import _check_import
from ray.data.datasource.datasink import Datasink

from .fragment import prepare_fragment_write_options, write_fragment
from .utils import (
    get_namespace_kwargs,
    get_or_create_namespace,
    materialize_initial_bases,
    normalize_initial_bases,
)

if TYPE_CHECKING:
    import pandas as pd


def _declare_table_with_fallback(
    namespace, table_id: list[str]
) -> tuple[str, Optional[dict[str, str]]]:
    """Declare a table using declare_table, falling back to create_empty_table.

    Returns:
        Tuple of (uri, storage_options)
    """
    try:
        from lance_namespace import DeclareTableRequest

        declare_request = DeclareTableRequest(id=table_id, location=None)
        declare_response = namespace.declare_table(declare_request)
        return declare_response.location, declare_response.storage_options
    except (AttributeError, NotImplementedError):
        # Fallback for older namespace implementations without declare_table
        from lance_namespace import CreateEmptyTableRequest

        create_request = CreateEmptyTableRequest(id=table_id)
        create_response = namespace.create_empty_table(create_request)
        return create_response.location, create_response.storage_options


class _BaseLanceDatasink(Datasink):
    """Base class for Lance Datasink."""

    def __init__(
        self,
        uri: Optional[str] = None,
        table_id: Optional[list[str]] = None,
        *args: Any,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        storage_options: Optional[dict[str, Any]] = None,
        base_store_params: Optional[dict[str, dict[str, Any]]] = None,
        initial_bases: Optional[list[Any]] = None,
        target_bases: Optional[list[str]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        if initial_bases and mode != "create":
            raise ValueError("'initial_bases' can only be used with mode='create'")

        merged_storage_options = dict()
        if storage_options:
            merged_storage_options.update(storage_options)

        # Store namespace_impl and namespace_properties for worker reconstruction
        self._namespace_impl = namespace_impl
        self._namespace_properties = namespace_properties

        # Construct namespace from impl and properties (cached per worker)
        namespace = get_or_create_namespace(namespace_impl, namespace_properties)

        if namespace is not None and table_id is not None:
            self.table_id = table_id

            if mode == "append":
                # For append mode, we need to get existing table URI
                describe_request = DescribeTableRequest(id=table_id)
                describe_response = namespace.describe_table(describe_request)
                self.uri = describe_response.location
                if describe_response.storage_options:
                    merged_storage_options.update(describe_response.storage_options)
            elif mode == "overwrite":
                # For overwrite mode, try to get existing table, fallback to declare
                try:
                    describe_request = DescribeTableRequest(id=table_id)
                    describe_response = namespace.describe_table(describe_request)
                    self.uri = describe_response.location
                    if describe_response.storage_options:
                        merged_storage_options.update(describe_response.storage_options)
                except Exception:
                    uri, ns_storage_options = _declare_table_with_fallback(
                        namespace, table_id
                    )
                    self.uri = uri
                    if ns_storage_options:
                        merged_storage_options.update(ns_storage_options)
            else:
                # create mode, declare a new table
                uri, ns_storage_options = _declare_table_with_fallback(
                    namespace, table_id
                )
                self.uri = uri
                if ns_storage_options:
                    merged_storage_options.update(ns_storage_options)
        else:
            self.table_id = None
            self.uri = uri

        self.schema = schema
        self.mode = mode
        self.read_version: Optional[int] = None
        self.storage_options = merged_storage_options
        self.base_store_params = base_store_params
        self.initial_bases = normalize_initial_bases(initial_bases)
        self.target_bases = target_bases

    @property
    def namespace_kwargs(self) -> dict[str, Any]:
        """Namespace wiring for pylance credential refresh."""
        return get_namespace_kwargs(
            self._namespace_impl, self._namespace_properties, self.table_id
        )

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self, schema: Optional[pa.Schema] = None):
        _check_import(self, module="lance", package="pylance")

        import lance

        if self.mode == "append":
            base_store_params_kwargs = {}
            if self.base_store_params:
                base_store_params_kwargs = {"base_store_params": self.base_store_params}
            ds = lance.LanceDataset(
                self.uri,
                storage_options=self.storage_options,
                **self.namespace_kwargs,
                **base_store_params_kwargs,
            )
            self.read_version = ds.version
            if self.schema is None:
                self.schema = ds.schema

    def on_write_complete(
        self,
        write_result: list[list[tuple[str, str]]],
    ):
        import warnings

        import lance

        write_results = write_result
        if not write_results:
            warnings.warn(
                "write_results is empty.",
                RuntimeWarning,
                stacklevel=2,
            )
            return
        if hasattr(write_results, "write_returns"):
            write_results = write_results.write_returns  # type: ignore

        if len(write_results) == 0:
            warnings.warn(
                "write results is empty. please check ray version or internal error",
                RuntimeWarning,
                stacklevel=2,
            )
            return

        fragments = []
        schema = None
        for batch in write_results:
            for fragment_str, schema_str in batch:
                fragment = pickle.loads(fragment_str)
                fragments.append(fragment)
                schema = pickle.loads(schema_str)
        # Check weather writer has fragments or not.
        # Skip commit when there are no fragments.
        if not schema:
            return
        op = None
        if self.mode in {"create", "overwrite"}:
            op = lance.LanceOperation.Overwrite(
                schema,
                fragments,
                initial_bases=(
                    materialize_initial_bases(self.initial_bases)
                    if self.mode == "create"
                    else None
                ),
            )
        elif self.mode == "append":
            op = lance.LanceOperation.Append(fragments)
        if op:
            base_store_params_kwargs = {}
            if self.base_store_params:
                base_store_params_kwargs = {"base_store_params": self.base_store_params}
            lance.LanceDataset.commit(
                self.uri,
                op,
                read_version=self.read_version,
                storage_options=self.storage_options,
                **self.namespace_kwargs,
                **base_store_params_kwargs,
            )


class LanceDatasink(_BaseLanceDatasink):
    """Lance Ray Datasink.

    Write a Ray dataset to lance.

    If we expect to write larger-than-memory files,
    we can use `LanceFragmentWriter` and `LanceFragmentCommitter`.

    Args:
        uri : the base URI of the dataset.
        table_id : List[str], optional
            The table identifier as a list of strings.
            Used together with namespace_impl and namespace_properties.
        schema : pyarrow.Schema, optional.
            The schema of the dataset.
        mode : str, optional
            The write mode. Default is 'append'.
            Choices are 'append', 'create', 'overwrite'.
        min_rows_per_file : int, optional
            The minimum number of rows per file. Default is 1024 * 1024.
        max_rows_per_file : int, optional
            The maximum number of rows per file. Default is 64 * 1024 * 1024.
        data_storage_version: optional, str, default None
            The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read.  The default is
            "legacy" which will use the legacy v1 version.  See the user guide
            for more details.
        storage_options : Dict[str, Any], optional
            The storage options for the writer. Default is None.
        base_store_params : dict, optional
            Runtime-only storage options keyed by registered base path URI.
            Used for BlobV2 references that live outside the dataset root.
        initial_bases : list, optional
            Lance DatasetBasePath objects to register when creating a new dataset.
        target_bases : list of str, optional
            References to base paths where data should be written. Each string
            is resolved by matching base name or base path URI from registered
            bases.
        external_blob_mode : {"reference", "ingest"}, default "reference"
            How external blob URIs are handled on write. ``"reference"`` stores
            external blob references, while ``"ingest"`` reads external bytes
            and writes them into Lance-managed storage.
        allow_external_blob_outside_bases : bool, default False
            Whether external blob references outside registered bases are allowed.
            Only applies when ``external_blob_mode="reference"``.
        namespace_impl : str, optional
            The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id for credentials
            vending in distributed workers.
        namespace_properties : Dict[str, str], optional
            Properties for connecting to the namespace.
            Used together with namespace_impl and table_id for credentials vending.
    """

    NAME = "Lance"
    WRITE_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    WRITE_FRAGMENTS_MAX_ATTEMPTS = 10
    WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: Optional[str] = None,
        table_id: Optional[list[str]] = None,
        *args: Any,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        min_rows_per_file: int = 1024 * 1024,
        max_rows_per_file: int = 64 * 1024 * 1024,
        data_storage_version: Optional[str] = None,
        storage_options: Optional[dict[str, Any]] = None,
        base_store_params: Optional[dict[str, dict[str, Any]]] = None,
        initial_bases: Optional[list[Any]] = None,
        target_bases: Optional[list[str]] = None,
        external_blob_mode: Literal["reference", "ingest"] = "reference",
        allow_external_blob_outside_bases: bool = False,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ):
        allow_external_blob_outside_bases = prepare_fragment_write_options(
            target_bases=target_bases,
            base_store_params=base_store_params,
            external_blob_mode=external_blob_mode,
            allow_external_blob_outside_bases=allow_external_blob_outside_bases,
            stacklevel=2,
        )

        super().__init__(
            uri,
            table_id,
            *args,
            schema=schema,
            mode=mode,
            storage_options=storage_options,
            base_store_params=base_store_params,
            initial_bases=initial_bases,
            target_bases=target_bases,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            **kwargs,
        )

        if min_rows_per_file is None or min_rows_per_file <= 0:
            raise ValueError("min_rows_per_file must not be None and must be positive")
        if max_rows_per_file is None or max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must not be None and must be positive")
        if min_rows_per_file > max_rows_per_file:
            raise ValueError(
                f"min_rows_per_file: {min_rows_per_file} must be less than max_rows_per_file: {max_rows_per_file}"
            )
        self.min_rows_per_file = min_rows_per_file
        self.max_rows_per_file = max_rows_per_file
        self.data_storage_version = data_storage_version
        self.external_blob_mode = external_blob_mode
        self.allow_external_blob_outside_bases = allow_external_blob_outside_bases
        # if mode is append, read_version is read from existing dataset.
        self.read_version: Optional[int] = None

        match = []
        match.extend(self.WRITE_FRAGMENTS_ERRORS_TO_RETRY)
        match.extend(DataContext.get_current().retried_io_errors)
        self._retry_params = {
            "description": "write lance fragments",
            "match": match,
            "max_attempts": self.WRITE_FRAGMENTS_MAX_ATTEMPTS,
            "max_backoff_s": self.WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS,
        }

    @property
    def min_rows_per_write(self) -> int:
        return self.min_rows_per_file

    def get_name(self) -> str:
        return self.NAME

    def write(
        self,
        blocks: Iterable[Union[pa.Table, "pd.DataFrame"]],
        ctx: Any,
    ):
        fragments_and_schema = write_fragment(
            blocks,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            base_store_params=self.base_store_params,
            initial_bases=self.initial_bases if self.mode == "create" else None,
            target_bases=self.target_bases,
            external_blob_mode=self.external_blob_mode,
            allow_external_blob_outside_bases=self.allow_external_blob_outside_bases,
            namespace_impl=self._namespace_impl,
            namespace_properties=self._namespace_properties,
            table_id=self.table_id,
            retry_params=self._retry_params,
        )
        return [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]


class LanceFragmentCommitter(_BaseLanceDatasink):
    """Lance Committer as Ray Datasink.

    This is used with `LanceFragmentWriter` to write large-than-memory data to
    lance file.
    """

    @property
    def num_rows_per_write(self) -> int:
        return 1

    def get_name(self) -> str:
        return f"LanceCommitter({self.mode})"

    def write(
        self,
        blocks: Iterable[Union[pa.Table, "pd.DataFrame"]],
        _ctx: Any,
    ):
        """Passthrough the fragments to commit phase"""
        v = []
        for block in blocks:
            # If block is empty, skip to get "fragment" and "schema" filed
            if len(block) == 0:
                continue

            for fragment, schema in zip(
                block["fragment"].to_pylist(), block["schema"].to_pylist(), strict=False
            ):
                v.append((fragment, schema))
        return v
