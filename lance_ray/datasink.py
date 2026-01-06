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
import ray
from ray.data import DataContext
from ray.data._internal.util import _check_import
from ray.data.datasource.datasink import Datasink

from .fragment import write_fragment


@ray.remote(num_cpus=0)
class _FragmentAccumulatorActor:
    """Actor used to accumulate serialized fragment payloads across Ray tasks."""

    def __init__(self):
        self._payloads: list[tuple[bytes, bytes]] = []

    def add(self, payloads: list[tuple[bytes, bytes]]) -> None:
        self._payloads.extend(payloads)

    def pop_all(self) -> list[tuple[bytes, bytes]]:
        payloads = self._payloads
        self._payloads = []
        return payloads

if TYPE_CHECKING:
    from lance_namespace import LanceNamespace

    import pandas as pd


class _BaseLanceDatasink(Datasink):
    """Base class for Lance Datasink."""

    def __init__(
        self,
        uri: Optional[str] = None,
        namespace: Optional["LanceNamespace"] = None,
        table_id: Optional[list[str]] = None,
        *args: Any,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        storage_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        merged_storage_options = dict()
        if storage_options:
            merged_storage_options.update(storage_options)

        # Handle namespace-based table writing
        if namespace is not None and table_id is not None:
            self.table_id = table_id

            if mode == "append":
                # For append mode, we need to get existing table URI
                from lance_namespace import DescribeTableRequest

                describe_request = DescribeTableRequest(id=table_id)
                describe_response = namespace.describe_table(describe_request)
                self.uri = describe_response.location
                if describe_response.storage_options:
                    merged_storage_options.update(describe_response.storage_options)
            elif mode == "overwrite":
                # For overwrite mode, try to get existing table, fallback to create
                from lance_namespace import (
                    CreateEmptyTableRequest,
                    DescribeTableRequest,
                )

                try:
                    describe_request = DescribeTableRequest(id=table_id)
                    describe_response = namespace.describe_table(describe_request)
                    self.uri = describe_response.location
                    if describe_response.storage_options:
                        merged_storage_options.update(describe_response.storage_options)
                except Exception:
                    create_request = CreateEmptyTableRequest(id=table_id)
                    create_response = namespace.create_empty_table(create_request)
                    self.uri = create_response.location
                    if create_response.storage_options:
                        merged_storage_options.update(create_response.storage_options)
            else:
                # create mode, create an empty table
                from lance_namespace import CreateEmptyTableRequest

                create_request = CreateEmptyTableRequest(id=table_id)
                create_response = namespace.create_empty_table(create_request)
                self.uri = create_response.location
                if create_response.storage_options:
                    merged_storage_options.update(create_response.storage_options)
        else:
            self.table_id = None
            self.uri = uri

        self.schema = schema
        self.mode = mode
        self.read_version: Optional[int] = None
        self.storage_options = merged_storage_options
        self._fragment_accumulator = None

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self):
        _check_import(self, module="lance", package="pylance")

        import lance

        if self.mode == "append":
            ds = lance.LanceDataset(self.uri, storage_options=self.storage_options)
            self.read_version = ds.version
            if self.schema is None:
                self.schema = ds.schema

        self._ensure_fragment_accumulator()

    def _collect_serialized_payloads(
        self, write_result: list[list[tuple[Any, ...]]]
    ) -> list[tuple[bytes, bytes]]:
        """Collect serialized fragment/schema pairs from accumulator or write results.

        Ray 2.40+ may attach additional metadata per write result entry (e.g. stats),
        so we only keep the first two elements (fragment, schema) when normalizing.
        """

        serialized_payloads = self._drain_fragment_payloads()
        if serialized_payloads:
            return serialized_payloads

        normalized_payloads: list[tuple[bytes, bytes]] = []
        for batch in write_result or []:
            if not batch:
                continue
            for payload in batch:
                if isinstance(payload, (bytes, bytearray)):
                    raise ValueError(
                        "Write result payload must include fragment and schema; "
                        "got raw bytes."
                    )
                if not isinstance(payload, Iterable):
                    raise ValueError(
                        "Write result payload must be iterable with fragment and schema."
                    )

                items = list(payload)
                if len(items) < 2:
                    raise ValueError(
                        "Write result payload must contain at least fragment and schema."
                    )

                normalized_payloads.append((items[0], items[1]))

        return normalized_payloads

    def on_write_complete(
        self,
        write_result: list[list[tuple[Any, ...]]],
    ):
        import warnings

        import lance

        serialized_payloads = self._collect_serialized_payloads(write_result)
        if not serialized_payloads:
            warnings.warn(
                "write results is empty. please check ray version or internal error",
                DeprecationWarning,
                stacklevel=2,
            )
            return super().on_write_complete(write_result)

        fragments = []
        schema = None
        for fragment_str, schema_str in serialized_payloads:
            fragment = pickle.loads(fragment_str)
            fragments.append(fragment)
            schema = pickle.loads(schema_str)
        if not schema:
            return super().on_write_complete(write_result)

        op = None
        if self.mode in {"create", "overwrite"}:
            op = lance.LanceOperation.Overwrite(schema, fragments)
        elif self.mode == "append":
            op = lance.LanceOperation.Append(fragments)
        if op:
            lance.LanceDataset.commit(
                self.uri,
                op,
                read_version=self.read_version,
                storage_options=self.storage_options,
            )

        return super().on_write_complete(write_result)

    def on_write_failed(self, error: Exception) -> None:
        self._cleanup_fragment_accumulator()
        super().on_write_failed(error)

    def _ensure_fragment_accumulator(self) -> None:
        if self._fragment_accumulator is None:
            self._fragment_accumulator = _FragmentAccumulatorActor.remote()

    def _drain_fragment_payloads(self) -> list[tuple[bytes, bytes]]:
        if self._fragment_accumulator is None:
            return []
        payloads = ray.get(self._fragment_accumulator.pop_all.remote())
        self._cleanup_fragment_accumulator()
        return payloads

    def _cleanup_fragment_accumulator(self) -> None:
        if self._fragment_accumulator is None:
            return
        try:
            ray.kill(self._fragment_accumulator, no_restart=True)
        except Exception:
            pass
        finally:
            self._fragment_accumulator = None


class LanceDatasink(_BaseLanceDatasink):
    """Lance Ray Datasink.

    Write a Ray dataset to lance.

    If we expect to write larger-than-memory files,
    we can use `LanceFragmentWriter` and `LanceFragmentCommitter`.

    Args:
        uri : the base URI of the dataset.
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
    """

    NAME = "Lance"
    WRITE_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    WRITE_FRAGMENTS_MAX_ATTEMPTS = 10
    WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: Optional[str] = None,
        namespace: Optional["LanceNamespace"] = None,
        table_id: Optional[list[str]] = None,
        *args: Any,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        min_rows_per_file: int = 1024 * 1024,
        max_rows_per_file: int = 64 * 1024 * 1024,
        data_storage_version: Optional[str] = None,
        storage_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            uri,
            namespace,
            table_id,
            *args,
            schema=schema,
            mode=mode,
            storage_options=storage_options,
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
            retry_params=self._retry_params,
        )
        serialized_fragments = [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]
        if serialized_fragments:
            self._ensure_fragment_accumulator()
            ray.get(self._fragment_accumulator.add.remote(serialized_fragments))
        return []


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
