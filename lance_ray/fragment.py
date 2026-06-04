# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import inspect
import pickle
import warnings
from collections.abc import Callable, Generator, Iterable, Mapping
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    Union,
)

import pyarrow as pa
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from lance.fragment import FragmentMetadata

    import pandas as pd

__all__ = [
    "LanceFragmentWriter",
    "write_fragment",
]

from .pandas import pd_to_arrow
from .utils import (
    get_write_fragments_kwargs,
    materialize_initial_bases,
    normalize_initial_bases,
)


def write_fragment(
    stream: Iterable[Union[pa.Table, "pd.DataFrame"]],
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    mode: Literal["create", "append", "overwrite"] = "append",
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    max_rows_per_group: int = 1024,  # Only useful for v1 writer.
    data_storage_version: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    initial_bases: Optional[list[Any]] = None,
    target_bases: Optional[list[str]] = None,
    external_blob_mode: Literal["reference", "ingest"] = "reference",
    allow_external_blob_outside_bases: bool = False,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    retry_params: Optional[dict[str, Any]] = None,
) -> list[tuple["FragmentMetadata", pa.Schema]]:
    from lance.dependencies import _PANDAS_AVAILABLE
    from lance.dependencies import pandas as pd
    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

    stream_iter = iter(stream)
    try:
        first = next(stream_iter)
    except StopIteration:
        return []

    if schema is None:
        if _PANDAS_AVAILABLE and isinstance(first, pd.DataFrame):
            schema = pa.Schema.from_pandas(first).remove_metadata()
        elif isinstance(first, dict):
            tbl = pa.Table.from_pydict(first)
            schema = tbl.schema.remove_metadata()
        else:
            schema = first.schema

    if schema is None or len(schema.names) == 0:
        return []

    stream = chain([first], stream_iter)

    def record_batch_converter():
        for block in stream:
            tbl = pd_to_arrow(block, schema)
            yield from tbl.to_batches()

    max_bytes_per_file = (
        DEFAULT_MAX_BYTES_PER_FILE if max_bytes_per_file is None else max_bytes_per_file
    )

    reader = pa.RecordBatchReader.from_batches(schema, record_batch_converter())

    # Use default retry params if not provided
    if retry_params is None:
        retry_params = {
            "description": "write lance fragments",
            "match": [],
            "max_attempts": 1,
            "max_backoff_s": 0,
        }

    write_kwargs = get_write_fragments_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    if initial_bases:
        initial_bases_kwargs = {
            "initial_bases": materialize_initial_bases(initial_bases)
        }
    else:
        initial_bases_kwargs = {}

    optional_write_kwargs = _get_optional_write_fragments_kwargs(
        write_fragments,
        target_bases=target_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
    )

    fragments = call_with_retry(
        lambda: write_fragments(
            reader,
            uri,
            schema=schema,
            mode=mode,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            max_bytes_per_file=max_bytes_per_file,
            data_storage_version=data_storage_version,
            storage_options=storage_options,
            **write_kwargs,
            **initial_bases_kwargs,
            **optional_write_kwargs,
        ),
        **retry_params,
    )
    return [(fragment, schema) for fragment in fragments]


def _get_optional_write_fragments_kwargs(
    write_fragments: Callable[..., Any],
    *,
    target_bases: Optional[list[str]],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    external_blob_mode: Literal["reference", "ingest"],
    allow_external_blob_outside_bases: bool,
) -> dict[str, Any]:
    """Return kwargs supported by the installed pylance fragment writer."""
    params, allow_external_blob_outside_bases = _prepare_write_fragments_options(
        write_fragments,
        target_bases=target_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
        stacklevel=4,
    )
    kwargs: dict[str, Any] = {}

    if "target_bases" in params and target_bases is not None:
        kwargs["target_bases"] = target_bases

    if "base_store_params" in params and base_store_params is not None:
        kwargs["base_store_params"] = base_store_params

    if "external_blob_mode" in params:
        kwargs["external_blob_mode"] = external_blob_mode

    if "allow_external_blob_outside_bases" in params:
        kwargs["allow_external_blob_outside_bases"] = allow_external_blob_outside_bases

    return kwargs


def prepare_fragment_write_options(
    *,
    target_bases: Optional[list[str]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    external_blob_mode: Literal["reference", "ingest"],
    allow_external_blob_outside_bases: bool,
    stacklevel: int = 2,
) -> bool:
    """Validate fragment write options and return normalized allow flag."""
    if (
        target_bases is None
        and base_store_params is None
        and external_blob_mode == "reference"
        and not allow_external_blob_outside_bases
    ):
        return allow_external_blob_outside_bases

    from lance.fragment import write_fragments

    _, allow_external_blob_outside_bases = _prepare_write_fragments_options(
        write_fragments,
        target_bases=target_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
        stacklevel=stacklevel + 2,
    )
    return allow_external_blob_outside_bases


def _prepare_write_fragments_options(
    write_fragments: Callable[..., Any],
    *,
    target_bases: Optional[list[str]],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    external_blob_mode: Literal["reference", "ingest"],
    allow_external_blob_outside_bases: bool,
    stacklevel: int,
) -> tuple[Mapping[str, inspect.Parameter], bool]:
    params = inspect.signature(write_fragments).parameters

    if target_bases is not None and "target_bases" not in params:
        raise _unsupported_write_fragments_option_error("target_bases")

    if base_store_params is not None and "base_store_params" not in params:
        raise _unsupported_write_fragments_option_error("base_store_params")

    if "external_blob_mode" not in params and external_blob_mode != "reference":
        raise _unsupported_write_fragments_option_error("external_blob_mode")

    if external_blob_mode == "reference":
        if (
            "allow_external_blob_outside_bases" not in params
            and allow_external_blob_outside_bases
        ):
            raise _unsupported_write_fragments_option_error(
                "allow_external_blob_outside_bases"
            )
    elif external_blob_mode == "ingest" and allow_external_blob_outside_bases:
        warnings.warn(
            "'allow_external_blob_outside_bases' only applies when "
            "'external_blob_mode=\"reference\"' and will be ignored when "
            "'external_blob_mode=\"ingest\"'.",
            stacklevel=stacklevel,
        )
        allow_external_blob_outside_bases = False

    return params, allow_external_blob_outside_bases


def _unsupported_write_fragments_option_error(option: str) -> RuntimeError:
    return RuntimeError(
        f"The installed pylance does not support '{option}' in "
        "lance.fragment.write_fragments. Install a pylance build with the "
        "required fragment write option."
    )


class LanceFragmentWriter:
    """Write a fragment to one of Lance fragment.

    This Writer can be used in case to write large-than-memory data to lance,
    in distributed fashion.

    Parameters
    ----------
    uri : str
        The base URI of the dataset.

        For namespace-based tables, resolve the URI first before distributing the writes:
        - namespace.describe_table(DescribeTableRequest(id=table_id)) to get existing table
        - namespace.create_empty_table(CreateEmptyTableRequest(id=table_id)) to create new table

        Then use the returned location as the uri. This ensures all distributed workers
        write to the same resolved location.
    transform : Callable[[pa.Table], Union[pa.Table, Generator]], optional
        A callable to transform the input batch. Default is None.
    schema : pyarrow.Schema, optional
        The schema of the dataset.
    max_rows_per_file : int, optional
        The maximum number of rows per file. Default is 1024 * 1024.
    max_bytes_per_file : int, optional
        The maximum number of bytes per file. Default is 90GB.
    max_rows_per_group : int, optional
        The maximum number of rows per group. Default is 1024.
        Only useful for v1 writer.
    data_storage_version: optional, str, default None
        The version of the data storage format to use. Newer versions are more
        efficient but require newer versions of lance to read.  The default
        (None) will use the 2.0 version.  See the user guide for more details.
    mode : {"create", "append", "overwrite"}, default "append"
        Fragment write mode passed to pylance. Append validates fragments against
        the existing dataset schema; create and overwrite assign field ids for
        the new schema before the final commit.
    use_legacy_format : optional, bool, default None
        Deprecated method for setting the data storage version. Use the
        `data_storage_version` parameter instead.
        storage_options : Dict[str, Any], optional
            The storage options for the writer. Default is None.
    initial_bases : list, optional
        Lance DatasetBasePath objects to register when creating a new dataset.
    target_bases : list of str, optional
        References to base paths where data should be written. Each string
        is resolved by matching base name or base path URI from registered
        bases.
    base_store_params : dict, optional
        Runtime-only storage options keyed by registered base path URI.
    external_blob_mode : {"reference", "ingest"}, default "reference"
        How external blob URIs are handled on write.
    allow_external_blob_outside_bases : bool, default False
        Whether external blob references outside registered bases are allowed.
    namespace_impl : str, optional
        The namespace implementation type (e.g., "rest", "dir").
        Used together with namespace_properties and table_id for credentials
        vending in distributed workers.
    namespace_properties : Dict[str, str], optional
        Properties for connecting to the namespace.
        Used together with namespace_impl and table_id for credentials vending.
    table_id : List[str], optional
        The table identifier as a list of strings.
        Used together with namespace_impl and namespace_properties for
        credentials vending.
    retry_params : Dict[str, Any], optional
        Retry parameters for write operations. Default is None.
        If provided, should contain keys like 'description', 'match',
        'max_attempts', and 'max_backoff_s'.

    """

    def __init__(
        self,
        uri: str,
        *,
        transform: Optional[Callable[[pa.Table], pa.Table | Generator]] = None,
        schema: Optional[pa.Schema] = None,
        max_rows_per_file: int = 1024 * 1024,
        max_bytes_per_file: Optional[int] = None,
        max_rows_per_group: Optional[int] = None,  # Only useful for v1 writer.
        data_storage_version: Optional[str] = None,
        mode: Literal["create", "append", "overwrite"] = "append",
        use_legacy_format: Optional[bool] = False,
        storage_options: Optional[dict[str, Any]] = None,
        base_store_params: Optional[dict[str, dict[str, Any]]] = None,
        initial_bases: Optional[list[Any]] = None,
        target_bases: Optional[list[str]] = None,
        external_blob_mode: Literal["reference", "ingest"] = "reference",
        allow_external_blob_outside_bases: bool = False,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[dict[str, str]] = None,
        table_id: Optional[list[str]] = None,
        retry_params: Optional[dict[str, Any]] = None,
    ):
        if use_legacy_format is not None and data_storage_version is None:
            warnings.warn(
                "The `use_legacy_format` parameter is deprecated. Use the "
                "`data_storage_version` parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )

            data_storage_version = "legacy" if use_legacy_format else "stable"

        allow_external_blob_outside_bases = prepare_fragment_write_options(
            target_bases=target_bases,
            base_store_params=base_store_params,
            external_blob_mode=external_blob_mode,
            allow_external_blob_outside_bases=allow_external_blob_outside_bases,
            stacklevel=2,
        )

        self.uri = uri
        self.schema = schema
        self.transform = transform if transform is not None else lambda x: x

        self.max_rows_per_group = max_rows_per_group
        self.max_rows_per_file = max_rows_per_file
        self.max_bytes_per_file = max_bytes_per_file
        self.data_storage_version = data_storage_version
        self.mode = mode
        self.storage_options = storage_options
        self.base_store_params = base_store_params
        self.initial_bases = normalize_initial_bases(initial_bases)
        self.target_bases = target_bases
        self.external_blob_mode = external_blob_mode
        self.allow_external_blob_outside_bases = allow_external_blob_outside_bases
        self.namespace_impl = namespace_impl
        self.namespace_properties = namespace_properties
        self.table_id = table_id
        self.retry_params = retry_params

    def __call__(self, batch: Union[pa.Table, "pd.DataFrame", dict]) -> pa.Table:
        """Write a Batch to the Lance fragment."""
        # Convert dict/numpy arrays to pyarrow table if needed
        if isinstance(batch, dict):
            batch = pa.Table.from_pydict(batch)
        else:
            # Only convert when the input is an actual pandas DataFrame.
            # Some objects (including pyarrow.Table) may implement the
            # dataframe interchange protocol `__dataframe__`, but they are
            # not pandas DataFrames. Using `hasattr(..., "__dataframe__")`
            # incorrectly routes them through `Table.from_pandas` and causes
            # errors. Perform a strict isinstance check instead.
            try:
                from pandas import DataFrame as _PandasDataFrame  # type: ignore
            except Exception:
                _PandasDataFrame = None  # type: ignore

            if _PandasDataFrame is not None and isinstance(batch, _PandasDataFrame):
                batch = pa.Table.from_pandas(batch)

        transformed = self.transform(batch)
        if not isinstance(transformed, Generator):
            transformed = (t for t in [transformed])

        fragments = write_fragment(
            transformed,
            self.uri,
            schema=self.schema,
            mode=self.mode,
            max_rows_per_file=self.max_rows_per_file,
            max_rows_per_group=self.max_rows_per_group,
            max_bytes_per_file=self.max_bytes_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            base_store_params=self.base_store_params,
            initial_bases=self.initial_bases,
            target_bases=self.target_bases,
            external_blob_mode=self.external_blob_mode,
            allow_external_blob_outside_bases=self.allow_external_blob_outside_bases,
            namespace_impl=self.namespace_impl,
            namespace_properties=self.namespace_properties,
            table_id=self.table_id,
            retry_params=self.retry_params,
        )
        return pa.Table.from_pydict(
            {
                "fragment": [pickle.dumps(fragment) for fragment, _ in fragments],
                "schema": [pickle.dumps(schema) for _, schema in fragments],
            }
        )
