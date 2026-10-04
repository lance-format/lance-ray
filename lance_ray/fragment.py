# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import inspect
import pickle
import warnings
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import ExitStack
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    TypeAlias,
    Union,
    cast,
)

import pyarrow as pa
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from lance.fragment import FragmentMetadata

    import pandas as pd

__all__ = [
    "FragmentBlock",
    "FragmentStream",
    "FragmentStreamFactory",
    "LanceFragmentWriter",
    "write_fragment",
]

from .pandas import pd_to_arrow
from .utils import (
    get_write_fragments_kwargs,
    materialize_initial_bases,
    normalize_initial_bases,
)

FragmentBlock: TypeAlias = Union[pa.Table, "pd.DataFrame", dict[str, Any]]
FragmentStream: TypeAlias = Iterable[FragmentBlock]
FragmentStreamFactory: TypeAlias = Callable[[], FragmentStream]


def write_fragment(
    stream: FragmentStream | FragmentStreamFactory,
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    # Only useful for v1 writer. ``None`` defers to the pylance writer default.
    max_rows_per_group: Optional[int] = 1024,
    data_storage_version: Optional[str] = None,
    enable_stable_row_ids: bool = False,
    storage_options: Optional[dict[str, Any]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    initial_bases: Optional[list[Any]] = None,
    target_bases: Optional[list[str]] = None,
    target_all_bases: Optional[bool] = None,
    external_blob_mode: Literal["reference", "ingest"] = "reference",
    allow_external_blob_outside_bases: bool = False,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    retry_params: Optional[dict[str, Any]] = None,
) -> list[tuple["FragmentMetadata", pa.Schema]]:
    """Write uncommitted fragments, checking their total row count against input.

    Without an explicit ``retry_params["max_attempts"]``, consume the input
    once using a streaming reader. Multiple attempts require a replayable
    iterable or a zero-argument factory returning a fresh stream per attempt;
    directly supplied one-shot iterators are rejected before consumption.
    The caller must ensure each iteration produces complete, logically
    equivalent input. A factory returning the same generator is not sufficient.

    Each attempt recreates the reader and infers its schema independently.
    Factory/reiterable-created iterators supporting ``close()`` are closed on
    exit; caller-supplied one-shot iterators remain the caller's responsibility.
    No input is staged or serialized for replay. Row-count validation does not
    check content equivalence or clean up files from failed attempts.
    """
    from lance.dependencies import _PANDAS_AVAILABLE
    from lance.dependencies import pandas as pd
    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

    max_bytes_per_file = (
        DEFAULT_MAX_BYTES_PER_FILE if max_bytes_per_file is None else max_bytes_per_file
    )

    # Use default retry params if not provided
    if retry_params is None:
        retry_params = {
            "description": "write lance fragments",
            "match": [],
            "max_attempts": 1,
            "max_backoff_s": 0,
        }
    else:
        # Override Ray's default of ten attempts without changing the caller's dict.
        retry_params = {"max_attempts": 1, **retry_params}

    if retry_params["max_attempts"] > 1 and isinstance(stream, Iterator):
        raise TypeError(
            "Retry-enabled write_fragment requires a stream factory or a "
            "replayable iterable, not a one-shot Iterator. Pass the generator "
            "function instead of calling it, or use a list or tuple."
        )

    def _write_fragments(
        reader: pa.RecordBatchReader, attempt_schema: pa.Schema
    ) -> list["FragmentMetadata"]:
        write_kwargs = get_write_fragments_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        initial_bases_kwargs: dict[str, Any] = {}
        if initial_bases:
            initial_bases_kwargs["initial_bases"] = materialize_initial_bases(
                initial_bases
            )

        optional_write_kwargs = _get_optional_write_fragments_kwargs(
            write_fragments,
            target_bases=target_bases,
            target_all_bases=target_all_bases,
            base_store_params=base_store_params,
            external_blob_mode=external_blob_mode,
            allow_external_blob_outside_bases=allow_external_blob_outside_bases,
        )

        return write_fragments(
            reader,
            uri,
            return_transaction=False,
            schema=attempt_schema,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            max_bytes_per_file=max_bytes_per_file,
            data_storage_version=data_storage_version,
            enable_stable_row_ids=enable_stable_row_ids,
            storage_options=storage_options,
            **write_kwargs,
            **initial_bases_kwargs,
            **optional_write_kwargs,
        )

    def write_once() -> tuple[list["FragmentMetadata"], Optional[pa.Schema], int]:
        with ExitStack() as resources:
            stream_iter = iter(stream() if callable(stream) else stream)
            if callable(stream) or not isinstance(stream, Iterator):
                close = getattr(stream_iter, "close", None)
                if callable(close):
                    resources.callback(close)
            try:
                first = next(stream_iter)
            except StopIteration:
                return [], schema, 0

            attempt_schema = schema
            if attempt_schema is None:
                if _PANDAS_AVAILABLE and isinstance(first, pd.DataFrame):
                    attempt_schema = pa.Schema.from_pandas(first).remove_metadata()
                elif isinstance(first, dict):
                    attempt_schema = pa.Table.from_pydict(
                        first
                    ).schema.remove_metadata()
                else:
                    attempt_schema = cast(pa.Table, first).schema

            if len(attempt_schema.names) == 0:
                return [], attempt_schema, 0

            input_rows = 0

            def record_batch_converter() -> Generator[pa.RecordBatch, None, None]:
                nonlocal input_rows
                for block in chain([first], stream_iter):
                    table = pd_to_arrow(block, attempt_schema)
                    for batch in table.to_batches():
                        input_rows += batch.num_rows
                        yield batch

            batches = record_batch_converter()
            resources.callback(batches.close)
            reader = resources.enter_context(
                pa.RecordBatchReader.from_batches(attempt_schema, batches)
            )
            fragments = _write_fragments(reader, attempt_schema)
            # Include an unexpected unread remainder so an early return from
            # the writer cannot hide missing rows.
            for _ in reader:
                pass
            return fragments, attempt_schema, input_rows

    fragments, result_schema, input_rows = call_with_retry(write_once, **retry_params)
    fragment_rows = sum(fragment.num_rows for fragment in fragments)
    if fragment_rows != input_rows:
        raise RuntimeError(
            "Lance fragment write row count mismatch: "
            f"expected {input_rows}, wrote {fragment_rows}"
        )
    if result_schema is None:
        return []
    return [(fragment, result_schema) for fragment in fragments]


def _get_optional_write_fragments_kwargs(
    write_fragments: Callable[..., Any],
    *,
    target_bases: Optional[list[str]],
    target_all_bases: Optional[bool],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    external_blob_mode: Literal["reference", "ingest"],
    allow_external_blob_outside_bases: bool,
) -> dict[str, Any]:
    """Return kwargs supported by the installed pylance fragment writer."""
    params, allow_external_blob_outside_bases = _prepare_write_fragments_options(
        write_fragments,
        target_bases=target_bases,
        target_all_bases=target_all_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
        stacklevel=7,
    )
    kwargs: dict[str, Any] = {}

    if "target_bases" in params and target_bases is not None:
        kwargs["target_bases"] = target_bases

    if "target_all_bases" in params and target_all_bases is not None:
        kwargs["target_all_bases"] = target_all_bases

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
    target_all_bases: Optional[bool] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    external_blob_mode: Literal["reference", "ingest"],
    allow_external_blob_outside_bases: bool,
    stacklevel: int = 2,
) -> bool:
    """Validate fragment write options and return normalized allow flag."""
    if (
        target_bases is None
        and target_all_bases is None
        and base_store_params is None
        and external_blob_mode == "reference"
        and not allow_external_blob_outside_bases
    ):
        return allow_external_blob_outside_bases

    from lance.fragment import write_fragments

    _, allow_external_blob_outside_bases = _prepare_write_fragments_options(
        write_fragments,
        target_bases=target_bases,
        target_all_bases=target_all_bases,
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
    target_all_bases: Optional[bool],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    external_blob_mode: Literal["reference", "ingest"],
    allow_external_blob_outside_bases: bool,
    stacklevel: int,
) -> tuple[Mapping[str, inspect.Parameter], bool]:
    if target_bases and target_all_bases is not None:
        raise ValueError("'target_bases' and 'target_all_bases' are mutually exclusive")

    params = inspect.signature(write_fragments).parameters

    if target_bases is not None and "target_bases" not in params:
        raise _unsupported_write_fragments_option_error("target_bases")

    if target_all_bases is not None and "target_all_bases" not in params:
        raise _unsupported_write_fragments_option_error("target_all_bases")

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
    transform : Callable[[pa.Table], Union[pa.Table, Iterable[pa.Table]]], optional
        A callable to transform the input batch. Default is None. Each retry
        invokes it again with the same original Arrow table. It must produce
        logically equivalent output; callers must handle repeated side effects.
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
    enable_stable_row_ids : bool, default False
        Enable stable row IDs for fragments written into a stable-row-ID dataset.
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
    target_all_bases : bool, optional
        Select all registered bases, including primary storage when True and
        excluding it when False. None preserves the default. Mutually exclusive
        with non-empty target_bases. Round-robin restarts on each invocation.
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
        None or an omitted 'max_attempts' means a single streaming attempt.
        Multiple attempts recreate the transformed stream from the original
        Arrow table, without IPC staging. The transform may execute more than
        once and must return complete, logically equivalent output each time.

    """

    def __init__(
        self,
        uri: str,
        *,
        transform: Optional[Callable[[pa.Table], pa.Table | Iterable[pa.Table]]] = None,
        schema: Optional[pa.Schema] = None,
        max_rows_per_file: int = 1024 * 1024,
        max_bytes_per_file: Optional[int] = None,
        max_rows_per_group: Optional[int] = None,  # Only useful for v1 writer.
        data_storage_version: Optional[str] = None,
        enable_stable_row_ids: bool = False,
        use_legacy_format: Optional[bool] = False,
        storage_options: Optional[dict[str, Any]] = None,
        base_store_params: Optional[dict[str, dict[str, Any]]] = None,
        initial_bases: Optional[list[Any]] = None,
        target_bases: Optional[list[str]] = None,
        target_all_bases: Optional[bool] = None,
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
            target_all_bases=target_all_bases,
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
        self.enable_stable_row_ids = enable_stable_row_ids
        self.storage_options = storage_options
        self.base_store_params = base_store_params
        self.initial_bases = normalize_initial_bases(initial_bases)
        self.target_bases = target_bases
        self.target_all_bases = target_all_bases
        self.external_blob_mode = external_blob_mode
        self.allow_external_blob_outside_bases = allow_external_blob_outside_bases
        self.namespace_impl = namespace_impl
        self.namespace_properties = namespace_properties
        self.table_id = table_id
        self.retry_params = retry_params

    def __call__(
        self, batch: Union[pa.Table, "pd.DataFrame", dict[str, Any]]
    ) -> pa.Table:
        """Write a Batch to the Lance fragment."""
        # Convert dict/numpy arrays to pyarrow table if needed
        table: pa.Table
        if isinstance(batch, dict):
            table = pa.Table.from_pydict(batch)
        else:
            # Only convert when the input is an actual pandas DataFrame.
            # Some objects (including pyarrow.Table) may implement the
            # dataframe interchange protocol `__dataframe__`, but they are
            # not pandas DataFrames. Using `hasattr(..., "__dataframe__")`
            # incorrectly routes them through `Table.from_pandas` and causes
            # errors. Perform a strict isinstance check instead.
            try:
                from pandas import DataFrame as _PandasDataFrame
            except Exception:
                _PandasDataFrame = None  # type: ignore[assignment,misc]

            if _PandasDataFrame is not None and isinstance(batch, _PandasDataFrame):
                table = pa.Table.from_pandas(batch)
            else:
                # Anything else is handed to the transform unchanged, as an
                # Arrow table is the only remaining documented block type.
                table = cast(pa.Table, batch)

        def stream_factory() -> Iterator[pa.Table]:
            transformed = self.transform(table)
            if isinstance(transformed, pa.Table):
                return iter((transformed,))
            return iter(transformed)

        fragments = write_fragment(
            stream_factory,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            max_rows_per_group=self.max_rows_per_group,
            max_bytes_per_file=self.max_bytes_per_file,
            data_storage_version=self.data_storage_version,
            enable_stable_row_ids=self.enable_stable_row_ids,
            storage_options=self.storage_options,
            base_store_params=self.base_store_params,
            initial_bases=self.initial_bases,
            target_bases=self.target_bases,
            target_all_bases=self.target_all_bases,
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
