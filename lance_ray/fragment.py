# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import inspect
import io
import os
import pickle
import tempfile
import warnings
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    Union,
    cast,
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

_WRITE_REPLAY_MEMORY_THRESHOLD_ENV = "LANCE_RAY_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES"
_DEFAULT_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES = 128 * 1024 * 1024


def _write_replay_memory_threshold() -> int:
    value = os.environ.get(_WRITE_REPLAY_MEMORY_THRESHOLD_ENV)
    if value is None:
        return _DEFAULT_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES
    try:
        threshold = int(value)
    except ValueError:
        raise ValueError(
            f"{_WRITE_REPLAY_MEMORY_THRESHOLD_ENV} must be a non-negative integer "
            "number of bytes"
        ) from None
    if threshold < 0:
        raise ValueError(
            f"{_WRITE_REPLAY_MEMORY_THRESHOLD_ENV} must be a non-negative integer "
            "number of bytes"
        )
    return threshold


def write_fragment(
    stream: Iterable[Union[pa.Table, "pd.DataFrame", dict[str, Any]]],
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    # Only useful for v1 writer. ``None`` defers to the pylance writer default,
    # which pylance's own annotation (``int``) does not model.
    max_rows_per_group: Optional[int] = 1024,
    data_storage_version: Optional[str] = None,
    enable_stable_row_ids: bool = False,
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
    """Write uncommitted fragments, checking their total row count against input.

    Without ``retry_params``, write once using a streaming reader. When multiple
    attempts are allowed, spool this call's input to a temporary Arrow IPC stream
    before writing, then open a fresh reader for each attempt. The stream stays
    in memory up to 128 MiB by default, then rolls entirely to a temporary file.
    ``LANCE_RAY_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES`` overrides this threshold
    per call; zero forces disk immediately. Invalid values raise ``ValueError``
    only for nonempty, retry-enabled calls. This is not a peak memory limit:
    writes can overshoot it, and source data and Arrow buffers need extra memory.
    """
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
            # Neither a pandas DataFrame nor a dict, so the block is an Arrow
            # table (a DataFrame cannot reach here: it implies pandas is
            # importable, which makes ``_PANDAS_AVAILABLE`` true).
            schema = cast(pa.Table, first).schema

    if schema is None or len(schema.names) == 0:
        return []

    stream = chain([first], stream_iter)

    input_rows = 0

    def record_batch_converter() -> Iterator[pa.RecordBatch]:
        nonlocal input_rows
        for block in stream:
            tbl = pd_to_arrow(block, schema)
            for batch in tbl.to_batches():
                input_rows += batch.num_rows
                yield batch

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

    write_kwargs = get_write_fragments_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    initial_bases_kwargs: dict[str, Any] = {}
    if initial_bases:
        initial_bases_kwargs["initial_bases"] = materialize_initial_bases(initial_bases)

    optional_write_kwargs = _get_optional_write_fragments_kwargs(
        write_fragments,
        target_bases=target_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
    )

    def _write_fragments(reader: pa.RecordBatchReader) -> list["FragmentMetadata"]:
        # ``write_fragments`` is overloaded on ``return_transaction``. The
        # version-dependent kwargs are assembled dynamically, which makes mypy
        # pick the ``return_transaction=True`` overload; ``return_transaction``
        # is left at its default here, so a list of fragments comes back.
        return write_fragments(  # type: ignore[return-value]
            reader,
            uri,
            schema=schema,
            max_rows_per_file=max_rows_per_file,
            # ``None`` means "use the writer default" upstream, even though
            # pylance annotates the parameter as a plain ``int``.
            max_rows_per_group=max_rows_per_group,  # type: ignore[arg-type]
            max_bytes_per_file=max_bytes_per_file,
            data_storage_version=data_storage_version,
            enable_stable_row_ids=enable_stable_row_ids,
            storage_options=storage_options,
            **write_kwargs,
            **initial_bases_kwargs,
            **optional_write_kwargs,
        )

    if retry_params.get("max_attempts", 10) > 1:
        # A failed write can consume part or all of its reader. Spool the input
        # once so every attempt replays the same batches from the beginning.
        threshold = _write_replay_memory_threshold()
        with tempfile.SpooledTemporaryFile(max_size=threshold, mode="w+b") as replay:
            # max_size=0 disables automatic rollover in the standard library.
            if threshold == 0:
                replay.rollover()
            # Python 3.10's spool is file-like but does not inherit IOBase,
            # which the Arrow stubs require. Arrow accepts it at runtime.
            replay_stream = cast(io.IOBase, replay)
            with pa.ipc.new_stream(replay_stream, schema) as writer:
                for batch in record_batch_converter():
                    writer.write_batch(batch)

            def write_once() -> list["FragmentMetadata"]:
                replay.seek(0)
                with pa.ipc.open_stream(replay_stream) as reader:
                    return _write_fragments(reader)

            fragments = call_with_retry(write_once, **retry_params)
    else:
        with pa.RecordBatchReader.from_batches(
            schema, record_batch_converter()
        ) as reader:

            def write_once_streaming() -> list["FragmentMetadata"]:
                return _write_fragments(reader)

            fragments = call_with_retry(write_once_streaming, **retry_params)
            # Include any unexpected unread remainder in the input row count
            # so an early return from the writer cannot hide missing rows.
            for _ in reader:
                pass

    fragment_rows = sum(fragment.num_rows for fragment in fragments)
    if fragment_rows != input_rows:
        raise RuntimeError(
            "Lance fragment write row count mismatch: "
            f"expected {input_rows}, wrote {fragment_rows}"
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
        None means a single streaming attempt. Allowing multiple attempts spools
        the complete input of each write call to a temporary Arrow IPC stream,
        even if the first attempt succeeds. The default memory threshold is
        128 MiB, configurable per call with the worker environment variable
        LANCE_RAY_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES (zero forces disk).
        Exceeding the threshold moves the entire stream to the worker's
        temporary directory (for example, configured with TMPDIR). Budget for
        concurrent writes, threshold overshoot, source data, and Arrow buffers;
        this is not a peak memory limit. max_bytes_per_file does not limit
        replay storage. See the writing guide for Ray environment propagation.

    """

    def __init__(
        self,
        uri: str,
        *,
        transform: Optional[
            Callable[[pa.Table], pa.Table | Generator[pa.Table, None, None]]
        ] = None,
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
        self.enable_stable_row_ids = enable_stable_row_ids
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

        transformed = self.transform(table)
        blocks: Iterable[pa.Table]
        if isinstance(transformed, Generator):
            blocks = transformed
        else:
            blocks = (t for t in [transformed])

        fragments = write_fragment(
            blocks,
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
