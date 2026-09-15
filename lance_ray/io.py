"""
I/O operations for Lance-Ray integration.
"""

import logging
import pickle
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

import pyarrow as pa
import pyarrow.compute as pc
import ray
from lance.dataset import LanceDataset, LanceOperation
from lance.udf import BatchUDF
from ray.data import Dataset, read_datasource
from ray.util.multiprocessing import Pool

from .datasink import LanceDatasink
from .datasource import LanceDatasource, _read_fragments, blob_field_kind
from .fragment import prepare_fragment_write_options
from .utils import (
    get_namespace_kwargs,
    has_namespace_params,
    materialize_initial_bases,
    normalize_initial_bases,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

if TYPE_CHECKING:
    from lance.types import ReaderLike

    TransformType = (
        dict[str, str]
        | BatchUDF
        | ReaderLike
        | Callable[[pa.RecordBatch], pa.RecordBatch]
    )

    #: ``add_columns_from`` hands each batch to the transform as a mapping of
    #: column name to a list of Python values, and expects only the new columns
    #: back. See the ``add_columns_from`` docstring.
    BatchDictTransform = Callable[
        [dict[str, list[Any]]], dict[str, Any] | pa.RecordBatch | pa.Table
    ]
    TransformFromType = dict[str, str] | BatchUDF | ReaderLike | BatchDictTransform


logger = logging.getLogger(__name__)

#: Internal transform contract for :func:`update_columns`.
#:
#: This is deliberately not exported as a public type alias.  The transform
#: receives a ``pa.RecordBatch`` holding only user columns and must return a
#: record batch whose columns are exactly the requested ``columns``, with the
#: same number of rows **in the same order**.
_UpdateColumnsTransform = BatchUDF | Callable[[pa.RecordBatch], pa.RecordBatch]

_METADATA_COLUMNS = frozenset({"_rowaddr", "_fragid", "_rowid"})

# Lance's built-in commit performs a conflict-checked rebase on every attempt.
# Bound retries only to limit tail latency.
_UPDATE_COMMIT_MAX_RETRIES = 5


@dataclass(frozen=True)
class UpdateColumnsResult:
    """Outcome of a distributed :func:`update_columns` run.

    ``version`` identifies the committed snapshot.  When the filter matches no
    rows, no transaction is created and ``rows_updated`` is zero.
    """

    version: int
    rows_updated: int


def read_lance(
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    columns: Optional[list[str]] = None,
    filter: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    scanner_options: Optional[dict[str, Any]] = None,
    dataset_options: Optional[dict[str, Any]] = None,
    fragment_ids: Optional[list[int]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    with_metadata: bool = False,
) -> Dataset:
    """
    Create a :class:`~ray.data.Dataset` from a
    `Lance Dataset <https://lancedb.github.io/lance-python-doc/all-modules.html#lance.LanceDataset>`_.

    Examples:
        Using a URI directly:
        >>> import lance_ray as lr
        >>> ds = lr.read_lance( # doctest: +SKIP
        ...     uri="./db_name.lance",
        ...     columns=["image", "label"],
        ...     filter="label = 2 AND text IS NOT NULL",
        ... )

        Using namespace_impl and namespace_properties:
        >>> ds = lr.read_lance( # doctest: +SKIP
        ...     namespace_impl="dir",
        ...     namespace_properties={"root": "/path/to/tables"},
        ...     table_id=["my_table"],
        ...     columns=["image", "label"],
        ... )

    Args:
        uri: The URI of the Lance dataset to read from. Local file paths, S3, and GCS
            are supported. Either uri OR (namespace_impl + namespace_properties + table_id)
            must be provided.
        table_id: The table identifier as a list of strings. Must be provided together
            with namespace_impl and namespace_properties.
        columns: The columns to read. By default, all columns are read.
        filter: Read returns only the rows matching the filter. By default, no
            filter is applied.
        storage_options: Extra options that make sense for a particular storage
            connection. This is used to store connection parameters like credentials,
            endpoint, etc. For more information, see `Object Store Configuration <https://lancedb.github.io/lance/guide/object_store/>`_.
        base_store_params: Runtime-only storage options keyed by registered
            base path URI. Used for BlobV2 references that live outside the
            dataset root.
        scanner_options: Additional options to configure the `LanceDataset.scanner()`
            method, such as `batch_size`. For more information,
            see `Lance API doc <https://lancedb.github.io/lance-python-doc/all-modules.html#lance.LanceDataset.scanner>`_
        dataset_options: Additional options to configure the `LanceDataset` instance.
            This can include options like `version`, `block_size`, etc. For more
            information, see `Lance API doc <https://lancedb.github.io/lance-python-doc/all-modules.html#lance.LanceDataset>`_.
        fragment_ids: The fragment IDs to read. If provided, only the fragments with the given IDs will be read.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id.
        ray_remote_args: kwargs passed to :func:`ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.
        with_metadata: If True, include ``_rowaddr`` and ``_fragid`` columns in the
            output. ``_rowaddr`` is a ``UInt64`` encoding ``(fragment_id << 32) |
            row_offset``. ``_fragid`` is the fragment ID derived from ``_rowaddr``.
            These columns are needed for :func:`add_columns_from`. Default is False.

    Returns:
        A :class:`~ray.data.Dataset` producing records read from the Lance dataset.
    """  # noqa: E501
    validate_uri_or_namespace(uri, namespace_impl, table_id)

    datasource = LanceDatasource(
        uri=uri,
        table_id=table_id,
        columns=columns,
        filter=filter,
        storage_options=storage_options,
        base_store_params=base_store_params,
        scanner_options=scanner_options,
        dataset_options=dataset_options,
        fragment_ids=fragment_ids,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        with_metadata=with_metadata,
    )

    return cast(
        Dataset,
        read_datasource(
            datasource=datasource,
            ray_remote_args=ray_remote_args or {},
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        ),
    )


def write_lance(
    ds: Dataset,
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    schema: Optional[pa.Schema] = None,
    mode: Literal["create", "append", "overwrite"] = "create",
    min_rows_per_file: int = 1024 * 1024,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
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
    ray_remote_args: Optional[dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    # Streaming parameters (only effective when stream=True)
    stream: bool = False,
    batch_size: Optional[int] = None,
    resume_rows: int = 0,
) -> None:
    """Write the dataset to a Lance dataset.

    Examples:
        Using a URI directly:
        .. testcode::
            import lance_ray as lr
            import pandas as pd

            docs = [{"title": "Lance data sink test"} for key in range(4)]
            ds = ray.data.from_pandas(pd.DataFrame(docs))
            lr.write_lance(ds, "/tmp/data/")

        Using namespace_impl and namespace_properties:
        .. testcode::
            import lance_ray as lr
            import pandas as pd

            docs = [{"title": "Lance data sink test"} for key in range(4)]
            ds = ray.data.from_pandas(pd.DataFrame(docs))
            lr.write_lance(  # doctest: +SKIP
                ds,
                namespace_impl="dir",
                namespace_properties={"root": "/tmp/tables"},
                table_id=["my_table"],
            )

    Args:
        ds: The Ray dataset to write.
        uri: The path to the destination Lance dataset. Can only be provided together
            with namespace parameters when creating a new dataset (mode='create' or 'overwrite').
        table_id: The table identifier as a list of strings. Must be provided together
            with namespace_impl and namespace_properties.
        schema: The schema of the dataset. If not provided, it is inferred from the data.
        mode: The write mode. Can be "create", "append", or "overwrite".
        min_rows_per_file: The minimum number of rows per file.
        max_rows_per_file: The maximum number of rows per file.
        max_bytes_per_file: The maximum number of bytes per file. This is a soft
            limit. If not provided, the PyLance default is used.
        data_storage_version: The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read.  The default is
            "legacy" which will use the legacy v1 version.  See the user guide
            for more details.
        enable_stable_row_ids: Enable stable row IDs for the dataset and all
            fragments written by this operation. Default is False.
        storage_options: The storage options for the writer. Default is None.
        base_store_params: Runtime-only storage options keyed by registered
            base path URI. Used for BlobV2 references that live outside the
            dataset root.
        initial_bases: Lance DatasetBasePath objects to register when creating
            a new dataset.
        target_bases: References to base paths where data should be written.
            Each string is resolved by matching base name or base path URI
            from registered bases.  In CREATE mode, references must match
            bases in ``initial_bases``.  In APPEND/OVERWRITE modes,
            references must match bases in the existing manifest.
        external_blob_mode: How external blob URIs are handled on write.
            ``"reference"`` stores external blob references, while ``"ingest"``
            reads external bytes and writes them into Lance-managed storage.
        allow_external_blob_outside_bases: Allow external blob references that
            do not map to a registered non-dataset-root base path. Only applies
            when ``external_blob_mode="reference"``.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id.
        stream: Enable incremental batch streaming write. Default False.
        batch_size: Batch size when streaming. If None, defaults to 1024.
        resume_rows: Number of leading rows to skip when streaming (for resume).
    """
    _validate_write_args(uri, namespace_impl, table_id, mode)
    if initial_bases and mode != "create":
        raise ValueError("'initial_bases' can only be used with mode='create'")
    allow_external_blob_outside_bases = prepare_fragment_write_options(
        target_bases=target_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
        stacklevel=2,
    )
    initial_bases = normalize_initial_bases(initial_bases)

    # Fast path: non-streaming write using the Datasink API.
    if not stream:
        datasink = LanceDatasink(
            uri,
            table_id=table_id,
            schema=schema,
            mode=mode,
            min_rows_per_file=min_rows_per_file,
            max_rows_per_file=max_rows_per_file,
            max_bytes_per_file=max_bytes_per_file,
            data_storage_version=data_storage_version,
            enable_stable_row_ids=enable_stable_row_ids,
            storage_options=storage_options,
            base_store_params=base_store_params,
            initial_bases=initial_bases,
            target_bases=target_bases,
            external_blob_mode=external_blob_mode,
            allow_external_blob_outside_bases=allow_external_blob_outside_bases,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
        )

        ds.write_datasink(
            datasink,
            ray_remote_args=ray_remote_args or {},
            concurrency=concurrency,
        )
        return

    # Streaming path: commit one fragment per batch to minimize memory usage.
    import lance

    if (namespace_impl is not None or namespace_properties is not None) and table_id:
        raise ValueError(
            "Streaming write with 'namespace_impl' + 'table_id' is not supported; "
            "use non-stream mode or provide a direct 'uri'.",
        )

    if uri is None:
        raise ValueError(
            "Streaming write requires 'uri' to be provided when no namespace is used.",
        )

    dest_uri: str = uri
    dest_exists = False
    dest_version: Optional[int] = None
    base_store_params_kwargs: dict[str, Any] = {}
    if base_store_params:
        base_store_params_kwargs = {"base_store_params": base_store_params}

    try:
        _dest = lance.LanceDataset(
            dest_uri,
            storage_options=storage_options,
            **base_store_params_kwargs,
        )
        dest_exists = True
        dest_version = _dest.version
    except Exception:
        dest_exists = False
        dest_version = None

    # Enforce mode semantics.
    if mode == "create" and dest_exists:
        raise ValueError("Destination exists but mode='create' was specified.")
    if mode == "append" and not dest_exists:
        raise ValueError("Destination does not exist but mode='append' was specified.")

    from .fragment import LanceFragmentWriter

    effective_batch_size = batch_size if batch_size is not None else 1024

    rows_seen = 0
    first_commit_done = False

    for batch in ds.iter_batches(
        batch_size=effective_batch_size, batch_format="pyarrow"
    ):
        # Convert to pyarrow.Table if needed.
        # ``batch_format="pyarrow"`` yields ``pa.Table``; the mapping branch is
        # only a safeguard for older Ray releases.
        tbl = (
            batch
            if isinstance(batch, pa.Table)
            else pa.Table.from_pydict(cast("dict[str, Any]", batch))
        )

        # Apply resume_rows skipping across batches.
        if resume_rows > rows_seen:
            to_skip = min(resume_rows - rows_seen, tbl.num_rows)
            rows_seen += to_skip
            if to_skip >= tbl.num_rows:
                # Whole batch skipped.
                continue
            tbl = tbl.slice(to_skip)

        # Skip empty batches (possible after slicing).
        if tbl.num_rows == 0:
            continue

        # Write this batch as one fragment and collect metadata.
        fragment_initial_bases = (
            initial_bases if mode == "create" and not first_commit_done else None
        )
        writer = LanceFragmentWriter(
            uri=dest_uri,
            schema=schema,  # if None, writer infers from first batch (preserves Arrow metadata)
            max_rows_per_file=max_rows_per_file,
            max_bytes_per_file=max_bytes_per_file,
            max_rows_per_group=min_rows_per_file,  # keep naming aligned with v1 semantics
            data_storage_version=data_storage_version,
            enable_stable_row_ids=enable_stable_row_ids,
            storage_options=storage_options,
            base_store_params=base_store_params,
            initial_bases=fragment_initial_bases,
            target_bases=target_bases,
            external_blob_mode=external_blob_mode,
            allow_external_blob_outside_bases=allow_external_blob_outside_bases,
            namespace_impl=None,
            namespace_properties=None,
            table_id=None,
        )
        frag_tbl = writer(tbl)
        fragments: list[Any] = []
        schema_obj: Optional[pa.Schema] = None
        frag_col = cast("list[bytes]", frag_tbl.column("fragment").to_pylist())
        sch_col = cast("list[bytes]", frag_tbl.column("schema").to_pylist())
        for frag_bytes, schema_bytes in zip(frag_col, sch_col, strict=False):
            fragment = pickle.loads(frag_bytes)
            fragments.append(fragment)
            schema_obj = pickle.loads(schema_bytes)

        if schema_obj is None:
            raise RuntimeError(
                "LanceFragmentWriter returned no fragments for a non-empty batch"
            )

        # Commit after each batch.
        op: LanceOperation.BaseOperation
        if not first_commit_done:
            # First commit: respect mode.
            if mode in ("create", "overwrite") or not dest_exists:
                op = LanceOperation.Overwrite(
                    schema_obj,
                    fragments,
                    initial_bases=(
                        materialize_initial_bases(initial_bases)
                        if mode == "create"
                        else None
                    ),
                )
                LanceDataset.commit(
                    dest_uri,
                    op,
                    read_version=None,
                    storage_options=storage_options,
                    enable_stable_row_ids=enable_stable_row_ids,
                    **base_store_params_kwargs,
                )
                first_commit_done = True
                dest_exists = True
                try:
                    _dest = lance.LanceDataset(
                        dest_uri,
                        storage_options=storage_options,
                        **base_store_params_kwargs,
                    )
                    dest_version = _dest.version
                except Exception:
                    dest_version = None
            elif mode == "append":
                op = LanceOperation.Append(fragments)
                LanceDataset.commit(
                    dest_uri,
                    op,
                    read_version=dest_version,
                    storage_options=storage_options,
                    enable_stable_row_ids=enable_stable_row_ids,
                    **base_store_params_kwargs,
                )
                first_commit_done = True
                try:
                    _dest = lance.LanceDataset(
                        dest_uri,
                        storage_options=storage_options,
                        **base_store_params_kwargs,
                    )
                    dest_version = _dest.version
                except Exception:
                    pass
            else:
                # Fallback: overwrite.
                op = LanceOperation.Overwrite(
                    schema_obj,
                    fragments,
                    initial_bases=(
                        materialize_initial_bases(initial_bases)
                        if mode == "create"
                        else None
                    ),
                )
                LanceDataset.commit(
                    dest_uri,
                    op,
                    read_version=None,
                    storage_options=storage_options,
                    enable_stable_row_ids=enable_stable_row_ids,
                    **base_store_params_kwargs,
                )
                first_commit_done = True
        else:
            # Subsequent commits always append.
            op = LanceOperation.Append(fragments)
            LanceDataset.commit(
                dest_uri,
                op,
                read_version=dest_version,
                storage_options=storage_options,
                enable_stable_row_ids=enable_stable_row_ids,
                **base_store_params_kwargs,
            )
            try:
                _dest = lance.LanceDataset(
                    dest_uri,
                    storage_options=storage_options,
                    **base_store_params_kwargs,
                )
                dest_version = _dest.version
            except Exception:
                pass

        rows_seen += tbl.num_rows


def _handle_fragment(
    uri: str,
    transform: "TransformType",
    read_columns: Optional[list[str]] = None,
    batch_size: Optional[int] = None,
    reader_schema: Optional[pa.Schema] = None,
    read_version: Optional[int | str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
) -> Callable[[int], tuple[bytes, bytes]]:
    """
    Handle a fragment of a Lance dataset.
    """

    def func(fragment_id: int) -> tuple[bytes, bytes]:
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )

        lance_ds = LanceDataset(
            uri=uri,
            storage_options=storage_options,
            version=read_version,
            **namespace_kwargs,
        )
        fragment = lance_ds.get_fragment(fragment_id)
        if fragment is None:
            raise ValueError(f"Fragment {fragment_id} does not exist in {uri}")
        fragment_meta, schema = fragment.merge_columns(
            transform, read_columns, batch_size, reader_schema
        )
        return pickle.dumps(fragment_meta), pickle.dumps(schema)

    return func


def add_columns(
    uri: Optional[str] = None,
    *,
    transform: "TransformType",
    filter: Optional[str] = None,
    read_columns: Optional[list[str]] = None,
    reader_schema: Optional[pa.Schema] = None,
    read_version: Optional[int | str] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    batch_size: int = 1024,
    concurrency: Optional[int] = None,
) -> None:
    """
    Add columns to a Lance dataset, currently use ray.util.multiprocessing.Pool to implement it. ray.data API is hard to implement.

    Examples:
        Using a URI directly:
        >>> import lance_ray as lr
        >>> import pyarrow as pa
        >>> import pandas as pd
        >>> ds = ray.data.from_pandas(pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}))
        >>> lr.write_lance(ds, "/tmp/data/")
        >>> def double_score(x: pa.RecordBatch) -> pa.RecordBatch:
        ...     df = x.to_pandas()
        ...     return pa.RecordBatch.from_pandas(
        ...         pd.DataFrame({"new_column": df["score"] * 2}),
        ...         schema=pa.schema([pa.field("new_column", pa.float64())]),
        ...     )
        >>> lr.add_columns("/tmp/data/", transform=double_score, concurrency=2)

    Args:
        uri: The path to the destination Lance dataset. If omitted, provide
            ``namespace_impl`` and ``table_id`` to resolve the location from
            the namespace.
        transform: The transform to apply to the dataset. It support a lot of types,
            see `LanceDB API doc https://lancedb.github.io/lance-python-doc/data-evolution.html ` for more details.
        filter: The filter to apply to the dataset. It is not supported yet, will be
            supported when `get_fragments` support filter see
            `LanceDB API doc <https://lancedb.github.io/lance-python-doc/all-modules.html#lance.LanceDataset.get_fragments>`_.
        read_columns: The columns from the original dataset to read.
        reader_schema: The schema to use for the reader.
        read_version: The version to read.
        ray_remote_args: The arguments to pass to the ray remote function.
        storage_options: The storage options to use for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id for credentials
            vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id for credentials vending.
        table_id: The table identifier as a list of strings.
            Used together with namespace_impl and namespace_properties for
            credentials vending.
        batch_size: The batch size to use for the reader.
        concurrency: The number of processes to use for the pool.
    """
    validate_uri_or_namespace(uri, namespace_impl, table_id)

    uri, storage_options = resolve_namespace_table(
        uri,
        storage_options,
        namespace_impl,
        namespace_properties,
        table_id,
    )

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    lance_ds = LanceDataset(
        uri=uri,
        storage_options=storage_options,
        version=read_version,
        **namespace_kwargs,
    )
    fragment_ids = [f.metadata.id for f in lance_ds.get_fragments()]
    pool = Pool(processes=concurrency, ray_remote_args=ray_remote_args)
    rst_futures = pool.map_async(
        _handle_fragment(
            uri,
            transform,
            read_columns,
            batch_size,
            reader_schema,
            read_version,
            storage_options,
            namespace_impl,
            namespace_properties,
            table_id,
        ),
        fragment_ids,
        chunksize=1,
    )
    try:
        result = rst_futures.get()
    except Exception as exc:
        raise RuntimeError(f"Failed to add columns: {exc}") from exc
    finally:
        pool.close()
        pool.join()

    commit_messages = []
    new_schema = None
    for fragment_meta, schema in result:
        commit_messages.append(pickle.loads(fragment_meta))
        schema = pickle.loads(schema)
        if new_schema is None:
            new_schema = schema
            continue
        if new_schema != schema:
            raise ValueError(
                f"Schema mismatch, previous schema: {new_schema}, new schema: {schema}"
            )
    if new_schema is None:
        raise ValueError("No schema for new fragment found")
    op = LanceOperation.Merge(commit_messages, new_schema)
    lance_ds.commit(
        uri,
        op,
        read_version=lance_ds.version,
        storage_options=storage_options,
        **namespace_kwargs,
    )


def _derive_fragid_from_rowaddr(batch: pa.Table) -> pa.Table:
    # pyarrow-stubs has no overload for shifting an array by a plain Python
    # int, and types the result as a scalar.
    fragid = cast(
        "pa.ChunkedArray[Any]",
        pc.cast(pc.shift_right(batch.column("_rowaddr"), 32), pa.uint64()),
    )
    return batch.append_column("_fragid", fragid)


_COMMIT_MAX_RETRIES = 3
_COMMIT_RETRY_DELAY_S = 1.0


def _commit_with_retry(
    uri: str,
    op: LanceOperation.Merge,
    read_version: int,
    storage_options: dict[str, str],
    namespace_kwargs: dict[str, Any],
    original_fragments: set[int],
) -> None:
    last_exc = None
    for attempt in range(_COMMIT_MAX_RETRIES):
        try:
            LanceDataset.commit(
                uri,
                op,
                read_version=read_version,
                storage_options=storage_options,
                **namespace_kwargs,
            )
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _COMMIT_MAX_RETRIES - 1:
                import time

                time.sleep(_COMMIT_RETRY_DELAY_S * (2**attempt))
                try:
                    current_ds = LanceDataset(
                        uri=uri, storage_options=storage_options, **namespace_kwargs
                    )
                    current_fragments = {
                        f.metadata.id for f in current_ds.get_fragments()
                    }
                    if current_fragments != original_fragments:
                        raise ValueError(
                            f"Concurrent write detected: fragment set changed from "
                            f"{sorted(original_fragments)} to {sorted(current_fragments)}. "
                            f"Cannot safely retry commit."
                        ) from exc
                    read_version = current_ds.version
                except ValueError:
                    raise
                except Exception:
                    pass
    if last_exc is None:  # pragma: no cover - the loop always sets it
        raise RuntimeError("Commit failed without raising an exception")
    raise last_exc


@ray.remote
def _fill_null_fragment(
    uri: str,
    storage_options: dict[str, str],
    read_version: int,
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
    frag_id: int,
    null_udf: BatchUDF,
    batch_size: int,
) -> tuple[Any, Any]:
    ns_kwargs = get_namespace_kwargs(namespace_impl, namespace_properties, table_id)
    local_ds = LanceDataset(
        uri=uri,
        storage_options=storage_options,
        version=read_version,
        **ns_kwargs,
    )
    fragment = local_ds.get_fragment(frag_id)
    if fragment is None:
        raise ValueError(f"Fragment {frag_id} not found in Lance dataset at {uri}")
    return fragment.merge_columns(null_udf, columns=None, batch_size=batch_size)


def add_columns_from(
    uri: Optional[str] = None,
    *,
    transform: "TransformFromType",
    read_columns: Optional[list[str]] = None,
    read_version: Optional[int | str] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    batch_size: int = 1024,
) -> None:
    """
    Add columns to a Lance dataset by applying a transform via Ray Data.

    Unlike :func:`add_columns` (which uses ``ray.util.multiprocessing.Pool``),
    this function uses Ray Data's distributed ``groupby().map_groups()`` so
    that per-fragment data stays on workers and the driver only collects small
    per-fragment commit metadata. This avoids materializing the entire dataset
    on the driver.

    The transform receives the original columns (plus ``_rowaddr`` / ``_fragid``
    for row alignment) and must return **only the new columns**. Row-address
    columns are handled automatically — you do not need to forward them.

    Examples:
        >>> import lance_ray as lr
        >>> import pyarrow as pa
        >>> import pandas as pd
        >>> ds = ray.data.from_pandas(pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}))
        >>> lr.write_lance(ds, "/tmp/data/", max_rows_per_file=2)
        >>> def compute_name_len(batch):
        ...     return {"name_len": [len(x) for x in batch["name"]]}
        >>> lr.add_columns_from("/tmp/data/", transform=compute_name_len)

    Args:
        uri: The path to the destination Lance dataset. If omitted, provide
            ``namespace_impl`` and ``table_id`` to resolve the location from
            the namespace.
        transform: The transform to apply to each batch. It receives a dict
            mapping column names to Python lists (metadata columns like
            ``_rowaddr`` are excluded) and must return only the new columns
            as a dict or ``pa.RecordBatch``. Supported types are the same as
            :func:`add_columns`.
        read_columns: The columns from the original dataset to read and pass
            to the transform. If None, all columns are read.
        read_version: The version to read. If None, uses the latest version.
        ray_remote_args: kwargs passed to ``ray.remote`` for map_groups tasks.
        storage_options: The storage options to use for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace.
        table_id: The table identifier as a list of strings.
        batch_size: The batch size to use for the reader inside merge_columns.
    """
    dataset_options: dict[str, Any] = {}
    if read_version is not None:
        dataset_options["version"] = read_version

    validate_uri_or_namespace(uri, namespace_impl, table_id)

    ray_ds = read_lance(
        uri,
        columns=read_columns,
        dataset_options=dataset_options or None,
        storage_options=storage_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        ray_remote_args=ray_remote_args,
        with_metadata=True,
    )

    _metadata_cols = {"_rowaddr", "_fragid", "_rowid"}

    def _wrap_transform(batch: pa.Table) -> pa.Table:
        rowaddr = batch.column("_rowaddr") if "_rowaddr" in batch.column_names else None

        new_cols: dict[str, Any] | pa.Table | pa.RecordBatch | Any
        if isinstance(transform, dict):
            new_cols = transform
        elif isinstance(transform, BatchUDF):
            result_batches = []
            for rb in batch.to_batches(max_chunksize=batch_size):
                result_batches.append(transform(rb))
            new_cols = pa.Table.from_batches(result_batches)
        elif callable(transform):
            batch_dict = {
                col: batch.column(col).to_pylist()
                for col in batch.column_names
                if col not in _metadata_cols
            }
            result = transform(batch_dict)
            if isinstance(result, pa.RecordBatch):
                new_cols = pa.Table.from_batches([result])
            elif isinstance(result, pa.Table | dict):
                new_cols = result
            else:
                new_cols = result
        else:
            reader = pa.RecordBatchReader.from_batches(
                batch.schema, batch.to_batches(max_chunksize=batch_size)
            )
            result_batches = []
            # A reader-consuming callable is not part of ``TransformType``; the
            # ``callable()`` branch above only covers the record-batch variant.
            for rb in transform(reader):  # type: ignore[operator]
                result_batches.append(rb)
            new_cols = pa.Table.from_batches(result_batches)

        if isinstance(new_cols, dict):
            new_table = pa.table(new_cols)
        elif isinstance(new_cols, pa.RecordBatch):
            new_table = pa.Table.from_batches([new_cols])
        else:
            new_table = new_cols

        if rowaddr is not None:
            new_table = new_table.append_column("_rowaddr", rowaddr)

        return new_table

    # ``batch_format="pyarrow"`` means the callable only ever sees ``pa.Table``,
    # which is narrower than the union Ray's signature declares.
    ray_ds = ray_ds.map_batches(_wrap_transform, batch_format="pyarrow")  # type: ignore[arg-type]

    merge_columns_from(
        uri,
        ray_ds,
        read_version=read_version,
        ray_remote_args=ray_remote_args,
        storage_options=storage_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        batch_size=batch_size,
    )


def merge_columns_from(
    uri: Optional[str] = None,
    ds: Optional[Dataset] = None,
    *,
    read_version: Optional[int | str] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    batch_size: int = 1024,
    require_full_coverage: bool = True,
) -> None:
    """
    Merge new columns into a Lance dataset from a Ray Dataset that contains
    ``_rowaddr`` and the new column(s).

    This is the low-level counterpart of :func:`add_columns_from`. Use it when
    you need full control over the Ray Data pipeline (e.g. joins, filters,
    multi-step transforms) and are willing to manage ``_rowaddr`` yourself.

    The Ray Dataset **must** contain ``_rowaddr`` (and optionally ``_fragid``;
    if absent it will be auto-derived). Every fragment in the target Lance
    dataset must be represented unless ``require_full_coverage=False``.

    The implementation uses Ray's distributed ``groupby("_fragid").map_groups``
    so that per-fragment data stays on workers and the driver only collects
    small per-fragment commit metadata.

    Examples:
        >>> import lance_ray as lr
        >>> ray_ds = lr.read_lance("/tmp/data/", with_metadata=True)
        >>> ray_ds = ray_ds.map_batches(my_udf)  # must forward _rowaddr
        >>> lr.merge_columns_from("/tmp/data/", ray_ds)

    Args:
        uri: The path to the destination Lance dataset. If omitted, provide
            ``namespace_impl`` and ``table_id`` to resolve the location from
            the namespace.
        ds: A Ray Dataset containing ``_rowaddr`` and the new column(s) to add.
            Every fragment in the target Lance dataset must be represented
            (unless ``require_full_coverage=False``).
        read_version: The version to read. If None, uses the latest version.
        ray_remote_args: kwargs passed to ``ray.remote`` for map_groups tasks.
        storage_options: The storage options to use for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace.
        table_id: The table identifier as a list of strings.
        batch_size: The batch size to use for the reader inside merge_columns.
        require_full_coverage: If True (default), raise ValueError when the
            input Ray Dataset does not contain rows for every fragment in the
            target Lance dataset. Set to False to allow merging new columns
            into a subset of fragments only.
    """
    if ds is None:
        raise ValueError("'ds' must be provided")

    validate_uri_or_namespace(uri, namespace_impl, table_id)

    uri, storage_options = resolve_namespace_table(
        uri,
        storage_options,
        namespace_impl,
        namespace_properties,
        table_id,
    )
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    ray_schema = ds.schema()
    if "_rowaddr" not in ray_schema.names:
        raise ValueError(
            "Input Dataset must contain '_rowaddr' column. "
            "Use read_lance(uri, with_metadata=True) to include it."
        )

    if "_fragid" not in ray_schema.names:
        ds = ds.map_batches(
            _derive_fragid_from_rowaddr,  # type: ignore[arg-type]
            batch_format="pyarrow",
        )
        ray_schema = ds.schema()

    pa_schema = ray_schema.base_schema

    lance_ds = LanceDataset(
        uri=uri,
        storage_options=storage_options,
        version=read_version,
        **namespace_kwargs,
    )
    resolved_read_version = lance_ds.version

    original_columns = set(lance_ds.schema.names)
    metadata_columns = {"_rowaddr", "_fragid", "_rowid"}
    new_columns = [
        name
        for name in pa_schema.names
        if name not in original_columns and name not in metadata_columns
    ]
    if not new_columns:
        raise ValueError("No new columns found in the input Dataset.")

    fragments_in_lance = {f.metadata.id for f in lance_ds.get_fragments()}

    # Capture closure variables for worker tasks.
    _uri = uri
    _storage_options = storage_options
    _namespace_impl = namespace_impl
    _namespace_properties = namespace_properties
    _table_id = table_id
    _read_version = resolved_read_version
    _new_columns = list(new_columns)
    _batch_size = batch_size

    _first_fragment = True

    def _merge_one_fragment(group: pa.Table) -> pa.Table:
        nonlocal _first_fragment
        if group.num_rows == 0:
            return pa.table(
                {
                    "frag_id": pa.array([], type=pa.int64()),
                    "fragment_meta": pa.array([], type=pa.binary()),
                    "result_schema": pa.array([], type=pa.binary()),
                }
            )

        frag_id = int(group.column("_fragid")[0].as_py())

        order = pc.sort_indices(group, sort_keys=[("_rowaddr", "ascending")])
        sorted_group = group.take(order)
        new_data = sorted_group.select(_new_columns).combine_chunks()

        local_ns_kwargs = get_namespace_kwargs(
            _namespace_impl, _namespace_properties, _table_id
        )
        local_ds = LanceDataset(
            uri=_uri,
            storage_options=_storage_options,
            version=_read_version,
            **local_ns_kwargs,
        )
        fragment = local_ds.get_fragment(frag_id)
        if fragment is None:
            raise ValueError(f"Fragment {frag_id} not found in Lance dataset at {_uri}")

        frag_row_count = fragment.metadata.num_rows
        new_data_schema = new_data.schema

        if new_data.num_rows == frag_row_count:
            reader = pa.RecordBatchReader.from_batches(
                new_data_schema,
                new_data.to_batches(max_chunksize=_batch_size),
            )
            fragment_meta, result_schema = fragment.merge_columns(
                reader, columns=None, batch_size=_batch_size
            )
        elif new_data.num_rows < frag_row_count:
            raise ValueError(
                f"Fragment {frag_id} has {frag_row_count} rows but the "
                f"input Dataset only contains {new_data.num_rows} rows for "
                f"this fragment. Partial-row coverage of a fragment is not "
                f"supported. Ensure the input Dataset includes all rows for "
                f"each fragment it covers."
            )
        else:
            raise ValueError(
                f"Fragment {frag_id} has {frag_row_count} rows but the "
                f"input Dataset contains {new_data.num_rows} rows for this "
                f"fragment, which exceeds the fragment size. This indicates "
                f"a data integrity issue."
            )

        schema_bytes = pickle.dumps(result_schema) if _first_fragment else b""
        _first_fragment = False

        return pa.table(
            {
                "frag_id": pa.array([frag_id], type=pa.int64()),
                "fragment_meta": pa.array(
                    [pickle.dumps(fragment_meta)], type=pa.binary()
                ),
                "result_schema": pa.array([schema_bytes], type=pa.binary()),
            }
        )

    map_groups_kwargs: dict[str, Any] = {}
    if ray_remote_args:
        map_groups_kwargs["ray_remote_args"] = ray_remote_args

    result_ds = ds.groupby("_fragid").map_groups(
        _merge_one_fragment,  # type: ignore[arg-type]
        batch_format="pyarrow",
        **map_groups_kwargs,
    )

    rows = result_ds.take_all()
    if not rows:
        raise ValueError("No fragments were processed")

    commit_messages = []
    new_schema = None
    seen_frag_ids: set[int] = set()
    for row in rows:
        frag_id = int(row["frag_id"])
        if frag_id not in fragments_in_lance:
            raise ValueError(
                f"_fragid {frag_id} from input Dataset is not present in the "
                f"Lance dataset at {uri}"
            )
        if frag_id in seen_frag_ids:
            raise ValueError(
                f"Duplicate _fragid {frag_id} encountered in map_groups output"
            )
        seen_frag_ids.add(frag_id)

        fragment_meta = pickle.loads(row["fragment_meta"])
        commit_messages.append(fragment_meta)
        schema_bytes = row["result_schema"]
        if schema_bytes:
            result_schema = pickle.loads(schema_bytes)
            if new_schema is None:
                new_schema = result_schema
            elif new_schema != result_schema:
                raise ValueError(f"Schema mismatch: {new_schema} vs {result_schema}")

    if require_full_coverage:
        missing = fragments_in_lance - seen_frag_ids
        if missing:
            raise ValueError(
                "Input Ray Dataset does not cover all fragments. Missing "
                f"fragment ids: {sorted(missing)}. Pass "
                "require_full_coverage=False to allow merging into a subset "
                "of fragments."
            )
    else:
        missing_frag_ids = sorted(fragments_in_lance - seen_frag_ids)
        if missing_frag_ids:
            new_data_arrow_schema = pa.schema(
                [pa.field(name, pa_schema.field(name).type) for name in _new_columns]
            )

            def _null_udf(in_batch: pa.RecordBatch) -> pa.RecordBatch:
                return pa.RecordBatch.from_pydict(
                    {
                        name: pa.nulls(
                            in_batch.num_rows,
                            type=new_data_arrow_schema.field(name).type,
                        )
                        for name in new_data_arrow_schema.names
                    },
                    schema=new_data_arrow_schema,
                )

            null_udf = BatchUDF(_null_udf, output_schema=new_data_arrow_schema)

            null_results = ray.get(
                [
                    _fill_null_fragment.remote(
                        uri,
                        storage_options,
                        resolved_read_version,
                        namespace_impl,
                        namespace_properties,
                        table_id,
                        fid,
                        null_udf,
                        batch_size,
                    )
                    for fid in missing_frag_ids
                ]
            )
            for fragment_meta, result_schema in null_results:
                commit_messages.append(fragment_meta)
                if new_schema is None:
                    new_schema = result_schema

    if new_schema is None:
        raise ValueError("No fragments were processed")

    op = LanceOperation.Merge(commit_messages, new_schema)
    _commit_with_retry(
        uri=uri,
        op=op,
        read_version=resolved_read_version,
        storage_options=storage_options,
        namespace_kwargs=namespace_kwargs,
        original_fragments=fragments_in_lance,
    )


def _blob_column_names(schema: pa.Schema) -> set[str]:
    """Names of top-level blob columns (legacy or v2) in a Lance schema."""
    return {f.name for f in schema if blob_field_kind(f) is not None}


def _leaf_field_ids(lance_field: Any) -> list[int]:
    """Leaf Lance field ids beneath (or of) ``lance_field``.

    A rewritten data file declares the *leaf* field ids it carries, which is
    what ``LanceFragment.update_columns`` reports back as ``fields_modified``.
    For a scalar column that is just the column's own id, but a ``list<int32>``
    column has a child item field and reports the child's id instead.
    """
    children = lance_field.children()
    if not children:
        return [lance_field.id()]
    ids: list[int] = []
    for child in children:
        ids.extend(_leaf_field_ids(child))
    return ids


def _resolve_update_targets(
    lance_ds: LanceDataset,
    columns: Sequence[str],
) -> tuple[tuple[int, ...], pa.Schema]:
    """Validate ``columns`` against the target dataset.

    Everything here runs on the driver, before any Ray task starts, so schema
    errors never surface after part of the fragments have been rewritten.

    Returns the *leaf* Lance field ids the columns cover -- which the driver
    later cross-checks against the ``fields_modified`` reported by every worker
    -- and the output schema the transform result is normalized to, holding the
    requested columns in the requested order.  That schema is taken from the
    target dataset rather than the caller: update_columns only overwrites
    existing columns, so their Arrow type and nullability are already fixed.
    """
    if isinstance(columns, str):
        # str satisfies Sequence[str], so this would otherwise iterate
        # characters and update whatever single-letter columns happen to exist.
        raise TypeError(
            f"'columns' must be a sequence of column names, not a bare string. "
            f"Did you mean ['{columns}']?"
        )
    if len(columns) == 0:
        raise ValueError(
            "'columns' must name at least one column to update. "
            "update_columns only overwrites existing columns; use "
            "add_columns_from() to add new ones."
        )

    target_schema = lance_ds.schema
    lance_schema = lance_ds.lance_schema
    target_names = set(target_schema.names)
    blob_columns = _blob_column_names(target_schema)

    fields: list[pa.Field[Any]] = []
    field_ids: list[int] = []
    seen: set[str] = set()

    for name in columns:
        if not isinstance(name, str):
            raise TypeError(
                f"'columns' must contain column names as str, got "
                f"{type(name).__name__}."
            )
        if name in seen:
            raise ValueError(f"Duplicate column '{name}' in 'columns'.")
        seen.add(name)

        if name in _METADATA_COLUMNS:
            raise ValueError(
                f"Cannot update metadata column '{name}'. Metadata columns are "
                "managed by lance-ray and must not appear in 'columns'."
            )
        if "." in name:
            raise ValueError(
                f"Nested field path '{name}' is not supported. Only top-level "
                "columns can be updated."
            )
        if name not in target_names:
            raise ValueError(
                f"Cannot update non-existent column '{name}'. update_columns "
                "only overwrites columns that already exist in the target "
                "dataset; use add_columns_from() to add new ones."
            )
        if name in blob_columns:
            raise ValueError(
                f"Cannot write blob column '{name}'. Blob columns may be read "
                "via 'read_columns' but are not valid update targets."
            )

        target_field = target_schema.field(name)
        if pa.types.is_struct(target_field.type):
            raise ValueError(
                f"Column '{name}' has a nested (struct) type, which is not "
                "supported by update_columns yet."
            )

        # ``LanceSchema.field()`` is missing from pylance's schema stub.
        lance_field = lance_schema.field(name)  # type: ignore[attr-defined]
        if lance_field is None:
            raise ValueError(
                f"Column '{name}' has no Lance field id; cannot update it."
            )
        fields.append(target_field)
        field_ids.extend(_leaf_field_ids(lance_field))

    return tuple(field_ids), pa.schema(fields)


def _resolve_read_columns(
    lance_ds: LanceDataset,
    read_columns: Optional[Sequence[str]],
) -> list[str]:
    """Resolve the projection handed to the user transform.

    ``None`` expands to every top-level **non-blob** user column. Blob columns
    are reconstructed into raw bytes on read, so pulling them in by default
    would materialize every image/video in the table.
    """
    target_schema = lance_ds.schema
    blob_columns = _blob_column_names(target_schema)

    if read_columns is None:
        return [name for name in target_schema.names if name not in blob_columns]

    resolved = list(read_columns)
    metadata_requested = [c for c in resolved if c in _METADATA_COLUMNS]
    if metadata_requested:
        raise ValueError(
            f"Metadata columns {metadata_requested} cannot be requested in "
            "'read_columns'. lance-ray reads and re-attaches them internally; "
            "the transform never sees them."
        )
    missing = [c for c in resolved if c not in target_schema.names]
    if missing:
        raise ValueError(
            f"'read_columns' references columns that do not exist in the "
            f"target dataset: {missing}"
        )
    return resolved


def _apply_update_transform(
    transform: "_UpdateColumnsTransform",
    output_schema: pa.Schema,
) -> Callable[[pa.RecordBatch], pa.RecordBatch]:
    """Build the per-batch transform used by a fragment-local worker."""
    column_list = list(output_schema.names)
    expected = set(column_list)
    non_nullable = [f.name for f in output_schema if not f.nullable]

    def _wrapped(batch: pa.RecordBatch) -> pa.RecordBatch:
        rowaddr = batch.column("_rowaddr")
        user_batch = batch.drop_columns(
            [c for c in _METADATA_COLUMNS if c in batch.column_names]
        )

        result = transform(user_batch)

        if not isinstance(result, pa.RecordBatch):
            raise TypeError(
                f"transform must return pa.RecordBatch, got {type(result).__name__}."
            )
        if result.num_rows != user_batch.num_rows:
            raise ValueError(
                f"transform changed the row count: got {result.num_rows} rows "
                f"for an input batch of {user_batch.num_rows}. The transform must be "
                "a row-order-preserving batch mapping; filtering, sorting, "
                "exploding or aggregating inside the transform is not allowed."
            )
        actual = set(result.schema.names)
        if actual != expected:
            unexpected = sorted(actual - expected)
            missing = sorted(expected - actual)
            raise ValueError(
                "transform output columns must match 'columns' exactly. "
                f"Unexpected: {unexpected}; missing: {missing}."
            )

        # Column order and types come from the target dataset, not from the
        # user's return value; cast is safe=True so precision loss raises.
        out = pa.Table.from_batches([result]).select(column_list).cast(output_schema)
        normalized = out.to_batches()[0]

        for name in non_nullable:
            if normalized.column(name).null_count:
                raise ValueError(
                    f"transform produced nulls for non-nullable column '{name}'."
                )

        return normalized.append_column("_rowaddr", rowaddr)

    return _wrapped


def _handle_update_fragment(
    uri: str,
    transform: "_UpdateColumnsTransform",
    output_schema: pa.Schema,
    projection: list[str],
    filter: Optional[str],
    batch_size: int,
    read_version: int,
    storage_options: dict[str, Any],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> Callable[[int], Optional[tuple[int, bytes, bytes, int]]]:
    """Create the fragment-local fast-path worker for ``update_columns``.

    The input always originates from the same pinned Lance fragment.  Keeping
    scan, transform and rewrite in one Ray task avoids the Ray Data shuffle and
    regrouping required by ``merge_columns_from`` for externally-created data.
    """
    apply_transform = _apply_update_transform(transform, output_schema)

    def _update_fragment(fragment_id: int) -> Optional[tuple[int, bytes, bytes, int]]:
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        lance_ds = LanceDataset(
            uri=uri,
            storage_options=storage_options,
            base_store_params=base_store_params,
            version=read_version,
            **namespace_kwargs,
        )
        fragment = lance_ds.get_fragment(fragment_id)
        if fragment is None:
            raise ValueError(
                f"Fragment {fragment_id} not found in Lance dataset at {uri}"
            )

        # _read_fragments is the shared scanner used by read_lance. It adds
        # _rowaddr, reconstructs explicitly projected Blob columns with
        # take_blobs(), and never materializes unrequested Blob columns. Keep
        # its result as a RecordBatch stream until Lance consumes it.
        scanned_batches = _read_fragments(
            [fragment_id],
            lance_ds,
            {
                "columns": projection,
                "filter": filter,
                "batch_size": batch_size,
            },
            with_metadata=True,
            as_record_batches=True,
        )
        first_scanned = next(scanned_batches, None)
        if first_scanned is None:
            # The exact filter is evaluated here.  A non-matching fragment
            # produces neither files nor transaction metadata.  Lance rejects an
            # empty reader outright ("HashJoiner: No data"), so this batch has to
            # be pulled before the rewrite starts; it is then fed straight back
            # into the stream below, so nothing is scanned twice.
            return None

        # apply_transform emits output_schema plus the _rowaddr it carries
        # through, so the reader schema is known without transforming a batch.
        reader_schema = pa.schema(
            [
                *output_schema,
                pa.field("_rowaddr", first_scanned.schema.field("_rowaddr").type),
            ]
        )
        rows_updated = 0

        def updated_batches() -> Iterator[pa.RecordBatch]:
            nonlocal rows_updated
            for scanned_batch in chain([first_scanned], scanned_batches):
                updated = apply_transform(scanned_batch)
                rows_updated += updated.num_rows
                yield updated

        update_reader = pa.RecordBatchReader.from_batches(
            reader_schema, updated_batches()
        )
        fragment_meta, fields_modified = fragment.update_columns(
            update_reader,
            left_on="_rowaddr",
            right_on="_rowaddr",
        )
        return (
            fragment_id,
            pickle.dumps(fragment_meta),
            pickle.dumps(list(fields_modified)),
            rows_updated,
        )

    return _update_fragment


def update_columns(
    uri: Optional[str] = None,
    *,
    transform: "_UpdateColumnsTransform",
    columns: Sequence[str],
    filter: Optional[str] = None,
    read_columns: Optional[Sequence[str]] = None,
    batch_size: int = 1024,
    ray_remote_args: Optional[Mapping[str, Any]] = None,
    concurrency: Optional[int] = None,
    storage_options: Optional[Mapping[str, Any]] = None,
    base_store_params: Optional[Mapping[str, Mapping[str, Any]]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[Mapping[str, str]] = None,
    table_id: Optional[Sequence[str]] = None,
) -> UpdateColumnsResult:
    """Overwrite existing columns of a Lance dataset with Ray.

    A pinned snapshot is processed one fragment per Ray task.  Each task scans,
    filters, transforms and rewrites its own fragment, avoiding the Ray Data
    shuffle required to write an externally-created Dataset back to Lance.
    The operation uses Lance's ``RewriteColumns`` update mode: rows do not
    move, ``_rowaddr`` is preserved, and untouched columns keep their original
    data files.

    Examples:
        >>> import lance_ray as lr
        >>> import pyarrow as pa
        >>> import pyarrow.compute as pc
        >>> def bump_price(batch: pa.RecordBatch) -> pa.RecordBatch:
        ...     return pa.RecordBatch.from_pydict(
        ...         {"price": pc.multiply(batch["price"], 1.1)}
        ...     )
        >>> lr.update_columns(  # doctest: +SKIP
        ...     "/tmp/products.lance",
        ...     transform=bump_price,
        ...     columns=["price"],
        ...     filter="status = 'active'",
        ...     read_columns=["price"],
        ... )

    Args:
        uri: The path to the target Lance dataset. If omitted, provide
            ``namespace_impl`` and ``table_id`` to resolve it from a namespace.
        transform: A callable or :class:`lance.udf.BatchUDF` taking a
            ``pa.RecordBatch`` of the requested columns and returning a
            ``pa.RecordBatch`` whose columns are exactly ``columns``. It **must
            preserve row count and row order**; reordering cannot be detected
            and silently writes values to the wrong rows. Do not filter, sort,
            join, deduplicate, aggregate or explode inside the transform.
        columns: Names of the existing columns to overwrite. Required, and
            checked against the target dataset before any Ray task starts. The
            output Arrow type and nullability of each column are taken from the
            dataset -- update_columns cannot change them -- and the transform
            result is cast to them with ``safe=True``. That rejects out-of-range
            integers, truncating time-unit conversions and unparseable strings,
            but it does **not** catch float narrowing: returning a ``float64``
            for a ``float32`` column rounds, and overflows to ``inf``, silently.
            Produce the column's own type in the transform when precision
            matters.
        filter: A Lance filter expression. Only matching rows get new values;
            unmatched rows keep their old values. Note that the physical rewrite
            is still fragment-wide, so a very sparse filter is a poor fit.
        read_columns: Columns handed to the transform. ``None`` expands to all
            top-level non-blob columns; blob columns must be requested
            explicitly because reading them materializes their raw bytes.
        batch_size: Maximum rows in one scanner/transform batch. Lance consumes
            these as a RecordBatch stream, although its underlying update join
            can still materialize a fragment's matching rows.
        ray_remote_args: ``ray.remote`` options for the complete fragment task,
            for example ``{"num_gpus": 1}``.
        concurrency: Maximum number of fragment tasks running concurrently.
            This bounds the number of fragment update tables held in memory.
        storage_options: Storage options for the dataset.
        base_store_params: Runtime object-store parameters keyed by base URI.
            Required to read blob v2 columns backed by an external base.
        namespace_impl: The namespace implementation type (e.g. "rest", "dir").
        namespace_properties: Properties for connecting to the namespace.
        table_id: The table identifier as a list of strings.

    Returns:
        An :class:`UpdateColumnsResult`. When the filter matches nothing the run
        is a successful no-op: no transaction is created and ``version`` equals
        ``read_version``.

    """
    validate_uri_or_namespace(uri, namespace_impl, list(table_id) if table_id else None)

    table_id_list = list(table_id) if table_id is not None else None
    namespace_properties_dict = (
        dict(namespace_properties) if namespace_properties is not None else None
    )
    uri, resolved_storage_options = resolve_namespace_table(
        uri,
        dict(storage_options) if storage_options is not None else None,
        namespace_impl,
        namespace_properties_dict,
        table_id_list,
    )
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties_dict, table_id_list
    )
    base_store_params_dict = (
        {k: dict(v) for k, v in base_store_params.items()}
        if base_store_params is not None
        else None
    )

    lance_ds = LanceDataset(
        uri=uri,
        storage_options=resolved_storage_options,
        base_store_params=base_store_params_dict,
        **namespace_kwargs,
    )
    resolved_read_version = lance_ds.version

    if lance_ds.has_stable_row_ids:
        raise NotImplementedError(
            "Distributed update_columns does not yet support datasets with "
            "stable row IDs: pylance does not expose the matched row offsets "
            "that Lance needs to advance _row_last_updated_at_version, so the "
            "change would be invisible to CDF consumers "
            "(lance-format/lance#6734)."
        )

    field_ids, output_schema = _resolve_update_targets(lance_ds, columns)
    projection = _resolve_read_columns(lance_ds, read_columns)

    fragment_ids = [f.metadata.id for f in lance_ds.get_fragments()]
    worker = _handle_update_fragment(
        uri,
        transform,
        output_schema,
        projection,
        filter,
        batch_size,
        resolved_read_version,
        dict(resolved_storage_options),
        base_store_params_dict,
        namespace_impl,
        namespace_properties_dict,
        table_id_list,
    )
    pool = Pool(processes=concurrency, ray_remote_args=dict(ray_remote_args or {}))
    try:
        results = pool.map_async(worker, fragment_ids, chunksize=1).get()
    except Exception as exc:
        raise RuntimeError(f"Failed to update columns: {exc}") from exc
    finally:
        pool.close()
        pool.join()

    rows = [row for row in results if row is not None]
    if not rows:
        # The filter matched nothing: no files were written, no transaction.
        return UpdateColumnsResult(
            version=resolved_read_version,
            rows_updated=0,
        )

    updated_fragments = []
    seen_frag_ids: set[int] = set()
    rows_updated = 0
    observed_field_ids: Optional[list[int]] = None

    for frag_id, fragment_meta_bytes, fields_modified_bytes, updated_rows in rows:
        if frag_id in seen_frag_ids:
            raise ValueError(f"Duplicate fragment {frag_id} in worker output")
        seen_frag_ids.add(frag_id)

        fragment_meta = pickle.loads(fragment_meta_bytes)
        if fragment_meta.id != frag_id:
            raise ValueError(
                f"Fragment rewrite changed the fragment id: expected {frag_id}, "
                f"got {fragment_meta.id}"
            )
        updated_fragments.append(fragment_meta)
        rows_updated += updated_rows

        worker_field_ids = sorted(pickle.loads(fields_modified_bytes))
        if observed_field_ids is None:
            observed_field_ids = worker_field_ids
        elif observed_field_ids != worker_field_ids:
            raise ValueError(
                "Workers disagree on the modified field ids: "
                f"{observed_field_ids} vs {worker_field_ids}"
            )

    if observed_field_ids != sorted(field_ids):
        raise ValueError(
            f"Modified field ids {observed_field_ids} do not match the leaf "
            f"field ids {sorted(field_ids)} of the target columns "
            f"{list(columns)}."
        )

    op = LanceOperation.Update(
        updated_fragments=updated_fragments,
        # Lance itself reported these while rewriting; the driver-derived leaf
        # ids above are only a cross-check.
        fields_modified=observed_field_ids,
        fields_for_preserving_frag_bitmap=[],
        update_mode="rewrite_columns",
    )
    # Unmodified fragments are intentionally not resubmitted: Lance's Update
    # transaction merges by fragment id and carries the rest through untouched.

    logger.info(
        "Committing update_columns: columns=%s read_version=%s fragments=%s",
        list(columns),
        resolved_read_version,
        len(updated_fragments),
    )
    committed = LanceDataset.commit(
        uri,
        op,
        read_version=resolved_read_version,
        max_retries=_UPDATE_COMMIT_MAX_RETRIES,
        storage_options=dict(resolved_storage_options),
        **namespace_kwargs,
    )

    return UpdateColumnsResult(
        version=committed.version,
        rows_updated=rows_updated,
    )


def _validate_write_args(
    uri: Optional[str],
    namespace_impl: Optional[str],
    table_id: Optional[list[str]],
    mode: str,
) -> None:
    """Validate write arguments.

    For create/overwrite modes, allows both uri and namespace parameters to be provided
    together (to create at a specific location and register with namespace).
    For append mode, requires exactly one of uri OR namespace parameters.
    """
    has_ns = has_namespace_params(namespace_impl, table_id)

    # For append mode, use the same validation as read operations
    if mode == "append" and uri is not None and has_ns:
        raise ValueError(
            "For append mode, cannot provide both 'uri' and namespace parameters. "
            "Use either 'uri' OR ('namespace_impl' + 'table_id')."
        )

    # Must provide at least one way to identify the dataset
    if uri is None and not has_ns:
        raise ValueError(
            "Must provide either 'uri' OR ('namespace_impl' + 'table_id')."
        )
