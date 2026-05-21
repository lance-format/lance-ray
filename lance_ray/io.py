"""
I/O operations for Lance-Ray integration.
"""

import pickle
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Optional

import pyarrow as pa
import pyarrow.compute as pc
import ray
from lance.dataset import LanceDataset, LanceOperation
from lance.udf import BatchUDF
from ray.data import Dataset, read_datasource
from ray.util.multiprocessing import Pool

from .datasink import LanceDatasink
from .datasource import LanceDatasource
from .utils import (
    get_namespace_kwargs,
    has_namespace_params,
    materialize_initial_bases,
    normalize_initial_bases,
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

    return read_datasource(
        datasource=datasource,
        ray_remote_args=ray_remote_args or {},
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
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
    data_storage_version: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    initial_bases: Optional[list[Any]] = None,
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
        data_storage_version: The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read.  The default is
            "legacy" which will use the legacy v1 version.  See the user guide
            for more details.
        storage_options: The storage options for the writer. Default is None.
        base_store_params: Runtime-only storage options keyed by registered
            base path URI. Used for BlobV2 references that live outside the
            dataset root.
        initial_bases: Lance DatasetBasePath objects to register when creating
            a new dataset.
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
            data_storage_version=data_storage_version,
            storage_options=storage_options,
            base_store_params=base_store_params,
            initial_bases=initial_bases,
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
    base_store_params_kwargs = {}
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
        tbl = batch if isinstance(batch, pa.Table) else pa.Table.from_pydict(batch)

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
            max_rows_per_group=min_rows_per_file,  # keep naming aligned with v1 semantics
            data_storage_version=data_storage_version,
            storage_options=storage_options,
            initial_bases=fragment_initial_bases,
            namespace_impl=None,
            namespace_properties=None,
            table_id=None,
        )
        frag_tbl = writer(tbl)
        fragments: list[Any] = []
        schema_obj: Optional[pa.Schema] = None
        frag_col = frag_tbl.column("fragment").to_pylist()
        sch_col = frag_tbl.column("schema").to_pylist()
        for frag_bytes, schema_bytes in zip(frag_col, sch_col, strict=False):
            fragment = pickle.loads(frag_bytes)
            fragments.append(fragment)
            schema_obj = pickle.loads(schema_bytes)

        # Commit after each batch.
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
):
    """
    Handle a fragment of a Lance dataset.
    """

    def func(fragment_id: int):
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
        fragment_meta, schema = fragment.merge_columns(
            transform, read_columns, batch_size, reader_schema
        )
        return pickle.dumps(fragment_meta), pickle.dumps(schema)

    return func


def add_columns(
    uri: str,
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
        uri: The path to the destination Lance dataset.
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
    storage_options = storage_options or {}

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
    fragid = pc.cast(pc.shift_right(batch.column("_rowaddr"), 32), pa.uint64())
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
    uri: str,
    ds: Dataset,
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
    Add columns to a Lance dataset from a Ray Dataset containing ``_rowaddr``,
    ``_fragid``, and the new column(s).

    This enables a "UDF first, merge later" workflow inspired by
    lance-spark's ``ALTER TABLE ADD COLUMNS FROM``:

    1. :func:`read_lance` with ``with_metadata=True`` to get ``_rowaddr`` / ``_fragid``
    2. Apply arbitrary UDF / join on the Ray Dataset
    3. :func:`add_columns_from` to merge new columns back

    The Ray Dataset **must** contain ``_rowaddr`` and ``_fragid`` columns.
    Use :func:`read_lance` with ``with_metadata=True`` to include them.

    The implementation uses Ray's distributed ``groupby("_fragid").map_groups``
    so that per-fragment data stays on workers and the driver only collects
    small per-fragment commit metadata. This avoids materializing the entire
    Ray Dataset on the driver.

    Examples:
        >>> import lance_ray as lr
        >>> import pyarrow as pa
        >>> import pandas as pd
        >>> ds = ray.data.from_pandas(pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}))
        >>> lr.write_lance(ds, "/tmp/data/", max_rows_per_file=2)
        >>> ray_ds = lr.read_lance("/tmp/data/", with_metadata=True)
        >>> def compute_hash(batch):
        ...     return {"name_hash": [hash(x) for x in batch["name"]]}
        >>> ray_ds = ray_ds.map_batches(compute_hash)
        >>> lr.add_columns_from("/tmp/data/", ray_ds)

    Args:
        uri: The path to the destination Lance dataset.
        ds: A Ray Dataset containing ``_rowaddr``, ``_fragid``, and the new
            column(s) to add. Every fragment in the target Lance dataset must
            be represented (unless ``require_full_coverage=False``).
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
    storage_options = storage_options or {}
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
            _derive_fragid_from_rowaddr,
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
        _merge_one_fragment,
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
