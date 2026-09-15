"""
I/O operations for Lance-Ray integration.
"""

import logging
import os
import pickle
import sqlite3
import tempfile
from collections import defaultdict
from collections.abc import Callable, Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    Optional,
    Protocol,
    cast,
)

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray
from lance.dataset import LanceDataset, LanceOperation
from lance.udf import BatchUDF
from ray.data import Dataset, read_datasource
from ray.data.block import DataBatch
from ray.util.multiprocessing import Pool

from .datasink import LanceDatasink
from .datasource import (
    LanceDatasource,
    dataset_identity_digest,
    parse_source_provenance,
)
from .fragment import prepare_fragment_write_options
from .utils import (
    get_namespace_kwargs,
    has_namespace_params,
    materialize_initial_bases,
    normalize_initial_bases,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

logger = logging.getLogger(__name__)


class _LogicalSource(Protocol):
    @property
    def name(self) -> str: ...


class _LogicalPlanWithSources(Protocol):
    def sources(self) -> list[_LogicalSource]: ...


class _DatasetWithLogicalPlan(Protocol):
    _logical_plan: _LogicalPlanWithSources


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
            These columns are needed for :func:`add_columns_from` and
            :func:`update_columns_from`. Default is False.

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

    datasource.pin_source_version()
    dataset = cast(
        Dataset,
        read_datasource(
            datasource=datasource,
            ray_remote_args=ray_remote_args or {},
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        ),
    )
    return dataset


def _source_provenance_from_dataset_lineage(
    ds: Dataset,
) -> tuple[int, Optional[str]] | None:
    """Return unique Lance version and identity when every logical source is Lance."""
    logical_plan = cast(_DatasetWithLogicalPlan, ds)._logical_plan
    sources = logical_plan.sources()
    if not sources:
        return None

    source_versions: set[int] = set()
    source_identities: set[Optional[str]] = set()
    for source in sources:
        provenance = parse_source_provenance(source.name)
        if provenance is None:
            return None
        version, identity = provenance
        source_versions.add(version)
        source_identities.add(identity)

    if len(source_identities) > 1:
        raise ValueError(
            "Input Dataset combines multiple Lance source datasets. "
            "Use rows from one source dataset."
        )
    if len(source_versions) > 1:
        raise ValueError(
            "Input Dataset combines multiple Lance source versions: "
            f"{sorted(source_versions)}. Use rows from one source version."
        )
    return next(iter(source_versions)), next(iter(source_identities))


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


class _UpdateFragmentArgs(NamedTuple):
    frag_id: int
    refs: list[ray.ObjectRef[pa.Table]]
    uri: str
    storage_options: dict[str, Any] | None
    namespace_impl: str | None
    namespace_properties: dict[str, str] | None
    table_id: list[str] | None
    read_version: int
    columns: list[str]
    target_types: dict[str, pa.DataType]
    batch_size: int


@ray.remote
def _partition_block_by_fragid(
    block: pa.Table,
) -> dict[int, ray.ObjectRef[pa.Table]]:
    """Partition one Ray Data block into per-fragment object-store tables."""
    if block.num_rows == 0:
        return {}

    order = pc.sort_indices(
        block,
        sort_keys=[("_fragid", "ascending"), ("_rowaddr", "ascending")],
    )
    # Materialize only the routing column globally. Taking the full, potentially
    # wide table here would temporarily duplicate an entire Ray block.
    fragids = block.column("_fragid").take(order).to_numpy(zero_copy_only=False)
    unique_fragids = np.unique(fragids)
    starts = np.searchsorted(fragids, unique_fragids, side="left")
    ends = np.searchsorted(fragids, unique_fragids, side="right")

    partitions: dict[int, ray.ObjectRef[pa.Table]] = {}
    for frag_id, start, end in zip(unique_fragids, starts, ends, strict=True):
        start_index = int(start)
        partition_length = int(end) - start_index
        partition = block.take(order.slice(start_index, partition_length))
        partitions[int(frag_id)] = cast(
            ray.ObjectRef[pa.Table],
            ray.put(partition),
        )
    return partitions


@ray.remote
def _update_fragment_with_refs(
    args: _UpdateFragmentArgs,
) -> tuple[bytes, bytes]:
    """Stream one fragment's update rows and return commit metadata."""
    frag_id = args.frag_id
    refs = args.refs
    uri = args.uri
    storage_options = args.storage_options
    namespace_impl = args.namespace_impl
    namespace_properties = args.namespace_properties
    table_id = args.table_id
    read_version = args.read_version
    columns = args.columns
    target_types = args.target_types
    batch_size = args.batch_size

    local_ns_kwargs = get_namespace_kwargs(
        namespace_impl,
        namespace_properties,
        table_id,
    )
    local_ds = LanceDataset(
        uri=uri,
        storage_options=storage_options,
        version=read_version,
        **local_ns_kwargs,
    )
    fragment = local_ds.get_fragment(frag_id)
    if fragment is None:
        raise ValueError(f"Fragment {frag_id} not found in Lance dataset at {uri}")

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "rowaddrs.sqlite")
        connection = sqlite3.connect(db_path)
        try:
            # Bound SQLite's page cache per worker; aggregate memory still scales
            # with concurrent fragment update tasks.
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute("PRAGMA cache_size=-65536")
            connection.execute("PRAGMA mmap_size=0")
            connection.execute("CREATE TABLE rowaddrs (value BLOB)")
            for ref in refs:
                table = ray.get(ref)
                if table.num_rows == 0:
                    continue

                if table.schema.field("_rowaddr").type != pa.uint64():
                    raise ValueError(
                        f"Fragment {frag_id} contains a non-uint64 _rowaddr."
                    )
                for column in columns:
                    if table.schema.field(column).type != target_types[column]:
                        raise ValueError(
                            f"Update column type mismatch in fragment {frag_id}: "
                            f"{column}: source "
                            f"{table.schema.field(column).type}, target "
                            f"{target_types[column]}"
                        )
                fragid_scalar = pa.scalar(
                    frag_id,
                    type=table.schema.field("_fragid").type,
                )
                if not pc.all(pc.equal(table.column("_fragid"), fragid_scalar)).as_py():
                    raise ValueError(
                        f"Fragment {frag_id} received rows routed to another fragment."
                    )

                for batch in table.to_batches(max_chunksize=batch_size):
                    rowaddrs = batch.column("_rowaddr")
                    if rowaddrs.null_count:
                        raise ValueError(
                            f"Null _rowaddr values are not allowed in fragment "
                            f"{frag_id}."
                        )
                    values = [
                        cast(int, value).to_bytes(8, "big")
                        for value in rowaddrs.to_pylist()
                    ]
                    connection.executemany(
                        "INSERT INTO rowaddrs (value) VALUES (?)",
                        [(value,) for value in values],
                    )

            connection.execute("CREATE INDEX rowaddrs_value_idx ON rowaddrs (value)")
            connection.commit()
            duplicates = connection.execute(
                "SELECT value FROM rowaddrs GROUP BY value HAVING COUNT(*) > 1"
            ).fetchall()
            if duplicates:
                duplicate_rowaddrs = [
                    int.from_bytes(row[0], "big") for row in duplicates
                ]
                raise ValueError(
                    f"Duplicate _rowaddr values in fragment {frag_id}: "
                    f"{duplicate_rowaddrs}"
                )
        finally:
            connection.close()

    update_schema = pa.schema(
        [pa.field("_rowaddr", pa.uint64())]
        + [pa.field(column, target_types[column]) for column in columns]
    )

    def _update_batches() -> Iterator[pa.RecordBatch]:
        # This second pass is intentional: validation must finish before Lance
        # starts writing update files. Retaining every resolved table would trade
        # the object-store lookup for unbounded per-fragment worker heap usage.
        for ref in refs:
            table = ray.get(ref)
            for batch in table.to_batches(max_chunksize=batch_size):
                yield batch.select(["_rowaddr", *columns])

    reader = pa.RecordBatchReader.from_batches(update_schema, _update_batches())
    fragment_meta, fields_modified = fragment.update_columns(
        reader,
        left_on="_rowaddr",
        right_on="_rowaddr",
    )
    return pickle.dumps(fragment_meta), pickle.dumps(fields_modified)


def update_columns_from(
    uri: Optional[str] = None,
    ds: Optional[Dataset] = None,
    *,
    columns: list[str],
    read_version: Optional[int | str] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    batch_size: int = 1024,
) -> None:
    """Update existing columns in a Lance dataset using row metadata.

    Unlike :func:`merge_columns_from`, which adds new columns, this function
    updates existing columns by matching ``_rowaddr`` inside each fragment.
    The source Ray Dataset must contain ``_rowaddr`` and every column listed
    in ``columns``. If ``_fragid`` is absent, it is derived from ``_rowaddr``.
    If supplied, ``_fragid`` must match the fragment encoded in ``_rowaddr``.
    Row addresses must be non-null, unique integer values and are normalized
    to ``uint64``. Update column names must be unique and their Arrow types
    must match the target columns.
    Unmatched source rows are ignored. Before partitioning, the source is projected
    to ``_rowaddr``, ``_fragid``, and the requested update columns. The final
    per-fragment update is streamed as bounded ``RecordBatch`` values, and a
    fragment-local disk-backed index rejects duplicate row addresses. The final
    operation is committed once; commit conflicts are returned to the caller
    without retrying stale work. When source lineage is present, the source
    dataset identity must match the update target.

    Examples:
        >>> import lance_ray as lr
        >>> source = lr.read_lance("/tmp/data/", with_metadata=True)
        >>> source = source.map_batches(modify_status)  # preserve metadata
        >>> lr.update_columns_from(
        ...     "/tmp/data/",
        ...     source,
        ...     columns=["status"],
        ... )

    Args:
        uri: Path to the Lance dataset. If omitted, provide ``namespace_impl``
            and ``table_id`` to resolve the location from the namespace.
        ds: Ray Dataset containing ``_rowaddr`` and the columns to update.
            ``_fragid`` is derived from ``_rowaddr`` when absent and validated
            against it when supplied.
        columns: Existing columns to update. Metadata columns cannot be used.
        read_version: Dataset version to update. Defaults to the unique Lance
            source version retained in the Ray Dataset's logical lineage. It is
            required when that lineage is unavailable, such as after
            materializing the source. An explicit version identifies the target
            version but does not prove the source rows came from that dataset;
            the caller must ensure source provenance when lineage is lost. When
            lineage is present, the source dataset identity must match the
            update target.
        ray_remote_args: Options passed to the Ray partition and fragment-update tasks.
        storage_options: Storage options used to open the dataset.
        namespace_impl: Namespace implementation type, such as ``"dir"`` or
            ``"rest"``.
        namespace_properties: Namespace connection properties.
        table_id: Table identifier used with namespace parameters.
        batch_size: Batch size for the update reader. Must be positive.
    """
    if ds is None:
        raise ValueError("'ds' must be provided")
    if not columns:
        raise ValueError("'columns' must be non-empty")
    if batch_size <= 0:
        raise ValueError("'batch_size' must be positive")

    seen_columns: set[str] = set()
    duplicate_columns: set[str] = set()
    for column in columns:
        if column in seen_columns:
            duplicate_columns.add(column)
        else:
            seen_columns.add(column)
    if duplicate_columns:
        raise ValueError(
            f"Duplicate columns are not allowed: {sorted(duplicate_columns)}"
        )

    metadata_columns = {"_rowaddr", "_fragid", "_rowid"}
    invalid_columns = [column for column in columns if column in metadata_columns]
    if invalid_columns:
        raise ValueError(
            f"Metadata columns cannot be updated: {sorted(invalid_columns)}"
        )

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
    if ray_schema is None:
        logger.warning(
            "No rows to update; update_columns_from completed without changes."
        )
        return
    if "_rowaddr" not in ray_schema.names:
        raise ValueError(
            "Input Dataset must contain '_rowaddr'. "
            "Use read_lance(uri, with_metadata=True) to include row metadata."
        )

    source_types = dict(zip(ray_schema.names, ray_schema.types, strict=True))
    rowaddr_type = source_types["_rowaddr"]
    if not (
        isinstance(rowaddr_type, pa.DataType) and pa.types.is_integer(rowaddr_type)
    ):
        raise ValueError(
            "Input Dataset '_rowaddr' must have an integer type, "
            f"but found {rowaddr_type}."
        )

    has_fragid = "_fragid" in ray_schema.names
    if has_fragid:
        fragid_type = source_types["_fragid"]
        if not (
            isinstance(fragid_type, pa.DataType) and pa.types.is_integer(fragid_type)
        ):
            raise ValueError(
                "Input Dataset '_fragid' must have an integer type, "
                f"but found {fragid_type}."
            )

    source_names = set(ray_schema.names)
    missing_columns = [column for column in columns if column not in source_names]
    if missing_columns:
        raise ValueError(
            f"Input Dataset is missing requested update columns: {missing_columns}"
        )

    projected_columns = ["_rowaddr"]
    if has_fragid:
        projected_columns.append("_fragid")
    projected_columns.extend(columns)

    source_provenance = _source_provenance_from_dataset_lineage(ds)
    source_version = None if source_provenance is None else source_provenance[0]
    source_identity = None if source_provenance is None else source_provenance[1]
    read_version_was_explicit = read_version is not None
    if read_version is None and source_version is not None:
        read_version = source_version
    if (
        source_version is not None
        and source_identity is None
        and not read_version_was_explicit
    ):
        raise ValueError(
            "A reliable dataset identity is unavailable from the Ray Dataset's "
            "logical lineage. Pass 'read_version' explicitly to update the "
            "target dataset."
        )

    def _validate_and_derive_fragid(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        rowaddrs = table.column("_rowaddr")
        if rowaddrs.null_count:
            raise ValueError(
                "Null _rowaddr values are not allowed before fragment routing."
            )

        if rowaddrs.type != pa.uint64():
            rowaddrs = pc.cast(rowaddrs, pa.uint64())
            table = table.set_column(
                table.schema.get_field_index("_rowaddr"),
                "_rowaddr",
                rowaddrs,
            )

        derived_fragids = pc.cast(pc.shift_right(rowaddrs, 32), pa.uint64())
        if has_fragid:
            fragids = table.column("_fragid")
            if fragids.null_count:
                raise ValueError("Null _fragid values are not allowed.")

            if fragids.type != pa.uint64():
                fragids = pc.cast(fragids, pa.uint64())
                table = table.set_column(
                    table.schema.get_field_index("_fragid"),
                    "_fragid",
                    fragids,
                )

            mismatch_mask = pc.not_equal(fragids, derived_fragids)
            if pc.any(mismatch_mask).as_py():
                mismatch_index = int(pc.indices_nonzero(mismatch_mask)[0].as_py())
                rowaddr = rowaddrs[mismatch_index].as_py()
                fragid = fragids[mismatch_index].as_py()
                expected_fragid = derived_fragids[mismatch_index].as_py()
                raise ValueError(
                    f"_fragid {fragid} does not match _rowaddr {rowaddr}; "
                    f"expected fragment id {expected_fragid}."
                )

            return table.set_column(
                table.schema.get_field_index("_fragid"),
                "_fragid",
                derived_fragids,
            )

        return table.append_column("_fragid", derived_fragids)

    normalized_ds = ds.select_columns(projected_columns).map_batches(
        _validate_and_derive_fragid,
        batch_format="pyarrow",
    )

    lance_ds = LanceDataset(
        uri=uri,
        storage_options=storage_options,
        version=read_version,
        **namespace_kwargs,
    )
    if source_identity is not None:
        target_identity = dataset_identity_digest(
            lance_ds,
            uri=uri,
            storage_options=storage_options,
        )
        if target_identity is None:
            raise ValueError(
                "Target dataset identity could not be reliably determined."
            )
        if source_identity != target_identity:
            raise ValueError(
                "Input Dataset was read from a different Lance dataset "
                "than the update target."
            )
    resolved_read_version = lance_ds.version
    if source_version is not None and resolved_read_version != source_version:
        raise ValueError(
            "Source Dataset was read from Lance version "
            f"{source_version}, but update requested version "
            f"{resolved_read_version}."
        )

    unavailable_columns = [
        column for column in columns if column not in lance_ds.schema.names
    ]
    if unavailable_columns:
        raise ValueError(
            f"Columns do not exist in target Lance dataset: {unavailable_columns}"
        )

    source_types = dict(zip(ray_schema.names, ray_schema.types, strict=True))
    target_types = {column: lance_ds.schema.field(column).type for column in columns}
    type_mismatches: list[str] = []
    for column in columns:
        source_type = source_types[column]
        target_type = target_types[column]
        if isinstance(source_type, pa.DataType) and source_type != target_type:
            type_mismatches.append(
                f"{column}: source {source_type}, target {target_type}"
            )
    if type_mismatches:
        raise ValueError("Update column type mismatch: " + "; ".join(type_mismatches))

    if read_version is None and source_provenance is None:
        raise ValueError(
            "'read_version' is required because the source Lance version "
            "is unavailable from the Ray Dataset's logical lineage."
        )

    fragments_in_lance = {f.metadata.id for f in lance_ds.get_fragments()}

    remote_options = dict(ray_remote_args or {})
    partition_fn = _partition_block_by_fragid.options(**remote_options)
    update_fn = _update_fragment_with_refs.options(**remote_options)

    # This documented DeveloperAPI is available in the minimum supported Ray
    # version (2.41) and is the zero-copy API for distributed Arrow block refs.
    block_refs = normalized_ds.to_arrow_refs()
    partition_tasks = [partition_fn.remote(block_ref) for block_ref in block_refs]
    partition_results = ray.get(partition_tasks)
    del partition_tasks
    del block_refs

    fragment_refs: dict[int, list[ray.ObjectRef[pa.Table]]] = defaultdict(list)
    for partitions in partition_results:
        for frag_id, partition_ref in partitions.items():
            fragment_refs[frag_id].append(partition_ref)
    del partition_results

    unknown_fragments = set(fragment_refs) - fragments_in_lance
    if unknown_fragments:
        raise ValueError(
            f"_fragid values from input Dataset are not present in the Lance "
            f"dataset at {uri}: {sorted(unknown_fragments)}"
        )
    if not fragment_refs:
        logger.warning(
            "No rows to update; update_columns_from completed without changes."
        )
        return
    update_tasks = [
        update_fn.remote(
            _UpdateFragmentArgs(
                frag_id=frag_id,
                refs=refs,
                uri=uri,
                storage_options=storage_options,
                namespace_impl=namespace_impl,
                namespace_properties=namespace_properties,
                table_id=table_id,
                read_version=resolved_read_version,
                columns=list(columns),
                target_types=target_types,
                batch_size=batch_size,
            ),
        )
        for frag_id, refs in fragment_refs.items()
    ]
    results = ray.get(update_tasks)

    updated_fragments = []
    all_fields_modified: set[int] = set()

    for fragment_meta_bytes, fields_modified_bytes in results:
        fragment_meta = pickle.loads(fragment_meta_bytes)
        fields_modified = pickle.loads(fields_modified_bytes)
        updated_fragments.append(fragment_meta)
        all_fields_modified.update(fields_modified)

    op = LanceOperation.Update(
        updated_fragments=updated_fragments,
        fields_modified=list(all_fields_modified),
        update_mode="rewrite_columns",
    )
    LanceDataset.commit(
        uri,
        op,
        read_version=resolved_read_version,
        storage_options=storage_options,
        **namespace_kwargs,
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
