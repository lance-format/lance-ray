"""
I/O operations for Lance-Ray integration.
"""

import logging
import pickle
import uuid as uuid_module
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

import pyarrow as pa
import pyarrow.compute as pc
import ray
from lance.dataset import LanceDataset, LanceOperation
from lance.udf import BatchUDF
from ray.data import Dataset, read_datasource
from ray.util.multiprocessing import Pool

from .datasink import LanceDatasink
from .datasource import LanceDatasource, blob_field_kind
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

#: Transform contract for :func:`update_columns`.
#:
#: Receives a ``pa.Table`` holding only the requested user columns (metadata
#: columns are hidden) and must return a ``pa.Table`` whose column set is
#: exactly ``output_schema.names``, with the same number of rows **in the same
#: order**. Row reordering cannot be detected and silently corrupts data.
UpdateColumnsTransform = Callable[[pa.Table], pa.Table]

_METADATA_COLUMNS = frozenset({"_rowaddr", "_fragid", "_rowid"})

# Lance's built-in commit performs a conflict-checked rebase on every attempt.
# Bounded here only to limit tail latency (0 would still rebase once, it just
# would not retry).
_UPDATE_COMMIT_MAX_RETRIES = 5


@dataclass(frozen=True)
class UpdateColumnsResult:
    """Outcome of a distributed :func:`update_columns` run.

    The transaction UUID is intentionally absent: after a successful commit
    ``version`` is the better handle, and ``dataset.read_transaction(version)``
    recovers the full transaction (including its UUID). The UUID only matters
    when it is unknown whether a version was produced at all, so it is carried
    by :class:`CommitOutcomeUnknown` instead.
    """

    read_version: int
    version: int
    columns: tuple[str, ...]
    rows_updated: int
    fragments_rewritten: int


class CommitOutcomeUnknown(RuntimeError):
    """Raised when a commit neither clearly succeeded nor clearly failed.

    The transaction file is written *before* the manifest, so its presence does
    not prove the commit landed. To confirm, enumerate versions after
    ``read_version`` and match ``dataset.read_transaction(version).uuid``
    against :attr:`transaction_uuid`. Do not re-run the backfill until the
    outcome is established.
    """

    def __init__(
        self,
        message: str,
        *,
        transaction_uuid: str,
        read_version: int,
        columns: tuple[str, ...],
        field_ids: tuple[int, ...],
        fragment_ids: tuple[int, ...],
    ):
        super().__init__(message)
        self.transaction_uuid = transaction_uuid
        self.read_version = read_version
        self.columns = columns
        self.field_ids = field_ids
        self.fragment_ids = fragment_ids


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


def _read_resolved_lance(
    uri: str,
    *,
    table_id: Optional[list[str]],
    columns: Optional[list[str]],
    filter: Optional[str],
    storage_options: Optional[dict[str, Any]],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    dataset_options: Optional[dict[str, Any]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    with_metadata: bool,
) -> Dataset:
    """Read an already-resolved URI while retaining namespace credentials.

    ``read_lance`` deliberately rejects an explicit URI combined with namespace
    arguments.  ``update_columns`` is different: the driver resolves a
    namespace target once to pin its location and version, but worker dataset
    opens still need the namespace client for credential refresh.  Keep this
    narrow internal path so the public validation contract remains unchanged.
    """
    datasource = LanceDatasource(
        uri=uri,
        table_id=table_id,
        columns=columns,
        filter=filter,
        storage_options=storage_options,
        base_store_params=base_store_params,
        dataset_options=dataset_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        with_metadata=with_metadata,
    )
    return read_datasource(datasource=datasource, ray_remote_args={})


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
    target_all_bases: Optional[bool] = None,
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
            efficient but require newer versions of lance to read. The default
            (None) uses PyLance's stable format. Legacy blob columns require an
            explicit compatible version such as "2.1".
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
        target_all_bases: Select all registered bases, including primary storage
            when True and excluding it when False. None preserves the default.
            Mutually exclusive with non-empty target_bases. Round-robin restarts
            for each write task or streaming batch, not across the dataset.
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
        target_all_bases=target_all_bases,
        base_store_params=base_store_params,
        external_blob_mode=external_blob_mode,
        allow_external_blob_outside_bases=allow_external_blob_outside_bases,
        stacklevel=2,
    )
    initial_bases = normalize_initial_bases(initial_bases)
    if data_storage_version is None:
        write_schema = schema
        if write_schema is None:
            ray_schema = ds.schema()
            if ray_schema is not None and isinstance(ray_schema.base_schema, pa.Schema):
                write_schema = ray_schema.base_schema
        _validate_legacy_blob_storage_version(write_schema)

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
            target_all_bases=target_all_bases,
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
            target_all_bases=target_all_bases,
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
    output_schema: pa.Schema,
) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """Validate ``output_schema`` against the target dataset.

    Everything here runs on the driver, before any Ray task starts, so schema
    errors never surface after part of the fragments have been rewritten.

    Returns the target column names (in ``output_schema`` order) and the *leaf*
    Lance field ids they cover, which the driver later cross-checks against the
    ``fields_modified`` reported by every worker.
    """
    if len(output_schema) == 0:
        raise ValueError(
            "'output_schema' must declare at least one column to update. "
            "update_columns only overwrites existing columns; use "
            "add_columns_from() to add new ones."
        )

    target_schema = lance_ds.schema
    lance_schema = lance_ds.lance_schema
    target_names = set(target_schema.names)
    blob_columns = _blob_column_names(target_schema)

    names: list[str] = []
    field_ids: list[int] = []
    seen: set[str] = set()

    for field in output_schema:
        name = field.name
        if name in seen:
            raise ValueError(f"Duplicate column '{name}' in 'output_schema'.")
        seen.add(name)

        if name in _METADATA_COLUMNS:
            raise ValueError(
                f"Cannot update metadata column '{name}'. Metadata columns are "
                "managed by lance-ray and must not appear in 'output_schema'."
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
        if target_field.type != field.type:
            raise ValueError(
                f"Type mismatch for column '{name}': target dataset has "
                f"{target_field.type}, 'output_schema' declares {field.type}."
            )
        if target_field.nullable != field.nullable:
            raise ValueError(
                f"Nullability mismatch for column '{name}': target dataset has "
                f"nullable={target_field.nullable}, 'output_schema' declares "
                f"nullable={field.nullable}. Arrow cast does not check "
                "nullability, so this must match exactly."
            )

        lance_field = lance_schema.field(name)
        if lance_field is None:
            raise ValueError(
                f"Column '{name}' has no Lance field id; cannot update it."
            )
        names.append(name)
        field_ids.extend(_leaf_field_ids(lance_field))

    return tuple(names), tuple(field_ids)


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


def _make_update_transform(
    transform: "UpdateColumnsTransform",
    output_schema: pa.Schema,
    columns: tuple[str, ...],
) -> Callable[[pa.Table], pa.Table]:
    """Wrap the user transform: hide metadata columns, validate, re-attach."""
    expected = set(columns)
    column_list = list(columns)
    non_nullable = [f.name for f in output_schema if not f.nullable]

    def _wrapped(batch: pa.Table) -> pa.Table:
        rowaddr = batch.column("_rowaddr")
        fragid = batch.column("_fragid")
        user_batch = batch.drop_columns(
            [c for c in _METADATA_COLUMNS if c in batch.column_names]
        )

        result = transform(user_batch)

        if isinstance(result, pa.RecordBatch):
            raise TypeError(
                "transform must return pa.Table, got pa.RecordBatch. Use "
                "pa.Table.from_batches([rb]) to convert it."
            )
        if not isinstance(result, pa.Table):
            raise TypeError(
                f"transform must return pa.Table, got {type(result).__name__}."
            )
        if result.num_rows != batch.num_rows:
            raise ValueError(
                f"transform changed the row count: got {result.num_rows} rows "
                f"for an input batch of {batch.num_rows}. The transform must be "
                "a row-order-preserving batch mapping; filtering, sorting, "
                "exploding or aggregating inside the transform is not allowed."
            )
        actual = set(result.column_names)
        if actual != expected:
            unexpected = sorted(actual - expected)
            missing = sorted(expected - actual)
            raise ValueError(
                "transform output columns must match 'output_schema' exactly. "
                f"Unexpected: {unexpected}; missing: {missing}."
            )

        # Column order and types come from output_schema, not from the user's
        # return value; cast is safe=True so precision loss raises.
        out = result.select(column_list).cast(output_schema)

        for name in non_nullable:
            if out.column(name).null_count:
                raise ValueError(
                    f"transform produced nulls for non-nullable column '{name}'."
                )

        return out.append_column("_rowaddr", rowaddr).append_column("_fragid", fragid)

    return _wrapped


_UPDATE_RESULT_SCHEMA = pa.schema(
    [
        pa.field("frag_id", pa.int64()),
        pa.field("fragment_meta", pa.binary()),
        pa.field("fields_modified", pa.binary()),
        pa.field("rows_updated", pa.int64()),
    ]
)


def _is_commit_conflict(exc: BaseException) -> bool:
    """Whether a commit failure is a definite conflict (nothing was written).

    pylance maps every Lance error onto a builtin exception type, so the only
    signal available is the message text produced by ``lance-core``'s
    ``CommitConflict`` / ``RetryableCommitConflict`` / ``IncompatibleTransaction``
    variants. Anything we cannot positively identify is treated as an unknown
    outcome, which is the conservative direction.
    """
    message = str(exc).lower()
    return "commit conflict" in message or "incompatible transaction" in message


def update_columns(
    uri: Optional[str] = None,
    *,
    transform: "UpdateColumnsTransform",
    output_schema: pa.Schema,
    filter: Optional[str] = None,
    read_columns: Optional[Sequence[str]] = None,
    transform_batch_size: Optional[int] = None,
    transform_ray_remote_args: Optional[Mapping[str, Any]] = None,
    fragment_ray_remote_args: Optional[Mapping[str, Any]] = None,
    fragment_concurrency: Optional[int] = None,
    storage_options: Optional[Mapping[str, Any]] = None,
    base_store_params: Optional[Mapping[str, Mapping[str, Any]]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[Mapping[str, str]] = None,
    table_id: Optional[Sequence[str]] = None,
) -> UpdateColumnsResult:
    """Overwrite existing columns of a Lance dataset with Ray.

    This is the column-level counterpart of :func:`add_columns_from`. It pins a
    snapshot, computes new values for existing columns with a distributed
    transform, and rewrites only those columns via Lance's
    ``RewriteColumns`` update mode: rows do not move, ``_rowaddr`` is preserved,
    and untouched columns keep their original data files. It fits "most rows,
    few columns" backfills; it is a poor fit for updating a handful of rows
    scattered across many fragments, because a fragment's target column is
    rewritten in full even for a single matching row.

    Examples:
        >>> import lance_ray as lr
        >>> import pyarrow as pa
        >>> import pyarrow.compute as pc
        >>> def bump_price(batch: pa.Table) -> pa.Table:
        ...     return pa.table({"price": pc.multiply(batch["price"], 1.1)})
        >>> lr.update_columns(  # doctest: +SKIP
        ...     "/tmp/products.lance",
        ...     transform=bump_price,
        ...     output_schema=pa.schema([pa.field("price", pa.float64())]),
        ...     filter="status = 'active'",
        ...     read_columns=["price"],
        ... )

    Args:
        uri: The path to the target Lance dataset. If omitted, provide
            ``namespace_impl`` and ``table_id`` to resolve it from a namespace.
        transform: A callable taking a ``pa.Table`` of the requested columns and
            returning a ``pa.Table`` whose columns are exactly
            ``output_schema.names``. It **must preserve row count and row
            order**; reordering cannot be detected and silently writes values to
            the wrong rows. Do not filter, sort, join, deduplicate, aggregate or
            explode inside the transform.
        output_schema: Schema of the transform output. Required, so that column
            existence, Arrow type and nullability are checked before any Ray
            task starts. Only names, types and nullability are compared; Lance
            field ids and field metadata are resolved from the target dataset.
        filter: A Lance filter expression. Only matching rows get new values;
            unmatched rows keep their old values. Note that the physical rewrite
            is still fragment-wide, so a very sparse filter is a poor fit.
        read_columns: Columns handed to the transform. ``None`` expands to all
            top-level non-blob columns; blob columns must be requested
            explicitly because reading them materializes their raw bytes.
        transform_batch_size: Arrow batch size for the transform stage, i.e. the
            size of a single UDF/model-inference input. ``None`` uses Ray's
            default.
        transform_ray_remote_args: ``ray.remote`` args for the transform tasks
            only, e.g. ``{"num_gpus": 1}``.
        fragment_ray_remote_args: ``ray.remote`` args for the per-fragment
            rewrite tasks only; these are CPU/memory/IO bound.
        fragment_concurrency: Maximum number of fragments rewritten
            concurrently. This is the knob that bounds fragment-worker peak
            memory; it does not bound data accumulated by the transform stage.
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

    Raises:
        CommitOutcomeUnknown: The commit neither clearly succeeded nor clearly
            failed (timeout, dropped connection). Confirm before re-running.
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

    columns, field_ids = _resolve_update_targets(lance_ds, output_schema)
    projection = _resolve_read_columns(lance_ds, read_columns)

    fragments_in_lance = {f.metadata.id for f in lance_ds.get_fragments()}

    ray_ds = _read_resolved_lance(
        uri,
        columns=projection,
        filter=filter,
        dataset_options={"version": resolved_read_version},
        storage_options=dict(resolved_storage_options),
        base_store_params=base_store_params_dict,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties_dict,
        table_id=table_id_list,
        with_metadata=True,
    )

    map_batches_kwargs: dict[str, Any] = {}
    if transform_batch_size is not None:
        map_batches_kwargs["batch_size"] = transform_batch_size
    if transform_ray_remote_args:
        # map_batches ends in **ray_remote_args, so the options are splatted
        # into the call rather than nested under a 'ray_remote_args' key.
        map_batches_kwargs.update(transform_ray_remote_args)

    ray_ds = ray_ds.map_batches(
        _make_update_transform(transform, output_schema, columns),
        batch_format="pyarrow",
        **map_batches_kwargs,
    )

    # Capture closure variables for worker tasks.
    _uri = uri
    _storage_options = dict(resolved_storage_options)
    _base_store_params = base_store_params_dict
    _namespace_impl = namespace_impl
    _namespace_properties = namespace_properties_dict
    _table_id = table_id_list
    _read_version = resolved_read_version
    _columns = list(columns)

    def _update_one_fragment(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return _UPDATE_RESULT_SCHEMA.empty_table()

        frag_ids = pc.unique(group.column("_fragid"))
        if len(frag_ids) != 1:
            raise ValueError(
                f"map_groups received {len(frag_ids)} fragment ids in one "
                f"group: {frag_ids.to_pylist()}. Expected exactly one."
            )
        frag_id = int(frag_ids[0].as_py())

        rowaddr = group.column("_rowaddr")
        if pc.count_distinct(rowaddr).as_py() != group.num_rows:
            raise ValueError(
                f"Duplicate _rowaddr values for fragment {frag_id}. Each row "
                "address must appear at most once; update_columns will not "
                "silently pick one of several candidate values."
            )
        derived = pc.cast(pc.shift_right(rowaddr, 32), pa.uint64())
        if pc.any(pc.not_equal(derived, frag_id)).as_py():
            raise ValueError(
                f"Some _rowaddr values in the group for fragment {frag_id} do "
                "not belong to that fragment."
            )

        # No sort: the fragment update is a hash join on _rowaddr, so input
        # order carries no meaning, and duplicates were already rejected above.
        update_table = group.select(["_rowaddr", *_columns]).combine_chunks()

        local_ns_kwargs = get_namespace_kwargs(
            _namespace_impl, _namespace_properties, _table_id
        )
        local_ds = LanceDataset(
            uri=_uri,
            storage_options=_storage_options,
            base_store_params=_base_store_params,
            version=_read_version,
            **local_ns_kwargs,
        )
        fragment = local_ds.get_fragment(frag_id)
        if fragment is None:
            raise ValueError(f"Fragment {frag_id} not found in Lance dataset at {_uri}")

        fragment_meta, fields_modified = fragment.update_columns(
            update_table,
            left_on="_rowaddr",
            right_on="_rowaddr",
        )

        return pa.table(
            {
                "frag_id": pa.array([frag_id], type=pa.int64()),
                "fragment_meta": pa.array(
                    [pickle.dumps(fragment_meta)], type=pa.binary()
                ),
                "fields_modified": pa.array(
                    [pickle.dumps(list(fields_modified))], type=pa.binary()
                ),
                "rows_updated": pa.array([group.num_rows], type=pa.int64()),
            },
            schema=_UPDATE_RESULT_SCHEMA,
        )

    map_groups_kwargs: dict[str, Any] = {}
    if fragment_ray_remote_args:
        # Same as above: map_groups ends in **ray_remote_args.
        map_groups_kwargs.update(fragment_ray_remote_args)
    if fragment_concurrency is not None:
        map_groups_kwargs["concurrency"] = fragment_concurrency

    result_ds = ray_ds.groupby("_fragid").map_groups(
        _update_one_fragment,
        batch_format="pyarrow",
        **map_groups_kwargs,
    )

    rows = result_ds.take_all()
    if not rows:
        # The filter matched nothing: no files were written, no transaction.
        return UpdateColumnsResult(
            read_version=resolved_read_version,
            version=resolved_read_version,
            columns=columns,
            rows_updated=0,
            fragments_rewritten=0,
        )

    updated_fragments = []
    seen_frag_ids: set[int] = set()
    rows_updated = 0
    observed_field_ids: Optional[list[int]] = None

    for row in rows:
        frag_id = int(row["frag_id"])
        if frag_id not in fragments_in_lance:
            raise ValueError(
                f"Fragment {frag_id} is not part of the pinned snapshot "
                f"(version {resolved_read_version}) of {uri}"
            )
        if frag_id in seen_frag_ids:
            raise ValueError(f"Duplicate fragment {frag_id} in map_groups output")
        seen_frag_ids.add(frag_id)

        fragment_meta = pickle.loads(row["fragment_meta"])
        if fragment_meta.id != frag_id:
            raise ValueError(
                f"Fragment rewrite changed the fragment id: expected {frag_id}, "
                f"got {fragment_meta.id}"
            )
        updated_fragments.append(fragment_meta)
        rows_updated += int(row["rows_updated"])

        worker_field_ids = sorted(pickle.loads(row["fields_modified"]))
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

    transaction_uuid = str(uuid_module.uuid4())
    logger.info(
        "Committing update_columns: columns=%s read_version=%s fragments=%s "
        "transaction_uuid=%s",
        list(columns),
        resolved_read_version,
        len(updated_fragments),
        transaction_uuid,
    )

    committed = _commit_update(
        uri=uri,
        op=op,
        read_version=resolved_read_version,
        transaction_uuid=transaction_uuid,
        storage_options=_storage_options,
        namespace_kwargs=namespace_kwargs,
        columns=columns,
        field_ids=field_ids,
        fragment_ids=tuple(sorted(seen_frag_ids)),
    )

    return UpdateColumnsResult(
        read_version=resolved_read_version,
        version=committed,
        columns=columns,
        rows_updated=rows_updated,
        fragments_rewritten=len(updated_fragments),
    )


_COMMIT_SLOW_WARNING_S = 300.0


def _commit_update(
    *,
    uri: str,
    op: "LanceOperation.Update",
    read_version: int,
    transaction_uuid: str,
    storage_options: dict[str, Any],
    namespace_kwargs: dict[str, Any],
    columns: tuple[str, ...],
    field_ids: tuple[int, ...],
    fragment_ids: tuple[int, ...],
) -> int:
    """Commit the RewriteColumns transaction exactly once.

    Retries are delegated to Lance, whose commit loop re-runs
    ``TransactionRebase.check_txn`` against every concurrent transaction and
    only rebases when that check passes. lance-ray's own
    ``_commit_with_retry`` must not be used here: it compares fragment id sets
    and then advances ``read_version`` on its own, which would let an Update
    built from stale fragment metadata overwrite a concurrent writer.
    """
    import threading

    from lance.dataset import Transaction

    txn = Transaction(
        read_version=read_version,
        operation=op,
        uuid=transaction_uuid,
    )

    timer = threading.Timer(
        _COMMIT_SLOW_WARNING_S,
        logger.warning,
        args=(
            "update_columns commit still pending after %.0fs "
            "(transaction_uuid=%s, read_version=%s). If it never returns, the "
            "outcome is unknown and must be confirmed against version history "
            "before re-running.",
            _COMMIT_SLOW_WARNING_S,
            transaction_uuid,
            read_version,
        ),
    )
    timer.daemon = True
    timer.start()
    try:
        committed_ds = LanceDataset.commit(
            uri,
            txn,
            max_retries=_UPDATE_COMMIT_MAX_RETRIES,
            storage_options=storage_options,
            **namespace_kwargs,
        )
    except Exception as exc:
        if _is_commit_conflict(exc):
            # Nothing was committed. The caller must recompute from a fresh
            # snapshot; retrying with this stale metadata is never safe.
            raise
        raise CommitOutcomeUnknown(
            "update_columns could not determine whether its commit succeeded. "
            "The transaction file is written before the manifest, so its "
            "presence proves nothing. Enumerate versions after "
            f"{read_version} and match read_transaction(version).uuid against "
            f"{transaction_uuid!r} before re-running this backfill.",
            transaction_uuid=transaction_uuid,
            read_version=read_version,
            columns=columns,
            field_ids=field_ids,
            fragment_ids=fragment_ids,
        ) from exc
    finally:
        timer.cancel()

    return committed_ds.version


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


def _validate_legacy_blob_storage_version(schema: Optional[pa.Schema]) -> None:
    if schema is None:
        return

    def legacy_blob_paths(field: "pa.Field[Any]", parent: str = "") -> list[str]:
        path = f"{parent}.{field.name}" if parent else field.name
        metadata = field.metadata or {}
        paths = [path] if metadata.get(b"lance-encoding:blob") == b"true" else []
        if pa.types.is_struct(field.type):
            for child in field.type:
                paths.extend(legacy_blob_paths(child, path))
        return paths

    paths = [path for field in schema for path in legacy_blob_paths(field)]
    if paths:
        fields = ", ".join(repr(path) for path in paths)
        raise ValueError(
            f"Legacy blob field(s) {fields} are incompatible with the default "
            'stable data storage version. Set data_storage_version="2.1" to '
            "keep using legacy blob encoding, or migrate the field(s) to Blob v2."
        )
