import inspect
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional

import pyarrow as pa
from ray.data._internal.util import _check_import, call_with_retry
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask

from .utils import array_split, create_storage_options_provider, get_or_create_namespace

if TYPE_CHECKING:
    import lance


class LanceDatasource(Datasource):
    """Lance datasource, for reading Lance dataset."""

    # Errors to retry when reading Lance fragments.
    READ_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    # Maximum number of attempts to read Lance fragments.
    READ_FRAGMENTS_MAX_ATTEMPTS = 10
    # Maximum backoff seconds between attempts to read Lance fragments.
    READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: Optional[str] = None,
        table_id: Optional[list[str]] = None,
        columns: Optional[list[str]] = None,
        filter: Optional[str] = None,
        storage_options: Optional[dict[str, str]] = None,
        scanner_options: Optional[dict[str, Any]] = None,
        dataset_options: Optional[dict[str, Any]] = None,
        fragment_ids: Optional[list[int]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[dict[str, str]] = None,
    ):
        _check_import(self, module="lance", package="pylance")

        self._dataset_options = dataset_options or {}
        self._scanner_options = scanner_options or {}
        if columns is not None:
            self._scanner_options["columns"] = columns
        if filter is not None:
            self._scanner_options["filter"] = filter

        self._uri = uri
        self._table_id = table_id
        self._storage_options = storage_options

        # Store namespace_impl and namespace_properties for worker reconstruction.
        # Workers will use these to reconstruct the namespace and storage options provider.
        self._namespace_impl = namespace_impl
        self._namespace_properties = namespace_properties

        # Construct namespace from impl and properties (cached per worker)
        self._namespace = get_or_create_namespace(namespace_impl, namespace_properties)

        match = []
        match.extend(self.READ_FRAGMENTS_ERRORS_TO_RETRY)
        match.extend(DataContext.get_current().retried_io_errors)
        self._retry_params = {
            "description": "read lance fragments",
            "match": match,
            "max_attempts": self.READ_FRAGMENTS_MAX_ATTEMPTS,
            "max_backoff_s": self.READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS,
        }
        self._fragment_ids = set(fragment_ids) if fragment_ids else None

        self._lance_ds = None
        self._fragments = None

    @property
    def lance_dataset(self) -> "lance.LanceDataset":
        if self._lance_ds is None:
            import lance

            dataset_options = self._dataset_options.copy()
            dataset_options["uri"] = self._uri
            dataset_options["namespace"] = self._namespace
            dataset_options["table_id"] = self._table_id
            dataset_options["storage_options"] = self._storage_options
            # Note: lance.dataset() doesn't accept storage_options_provider.
            # When namespace is provided, it handles credential refresh internally.
            # For workers, we pass namespace_impl/properties to reconstruct the provider.
            self._lance_ds = lance.dataset(**dataset_options)
        return self._lance_ds

    @property
    def fragments(self) -> list["lance.LanceFragment"]:
        if self._fragments is None:
            self._fragments = self.lance_dataset.get_fragments() or []
            if self._fragment_ids:
                self._fragments = [
                    f for f in self._fragments if f.metadata.id in self._fragment_ids
                ]
        return self._fragments

    def get_read_tasks(self, parallelism: int, **kwargs) -> list[ReadTask]:
        if not self.fragments:
            return []

        read_tasks = []

        # Extract dataset components for worker reconstruction.
        # We pass namespace_impl/properties/table_id instead of the provider object
        # because namespace objects are not serializable. Workers will reconstruct
        # the namespace and provider using these serializable parameters.
        dataset_uri = self.lance_dataset.uri
        dataset_version = self.lance_dataset.version
        dataset_storage_options = self._lance_ds._storage_options
        serialized_manifest = self._lance_ds._ds.serialized_manifest()
        namespace_impl = self._namespace_impl
        namespace_properties = self._namespace_properties
        table_id = self._table_id

        for fragments in array_split(self.fragments, parallelism):
            if len(fragments) == 0:
                continue

            # Use scanner.count_rows with filter to count rows meeting specified conditions
            scanner_options = self._scanner_options.copy()
            scanner_options["fragments"] = fragments
            scanner_options["columns"] = []
            scanner_options["with_row_id"] = True
            scanner = self._lance_ds.scanner(**scanner_options)
            num_rows = scanner.count_rows()

            fragment_ids = [f.metadata.id for f in fragments]
            input_files = [
                data_file.path
                for fragment in fragments
                for data_file in fragment.data_files()
            ]

            # Ray 2.48+ no longer has the schema argument...
            if "schema" in inspect.signature(BlockMetadata.__init__).parameters:
                # TODO(chengsu): Take column projection into consideration for schema.
                metadata = BlockMetadata(
                    num_rows=num_rows,
                    schema=fragments[0].schema,
                    input_files=input_files,
                    size_bytes=None,
                    exec_stats=None,
                )
            else:
                metadata = BlockMetadata(
                    num_rows=num_rows,
                    input_files=input_files,
                    size_bytes=None,
                    exec_stats=None,
                )

            read_task = ReadTask(
                lambda fids=fragment_ids,
                uri=dataset_uri,
                version=dataset_version,
                storage_options=dataset_storage_options,
                manifest=serialized_manifest,
                ns_impl=namespace_impl,
                ns_props=namespace_properties,
                tbl_id=table_id,
                scanner_options=self._scanner_options,
                retry_params=self._retry_params: (
                    _read_fragments_with_retry(
                        fids,
                        uri,
                        version,
                        storage_options,
                        manifest,
                        ns_impl,
                        ns_props,
                        tbl_id,
                        scanner_options,
                        retry_params,
                    )
                ),
                metadata,
            )

            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.fragments:
            return 0

        return sum(
            data_file.file_size_bytes
            for fragment in self.fragments
            for data_file in fragment.data_files()
            if data_file.file_size_bytes is not None
        )


def _read_fragments_with_retry(
    fragment_ids: list[int],
    uri: str,
    version: int,
    storage_options: Optional[dict[str, str]],
    manifest: bytes,
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    scanner_options: dict[str, Any],
    retry_params: dict[str, Any],
) -> Iterator[pa.Table]:
    # Reconstruct storage options provider on worker for credential refresh
    storage_options_provider = create_storage_options_provider(
        namespace_impl, namespace_properties, table_id
    )

    import lance

    lance_ds = lance.LanceDataset(
        uri,
        version=version,
        storage_options=storage_options,
        serialized_manifest=manifest,
        storage_options_provider=storage_options_provider,
    )

    return call_with_retry(
        lambda: _read_fragments(fragment_ids, lance_ds, scanner_options),
        **retry_params,
    )


def _read_fragments(
    fragment_ids: list[int],
    lance_ds: "lance.LanceDataset",
    scanner_options: dict[str, Any],
) -> Iterator[pa.Table]:
    """Read Lance fragments in batches.

    This enhanced reader detects Lance blob-encoded columns and reconstructs
    raw bytes using the :meth:`LanceDataset.take_blobs` API, returning
    :class:`pyarrow.LargeBinaryArray` columns instead of the default
    struct-based descriptors.

    Row ordering is preserved by using per-batch row IDs.

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    :class:`lance.LanceFragment` is expensive.
    """
    # Resolve fragments
    fragments = [lance_ds.get_fragment(id) for id in fragment_ids]

    # Copy scanner options so we can safely mutate
    scan_opts: dict[str, Any] = dict(scanner_options)
    scan_opts["fragments"] = fragments

    # Detect blob columns from the dataset schema and requested projection
    ds_schema: pa.Schema = lance_ds.schema
    requested_columns = scan_opts.get("columns")
    # Map column name -> blob kind ("legacy" or "v2")
    blob_columns: dict[str, str] = {}

    def _is_blob_field(f: pa.Field) -> Optional[str]:
        """Detect Lance blob columns.

        Returns:
            "v2" for blob v2 extension columns,
            "legacy" for legacy metadata-based blob columns,
            or None if the field is not a blob.
        """
        field_type = f.type

        # Blob v2: extension type `lance.blob.v2`
        if isinstance(field_type, pa.ExtensionType):
            ext_name = getattr(field_type, "extension_name", None)
            if ext_name == "lance.blob.v2":
                return "v2"

        # Legacy: LargeBinary with field metadata {"lance-encoding:blob": "true"}
        try:
            is_large_bin = field_type == pa.large_binary()
        except Exception:
            is_large_bin = False
        if not is_large_bin:
            return None

        meta = f.metadata
        if meta is None:
            return None

        # pyarrow may store metadata keys/values as str
        if (meta.get("lance-encoding:blob") == "true") or (
            meta.get(b"lance-encoding:blob") == b"true"
        ):
            return "legacy"

        return None

    # Build list of blob columns to reconstruct, honoring column projection
    ds_field_names = ds_schema.names
    for idx, name in enumerate(ds_field_names):
        field = ds_schema.field(idx)
        kind = _is_blob_field(field)
        if kind is None:
            continue
        if requested_columns is None:
            blob_columns[name] = kind
        elif isinstance(requested_columns, list):
            if name in requested_columns:
                blob_columns[name] = kind
        elif isinstance(requested_columns, dict) and name in requested_columns:
            # If columns are SQL expressions, only reconstruct if explicitly requested
            blob_columns[name] = kind

    # If blob columns are present, ensure row IDs are included for reconstruction
    if blob_columns:
        scan_opts["with_row_id"] = True

    scanner = lance_ds.scanner(**scan_opts)

    for batch in scanner.to_reader():
        # Fast path: no blob columns requested in this scan
        if not blob_columns:
            yield pa.Table.from_batches([batch])
            continue

        # Build a table so we can manipulate columns easily
        table = pa.Table.from_batches([batch])

        # Extract row IDs used to reconstruct bytes in the same order
        if "_rowid" not in table.column_names:
            # Safety: if row ids are missing for any reason, fall back to original
            yield table
            continue
        row_ids = table.column("_rowid").to_pylist()

        # For each blob column, reconstruct a LargeBinary array
        for col, kind in blob_columns.items():
            if col not in table.column_names:
                # Column not projected in this batch
                continue

            # The scanned representation may be a struct descriptor or extension-backed
            # array. We only rely on the null bitmap: None -> null bytes; non-null ->
            # fetch bytes via LanceDataset.take_blobs.
            desc_py = table.column(col).to_pylist()

            # Fetch BlobFile handles in batch order
            blob_files = lance_ds.take_blobs(col, ids=row_ids)

            # Convert BlobFile -> bytes, respecting nulls
            values: list[Optional[bytes]] = []
            for i, desc in enumerate(desc_py):
                if desc is None:
                    values.append(None)
                    continue
                # Backward compatibility: older legacy blob layouts may encode
                # nulls as a sentinel struct {position: 1, size: 0} instead of
                # using the Arrow null bitmap. Treat this sentinel as null for
                # metadata-based blob columns only.
                if kind == "legacy" and isinstance(desc, dict):
                    pos = desc.get("position")
                    size = desc.get("size")
                    if pos == 1 and size == 0:
                        values.append(None)
                        continue
                with blob_files[i] as bf:
                    values.append(bf.read())

            # Construct LargeBinary array for Ray, preserving legacy metadata only
            # for metadata-based blob columns. Blob v2 extension columns are exposed
            # as plain LargeBinary bytes.
            new_arr = pa.array(values, type=pa.large_binary())
            ds_field_index = ds_schema.get_field_index(col)
            ds_field = ds_schema.field(ds_field_index)
            nullable = ds_field.nullable
            metadata = ds_field.metadata if kind == "legacy" else None
            new_field = pa.field(
                col, pa.large_binary(), nullable=nullable, metadata=metadata
            )
            table = table.set_column(
                table.schema.get_field_index(col),
                new_field,
                pa.chunked_array([new_arr]),
            )

        # Drop helper row ID column before returning
        if "_rowid" in table.column_names:
            table = table.drop_columns(["_rowid"])

        yield table
