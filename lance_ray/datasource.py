from __future__ import annotations

import inspect
import logging
from collections.abc import Iterator
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, cast

import pyarrow as pa
import pyarrow.compute as pc
from ray.data._internal.util import _check_import, call_with_retry
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask

from .utils import (
    array_split,
    get_namespace_kwargs,
    get_or_create_namespace,
)

if TYPE_CHECKING:
    import lance

logger = logging.getLogger(__name__)

# ``estimate_inmemory_data_size`` samples variable-width columns from up to this
# many fragments, spread over the dataset, and this many leading rows of each.
_ESTIMATE_SAMPLE_FRAGMENTS = 8
_ESTIMATE_SAMPLE_ROWS = 256
# A ``with_row_id`` scan keeps the uint64 ``_rowid`` column, and a
# ``with_metadata`` read adds the uint64 ``_rowaddr`` and ``_fragid`` columns.
_ROW_ID_BYTES = 8
_METADATA_ROW_BYTES = 16


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
        base_store_params: Optional[dict[str, dict[str, Any]]] = None,
        fragment_ids: Optional[list[int]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[dict[str, str]] = None,
        with_metadata: bool = False,
    ):
        _check_import(self, module="lance", package="pylance")

        self._dataset_options = dict(dataset_options or {})
        dataset_base_store_params = self._dataset_options.pop("base_store_params", None)
        if (
            base_store_params is not None
            and dataset_base_store_params is not None
            and base_store_params != dataset_base_store_params
        ):
            raise ValueError(
                "'base_store_params' conflicts with "
                "dataset_options['base_store_params']"
            )
        self._base_store_params = (
            base_store_params
            if base_store_params is not None
            else dataset_base_store_params
        )
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
        self._with_metadata = with_metadata

        self._lance_ds: Optional[lance.LanceDataset] = None
        self._fragments: Optional[list[lance.LanceFragment]] = None

    @property
    def lance_dataset(self) -> lance.LanceDataset:
        if self._lance_ds is None:
            import lance

            dataset_options = self._dataset_options.copy()
            dataset_options["uri"] = self._uri
            dataset_options["storage_options"] = self._storage_options
            ns_kwargs = get_namespace_kwargs(
                self._namespace_impl, self._namespace_properties, self._table_id
            )
            dataset_options.update(ns_kwargs)
            base_store_params_kwargs: dict[str, Any] = {}
            if self._base_store_params:
                base_store_params_kwargs = {
                    "base_store_params": self._base_store_params
                }
            self._lance_ds = lance.dataset(
                **dataset_options,
                **base_store_params_kwargs,
            )
        return self._lance_ds

    @property
    def fragments(self) -> list[lance.LanceFragment]:
        if self._fragments is None:
            fragments = self.lance_dataset.get_fragments() or []
            if self._fragment_ids:
                fragments = [
                    f for f in fragments if f.metadata.id in self._fragment_ids
                ]
            self._fragments = fragments
        return self._fragments

    def _get_storage_options(self) -> Optional[dict[str, str]]:
        dataset = self.lance_dataset
        try:
            return dataset.initial_storage_options
        except AttributeError:
            # pylance < 5 only exposes the private attribute.
            return cast(
                Optional[dict[str, str]], getattr(dataset, "_storage_options", None)
            )

    def _get_serialized_manifest(self) -> Optional[bytes]:
        try:
            return self.lance_dataset._ds.serialized_manifest()
        except AttributeError:
            return None

    def get_read_tasks(
        self, parallelism: int, *args: Any, **kwargs: Any
    ) -> list[ReadTask]:
        if not self.fragments:
            return []

        read_tasks: list[ReadTask] = []

        # Extract dataset components for worker reconstruction.
        # We pass namespace_impl/properties/table_id instead of the provider object
        # because namespace objects are not serializable. Workers will reconstruct
        # the namespace and provider using these serializable parameters.
        dataset_uri = self.lance_dataset.uri
        dataset_version = self.lance_dataset.version
        dataset_storage_options = self._get_storage_options()
        serialized_manifest = self._get_serialized_manifest()
        namespace_impl = self._namespace_impl
        namespace_properties = self._namespace_properties
        table_id = self._table_id
        base_store_params = self._base_store_params

        for fragments in array_split(self.fragments, parallelism):
            if len(fragments) == 0:
                continue

            # Use scanner.count_rows with filter to count rows meeting specified conditions
            scanner_options = self._scanner_options.copy()
            scanner_options["fragments"] = fragments
            scanner_options["columns"] = []
            scanner_options["with_row_id"] = True
            scanner = self.lance_dataset.scanner(**scanner_options)
            num_rows = scanner.count_rows()

            fragment_ids = [f.metadata.id for f in fragments]
            input_files = tuple(
                data_file.path
                for fragment in fragments
                for data_file in fragment.data_files()
            )

            # Ray 2.48+ no longer has the schema argument...
            if "schema" in inspect.signature(BlockMetadata.__init__).parameters:
                # TODO(chengsu): Take column projection into consideration for schema.
                metadata = BlockMetadata(
                    num_rows=num_rows,
                    schema=fragments[0].schema,  # type: ignore[call-arg]
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

            # ``partial`` binds this iteration's fragment ids eagerly, which a
            # closure over the loop variable would not do.
            read_task = ReadTask(
                partial(
                    _read_fragments_with_retry,
                    fragment_ids,
                    dataset_uri,
                    dataset_version,
                    dataset_storage_options,
                    serialized_manifest,
                    namespace_impl,
                    namespace_properties,
                    table_id,
                    base_store_params,
                    self._scanner_options,
                    self._retry_params,
                    self._with_metadata,
                ),
                metadata,
            )

            read_tasks.append(read_task)

        return read_tasks

    def _estimate_columns_size(self) -> Optional[int]:
        """Estimate the in-memory size of the columns this read returns, or None.

        Rows times the Arrow width of the columns: exact from the schema for
        fixed-width types, and the mean of a sample of leading rows (from
        spread-out fragments) for the rest, so only variable-width columns are
        read. The filter is ignored, which can overestimate; a variable-width
        mean taken from leading rows can be off either way.

        None when the read projects expressions, includes a blob column (read
        back as raw bytes, not the descriptors a sample sees), or the schema or
        sample cannot be read.
        """
        columns = self._scanner_options.get("columns")
        try:
            dataset = self.lance_dataset
            schema = dataset.schema
            if columns is None:
                names = list(schema.names)
            elif isinstance(columns, list):
                names = columns
            else:
                return None
            fields = [schema.field(name) for name in names]
            if not fields or any(_is_blob_field(field) for field in fields):
                return None

            row_bytes = 0.0
            variable = []
            for field in fields:
                width = _fixed_width(field.type)
                if width is None:
                    variable.append(field.name)
                else:
                    row_bytes += width
            fragments = self.fragments
            if variable:
                row_bytes += self._sample_row_bytes(dataset, fragments, variable)
            if self._scanner_options.get("with_row_id"):
                row_bytes += _ROW_ID_BYTES
            if self._with_metadata:
                row_bytes += _METADATA_ROW_BYTES
            total_rows = sum(fragment.count_rows() for fragment in fragments)
            return int(total_rows * row_bytes)
        except Exception as e:
            # The estimate is advisory: never fail the read over it, and keep
            # the error text, which may carry the table URI, out of the log.
            logger.debug(
                "In-memory size estimate failed (%s); using file sizes",
                type(e).__name__,
            )
            return None

    @staticmethod
    def _sample_row_bytes(
        dataset: lance.LanceDataset,
        fragments: list[lance.LanceFragment],
        columns: list[str],
    ) -> float:
        """Mean in-memory bytes per row of ``columns``.

        Taken from leading rows of up to ``_ESTIMATE_SAMPLE_FRAGMENTS``
        fragments spread over the dataset.
        """
        stride = -(-len(fragments) // _ESTIMATE_SAMPLE_FRAGMENTS)
        sample_bytes = sample_rows = 0
        for fragment in fragments[::stride]:
            sample = dataset.scanner(
                columns=columns,
                fragments=[fragment],
                limit=_ESTIMATE_SAMPLE_ROWS,
            ).to_table()
            sample_bytes += sample.nbytes
            sample_rows += sample.num_rows
        return sample_bytes / sample_rows if sample_rows else 0.0

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.fragments:
            return 0

        in_memory = self._estimate_columns_size()
        if in_memory is not None:
            return in_memory

        # ``LanceFragment.data_files()`` is unannotated upstream, so pin the
        # element type here instead of returning ``Any``.
        file_sizes: list[int] = [
            data_file.file_size_bytes
            for fragment in self.fragments
            for data_file in fragment.data_files()
            if data_file.file_size_bytes is not None
        ]
        return sum(file_sizes)


def _read_fragments_with_retry(
    fragment_ids: list[int],
    uri: str,
    version: int,
    storage_options: Optional[dict[str, str]],
    manifest: Optional[bytes],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    scanner_options: dict[str, Any],
    retry_params: dict[str, Any],
    with_metadata: bool = False,
) -> Iterator[pa.Table]:
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    base_store_params_kwargs: dict[str, Any] = {}
    if base_store_params:
        base_store_params_kwargs = {"base_store_params": base_store_params}

    import lance

    ds_kwargs: dict[str, Any] = {
        "uri": uri,
        "version": version,
        "storage_options": storage_options,
    }
    if manifest is not None:
        ds_kwargs["serialized_manifest"] = manifest
    ds_kwargs.update(namespace_kwargs)
    ds_kwargs.update(base_store_params_kwargs)

    lance_ds = lance.LanceDataset(**ds_kwargs)

    return call_with_retry(
        partial(
            _read_fragments, fragment_ids, lance_ds, scanner_options, with_metadata
        ),
        **retry_params,
    )


def _fixed_width(data_type: pa.DataType) -> Optional[float]:
    """In-memory bytes per value of a fixed-width Arrow type, else None."""
    if isinstance(data_type, pa.ExtensionType) or pa.types.is_dictionary(data_type):
        return None
    if pa.types.is_fixed_size_list(data_type):
        item = _fixed_width(data_type.value_type)
        return None if item is None else item * data_type.list_size
    if pa.types.is_struct(data_type):
        fields = [data_type.field(i) for i in range(data_type.num_fields)]
        widths = [_fixed_width(field.type) for field in fields]
        return None if None in widths else sum(cast("list[float]", widths))
    try:
        return float(data_type.bit_width) / 8
    except ValueError:
        return None


def _is_blob_field(f: pa.Field[Any]) -> Optional[str]:
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
    if (meta.get("lance-encoding:blob") == "true") or (  # type: ignore[call-overload]
        meta.get(b"lance-encoding:blob") == b"true"
    ):
        return "legacy"

    return None


def _read_fragments(
    fragment_ids: list[int],
    lance_ds: lance.LanceDataset,
    scanner_options: dict[str, Any],
    with_metadata: bool = False,
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

    if with_metadata:
        scan_opts["with_row_address"] = True

    scanner = lance_ds.scanner(**scan_opts)

    for batch in scanner.to_reader():
        # Fast path: no blob columns requested in this scan
        if not blob_columns:
            table = pa.Table.from_batches([batch])

            if with_metadata and "_rowaddr" in table.column_names:
                rowaddr_col = table.column("_rowaddr")
                # pyarrow-stubs has no overload for shifting an array by a
                # plain Python int, and types the result as a scalar.
                fragid_values = cast(
                    "pa.ChunkedArray[Any]",
                    pc.cast(pc.shift_right(rowaddr_col, 32), pa.uint64()),
                )
                table = table.append_column("_fragid", fragid_values)

            if not with_metadata:
                for col in ("_rowaddr", "_fragid"):
                    if col in table.column_names:
                        table = table.drop_columns([col])

            yield table
            continue

        # Build a table so we can manipulate columns easily
        table = pa.Table.from_batches([batch])

        # Extract row IDs used to reconstruct bytes in the same order
        if "_rowid" not in table.column_names:
            # Safety: if row ids are missing for any reason, fall back to original
            yield table
            continue
        # ``_rowid`` is a non-nullable uint64 column, so every value is an int.
        row_ids = cast("list[int]", table.column("_rowid").to_pylist())

        # For each blob column, reconstruct a LargeBinary array
        for col, kind in blob_columns.items():
            if col not in table.column_names:
                # Column not projected in this batch
                continue

            # The scanned representation may be a struct descriptor or extension-backed
            # array. We rely on the scan output to decide whether a given row is null,
            # but we must be careful about how ``take_blobs`` aligns its results:
            #
            # - Legacy blob columns often return one handle per requested row ID
            #   (including null rows).
            # - Blob v2 currently *skips* null rows, returning fewer handles.
            desc_py = table.column(col).to_pylist()

            # Fetch BlobFile handles in batch order
            blob_files = lance_ds.take_blobs(col, ids=row_ids)

            values: list[Optional[bytes]] = []

            if len(blob_files) == len(desc_py):
                # 1:1 alignment with requested rows (legacy behavior).
                for desc, blob_file in zip(desc_py, blob_files, strict=False):
                    if desc is None:
                        values.append(None)
                        continue

                    if kind == "legacy" and isinstance(desc, dict):
                        pos = desc.get("position")
                        size = desc.get("size")
                        if pos == 1 and size == 0:
                            values.append(None)
                            continue

                    if kind == "v2" and isinstance(desc, dict):
                        v2_pos = desc.get("position")
                        v2_size = desc.get("size")
                        v2_blob_id = desc.get("blob_id")
                        v2_uri = desc.get("blob_uri")
                        if (
                            v2_pos == 0
                            and v2_size == 0
                            and v2_blob_id == 0
                            and (v2_uri == "" or v2_uri is None)
                        ):
                            values.append(None)
                            continue

                    if kind != "legacy":
                        if isinstance(desc, bytes | bytearray | memoryview):
                            values.append(bytes(desc))
                            continue
                        if isinstance(desc, dict) and "bytes" in desc:
                            values.append(bytes(desc["bytes"]))
                            continue

                    if blob_file is None:
                        raise RuntimeError(
                            "LanceDataset.take_blobs returned no blob for the "
                            f"non-null row {len(values)} of column {col!r}"
                        )
                    with blob_file as bf:
                        values.append(bf.read())
            else:
                # Sparse alignment (blob v2 behavior): consume a handle only for
                # non-null rows.
                blob_iter = iter(blob_files)
                for desc in desc_py:
                    if desc is None:
                        values.append(None)
                        continue

                    if kind == "legacy" and isinstance(desc, dict):
                        pos = desc.get("position")
                        size = desc.get("size")
                        if pos == 1 and size == 0:
                            values.append(None)
                            continue

                    if kind == "v2" and isinstance(desc, dict):
                        v2_pos = desc.get("position")
                        v2_size = desc.get("size")
                        v2_blob_id = desc.get("blob_id")
                        v2_uri = desc.get("blob_uri")
                        if (
                            v2_pos == 0
                            and v2_size == 0
                            and v2_blob_id == 0
                            and (v2_uri == "" or v2_uri is None)
                        ):
                            values.append(None)
                            continue

                    if kind != "legacy":
                        if isinstance(desc, bytes | bytearray | memoryview):
                            values.append(bytes(desc))
                            continue
                        if isinstance(desc, dict) and "bytes" in desc:
                            values.append(bytes(desc["bytes"]))
                            continue

                    try:
                        blob_file = next(blob_iter)
                    except StopIteration as exc:  # pragma: no cover
                        raise RuntimeError(
                            "LanceDataset.take_blobs returned fewer blobs than expected"
                        ) from exc
                    if blob_file is None:
                        raise RuntimeError(
                            "LanceDataset.take_blobs returned no blob for the "
                            f"non-null row {len(values)} of column {col!r}"
                        )
                    with blob_file as bf:
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

        if with_metadata and "_rowaddr" in table.column_names:
            rowaddr_col = table.column("_rowaddr")
            # pyarrow-stubs has no overload for shifting an array by a
            # plain Python int, and types the result as a scalar.
            fragid_values = cast(
                "pa.ChunkedArray[Any]",
                pc.cast(pc.shift_right(rowaddr_col, 32), pa.uint64()),
            )
            table = table.append_column("_fragid", fragid_values)

        for col in ("_rowid",):
            if col in table.column_names:
                table = table.drop_columns([col])

        if not with_metadata:
            for col in ("_rowaddr", "_fragid"):
                if col in table.column_names:
                    table = table.drop_columns([col])

        yield table
