from __future__ import annotations

import hashlib
import inspect
import ntpath
import os
import re
from collections.abc import Iterator
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, cast
from urllib.parse import quote, unquote, urlsplit, urlunsplit

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
)

if TYPE_CHECKING:
    import lance


# Ray 2.41+ builds each logical Read source name from ``Datasource.get_name()``.
# Keep provenance there instead of the user-controlled Dataset metrics name.
LANCE_SOURCE_VERSION_NAME_PREFIX = "LanceDatasource[lance_ray_source_version="
LANCE_SOURCE_ID_MARKER = ";lance_ray_source_id="
_WINDOWS_DRIVE_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_WINDOWS_FILE_URI_PATH = re.compile(r"^/[A-Za-z]:[\\/]")


def _uri_scheme(uri: str) -> str:
    if _WINDOWS_DRIVE_PATH.match(uri):
        return ""
    return urlsplit(uri).scheme.lower()


def _normalize_windows_path(path: str) -> str:
    if _WINDOWS_FILE_URI_PATH.match(path):
        path = path[1:]
    normalized = ntpath.normcase(ntpath.normpath(path)).replace("\\", "/")
    return normalized if len(normalized) == 3 else normalized.rstrip("/")


def normalize_dataset_uri(uri: str) -> str:
    """Return a comparable URI for dataset identity checks."""
    if _WINDOWS_DRIVE_PATH.match(uri):
        return _normalize_windows_path(uri)
    if "://" in uri:
        parsed = urlsplit(uri)
        if parsed.scheme == "file":
            path = unquote(parsed.path)
            if _WINDOWS_FILE_URI_PATH.match(path):
                return _normalize_windows_path(path)
            if parsed.netloc and parsed.netloc != "localhost":
                path = f"//{parsed.netloc}{path}"
            return os.path.realpath(os.path.abspath(path)).rstrip(os.sep)
        return uri.rstrip("/")
    return os.path.realpath(os.path.abspath(uri)).rstrip(os.sep)


def _non_sensitive_backend_identity(
    storage_options: Optional[dict[str, Any]],
) -> Optional[str]:
    """Return a stable, non-secret backend discriminator when one is available."""
    if not storage_options:
        return None

    endpoint_keys = (
        "endpoint",
        "endpoint_url",
        "aws_endpoint",
        "s3_endpoint",
        "azure_endpoint",
        "azure_storage_endpoint",
        "oss_endpoint",
        "tos_endpoint",
    )
    for key in endpoint_keys:
        value = storage_options.get(key)
        if not value:
            continue
        value = str(value)
        has_scheme = "://" in value
        parsed = urlsplit(value if has_scheme else f"//{value}")
        hostname = parsed.hostname
        if hostname is not None:
            if ":" in hostname:
                hostname = f"[{hostname}]"
            port = parsed.port
            netloc = hostname if port is None else f"{hostname}:{port}"
            value = urlunsplit(
                (parsed.scheme if has_scheme else "", netloc, parsed.path, "", "")
            )
            if not has_scheme:
                value = value.removeprefix("//")
        return f"endpoint:{value}"

    for key in ("account_name", "azure_storage_account_name"):
        value = storage_options.get(key)
        if value:
            return f"azure_account:{value}"
    return None


def _dataset_uuid(lance_ds: Any) -> Any:
    rust_ds = getattr(lance_ds, "_ds", None)
    if rust_ds is None:
        return None
    raw_uuid = getattr(rust_ds, "uuid", None)
    return raw_uuid() if callable(raw_uuid) else raw_uuid


def _has_reliable_identity(
    lance_ds: Any,
    storage_options: Optional[dict[str, Any]],
) -> bool:
    if _dataset_uuid(lance_ds) is not None:
        return True
    if _non_sensitive_backend_identity(storage_options) is not None:
        return True

    identity_uri = str(getattr(lance_ds, "uri", "") or "")
    scheme = _uri_scheme(identity_uri)
    # S3 and GCS bucket names identify the backend without credentials. Azure
    # container names are account-scoped, so Azure still requires an account or
    # endpoint discriminator when a native dataset UUID is unavailable.
    return scheme in {"", "file", "s3", "gs"}


def dataset_identity(
    lance_ds: Any,
    uri: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
) -> str:
    """Return a stable identity for an opened Lance dataset.

    Prefer a native dataset UUID when the installed PyLance exposes one, and
    always include the normalized URI so copied datasets stay distinct.
    Pass ``uri`` when the caller already resolved a canonical location, such
    as a namespace table path, so source and target compare the same string.
    """
    identity_uri = uri if uri is not None else str(getattr(lance_ds, "uri", "") or "")
    uuid_value = _dataset_uuid(lance_ds)
    if uuid_value is None:
        rust_ds = getattr(lance_ds, "_ds", None)
        if not identity_uri:
            rust_uri = getattr(rust_ds, "uri", None)
            rust_uri = rust_uri() if callable(rust_uri) else rust_uri
            if rust_uri:
                identity_uri = str(rust_uri)
    normalized_uri = normalize_dataset_uri(identity_uri)
    identity_parts = []
    if uuid_value:
        identity_parts.append(f"uuid:{uuid_value}")
    identity_parts.append(f"uri:{normalized_uri}")
    backend = _non_sensitive_backend_identity(storage_options)
    if backend is not None:
        identity_parts.append(f"backend:{backend}")
    return "|".join(identity_parts)


def dataset_identity_digest(
    lance_ds: Any,
    uri: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Return an irreversible digest of the stable dataset identity."""
    if not _has_reliable_identity(lance_ds, storage_options):
        return None
    return hashlib.sha256(
        dataset_identity(lance_ds, uri=uri, storage_options=storage_options).encode(
            "utf-8"
        )
    ).hexdigest()


def parse_source_provenance(name: str) -> tuple[int, Optional[str]] | None:
    """Parse version and dataset identity from a Ray logical source name."""
    prefix = f"Read{LANCE_SOURCE_VERSION_NAME_PREFIX}"
    if not name.startswith(prefix) or not name.endswith("]"):
        return None
    payload = name[len(prefix) : -1]
    version_text, separator, identity_text = payload.partition(LANCE_SOURCE_ID_MARKER)
    if not separator:
        return None
    try:
        version = int(version_text)
    except ValueError:
        return None
    identity = unquote(identity_text)
    if identity == "unavailable":
        return version, None
    return version, identity


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
        self._source_provenance: Optional[tuple[int, Optional[str]]] = None

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

    def _pin_source_provenance(self) -> None:
        if self._source_provenance is None:
            dataset = self.lance_dataset
            self._source_provenance = (
                dataset.version,
                dataset_identity_digest(
                    dataset,
                    storage_options=self._storage_options,
                ),
            )

    @property
    def source_version(self) -> int:
        """Return the Lance version fixed when this datasource is first used."""
        if self._source_provenance is None:
            self._pin_source_provenance()
        provenance = self._source_provenance
        assert provenance is not None
        return provenance[0]

    @property
    def source_identity(self) -> Optional[str]:
        """Return the dataset identity fixed when this datasource is first used."""
        if self._source_provenance is None:
            self._pin_source_provenance()
        provenance = self._source_provenance
        assert provenance is not None
        return provenance[1]

    def pin_source_version(self) -> None:
        """Resolve the source snapshot before Ray starts lazy execution."""
        self._pin_source_provenance()

    def get_name(self) -> str:
        """Return a logical source name carrying immutable snapshot provenance."""
        identity = self.source_identity
        identity_text = "unavailable" if identity is None else quote(identity, safe="")
        return (
            f"{LANCE_SOURCE_VERSION_NAME_PREFIX}{self.source_version}"
            f"{LANCE_SOURCE_ID_MARKER}{identity_text}]"
        )

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
        dataset_version = self.source_version
        dataset_storage_options = self._get_storage_options()
        serialized_manifest = self._get_serialized_manifest()
        namespace_impl = self._namespace_impl
        namespace_properties = self._namespace_properties
        table_id = self._table_id
        base_store_params = self._base_store_params

        metadata_accepts_schema = (
            "schema" in inspect.signature(BlockMetadata.__init__).parameters
        )
        block_schema: Optional[pa.Schema] = None
        if metadata_accepts_schema:
            schema_options = self._scanner_options.copy()
            if self._with_metadata:
                schema_options["with_row_address"] = True
            block_schema = self.lance_dataset.scanner(**schema_options).projected_schema
            if self._with_metadata and "_fragid" not in block_schema.names:
                block_schema = block_schema.append(pa.field("_fragid", pa.uint64()))
            if not self._with_metadata:
                block_schema = pa.schema(
                    field
                    for field in block_schema
                    if field.name not in {"_rowaddr", "_fragid"}
                )

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
            input_files: tuple[str, ...] = tuple(
                data_file.path
                for fragment in fragments
                for data_file in fragment.data_files()
            )

            # Ray 2.48+ no longer has the schema argument...
            if metadata_accepts_schema:
                assert block_schema is not None
                metadata = BlockMetadata(
                    num_rows=num_rows,
                    schema=block_schema,  # type: ignore[call-arg]
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

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.fragments:
            return 0

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
