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

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    LanceFragment is expensive.
    """
    fragments = [lance_ds.get_fragment(id) for id in fragment_ids]
    scanner_options["fragments"] = fragments
    scanner = lance_ds.scanner(**scanner_options)
    for batch in scanner.to_reader():
        yield pa.Table.from_batches([batch])
