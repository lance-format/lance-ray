import os
import sys
from collections.abc import Iterable, Sequence
from functools import lru_cache
from typing import Any, Optional, TypeVar

T = TypeVar("T")

# Cache size for namespace clients per worker, configurable via environment variable
_NAMESPACE_CACHE_SIZE = int(os.environ.get("LANCE_RAY_NAMESPACE_CACHE_SIZE", "16"))

_PYLANCE_5 = (5, 0, 0)


def normalize_initial_bases(
    initial_bases: Optional[list[Any]],
) -> Optional[list[dict[str, Any]]]:
    """Convert Lance DatasetBasePath objects into Ray-serializable specs.

    If a base path does not specify an ``id`` (or uses the pylance
    default of 0), a unique id is auto-assigned starting from 1
    (id 0 is reserved for the dataset root).  This mirrors the
    auto-assignment that pylance's Rust layer performs inside
    ``write_dataset`` / ``write_fragments(return_transaction=True)``,
    which lance-ray bypasses by splitting write and commit into
    separate steps.
    """
    if not initial_bases:
        return None

    specs: list[dict[str, Any]] = []
    next_auto_id = 1
    for base in initial_bases:
        raw_id = base.get("id", 0) if isinstance(base, dict) else base.id
        is_root = (
            base.get("is_dataset_root", False)
            if isinstance(base, dict)
            else base.is_dataset_root
        )
        if is_root:
            assigned_id = 0
        elif raw_id not in (0, None):
            assigned_id = raw_id
        else:
            assigned_id = next_auto_id
            next_auto_id += 1
        specs.append(
            {
                "path": base["path"] if isinstance(base, dict) else base.path,
                "name": base.get("name") if isinstance(base, dict) else base.name,
                "is_dataset_root": is_root,
                "id": assigned_id,
            }
        )

    seen_ids: set[int] = set()
    for spec in specs:
        if spec["id"] in seen_ids:
            raise ValueError(
                f"Duplicate base path ID {spec['id']} detected. "
                "Base path IDs must be unique."
            )
        seen_ids.add(spec["id"])

    return specs


def materialize_initial_bases(
    initial_bases: Optional[list[dict[str, Any]]],
) -> Optional[list[Any]]:
    """Rebuild Lance DatasetBasePath objects from serializable specs."""
    if not initial_bases:
        return None

    from lance import DatasetBasePath

    bases = []
    for base in initial_bases:
        if not isinstance(base, dict):
            raise TypeError("initial_bases must be normalized before materialization")
        if "id" not in base:
            raise ValueError(
                "Normalized base spec is missing 'id'. "
                "Ensure normalize_initial_bases() is called first."
            )
        bases.append(
            DatasetBasePath(
                base["path"],
                name=base.get("name"),
                is_dataset_root=base.get("is_dataset_root", False),
                id=base["id"],
            )
        )
    return bases


@lru_cache(maxsize=1)
def _pylance_version() -> tuple[int, ...]:
    """Return the installed pylance version as a comparable tuple."""
    import lance
    from packaging.version import parse

    v = parse(lance.__version__)
    return (v.major, v.minor, v.micro)


def has_namespace_params(
    namespace_impl: Optional[str],
    table_id: Optional[list[str]],
) -> bool:
    """Check if namespace parameters are provided.

    Only namespace_impl and table_id are required; namespace_properties can be None.

    Args:
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        table_id: The table identifier as a list of strings.

    Returns:
        True if both namespace_impl and table_id are provided, False otherwise.
    """
    return namespace_impl is not None and table_id is not None


def validate_uri_or_namespace(
    uri: Optional[str],
    namespace_impl: Optional[str],
    table_id: Optional[list[str]],
) -> None:
    """Validate that either uri OR (namespace_impl + table_id) is provided.

    Args:
        uri: The URI of the dataset.
        namespace_impl: The namespace implementation type.
        table_id: The table identifier.

    Raises:
        ValueError: If both uri and namespace params are provided, or neither.
    """
    has_ns = has_namespace_params(namespace_impl, table_id)

    if uri is not None and has_ns:
        raise ValueError(
            "Cannot provide both 'uri' and namespace parameters. "
            "Use either 'uri' OR ('namespace_impl' + 'table_id')."
        )

    if uri is None and not has_ns:
        raise ValueError(
            "Must provide either 'uri' OR ('namespace_impl' + 'table_id')."
        )


@lru_cache(maxsize=_NAMESPACE_CACHE_SIZE)
def _get_cached_namespace(
    namespace_impl: str,
    namespace_properties_tuple: Optional[tuple[tuple[str, str], ...]],
) -> Any:
    """Internal cached namespace loader. Use get_or_create_namespace() instead."""
    import lance_namespace as ln

    namespace_properties = (
        dict(namespace_properties_tuple) if namespace_properties_tuple else {}
    )
    return ln.connect(namespace_impl, namespace_properties)


def get_or_create_namespace(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
) -> Any | None:
    """Get or create a cached namespace client.

    This function loads a namespace client from cache or creates a new one.
    The namespace client is cached per-worker using lru_cache. Module-level state
    persists across task invocations within the same Ray worker process, avoiding
    redundant network calls to recreate namespace connections.

    Args:
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace (can be None).

    Returns:
        A namespace client instance, or None if namespace_impl is not provided.
    """
    if namespace_impl is None:
        return None

    # Convert dict to hashable tuple for lru_cache (None if no properties)
    namespace_properties_tuple = (
        tuple(sorted(namespace_properties.items())) if namespace_properties else None
    )
    return _get_cached_namespace(namespace_impl, namespace_properties_tuple)


def _create_storage_options_provider(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> Any | None:
    """Create a LanceNamespaceStorageOptionsProvider (pylance 4.x only)."""
    if not has_namespace_params(namespace_impl, table_id):
        return None

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return None

    import lance

    if not hasattr(lance, "LanceNamespaceStorageOptionsProvider"):
        return None

    return lance.LanceNamespaceStorageOptionsProvider(
        namespace=namespace, table_id=table_id
    )


def get_namespace_kwargs(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> dict[str, Any]:
    """Return kwargs for pylance namespace / auth integration.

    Handles API differences between pylance versions:
    - pylance 4.x: ``namespace``, ``table_id``, ``storage_options_provider``
    - pylance 5.0+: ``namespace_client``, ``table_id``
    """
    if not has_namespace_params(namespace_impl, table_id):
        return {}

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return {}

    kwargs: dict[str, Any] = {"table_id": table_id}

    if _pylance_version() >= _PYLANCE_5:
        kwargs["namespace_client"] = namespace
    else:
        kwargs["namespace"] = namespace
        provider = _create_storage_options_provider(
            namespace_impl, namespace_properties, table_id
        )
        if provider is not None:
            kwargs["storage_options_provider"] = provider

    return kwargs


def get_write_fragments_kwargs(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> dict[str, Any]:
    """Return kwargs for ``lance.fragment.write_fragments``.

    Handles API differences between pylance versions:
    - pylance 4.x: ``storage_options_provider``
    - pylance 5.0+: ``namespace_client``, ``table_id``
    """
    if not has_namespace_params(namespace_impl, table_id):
        return {}

    if _pylance_version() >= _PYLANCE_5:
        namespace = get_or_create_namespace(namespace_impl, namespace_properties)
        if namespace is None:
            return {}
        return {"namespace_client": namespace, "table_id": table_id}

    provider = _create_storage_options_provider(
        namespace_impl, namespace_properties, table_id
    )
    if provider is None:
        return {}
    return {"storage_options_provider": provider}


if sys.version_info >= (3, 12):
    from itertools import batched

    def array_split(iterable: Iterable[T], n: int) -> list[Sequence[T]]:
        """Split iterable into n chunks."""
        items = list(iterable)
        chunk_size = (len(items) + n - 1) // n
        return list(batched(items, chunk_size))
else:
    from more_itertools import divide

    def array_split(iterable: Iterable[T], n: int) -> list[Sequence[T]]:
        return list(map(list, divide(n, iterable)))
