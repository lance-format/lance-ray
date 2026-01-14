import sys
from collections.abc import Iterable, Sequence
from typing import Optional, TypeVar

T = TypeVar("T")


def create_storage_options_provider(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
):
    """Create a LanceNamespaceStorageOptionsProvider if namespace parameters are provided.

    This function reconstructs a namespace connection and creates a storage options
    provider for credential refresh in distributed workers. Workers receive serializable
    namespace_impl/properties/table_id instead of the non-serializable namespace object.

    Args:
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace.
        table_id: The table identifier as a list of strings.

    Returns:
        LanceNamespaceStorageOptionsProvider if all parameters are provided, None otherwise.
    """
    if namespace_impl is None or namespace_properties is None or table_id is None:
        return None

    import lance_namespace as ln
    from lance import LanceNamespaceStorageOptionsProvider

    namespace = ln.connect(namespace_impl, namespace_properties)
    return LanceNamespaceStorageOptionsProvider(namespace=namespace, table_id=table_id)


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
