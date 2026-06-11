"""
Lance-Ray: Ray integration for Lance columnar format.

This package provides integration between Ray and Lance for distributed
columnar data processing.
"""

__version__ = "0.4.2"
__author__ = "LanceDB Devs"
__email__ = "dev@lancedb.com"
from .cleanup import cleanup_database_old_versions, cleanup_old_versions
from .compaction import compact_database, compact_files

# Main imports
from .datasink import LanceFragmentCommitter

# Fragment API imports
from .fragment import LanceFragmentWriter
from .index import create_index, create_scalar_index, optimize_indices
from .io import (
    add_columns,
    add_columns_from,
    merge_columns_from,
    read_lance,
    write_lance,
)
from .pool import clear_global_pool, get_global_pool, init_global_pool, set_global_pool
from .search import vector_search

__all__ = [
    "read_lance",
    "write_lance",
    "vector_search",
    "init_global_pool",
    "set_global_pool",
    "get_global_pool",
    "clear_global_pool",
    "add_columns",
    "add_columns_from",
    "merge_columns_from",
    "create_scalar_index",
    "create_index",
    "optimize_indices",
    "compact_files",
    "compact_database",
    "cleanup_old_versions",
    "cleanup_database_old_versions",
    "LanceFragmentWriter",
    "LanceFragmentCommitter",
]
