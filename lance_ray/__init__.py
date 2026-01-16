"""
Lance-Ray: Ray integration for Lance columnar format.

This package provides integration between Ray and Lance for distributed
columnar data processing.
"""

__version__ = "0.2.0-beta.1"
__author__ = "LanceDB Devs"
__email__ = "dev@lancedb.com"
from .compaction import compact_files

# Main imports
from .datasink import LanceFragmentCommitter

# Fragment API imports
from .fragment import LanceFragmentWriter
from .index import create_index, create_scalar_index
from .io import add_columns, read_lance, write_lance

__all__ = [
    "read_lance",
    "write_lance",
    "add_columns",
    "create_scalar_index",
    "create_index",
    "compact_files",
    "LanceFragmentWriter",
    "LanceFragmentCommitter",
]
