# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import pickle
import warnings
from collections.abc import Callable, Generator, Iterable
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)

import pyarrow as pa
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from lance.fragment import FragmentMetadata

    import pandas as pd

__all__ = [
    "LanceFragmentWriter",
    "write_fragment",
]

from .pandas import pd_to_arrow


def write_fragment(
    stream: Iterable[Union[pa.Table, "pd.DataFrame"]],
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    max_rows_per_group: int = 1024,  # Only useful for v1 writer.
    data_storage_version: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    retry_params: Optional[dict[str, Any]] = None,
) -> list[tuple["FragmentMetadata", pa.Schema]]:
    from lance.dependencies import _PANDAS_AVAILABLE
    from lance.dependencies import pandas as pd
    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

    if schema is None:
        try:
            first = next(iter(stream))
        except StopIteration:
            return []

        if _PANDAS_AVAILABLE and isinstance(first, pd.DataFrame):
            schema = pa.Schema.from_pandas(first).remove_metadata()
        elif isinstance(first, dict):
            tbl = pa.Table.from_pydict(first)
            schema = tbl.schema.remove_metadata()
        else:
            schema = first.schema
        if len(schema.names) == 0:
            # Empty table.
            schema = None

        stream = chain([first], stream)

    if schema is None:
        return []

    def record_batch_converter():
        for block in stream:
            tbl = pd_to_arrow(block, schema)
            yield from tbl.to_batches()

    max_bytes_per_file = (
        DEFAULT_MAX_BYTES_PER_FILE if max_bytes_per_file is None else max_bytes_per_file
    )

    reader = pa.RecordBatchReader.from_batches(schema, record_batch_converter())

    # Use default retry params if not provided
    if retry_params is None:
        retry_params = {
            "description": "write lance fragments",
            "match": [],
            "max_attempts": 1,
            "max_backoff_s": 0,
        }

    fragments = call_with_retry(
        lambda: write_fragments(
            reader,
            uri,
            schema=schema,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            max_bytes_per_file=max_bytes_per_file,
            data_storage_version=data_storage_version,
            storage_options=storage_options,
        ),
        **retry_params,
    )
    return [(fragment, schema) for fragment in fragments]


class LanceFragmentWriter:
    """Write a fragment to one of Lance fragment.

    This Writer can be used in case to write large-than-memory data to lance,
    in distributed fashion.

    Parameters
    ----------
    uri : str
        The base URI of the dataset.

        For namespace-based tables, resolve the URI first before distributing the writes:
        - namespace.describe_table(DescribeTableRequest(id=table_id)) to get existing table
        - namespace.create_empty_table(CreateEmptyTableRequest(id=table_id)) to create new table

        Then use the returned location as the uri. This ensures all distributed workers
        write to the same resolved location.
    transform : Callable[[pa.Table], Union[pa.Table, Generator]], optional
        A callable to transform the input batch. Default is None.
    schema : pyarrow.Schema, optional
        The schema of the dataset.
    max_rows_per_file : int, optional
        The maximum number of rows per file. Default is 1024 * 1024.
    max_bytes_per_file : int, optional
        The maximum number of bytes per file. Default is 90GB.
    max_rows_per_group : int, optional
        The maximum number of rows per group. Default is 1024.
        Only useful for v1 writer.
    data_storage_version: optional, str, default None
        The version of the data storage format to use. Newer versions are more
        efficient but require newer versions of lance to read.  The default
        (None) will use the 2.0 version.  See the user guide for more details.
    use_legacy_format : optional, bool, default None
        Deprecated method for setting the data storage version. Use the
        `data_storage_version` parameter instead.
    storage_options : Dict[str, Any], optional
        The storage options for the writer. Default is None.
    retry_params : Dict[str, Any], optional
        Retry parameters for write operations. Default is None.
        If provided, should contain keys like 'description', 'match',
        'max_attempts', and 'max_backoff_s'.

    """

    def __init__(
        self,
        uri: str,
        *,
        transform: Optional[Callable[[pa.Table], pa.Table | Generator]] = None,
        schema: Optional[pa.Schema] = None,
        max_rows_per_file: int = 1024 * 1024,
        max_bytes_per_file: Optional[int] = None,
        max_rows_per_group: Optional[int] = None,  # Only useful for v1 writer.
        data_storage_version: Optional[str] = None,
        use_legacy_format: Optional[bool] = False,
        storage_options: Optional[dict[str, Any]] = None,
        retry_params: Optional[dict[str, Any]] = None,
    ):
        if use_legacy_format is not None:
            warnings.warn(
                "The `use_legacy_format` parameter is deprecated. Use the "
                "`data_storage_version` parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )

            data_storage_version = "legacy" if use_legacy_format else "stable"

        self.uri = uri
        self.schema = schema
        self.transform = transform if transform is not None else lambda x: x

        self.max_rows_per_group = max_rows_per_group
        self.max_rows_per_file = max_rows_per_file
        self.max_bytes_per_file = max_bytes_per_file
        self.data_storage_version = data_storage_version
        self.storage_options = storage_options
        self.retry_params = retry_params

    def __call__(self, batch: Union[pa.Table, "pd.DataFrame", dict]) -> pa.Table:
        """Write a Batch to the Lance fragment."""
        # Convert dict/numpy arrays to pyarrow table if needed
        if isinstance(batch, dict):
            batch = pa.Table.from_pydict(batch)
        elif hasattr(batch, "__dataframe__"):  # pandas DataFrame
            batch = pa.Table.from_pandas(batch)

        transformed = self.transform(batch)
        if not isinstance(transformed, Generator):
            transformed = (t for t in [transformed])

        fragments = write_fragment(
            transformed,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            max_rows_per_group=self.max_rows_per_group,
            max_bytes_per_file=self.max_bytes_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            retry_params=self.retry_params,
        )
        return pa.Table.from_pydict(
            {
                "fragment": [pickle.dumps(fragment) for fragment, _ in fragments],
                "schema": [pickle.dumps(schema) for _, schema in fragments],
            }
        )
