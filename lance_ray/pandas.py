# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Utility functions for lance-ray."""

from typing import TYPE_CHECKING, Optional, Union

import pyarrow as pa

if TYPE_CHECKING:
    import pandas as pd


def pd_to_arrow(
    df: Union[pa.Table, "pd.DataFrame", dict], schema: Optional[pa.Schema]
) -> pa.Table:
    """Convert a pandas DataFrame to pyarrow Table."""
    from lance.dependencies import _PANDAS_AVAILABLE
    from lance.dependencies import pandas as pd

    if isinstance(df, dict):
        return pa.Table.from_pydict(df, schema=schema)
    elif _PANDAS_AVAILABLE and isinstance(df, pd.DataFrame):
        tbl = pa.Table.from_pandas(df, schema=schema)
        new_schema = schema
        if schema is None or schema.metadata is None:
            new_schema = tbl.schema.remove_metadata()
        elif schema.metadata is not None:
            new_schema = tbl.schema.with_metadata(schema.metadata)
        new_table = tbl.replace_schema_metadata(new_schema.metadata)
        return new_table
    elif isinstance(df, pa.Table) and df.num_rows > 0 and schema is not None:
        # Align columns to the target schema by NAME before casting. A bare
        # df.cast(schema) requires the table's field names to already match the
        # schema's names in order and otherwise raises ValueError. Selecting by
        # name first reorders/projects the columns so the cast lines the right
        # data up with each field.
        if df.schema.names != schema.names:
            df = df.select(schema.names)
        return df.cast(schema)
    return df
