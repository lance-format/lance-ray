"""Shared helpers for tests."""

import inspect

import pyarrow as pa

import pandas as pd


def missing_fragment_write_options(*options: str) -> tuple[str, ...]:
    from lance.fragment import write_fragments

    params = inspect.signature(write_fragments).parameters
    return tuple(sorted(set(options).difference(params)))


def fragment_write_options_skip_reason(*options: str) -> str:
    missing = missing_fragment_write_options(*options)
    return (
        "Installed pylance does not expose the missing fragment write "
        "option(s) on lance.fragment.write_fragments: "
        f"{', '.join(missing)}"
    )


def to_numpy_backed(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Arrow-backed columns of ``df`` back to numpy dtypes.

    Ray 2.56 made ``Dataset.to_pandas()`` produce ``pd.ArrowDtype`` columns
    (``DataContext.enable_arrow_backed_pandas_conversion``), which report
    different dtypes and surface nulls as ``pd.NA`` rather than ``None``.
    Frames from older Ray are returned unchanged.
    """
    if not any(isinstance(dtype, pd.ArrowDtype) for dtype in df.dtypes):
        return df
    # ``ignore_metadata`` stops ``to_pandas`` restoring the Arrow-backed dtypes
    # that ``from_pandas`` recorded in the schema metadata.
    return pa.Table.from_pandas(df, preserve_index=False).to_pandas(
        ignore_metadata=True
    )
