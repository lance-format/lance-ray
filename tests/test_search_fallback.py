# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Unit tests for the flat fallback vector-distance helpers in search.py."""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa
import pytest
from lance_ray.search import (
    _compute_vector_distances,
    _fixed_size_list_to_matrix,
    _vector_column_to_numpy,
)


def _fixed_size_list_chunked(rows: list[list[float]], dim: int) -> pa.ChunkedArray[Any]:
    return pa.chunked_array([pa.array(rows, type=pa.list_(pa.float32(), dim))])


def test_fixed_size_list_fast_path_matches_generic() -> None:
    rows = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
    col = _fixed_size_list_chunked(rows, 2)

    matrix = _vector_column_to_numpy(col)

    assert matrix.dtype == np.float32
    np.testing.assert_array_equal(matrix, np.asarray(rows, dtype=np.float32))
    # The fast path must actually engage for fixed-size-list input.
    assert _fixed_size_list_to_matrix(col.combine_chunks(), np.float32) is not None


@pytest.mark.parametrize("value_type", [pa.float16(), pa.float64(), pa.uint8()])
def test_fast_path_on_sliced_array(value_type: pa.DataType) -> None:
    rows = np.arange(8).reshape(4, 2)
    values = pa.array(rows.ravel().astype(value_type.to_pandas_dtype()))
    array = pa.FixedSizeListArray.from_arrays(values, 2).slice(1, 2)

    matrix = _fixed_size_list_to_matrix(array, np.float32)

    assert matrix is not None
    np.testing.assert_array_equal(matrix, rows[1:3].astype(np.float32))


def test_fast_path_declines_null_elements() -> None:
    array = pa.array([[1.0, None], [3.0, 4.0]], type=pa.list_(pa.float32(), 2))
    assert _fixed_size_list_to_matrix(array, np.float32) is None


def test_generic_path_for_variable_list() -> None:
    rows = [[1.0, 2.0], [3.0, 4.0]]
    col = pa.chunked_array([pa.array(rows, type=pa.list_(pa.float32()))])

    # Not fixed-size: fast path declines, generic path still produces the matrix.
    assert _fixed_size_list_to_matrix(col.combine_chunks(), np.float32) is None
    np.testing.assert_array_equal(
        _vector_column_to_numpy(col), np.asarray(rows, dtype=np.float32)
    )


def test_null_vectors_raise() -> None:
    col = pa.chunked_array(
        [pa.array([[1.0, 2.0], None], type=pa.list_(pa.float32(), 2))]
    )
    with pytest.raises(ValueError, match="null vectors"):
        _vector_column_to_numpy(col)


def test_empty_column_returns_empty_matrix() -> None:
    col = pa.chunked_array([pa.array([], type=pa.list_(pa.float32(), 2))])
    assert _vector_column_to_numpy(col).shape == (0, 0)


def test_l2_distance_parity() -> None:
    rows = [[0.0, 0.0], [3.0, 4.0]]
    col = _fixed_size_list_chunked(rows, 2)
    dists = _compute_vector_distances(col, [0.0, 0.0], "l2")
    # Lance (and this fallback) returns squared L2: 3**2 + 4**2 == 25.
    np.testing.assert_allclose(dists, [0.0, 25.0], rtol=1e-6)
