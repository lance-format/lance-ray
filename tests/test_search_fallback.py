# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Unit tests for the flat fallback vector-distance helpers in search.py."""

import numpy as np
import pyarrow as pa
import pytest
from lance_ray.search import (
    _compute_vector_distances,
    _fixed_size_list_to_matrix,
    _vector_column_to_numpy,
)


def _fixed_size_list_chunked(rows, dim):
    return pa.chunked_array([pa.array(rows, type=pa.list_(pa.float32(), dim))])


def test_fixed_size_list_fast_path_matches_generic():
    rows = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
    col = _fixed_size_list_chunked(rows, 2)

    matrix = _vector_column_to_numpy(col)

    assert matrix.dtype == np.float32
    np.testing.assert_array_equal(matrix, np.asarray(rows, dtype=np.float32))
    # The fast path must actually engage for fixed-size-list input.
    assert _fixed_size_list_to_matrix(col.combine_chunks()) is not None


def test_generic_path_for_variable_list():
    rows = [[1.0, 2.0], [3.0, 4.0]]
    col = pa.chunked_array([pa.array(rows, type=pa.list_(pa.float32()))])

    # Not fixed-size: fast path declines, generic path still produces the matrix.
    assert _fixed_size_list_to_matrix(col.combine_chunks()) is None
    np.testing.assert_array_equal(
        _vector_column_to_numpy(col), np.asarray(rows, dtype=np.float32)
    )


def test_null_vectors_raise():
    col = pa.chunked_array(
        [pa.array([[1.0, 2.0], None], type=pa.list_(pa.float32(), 2))]
    )
    with pytest.raises(ValueError, match="null vectors"):
        _vector_column_to_numpy(col)


def test_empty_column_returns_empty_matrix():
    col = pa.chunked_array([pa.array([], type=pa.list_(pa.float32(), 2))])
    assert _vector_column_to_numpy(col).shape == (0, 0)


def test_l2_distance_parity():
    rows = [[0.0, 0.0], [3.0, 4.0]]
    col = _fixed_size_list_chunked(rows, 2)
    dists = _compute_vector_distances(col, [0.0, 0.0], "l2")
    np.testing.assert_allclose(dists, [0.0, 5.0], rtol=1e-6)
