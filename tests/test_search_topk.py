# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Unit tests for the top-k selection / merge helpers in search.py."""

import pyarrow as pa
from lance_ray.search import _merge_vector_search_results, _take_top_k


def _table(distances, ids):
    # Integer-valued floats are exactly representable in float32.
    return pa.table({"_distance": pa.array(distances, type=pa.float32()), "id": ids})


def test_take_top_k_selects_smallest_ascending():
    table = _table([5.0, 1.0, 9.0, 3.0], [1, 2, 3, 4])
    top2 = _take_top_k(table, 2)
    assert top2.column("_distance").to_pylist() == [1.0, 3.0]
    assert top2.column("id").to_pylist() == [2, 4]


def test_take_top_k_k_larger_than_rows_returns_all_sorted():
    table = _table([5.0, 1.0], [1, 2])
    top = _take_top_k(table, 10)
    assert top.num_rows == 2
    assert top.column("_distance").to_pylist() == [1.0, 5.0]


def test_merge_vector_search_results_global_topk():
    t1 = _table([5.0, 9.0], [1, 3])
    t2 = _table([1.0, 3.0], [2, 4])
    merged = _merge_vector_search_results([t1, t2], 3)
    assert merged.column("_distance").to_pylist() == [1.0, 3.0, 5.0]
    assert merged.column("id").to_pylist() == [2, 4, 1]
