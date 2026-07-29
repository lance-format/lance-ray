from types import SimpleNamespace

import lance
import lance_ray as lr
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from lance_ray import pool as pool_mod
from lance_ray import search as search_mod
from lance_ray.search import (
    _apply_distance_range,
    _apply_index_metric_default,
    _compute_vector_distances,
    _execute_vector_search_plan,
    _format_analyze_plan_results,
    _merge_vector_search_results,
    _plan_vector_search,
    _SearchPlan,
    _SearchPlanAnalysis,
    _select_vector_index,
    _validate_search_scanner_options,
)


class _FakeFragment:
    def __init__(self, fragment_id: int, rows: int = 1):
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self):
        return self._rows


def _index_with_segments(*segments):
    return SimpleNamespace(
        name="vector_idx",
        field_names=["vector"],
        index_type="IVF_PQ",
        segments=[
            SimpleNamespace(uuid=uuid, fragment_ids=set(fragment_ids))
            for uuid, fragment_ids in segments
        ],
    )


def _mock_pickled_dataset(monkeypatch, dataset):
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()
    pickled_dataset = f"pickled-dataset-{id(dataset)}".encode()

    def fake_loads(value):
        assert value == pickled_dataset
        return dataset

    monkeypatch.setattr(search_mod.pickle, "loads", fake_loads)
    return pickled_dataset


def _vector_table(vectors, ids=None, *, value_type=None):
    matrix = np.asarray(vectors)
    value_type = value_type or pa.float32()
    vector_array = pa.FixedSizeListArray.from_arrays(
        pa.array(matrix.reshape(-1), type=value_type),
        matrix.shape[1],
    )
    return pa.table(
        {
            "id": range(len(matrix)) if ids is None else ids,
            "vector": vector_array,
        }
    )


class _FallbackDataset:
    def __init__(self, table, scanner_options=None):
        self.table = table
        self.scanner_options = scanner_options

    def get_fragment(self, fragment_id):
        return f"fragment-{fragment_id}"

    def scanner(self, **kwargs):
        if self.scanner_options is not None:
            self.scanner_options.update(kwargs)
        return SimpleNamespace(to_table=lambda: self.table)


def _create_partial_index_dataset(
    path,
    indexed_vectors,
    appended_vectors,
    *,
    metric="l2",
):
    dataset = lance.write_dataset(
        _vector_table(indexed_vectors),
        path,
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
        metric=metric,
    )
    lance.write_dataset(
        _vector_table(
            appended_vectors,
            ids=range(
                len(indexed_vectors), len(indexed_vectors) + len(appended_vectors)
            ),
        ),
        path,
        mode="append",
    )
    return lance.dataset(path)


def test_select_vector_index_raises_for_missing_explicit_index_name():
    index = _index_with_segments(("S1", [1, 2]))
    dataset = SimpleNamespace(describe_indices=lambda: [index])

    with pytest.raises(ValueError, match="missing_idx.*vector_idx"):
        _select_vector_index(
            dataset,
            column="vector",
            index_name="missing_idx",
        )


def test_select_vector_index_matches_canonical_lance_field_path():
    index = SimpleNamespace(
        name="hyphen_idx",
        field_names=["`meta-data`.`user-id`"],
        index_type="IVF_PQ",
        segments=[],
    )
    dataset = SimpleNamespace(describe_indices=lambda: [index])

    assert (
        _select_vector_index(
            dataset,
            column="`meta-data`.`user-id`",
            index_name=None,
        )
        is index
    )


def test_plan_vector_search_keeps_segment_fragments_together():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 6)]
    index = _index_with_segments(
        ("S1", [1, 2]),
        ("S2", [3]),
        ("S3", [4, 5]),
    )

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=True,
    )

    segment_fragments = {
        segment: set(plan.fragment_ids)
        for plan in plans
        for segment in plan.index_segments
    }

    assert segment_fragments == {
        "S1": {1, 2},
        "S2": {3},
        "S3": {4, 5},
    }


def test_plan_vector_search_adds_unindexed_fragments_as_fallback():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=True,
    )

    fallback_fragments = {
        fragment_id
        for plan in plans
        if not plan.index_segments
        for fragment_id in plan.fragment_ids
    }

    assert fallback_fragments == {3, 4}
    assert all(
        not (plan.index_segments and fallback_fragments.intersection(plan.fragment_ids))
        for plan in plans
    )


def test_plan_vector_search_does_not_mix_indexed_and_fallback_units():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=1,
        include_unindexed=True,
    )

    assert plans == [
        _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
        _SearchPlan(fragment_ids=[3, 4], index_segments=[]),
    ]


def test_plan_vector_search_can_skip_unindexed_fragments():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=False,
    )

    assert plans == [_SearchPlan(fragment_ids=[1, 2], index_segments=["S1"])]


def test_plan_vector_search_without_index_uses_flat_fallback():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 4)]

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=None,
        num_workers=2,
        include_unindexed=True,
    )

    assert {fragment_id for plan in plans for fragment_id in plan.fragment_ids} == {
        1,
        2,
        3,
    }
    assert all(not plan.index_segments for plan in plans)


def test_execute_indexed_vector_search_plan_does_not_pass_fragments(monkeypatch):
    scanner_options = {}

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            raise AssertionError(f"unexpected fragment lookup: {fragment_id}")

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return SimpleNamespace(
                to_table=lambda: pa.table({"id": [1], "_distance": [0.1]})
            )

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
        candidate_k=1,
        analyze_plan=False,
    )

    assert result.num_rows == 1
    assert "fragments" not in scanner_options
    assert scanner_options["index_segments"] == ["S1"]
    assert scanner_options["fast_search"] is True


def test_execute_indexed_vector_search_plan_without_index_segments_support(monkeypatch):
    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def scanner(self, columns=None):
            return SimpleNamespace(to_table=lambda: pa.table({"id": [1]}))

    with pytest.raises(RuntimeError, match="index_segments"):
        _execute_vector_search_plan(
            _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
            pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
            base_scanner_options={"columns": ["id"], "fast_search": True},
            nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
            candidate_k=1,
            analyze_plan=False,
        )


def test_execute_fallback_vector_search_plan_computes_local_top_k(monkeypatch):
    scanner_options = {}
    dataset = _FallbackDataset(
        _vector_table([[10.0, 0.0], [1.0, 0.0], [0.0, 2.0]], ids=[1, 2, 3]),
        scanner_options,
    )

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, dataset),
        base_scanner_options={"columns": ["id", "_distance"], "fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 2},
        candidate_k=2,
        analyze_plan=False,
    )

    assert "nearest" not in scanner_options
    assert scanner_options["fragments"] == ["fragment-7"]
    assert scanner_options["columns"] == ["id", "vector"]
    assert result.column("id").to_pylist() == [2, 3]
    assert result.column("_distance").to_pylist() == [1.0, 4.0]
    assert "vector" not in result.column_names


def test_execute_batch_hamming_fallback_uses_bit_distance(monkeypatch):
    conversion_calls = 0
    dataset = _FallbackDataset(_vector_table([[0], [3], [255]], value_type=pa.uint8()))
    original_vector_column_to_numpy = search_mod._vector_column_to_numpy

    def count_vector_column_conversion(vector_column, metric):
        nonlocal conversion_calls
        conversion_calls += 1
        return original_vector_column_to_numpy(vector_column, metric)

    monkeypatch.setattr(
        search_mod,
        "_vector_column_to_numpy",
        count_vector_column_conversion,
    )

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, dataset),
        base_scanner_options={"columns": ["id", "_distance"], "fast_search": False},
        nearest={
            "column": "vector",
            "q": [[0], [255]],
            "k": 3,
            "metric": "hamming",
        },
        candidate_k=3,
        analyze_plan=False,
        is_batch_query=True,
    )

    assert result.column("query_index").to_pylist() == [0, 0, 0, 1, 1, 1]
    assert result.column("id").to_pylist() == [0, 1, 2, 2, 1, 0]
    assert result.column("_distance").to_pylist() == [0.0, 2.0, 8.0, 0.0, 6.0, 8.0]
    assert conversion_calls == 1


def test_apply_distance_range_uses_inclusive_lower_exclusive_upper():
    table = pa.table({"id": [0, 1, 2], "_distance": [0.5, 1.0, 4.0]})

    result = _apply_distance_range(table, {"distance_range": (0.5, 4.0)})

    assert result.column("id").to_pylist() == [0, 1]
    assert result.column("_distance").to_pylist() == [0.5, 1.0]


@pytest.mark.parametrize(
    ("metric", "vectors", "query", "expected"),
    [
        ("l2", [[0.0, 2.0], [3.0, 4.0]], [0.0, 0.0], [4.0, 25.0]),
        (
            "cosine",
            [[1.0, 0.0], [1.0, 1.0]],
            [1.0, 0.0],
            [0.0, 0.29289323],
        ),
        ("dot", [[1.0, 0.0], [1.0, 1.0]], [1.0, 1.0], [0.0, -1.0]),
        (
            "hamming",
            [[0, 0], [255, 0], [15, 240], [1, 2]],
            [0, 0],
            [0.0, 8.0, 8.0, 2.0],
        ),
    ],
)
def test_fallback_distance_conventions(metric, vectors, query, expected):
    dtype = np.uint8 if metric == "hamming" else np.float32
    distances = _compute_vector_distances(
        np.asarray(vectors, dtype=dtype), query, metric
    )

    assert distances.tolist() == pytest.approx(expected)


def test_execute_fallback_filters_null_and_invalid_cosine_vectors(monkeypatch):
    table = pa.table(
        {
            "id": [0, 1, 2, 3],
            "vector": pa.array(
                [[0.0, 0.0], None, [1.0, 0.0], [0.0, 1.0]],
                type=pa.list_(pa.float32(), 2),
            ),
            "_rowid": [0, 1, 2, 3],
        }
    )
    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(
            monkeypatch,
            _FallbackDataset(table),
        ),
        base_scanner_options={"fast_search": False, "with_row_id": True},
        nearest={
            "column": "vector",
            "q": [[1.0, 0.0], [0.0, 0.0]],
            "k": 4,
            "metric": "cosine",
        },
        candidate_k=4,
        analyze_plan=False,
        is_batch_query=True,
    )

    assert result.column("query_index").to_pylist() == [0, 0]
    assert result.column("id").to_pylist() == [2, 3]
    assert result.column("_distance").to_pylist() == [0.0, 1.0]


def test_execute_indexed_vector_search_plan_can_analyze_plan(monkeypatch):
    scanner_options = {}

    class FakeScanner:
        def analyze_plan(self):
            return "indexed plan"

        def to_table(self):
            raise AssertionError("analyze_plan should not execute to_table")

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            raise AssertionError(f"unexpected fragment lookup: {fragment_id}")

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return FakeScanner()

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
        candidate_k=1,
        analyze_plan=True,
    )

    assert result == _SearchPlanAnalysis(
        plan=_SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        analysis="indexed plan",
    )
    assert "fragments" not in scanner_options
    assert scanner_options["index_segments"] == ["S1"]
    assert scanner_options["fast_search"] is True


def test_execute_fallback_vector_search_plan_can_analyze_plan(monkeypatch):
    scanner_options = {}

    class FakeScanner:
        def analyze_plan(self):
            return "fallback plan"

        def to_table(self):
            raise AssertionError("analyze_plan should not execute to_table")

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            return f"fragment-{fragment_id}"

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return FakeScanner()

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"columns": ["id", "_distance"], "fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 2},
        candidate_k=2,
        analyze_plan=True,
    )

    assert result == _SearchPlanAnalysis(
        plan=_SearchPlan(fragment_ids=[7], index_segments=[]),
        analysis="fallback plan",
    )
    assert "nearest" not in scanner_options
    assert scanner_options["fragments"] == ["fragment-7"]
    assert scanner_options["columns"] == ["id", "vector"]


def test_format_analyze_plan_results():
    result = _format_analyze_plan_results(
        [
            _SearchPlanAnalysis(
                plan=_SearchPlan(fragment_ids=[1], index_segments=["S1"]),
                analysis="indexed plan",
            ),
            _SearchPlanAnalysis(
                plan=_SearchPlan(fragment_ids=[2], index_segments=[]),
                analysis="fallback plan",
            ),
        ]
    )

    assert "shard 0 (indexed)" in result
    assert "index_segments: ['S1']" in result
    assert "indexed plan" in result
    assert "shard 1 (flat_fallback)" in result
    assert "fallback plan" in result


def test_merge_vector_search_results_returns_global_top_k():
    left = pa.table({"id": [1, 2], "_distance": [0.4, 0.1], "_rowid": [40, 20]})
    right = pa.table({"id": [3, 4], "_distance": [0.1, 0.3], "_rowid": [10, 30]})

    result = _merge_vector_search_results([left, right], k=3)

    assert result.column("id").to_pylist() == [3, 2, 4]
    assert result.column("_distance").to_pylist() == [0.1, 0.1, 0.3]


def test_merge_batch_vector_search_results_returns_top_k_per_query():
    left = pa.table(
        {
            "query_index": pa.array([0, 0, 1], type=pa.int32()),
            "id": [1, 2, 3],
            "_distance": [0.4, 0.1, 0.2],
            "_rowid": [40, 10, 30],
        }
    )
    right = pa.table(
        {
            "query_index": pa.array([0, 1, 1], type=pa.int32()),
            "id": [4, 5, 6],
            "_distance": [0.2, 0.3, 0.1],
            "_rowid": [20, 50, 60],
        }
    )

    result = _merge_vector_search_results(
        [left, right],
        k=2,
        is_batch_query=True,
    )

    assert result.column("query_index").to_pylist() == [0, 0, 1, 1]
    assert result.column("id").to_pylist() == [2, 4, 6, 3]
    assert result.column("_distance").to_pylist() == [0.1, 0.2, 0.1, 0.2]


@pytest.mark.parametrize(
    ("table", "is_batch_query", "missing_column"),
    [
        (pa.table({"id": [1, 2]}), False, "_distance"),
        (pa.table({"id": [1], "_distance": [0.1]}), True, "query_index"),
    ],
)
def test_merge_vector_search_results_requires_managed_columns(
    table,
    is_batch_query,
    missing_column,
):
    with pytest.raises(RuntimeError, match=missing_column):
        _merge_vector_search_results(
            [table],
            k=1,
            is_batch_query=is_batch_query,
        )


@pytest.mark.parametrize(
    "scanner_options",
    [
        {"nearest": {"column": "vector"}},
        {"fast_search": True},
    ],
    ids=["nearest", "fast_search"],
)
def test_search_scanner_options_reject_managed_options(scanner_options):
    managed_option = next(iter(scanner_options))
    with pytest.raises(ValueError, match=managed_option):
        _validate_search_scanner_options(scanner_options)


def test_batch_vector_search_reuses_global_pool_in_one_round(monkeypatch):
    events = []

    class FakeAsyncResult:
        def get(self):
            events.append("get")
            return [
                pa.table(
                    {
                        "query_index": pa.array([0, 1], type=pa.int32()),
                        "id": [1, 2],
                        "_distance": [0.1, 0.2],
                    }
                )
            ]

    class FakeGlobalPool:
        def map_async(self, func, plans, chunksize):
            events.append(("map_async", plans, chunksize))
            return FakeAsyncResult()

        def close(self):
            events.append("close")

        def join(self):
            events.append("join")

    class FakeSchema:
        def field(self, column):
            return column

    class FakeDataset:
        uri = "dataset"
        version = 1
        schema = FakeSchema()

        def __init__(self, *args, **kwargs):
            pass

        def get_fragments(self):
            return [_FakeFragment(1)]

    plan = _SearchPlan(fragment_ids=[1], index_segments=["S1"])
    monkeypatch.setattr(search_mod, "LanceDataset", FakeDataset)
    monkeypatch.setattr(
        search_mod,
        "_select_vector_index",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(search_mod, "_plan_vector_search", lambda **kwargs: [plan])
    monkeypatch.setattr(
        search_mod,
        "_inspect_vector_search_query",
        lambda *args, **kwargs: (
            True,
            pa.schema(
                [
                    pa.field("query_index", pa.int32(), nullable=False),
                    pa.field("id", pa.int64()),
                    pa.field("_distance", pa.float32()),
                ]
            ),
        ),
    )
    monkeypatch.setattr(search_mod.pickle, "dumps", lambda dataset: b"pickled-dataset")
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: False)

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        result = search_mod.vector_search(
            uri="dataset",
            nearest={"column": "vector", "q": [[0.0], [1.0]], "k": 1},
            num_workers=4,
        )
    finally:
        pool_mod.clear_global_pool()

    assert result.column("query_index").to_pylist() == [0, 1]
    assert result.column("id").to_pylist() == [1, 2]
    assert events == [
        ("map_async", [plan], 1),
        "get",
    ]


def test_vector_search_puts_pickled_dataset_in_ray_object_store(monkeypatch):
    events = []

    class FakeObjectRef:
        def __init__(self, value):
            self.value = value

    class FakeAsyncResult:
        def __init__(self, results):
            self._results = results

        def get(self):
            events.append("get")
            return self._results

    class FakeGlobalPool:
        def map_async(self, func, plans, chunksize):
            events.append(("map_async", plans, chunksize))
            return FakeAsyncResult([func(plan) for plan in plans])

        def close(self):
            events.append("close")

        def join(self):
            events.append("join")

    class FakeSchema:
        def field(self, column):
            return column

    class FakeDataset:
        schema = FakeSchema()

        def get_fragments(self):
            return [_FakeFragment(1), _FakeFragment(2)]

        def scanner(self, **kwargs):
            events.append(("scanner", kwargs))
            return SimpleNamespace(
                to_table=lambda: pa.table({"id": [1], "_distance": [0.1]})
            )

    fake_dataset = FakeDataset()
    pickled_dataset = b"pickled-dataset"

    def fake_lance_dataset(*args, **kwargs):
        events.append(("LanceDataset", args, kwargs))
        return fake_dataset

    def fake_dumps(dataset):
        events.append(("pickle.dumps", dataset is fake_dataset))
        return pickled_dataset

    def fake_put(value):
        events.append(("ray.put", value))
        return FakeObjectRef(value)

    def fake_get(ref):
        events.append(("ray.get", ref.value))
        return ref.value

    def fake_loads(value):
        events.append(("pickle.loads", value))
        assert value == pickled_dataset
        return fake_dataset

    plans = [
        _SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        _SearchPlan(fragment_ids=[2], index_segments=["S2"]),
    ]

    monkeypatch.setattr(search_mod, "LanceDataset", fake_lance_dataset)
    monkeypatch.setattr(
        search_mod,
        "_select_vector_index",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(search_mod, "_plan_vector_search", lambda **kwargs: plans)
    monkeypatch.setattr(
        search_mod,
        "_inspect_vector_search_query",
        lambda *args, **kwargs: (
            False,
            pa.schema(
                [pa.field("id", pa.int64()), pa.field("_distance", pa.float32())]
            ),
        ),
    )
    monkeypatch.setattr(search_mod.pickle, "dumps", fake_dumps)
    monkeypatch.setattr(search_mod.pickle, "loads", fake_loads)
    monkeypatch.setattr(search_mod.ray, "ObjectRef", FakeObjectRef, raising=False)
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(search_mod.ray, "put", fake_put)
    monkeypatch.setattr(search_mod.ray, "get", fake_get)
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        result = search_mod.vector_search(
            uri="dataset",
            nearest={"column": "vector", "q": [0.0], "k": 1},
            num_workers=4,
        )
    finally:
        pool_mod.clear_global_pool()
        search_mod._load_pickled_dataset.cache_clear()
        search_mod._load_pickled_dataset_ref.cache_clear()

    assert result.column("id").to_pylist() == [1]
    assert events == [
        ("LanceDataset", ("dataset",), {"storage_options": {}}),
        ("pickle.dumps", True),
        ("ray.put", pickled_dataset),
        ("map_async", plans, 1),
        ("ray.get", pickled_dataset),
        ("pickle.loads", pickled_dataset),
        (
            "scanner",
            {
                "fast_search": True,
                "with_row_id": True,
                "nearest": {"column": "vector", "q": [0.0], "k": 1},
                "index_segments": ["S1"],
            },
        ),
        (
            "scanner",
            {
                "fast_search": True,
                "with_row_id": True,
                "nearest": {"column": "vector", "q": [0.0], "k": 1},
                "index_segments": ["S2"],
            },
        ),
        "get",
    ]


def test_batch_vector_search_without_index_matches_single_queries(tmp_path):
    vectors = np.asarray(
        [
            [0.0, 0.0],
            [1.0, 0.0],
            [0.0, 2.0],
            [3.0, 0.0],
            [0.0, 4.0],
            [5.0, 0.0],
        ],
        dtype=np.float32,
    )
    dataset = lance.write_dataset(
        _vector_table(vectors),
        tmp_path / "batch-flat.lance",
        max_rows_per_file=2,
    )
    queries = np.asarray([[0.0, 0.0], [0.0, 4.0]], dtype=np.float32)

    batch = lr.vector_search(
        dataset,
        nearest={"column": "vector", "q": queries, "k": 2},
        scanner_options={"columns": ["id", "_rowid"]},
        num_workers=2,
    )

    assert batch.column_names == ["query_index", "id", "_distance", "_rowid"]
    assert batch.column("query_index").to_pylist() == [0, 0, 1, 1]
    assert batch.num_rows == len(queries) * 2
    for query_index, query in enumerate(queries):
        single = lr.vector_search(
            dataset,
            nearest={"column": "vector", "q": query, "k": 2},
            scanner_options={"columns": ["id", "_rowid"]},
            num_workers=2,
        )
        batch_slice = batch.filter(pc.field("query_index") == query_index).drop_columns(
            ["query_index"]
        )
        assert batch_slice.column("id").to_pylist() == single.column("id").to_pylist()
        assert batch_slice.column("_distance").to_pylist() == pytest.approx(
            single.column("_distance").to_pylist()
        )
        assert (
            batch_slice.column("_rowid").to_pylist()
            == single.column("_rowid").to_pylist()
        )

    empty = lr.vector_search(
        dataset,
        nearest={"column": "vector", "q": queries, "k": 2},
        columns=["id"],
        num_workers=2,
        fast_search=True,
    )
    assert empty.num_rows == 0
    assert empty.column_names == ["query_index", "id", "_distance"]
    assert empty.schema.field("query_index").type == pa.int32()
    assert not empty.schema.field("query_index").nullable

    virtual_projection = lr.vector_search(
        dataset,
        nearest={"column": "vector", "q": queries, "k": 1},
        columns=["query_index"],
        num_workers=2,
    )
    assert virtual_projection.column_names == ["query_index", "_distance"]
    assert virtual_projection.column("query_index").to_pylist() == [0, 1]


def test_batch_vector_search_rejects_dataset_query_index_column(tmp_path):
    vectors = _vector_table([[0.0, 0.0], [1.0, 0.0]])["vector"]
    dataset = lance.write_dataset(
        pa.table({"query_index": [7, 8], "vector": vectors}),
        tmp_path / "batch-query-index.lance",
    )

    with pytest.raises(ValueError, match="column 'query_index'"):
        lr.vector_search(
            dataset,
            nearest={"column": "vector", "q": [[0.0, 0.0]], "k": 1},
        )


def test_multivector_fallback_reports_unsupported_boundary(tmp_path):
    vector_type = pa.list_(pa.list_(pa.float32(), 2))
    dataset = lance.write_dataset(
        pa.table(
            {
                "id": [0, 1],
                "vector": pa.array(
                    [
                        [[1.0, 0.0], [0.0, 1.0]],
                        [[-1.0, 0.0], [0.0, -1.0]],
                    ],
                    type=vector_type,
                ),
            }
        ),
        tmp_path / "multivector-flat.lance",
    )

    with pytest.raises(ValueError, match="does not support multivector"):
        lr.vector_search(
            dataset,
            nearest={
                "column": "vector",
                "q": [[1.0, 0.0], [0.0, 1.0]],
                "k": 1,
            },
        )


def test_batch_vector_search_with_partial_index_preserves_per_query_top_k(tmp_path):
    indexed_vectors = np.asarray(
        [[0.0, 0.0], [1.0, 0.0], [0.0, 2.0], [3.0, 0.0]],
        dtype=np.float32,
    )
    appended_vectors = np.asarray([[0.0, 4.0], [5.0, 0.0]], dtype=np.float32)
    dataset = _create_partial_index_dataset(
        tmp_path / "batch-partial.lance",
        indexed_vectors,
        appended_vectors,
    )
    queries = np.asarray([[0.0, 4.0], [3.0, 0.0]], dtype=np.float32)

    batch = lr.vector_search(
        dataset,
        nearest={"column": "vector", "q": queries, "k": 2},
        index_name="vector_idx",
        columns=["id", "_rowid"],
        num_workers=2,
    )

    assert batch.column_names == ["query_index", "id", "_distance", "_rowid"]
    assert batch.column("query_index").to_pylist() == [0, 0, 1, 1]
    assert batch.column("id").to_pylist() == [4, 2, 3, 1]
    assert batch.column("_distance").to_pylist() == [0.0, 4.0, 0.0, 4.0]

    ranged_batch = lr.vector_search(
        dataset,
        nearest={
            "column": "vector",
            "q": queries,
            "k": 2,
            "distance_range": (0.5, 10.0),
        },
        index_name="vector_idx",
        columns=["id"],
        num_workers=2,
    )
    assert ranged_batch.column("query_index").to_pylist() == [0, 1, 1]
    assert ranged_batch.column("id").to_pylist() == [2, 1, 5]
    assert ranged_batch.column("_distance").to_pylist() == [4.0, 4.0, 4.0]


@pytest.mark.parametrize("metric", ["COSINE", "DOT"])
def test_apply_index_metric_default(metric):
    index = SimpleNamespace(details={"metric_type": metric})
    nearest = {"column": "vector", "q": [[1.0, 0.0]], "k": 2}

    assert _apply_index_metric_default(nearest, index)["metric"] == metric.lower()
    assert (
        _apply_index_metric_default({**nearest, "metric": "l2"}, index)["metric"]
        == "l2"
    )


def test_partial_index_uses_index_metric_by_default(tmp_path):
    indexed_vectors = np.asarray(
        [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [-1.0, 0.0]],
        dtype=np.float32,
    )
    appended_vectors = np.asarray([[0.5, 0.5], [-1.0, -1.0]], dtype=np.float32)
    dataset = _create_partial_index_dataset(
        tmp_path / "batch-partial-cosine.lance",
        indexed_vectors,
        appended_vectors,
        metric="cosine",
    )
    queries = np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)

    result = lr.vector_search(
        dataset,
        nearest={"column": "vector", "q": queries, "k": 3},
        index_name="vector_idx",
        columns=["id"],
        num_workers=2,
    )

    assert result.column("query_index").to_pylist() == [0, 0, 0, 1, 1, 1]
    assert result.column("id").to_pylist() == [0, 1, 4, 2, 1, 4]
    assert result.column("_distance").to_pylist() == pytest.approx(
        [0.0, 0.29289323, 0.29289323, 0.0, 0.29289323, 0.29289323]
    )
