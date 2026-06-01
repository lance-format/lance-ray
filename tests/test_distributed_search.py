from types import SimpleNamespace

import pyarrow as pa
import pytest
from lance_ray import pool as pool_mod
from lance_ray import search as search_mod
from lance_ray.search import (
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


def test_select_vector_index_raises_for_missing_explicit_index_name(monkeypatch):
    dataset = object()
    index = _index_with_segments(("S1", [1, 2]))

    monkeypatch.setattr(
        "lance_ray.search._get_index_descriptions",
        lambda _: [index],
    )

    with pytest.raises(ValueError, match="missing_idx.*vector_idx"):
        _select_vector_index(
            dataset,
            column="vector",
            index_name="missing_idx",
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

    monkeypatch.setattr("lance_ray.search.LanceDataset", FakeDataset)

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
        dataset_uri="dataset",
        dataset_version=1,
        storage_options=None,
        block_size=None,
        namespace_impl=None,
        namespace_properties=None,
        table_id=None,
        base_scanner_options={"fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
        candidate_k=1,
        analyze_plan=False,
    )

    assert result.num_rows == 1
    assert "fragments" not in scanner_options
    assert scanner_options["index_segments"] == ["S1"]
    assert scanner_options["fast_search"] is True


def test_execute_fallback_vector_search_plan_computes_local_top_k(monkeypatch):
    scanner_options = {}
    vectors = pa.FixedSizeListArray.from_arrays(
        pa.array([10.0, 0.0, 1.0, 0.0, 0.0, 2.0], type=pa.float32()),
        2,
    )

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            return f"fragment-{fragment_id}"

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return SimpleNamespace(
                to_table=lambda: pa.table({"id": [1, 2, 3], "vector": vectors})
            )

    monkeypatch.setattr("lance_ray.search.LanceDataset", FakeDataset)

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        dataset_uri="dataset",
        dataset_version=1,
        storage_options=None,
        block_size=None,
        namespace_impl=None,
        namespace_properties=None,
        table_id=None,
        base_scanner_options={"columns": ["id", "_distance"], "fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 2},
        candidate_k=2,
        analyze_plan=False,
    )

    assert "nearest" not in scanner_options
    assert scanner_options["fragments"] == ["fragment-7"]
    assert scanner_options["columns"] == ["id", "vector"]
    assert result.column("id").to_pylist() == [2, 3]
    assert result.column("_distance").to_pylist() == [1.0, 2.0]
    assert "vector" not in result.column_names


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

    monkeypatch.setattr("lance_ray.search.LanceDataset", FakeDataset)

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        dataset_uri="dataset",
        dataset_version=1,
        storage_options=None,
        block_size=None,
        namespace_impl=None,
        namespace_properties=None,
        table_id=None,
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

    monkeypatch.setattr("lance_ray.search.LanceDataset", FakeDataset)

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        dataset_uri="dataset",
        dataset_version=1,
        storage_options=None,
        block_size=None,
        namespace_impl=None,
        namespace_properties=None,
        table_id=None,
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
    left = pa.table({"id": [1, 2], "_distance": [0.4, 0.1]})
    right = pa.table({"id": [3, 4], "_distance": [0.2, 0.3]})

    result = _merge_vector_search_results([left, right], k=3)

    assert result.column("id").to_pylist() == [2, 3, 4]
    assert result.column("_distance").to_pylist() == [0.1, 0.2, 0.3]


def test_merge_vector_search_results_requires_distance():
    table = pa.table({"id": [1, 2]})

    with pytest.raises(RuntimeError, match="_distance"):
        _merge_vector_search_results([table], k=1)


def test_search_scanner_options_reject_managed_options():
    with pytest.raises(ValueError, match="nearest"):
        _validate_search_scanner_options({"nearest": {"column": "vector"}})


def test_search_scanner_options_reject_fast_search_override():
    with pytest.raises(ValueError, match="fast_search"):
        _validate_search_scanner_options({"fast_search": True})


def test_vector_search_reuses_global_pool(monkeypatch):
    events = []

    class FakeAsyncResult:
        def get(self):
            events.append("get")
            return [pa.table({"id": [1], "_distance": [0.1]})]

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

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        result = search_mod.vector_search(
            uri="dataset",
            nearest={"column": "vector", "q": [0.0], "k": 1},
            num_workers=4,
        )
    finally:
        pool_mod.clear_global_pool()

    assert result.column("id").to_pylist() == [1]
    assert events == [
        ("map_async", [plan], 1),
        "get",
    ]
