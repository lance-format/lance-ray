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


def _mock_pickled_dataset(monkeypatch, dataset):
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()
    pickled_dataset = f"pickled-dataset-{id(dataset)}".encode()

    def fake_loads(value):
        assert value == pickled_dataset
        return dataset

    monkeypatch.setattr(search_mod.pickle, "loads", fake_loads)
    return pickled_dataset


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

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
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
    monkeypatch.setattr(search_mod.pickle, "dumps", lambda dataset: b"pickled-dataset")
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: False)
    search_mod._clear_vector_search_plan_cache()

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        result = search_mod.vector_search(
            uri="dataset",
            nearest={"column": "vector", "q": [0.0], "k": 1},
            num_workers=4,
        )
    finally:
        pool_mod.clear_global_pool()
        search_mod._clear_vector_search_plan_cache()

    assert result.column("id").to_pylist() == [1]
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
    monkeypatch.setattr(search_mod.pickle, "dumps", fake_dumps)
    monkeypatch.setattr(search_mod.pickle, "loads", fake_loads)
    monkeypatch.setattr(search_mod.ray, "ObjectRef", FakeObjectRef, raising=False)
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(search_mod.ray, "put", fake_put)
    monkeypatch.setattr(search_mod.ray, "get", fake_get)
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()
    search_mod._clear_vector_search_plan_cache()

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
        search_mod._clear_vector_search_plan_cache()

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
                "nearest": {"column": "vector", "q": [0.0], "k": 1},
                "index_segments": ["S1"],
            },
        ),
        (
            "scanner",
            {
                "fast_search": True,
                "nearest": {"column": "vector", "q": [0.0], "k": 1},
                "index_segments": ["S2"],
            },
        ),
        "get",
    ]


def test_vector_search_reuses_driver_search_plan_cache(monkeypatch):
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

    index = _index_with_segments(("S1", [1]))

    class FakeDataset:
        uri = "dataset"
        version = 11
        schema = FakeSchema()

        def __init__(self, *args, **kwargs):
            events.append(("LanceDataset", args, kwargs))

        def describe_indices(self):
            return [index]

        def get_fragments(self):
            events.append("get_fragments")
            return [_FakeFragment(1)]

    plan = _SearchPlan(fragment_ids=[1], index_segments=["S1"])

    def fake_plan_vector_search(**kwargs):
        events.append(
            (
                "plan",
                len(kwargs["fragments"]),
                kwargs["vector_index"] is index,
                kwargs["num_workers"],
                kwargs["include_unindexed"],
            )
        )
        return [plan]

    monkeypatch.setattr(search_mod, "LanceDataset", FakeDataset)
    monkeypatch.setattr(search_mod, "_plan_vector_search", fake_plan_vector_search)
    monkeypatch.setattr(search_mod.pickle, "dumps", lambda dataset: b"pickled-dataset")
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: False)
    search_mod._clear_vector_search_plan_cache()

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        for _ in range(2):
            result = search_mod.vector_search(
                uri="dataset",
                nearest={"column": "vector", "q": [0.0], "k": 1},
                num_workers=4,
            )
            assert result.column("id").to_pylist() == [1]
    finally:
        pool_mod.clear_global_pool()
        search_mod._clear_vector_search_plan_cache()

    assert events.count("get_fragments") == 1
    assert events.count(("plan", 1, True, 4, True)) == 1
    assert events.count(("map_async", [plan], 1)) == 2
