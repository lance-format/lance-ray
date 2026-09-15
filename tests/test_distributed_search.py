import asyncio
import pickle
import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import lance
import lance_ray as lr
import numpy as np
import pyarrow as pa
import pytest
import ray
from lance.dataset import LanceDataset
from lance_ray import search as search_mod
from lance_ray.search import (
    VectorSearchActorOptions,
    _apply_distance_range,
    _canonical_multivector_batch,
    _canonical_query_batch,
    _execute_vector_search_plan,
    _format_analyze_plan_results,
    _merge_vector_search_results,
    _plan_streaming_vector_search,
    _plan_vector_search,
    _SearchPlan,
    _SearchPlanAnalysis,
    _select_vector_index,
    _validate_search_scanner_options,
)


class _FakeFragment:
    def __init__(self, fragment_id: int, rows: int = 1) -> None:
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self) -> int:
        return self._rows


def _index_with_segments(*segments: Any) -> SimpleNamespace:
    return SimpleNamespace(
        name="vector_idx",
        field_names=["vector"],
        index_type="IVF_PQ",
        segments=[
            SimpleNamespace(uuid=uuid, fragment_ids=set(fragment_ids))
            for uuid, fragment_ids in segments
        ],
    )


def _mock_pickled_dataset(monkeypatch: pytest.MonkeyPatch, dataset: Any) -> bytes:
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()
    pickled_dataset = f"pickled-dataset-{id(dataset)}".encode()

    def fake_loads(value: Any) -> Any:
        assert value == pickled_dataset
        return dataset

    monkeypatch.setattr(pickle, "loads", fake_loads)
    return pickled_dataset


def _vector_table(vectors: Any, ids: Any = None, *, value_type: Any = None) -> pa.Table:
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
    def __init__(
        self,
        table: pa.Table,
        scanner_options: dict[str, Any] | None = None,
    ) -> None:
        self.table = table
        self.scanner_options = scanner_options

    def get_fragment(self, fragment_id: int) -> str:
        return f"fragment-{fragment_id}"

    def scanner(self, **kwargs: Any) -> SimpleNamespace:
        if self.scanner_options is not None:
            self.scanner_options.update(kwargs)
        return SimpleNamespace(to_table=lambda: self.table)


def _create_partial_index_dataset(
    path: Any,
    indexed_vectors: Any,
    appended_vectors: Any,
    *,
    metric: str = "l2",
) -> lance.LanceDataset:
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


def test_select_vector_index_raises_for_missing_explicit_index_name() -> None:
    index = _index_with_segments(("S1", [1, 2]))
    dataset: Any = SimpleNamespace(describe_indices=lambda: [index])

    with pytest.raises(ValueError, match="missing_idx.*vector_idx"):
        _select_vector_index(
            dataset,
            column="vector",
            index_name="missing_idx",
        )


def test_select_vector_index_matches_canonical_lance_field_path() -> None:
    index = SimpleNamespace(
        name="hyphen_idx",
        field_names=["`meta-data`.`user-id`"],
        index_type="IVF_PQ",
        segments=[],
    )
    dataset: Any = SimpleNamespace(describe_indices=lambda: [index])

    assert (
        _select_vector_index(
            dataset,
            column="`meta-data`.`user-id`",
            index_name=None,
        )
        is index
    )


def test_plan_vector_search_keeps_segment_fragments_together() -> None:
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


def test_plan_vector_search_adds_unindexed_fragments_as_fallback() -> None:
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


def test_plan_vector_search_does_not_mix_indexed_and_fallback_units() -> None:
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


def test_plan_vector_search_can_skip_unindexed_fragments() -> None:
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=False,
    )

    assert plans == [_SearchPlan(fragment_ids=[1, 2], index_segments=["S1"])]


def test_plan_vector_search_without_index_uses_flat_fallback() -> None:
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


def test_execute_indexed_vector_search_plan_does_not_pass_fragments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanner_options: dict[str, Any] = {}

    class FakeDataset:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def get_fragment(self, fragment_id: int) -> None:
            raise AssertionError(f"unexpected fragment lookup: {fragment_id}")

        def scanner(self, **kwargs: Any) -> SimpleNamespace:
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

    assert isinstance(result, pa.Table)
    assert result.num_rows == 1
    assert "fragments" not in scanner_options
    assert scanner_options["index_segments"] == ["S1"]
    assert scanner_options["fast_search"] is True


def test_execute_indexed_vector_search_plan_without_index_segments_support(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeDataset:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def scanner(self, columns: Any = None) -> SimpleNamespace:
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


def test_execute_fallback_vector_search_plan_computes_local_top_k(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanner_options: dict[str, Any] = {}
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
    assert isinstance(result, pa.Table)
    assert result.column("id").to_pylist() == [2, 3]
    assert result.column("_distance").to_pylist() == [1.0, 4.0]
    assert "vector" not in result.column_names


def test_execute_indexed_vector_search_plan_can_analyze_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanner_options: dict[str, Any] = {}

    class FakeScanner:
        def analyze_plan(self) -> str:
            return "indexed plan"

        def to_table(self) -> None:
            raise AssertionError("analyze_plan should not execute to_table")

    class FakeDataset:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def get_fragment(self, fragment_id: int) -> None:
            raise AssertionError(f"unexpected fragment lookup: {fragment_id}")

        def scanner(self, **kwargs: Any) -> "FakeScanner":
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


def test_execute_fallback_vector_search_plan_can_analyze_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanner_options: dict[str, Any] = {}

    class FakeScanner:
        def analyze_plan(self) -> str:
            return "fallback plan"

        def to_table(self) -> None:
            raise AssertionError("analyze_plan should not execute to_table")

    class FakeDataset:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def get_fragment(self, fragment_id: int) -> str:
            return f"fragment-{fragment_id}"

        def scanner(self, **kwargs: Any) -> "FakeScanner":
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


def test_format_analyze_plan_results() -> None:
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


def test_merge_vector_search_results_returns_global_top_k() -> None:
    left = pa.table({"id": [1, 2], "_distance": [0.4, 0.1]})
    right = pa.table({"id": [3, 4], "_distance": [0.2, 0.3]})

    result = _merge_vector_search_results([left, right], k=3)

    assert result.column("id").to_pylist() == [2, 3, 4]
    assert result.column("_distance").to_pylist() == [0.1, 0.2, 0.3]


def test_merge_vector_search_results_requires_distance() -> None:
    table = pa.table({"id": [1, 2]})

    with pytest.raises(RuntimeError, match="_distance"):
        _merge_vector_search_results([table], k=1)


def test_merge_vector_search_results_can_merge_per_query() -> None:
    left = pa.table(
        {
            "query_index": [0, 0, 1],
            "id": [1, 2, 3],
            "_distance": [0.4, 0.1, 0.3],
        }
    )
    right = pa.table(
        {
            "query_index": [0, 1, 1],
            "id": [4, 5, 6],
            "_distance": [0.2, 0.4, 0.1],
        }
    )

    result = _merge_vector_search_results([left, right], k=2, per_query=True)

    assert result["query_index"].to_pylist() == [0, 0, 1, 1]
    assert result["id"].to_pylist() == [2, 4, 6, 3]


def test_search_scanner_options_reject_managed_options() -> None:
    with pytest.raises(ValueError, match="nearest"):
        _validate_search_scanner_options({"nearest": {"column": "vector"}})


def test_search_scanner_options_reject_fast_search_override() -> None:
    with pytest.raises(ValueError, match="fast_search"):
        _validate_search_scanner_options({"fast_search": True})


def test_streaming_option_defaults() -> None:
    assert VectorSearchActorOptions() == VectorSearchActorOptions(
        num_actors=4,
        ray_remote_args=None,
        index_cache_size_bytes=None,
        metadata_cache_size_bytes=None,
        prewarm_index=False,
    )
    assert VectorSearchActorOptions(
        index_cache_size_bytes=0,
        metadata_cache_size_bytes=0,
    )


def test_streaming_distance_range_is_lower_inclusive_upper_exclusive() -> None:
    table = pa.table({"id": [0, 1, 2], "_distance": [0.5, 1.0, 4.0]})

    result = _apply_distance_range(table, {"distance_range": (0.5, 4.0)})

    assert result["id"].to_pylist() == [0, 1]


def test_open_vector_search_rejects_dataset_query_index_column(tmp_path: Path) -> None:
    table = _vector_table([[0.0, 0.0], [1.0, 0.0]])
    table = table.append_column("query_index", pa.array([1, 2], type=pa.int32()))
    dataset = lance.write_dataset(table, tmp_path / "query-index.lance")

    with pytest.raises(ValueError, match="containing column 'query_index'"):
        lr.open_vector_search(
            dataset,
            column="vector",
        )


def test_streaming_query_batches_are_canonicalized_by_column_type() -> None:
    source = np.arange(12, dtype=np.float32).reshape(3, 4)[:, ::-1]
    regular = _canonical_query_batch(source, "l2")

    assert regular.flags.c_contiguous
    assert regular.flags.owndata
    assert not np.shares_memory(regular, source)

    multivector = _canonical_multivector_batch(
        [
            np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            np.asarray([[1.0, 1.0]], dtype=np.float32),
        ],
        "cosine",
    )
    assert [query.shape for query in multivector] == [(2, 2), (1, 2)]
    assert all(query.flags.c_contiguous for query in multivector)


def test_streaming_planner_balances_indexed_and_fallback_units() -> None:
    fragments = [
        _FakeFragment(1, 100),
        _FakeFragment(2, 90),
        _FakeFragment(3, 80),
    ]
    plans = _plan_streaming_vector_search(
        fragments=fragments,
        vector_index=_index_with_segments(("S1", [1]), ("S2", [2])),
        num_actors=2,
    )

    assert len(plans) == 2
    assert {segment for plan in plans for segment in plan.index_segments} == {
        "S1",
        "S2",
    }
    assert {
        fragment_id for plan in plans for fragment_id in plan.fallback_fragment_ids
    } == {3}
    assert any(plan.index_segments and plan.fallback_fragment_ids for plan in plans)


def test_streaming_fallback_preserves_global_query_indices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset = lance.write_dataset(
        _vector_table(
            [
                [0.0, 0.0],
                [1.0, 0.0],
                [0.0, 2.0],
                [3.0, 0.0],
                [0.0, 4.0],
                [5.0, 0.0],
            ]
        ),
        tmp_path / "streaming-flat.lance",
        max_rows_per_file=2,
    )
    resolve_schema = search_mod._resolve_vector_search_schema
    schema_calls = 0

    def count_schema_resolution(*args: Any, **kwargs: Any) -> pa.Schema:
        nonlocal schema_calls
        schema_calls += 1
        return resolve_schema(*args, **kwargs)

    monkeypatch.setattr(
        search_mod,
        "_resolve_vector_search_schema",
        count_schema_resolution,
    )

    with lr.open_vector_search(
        dataset,
        column="vector",
        actor_options=VectorSearchActorOptions(num_actors=2),
        max_concurrent_requests=2,
    ) as session:
        results = list(
            session.map_batches(
                [
                    np.asarray([[0.0, 0.0], [0.0, 4.0]], dtype=np.float32),
                    np.asarray([[3.0, 0.0]], dtype=np.float32),
                ],
                nearest={"k": 2},
                columns=["id"],
            )
        )

    assert [result["query_index"].to_pylist() for result in results] == [
        [0, 0, 1, 1],
        [2, 2],
    ]
    assert results[0]["query_index"].type == pa.int64()
    assert results[0]["id"].to_pylist() == [0, 1, 4, 2]
    assert results[1]["id"].to_pylist() == [3, 1]
    assert schema_calls == 2


def test_streaming_fast_search_without_index_returns_empty_result(
    tmp_path: Path,
) -> None:
    dataset = lance.write_dataset(
        _vector_table([[0.0, 0.0], [1.0, 0.0]]),
        tmp_path / "streaming-fast.lance",
    )

    with lr.open_vector_search(dataset, column="vector") as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 0.0], [1.0, 0.0]], dtype=np.float32)],
                nearest={"k": 1},
                columns=["id"],
                fast_search=True,
            )
        )

    assert result.num_rows == 0
    assert result.column_names == ["query_index", "id", "_distance"]
    assert result["query_index"].type == pa.int64()


def test_streaming_partial_index_merges_fallback_results(tmp_path: Path) -> None:
    path = tmp_path / "streaming-partial.lance"
    dataset = lance.write_dataset(
        _vector_table([[0.0, 0.0], [1.0, 0.0], [0.0, 2.0], [3.0, 0.0]]),
        path,
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
    )
    lance.write_dataset(
        _vector_table([[0.0, 4.0], [5.0, 0.0]], ids=[4, 5]),
        path,
        mode="append",
    )

    with lr.open_vector_search(
        lance.dataset(path),
        column="vector",
        index_name="vector_idx",
        actor_options=VectorSearchActorOptions(num_actors=2),
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 4.0], [3.0, 0.0]], dtype=np.float32)],
                nearest={"k": 2, "nprobes": 1},
                columns=["id"],
            )
        )

    assert result["query_index"].to_pylist() == [0, 0, 1, 1]
    assert result["id"].to_pylist() == [4, 2, 3, 1]


def test_streaming_fallback_inherits_legacy_index_metric(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "streaming-partial-cosine.lance"
    dataset = lance.write_dataset(
        _vector_table([[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [-1.0, 0.0]]),
        path,
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
        metric="cosine",
    )
    lance.write_dataset(
        _vector_table([[0.5, 0.5], [-1.0, -1.0]], ids=[4, 5]),
        path,
        mode="append",
    )

    describe = LanceDataset.describe_indices

    def describe_legacy(self: Any) -> list[Any]:
        return [
            SimpleNamespace(
                name=search_mod._index_value(index, "name"),
                field_names=search_mod._index_value(index, "field_names"),
                index_type=search_mod._index_value(index, "index_type"),
                segments=search_mod._index_value(index, "segments"),
                details={},
            )
            for index in describe(self)
        ]

    monkeypatch.setattr(LanceDataset, "describe_indices", describe_legacy)

    with lr.open_vector_search(
        lance.dataset(path),
        column="vector",
        index_name="vector_idx",
        actor_options=VectorSearchActorOptions(num_actors=2),
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)],
                nearest={"k": 3, "nprobes": 1},
                columns=["id"],
            )
        )

    assert result["query_index"].to_pylist() == [0, 0, 0, 1, 1, 1]
    assert result["id"].to_pylist() == [0, 1, 4, 2, 1, 4]
    assert result["_distance"].to_pylist() == pytest.approx(
        [0.0, 0.29289323, 0.29289323, 0.0, 0.29289323, 0.29289323]
    )


def test_streaming_can_prewarm_owned_index_segments(tmp_path: Path) -> None:
    dataset = lance.write_dataset(
        _vector_table([[0.0, 0.0], [1.0, 0.0], [0.0, 2.0], [3.0, 0.0]]),
        tmp_path / "streaming-prewarm.lance",
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
    )

    with lr.open_vector_search(
        lance.dataset(dataset.uri),
        column="vector",
        index_name="vector_idx",
        actor_options=VectorSearchActorOptions(
            num_actors=1,
            prewarm_index=True,
        ),
    ) as session:
        assert session.prewarm_results[0]["skipped"] is False
        assert session.prewarm_results[0]["index_segments"] == 1
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 0.0]], dtype=np.float32)],
                nearest={"k": 1, "nprobes": 1},
                columns=["id"],
            )
        )

    assert result["id"].to_pylist() == [0]


def test_streaming_inherits_checked_out_dataset_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "streaming-branch.lance"
    dataset = lance.write_dataset(_vector_table([[0.0, 0.0]], ids=[0]), path)
    branch_dataset = dataset.create_branch("experiment")
    lance.write_dataset(
        _vector_table([[1.0, 0.0]], ids=[1]),
        path,
        mode="append",
    )

    with lr.open_vector_search(
        branch_dataset,
        column="vector",
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 0.0]], dtype=np.float32)],
                nearest={"k": 2},
                columns=["id"],
            )
        )

    assert result["id"].to_pylist() == [0]
    assert session.actor_states[0]["version"] == branch_dataset.version


def test_streaming_uri_branch_uses_branch_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "streaming-uri-branch.lance"
    dataset = lance.write_dataset(_vector_table([[0.0, 0.0]], ids=[0]), path)
    branch_dataset = dataset.create_branch("experiment")
    branch_dataset = lance.write_dataset(
        _vector_table([[1.0, 0.0]], ids=[1]),
        branch_dataset.uri,
        mode="append",
    )

    assert lance.dataset(path).version < branch_dataset.version

    with lr.open_vector_search(
        str(path),
        column="vector",
        branch="experiment",
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 0.0]], dtype=np.float32)],
                nearest={"k": 2},
                columns=["id"],
            )
        )

    assert result["id"].to_pylist() == [0, 1]
    assert session.actor_states[0]["version"] == branch_dataset.version


def test_streaming_multivector_uses_additive_maxsim_distance(tmp_path: Path) -> None:
    vector_type = pa.list_(pa.list_(pa.float32(), 2))
    dataset = lance.write_dataset(
        pa.table(
            {
                "id": [0, 1, 2],
                "vector": pa.array(
                    [
                        [[1.0, 0.0], [0.0, 1.0]],
                        [[1.0, 0.0]],
                        [[-1.0, 0.0], [0.0, -1.0]],
                    ],
                    type=vector_type,
                ),
            }
        ),
        tmp_path / "streaming-multivector.lance",
    )
    query_batch = pa.array(
        [
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0]],
        ],
        type=vector_type,
    )

    with lr.open_vector_search(
        dataset,
        column="vector",
        metric="cosine",
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches([query_batch], nearest={"k": 2}, columns=["id"])
        )

    assert result["query_index"].to_pylist() == [0, 0, 1, 1]
    assert result["id"].to_pylist() == [0, 1, 0, 1]
    assert result["_distance"].to_pylist() == pytest.approx([0.0, 1.0, 0.0, 0.0])


def test_streaming_multivector_uses_core_indexed_search(tmp_path: Path) -> None:
    vector_type = pa.list_(pa.list_(pa.float32(), 2))
    rows = [
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0]],
        [[-1.0, 0.0], [0.0, -1.0]],
    ] * 20
    dataset = lance.write_dataset(
        pa.table(
            {
                "id": range(len(rows)),
                "vector": pa.array(rows, type=vector_type),
            }
        ),
        tmp_path / "streaming-multivector-indexed.lance",
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="multivector_idx",
        metric="cosine",
    )

    with lr.open_vector_search(
        lance.dataset(dataset.uri),
        column="vector",
        index_name="multivector_idx",
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches(
                [
                    pa.array(
                        [
                            [[1.0, 0.0], [0.0, 1.0]],
                            [[1.0, 0.0]],
                        ],
                        type=vector_type,
                    )
                ],
                nearest={"k": 1, "nprobes": 1},
                columns=["id"],
            )
        )

    assert result["query_index"].to_pylist() == [0, 1]
    assert result["_distance"].to_pylist() == pytest.approx([0.0, 0.0])


def _dataset(path: Path) -> Any:
    return lance.write_dataset(
        pa.table(
            {
                "id": range(6),
                "vector": pa.array(
                    [
                        [0.0, 0.0],
                        [1.0, 0.0],
                        [2.0, 0.0],
                        [3.0, 0.0],
                        [4.0, 0.0],
                        [5.0, 0.0],
                    ],
                    type=pa.list_(pa.float32(), 2),
                ),
            }
        ),
        path,
        max_rows_per_file=2,
    )


def test_one_shot_queries(tmp_path: Path) -> None:
    ds = _dataset(tmp_path / "one-shot.lance")
    single = lr.vector_search(
        ds,
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 2},
        columns=["id"],
        num_workers=2,
    )
    batch = lr.vector_search(
        ds,
        nearest={"column": "vector", "q": [[0.0, 0.0], [5.0, 0.0]], "k": 2},
        columns=["id"],
        num_workers=2,
    )
    assert isinstance(single, pa.Table) and isinstance(batch, pa.Table)
    assert single.column_names == ["id", "_distance"]
    assert single["id"].to_pylist() == [0, 1]
    assert batch["id"].to_pylist() == [0, 1, 5, 4]
    assert batch["query_index"].to_pylist() == [0, 0, 1, 1]


@pytest.mark.asyncio
async def test_request_isolation(
    tmp_path: Path,
) -> None:
    ds = _dataset(tmp_path / "requests.lance")
    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=2,
        actor_options=lr.VectorSearchActorOptions(num_actors=2),
    ) as search:
        actors = list(search._actors)
        left, right = await asyncio.gather(
            search.search_async(
                [[0.0, 0.0], [5.0, 0.0]],
                nearest={"k": 2},
                columns=["id"],
                filter="id < 3",
            ),
            search.search_async(
                [5.0, 0.0], nearest={"k": 1}, columns={"item": "id"}, filter="id >= 3"
            ),
        )
        assert left["query_index"].to_pylist() == [0, 0, 1, 1]
        assert left["id"].to_pylist() == [0, 1, 2, 1]
        assert right.column_names == ["item", "_distance"]
        assert right["item"].to_pylist() == [5]
        empty = await search.search_async(
            [[0.0, 0.0]], nearest={"k": 3}, columns=["id"], filter="id < 0"
        )
        assert empty.num_rows == 0
        assert empty.column_names == ["query_index", "id", "_distance"]
        assert search._actors == actors


def test_prefilter_search_modes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "modes.lance"
    ds = _dataset(path)
    ds.create_index("vector", "IVF_FLAT", num_partitions=1)
    lance.write_dataset(
        pa.table(
            {
                "id": [6],
                "vector": pa.array([[0.1, 0.0]], type=pa.list_(pa.float32(), 2)),
            }
        ),
        path,
        mode="append",
    )
    with lr.open_vector_search(
        lance.dataset(path),
        column="vector",
        actor_options=lr.VectorSearchActorOptions(num_actors=2),
    ) as search:
        for mode in ({}, {"use_index": False}):
            result = search.search(
                [[0.0, 0.0]],
                nearest={"k": 3, "nprobes": 1, **mode},
                filter="id >= 3",
                columns=["id"],
            )
            assert result["id"].to_pylist() == [6, 3, 4]
        indexed = search.search(
            [0.0, 0.0],
            nearest={"k": 3, "refine_factor": 1},
            fast_search=True,
            filter="id >= 3",
            columns=["id"],
        )
        assert indexed["id"].to_pylist() == [3, 4, 5]
        with pytest.raises(ValueError, match="cannot be combined"):
            search.search(
                [0.0, 0.0], nearest={"k": 1, "use_index": False}, fast_search=True
            )


def test_stream_blocked_input(tmp_path: Path) -> None:
    ds = _dataset(tmp_path / "arrivals.lance")
    waiting = threading.Event()
    allow_next = threading.Event()

    def inputs() -> Any:
        yield [[0.0, 0.0]]
        waiting.set()
        assert allow_next.wait(10)
        yield np.empty((0, 2), dtype=np.float32)
        yield [[5.0, 0.0], [2.0, 0.0]]

    with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=2,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        stream = search.map_batches(inputs(), nearest={"k": 1}, columns=["id"])
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(next, stream)
            try:
                assert waiting.wait(5)
                assert first.result(timeout=5)["id"].to_pylist() == [0]
            finally:
                allow_next.set()
        rest = list(stream)
        assert len(rest) == 2 and rest[0].num_rows == 0
        assert rest[1]["query_index"].to_pylist() == [1, 2]
        assert rest[1]["id"].to_pylist() == [5, 2]


def test_empty_result_schema(tmp_path: Path) -> None:
    ds = lance.write_dataset(
        pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "vector": pa.array([], type=pa.list_(pa.float32(), 2)),
            }
        ),
        tmp_path / "empty.lance",
    )
    with lr.open_vector_search(ds, column="vector") as search:
        single = search.search([0.0, 0.0], nearest={"k": 2}, columns=["id"])
        batch = search.search(
            np.empty((0, 2)), nearest={"k": 2}, columns={"item": "id"}
        )
        assert single.column_names == ["id", "_distance"]
        assert batch.column_names == ["query_index", "item", "_distance"]
        assert single.num_rows == batch.num_rows == 0


@pytest.mark.parametrize("columns", [["id", "query_index"], {"query_index": "id"}])
def test_invalid_request(tmp_path: Path, columns: Any) -> None:
    ds = _dataset(tmp_path / "validation.lance")
    with lr.open_vector_search(
        ds, column="vector", actor_options=lr.VectorSearchActorOptions(num_actors=1)
    ) as search:
        with pytest.raises(ValueError, match="query_index is managed"):
            search.search([0.0, 0.0], nearest={"k": 1}, columns=columns)
        with pytest.raises(ValueError, match="must include 'k'"):
            search.search([0.0, 0.0], nearest={})
        with pytest.raises(ValueError, match="prefilter=True"):
            search.search(
                [0.0, 0.0], nearest={"k": 1}, scanner_options={"prefilter": False}
            )
        with pytest.raises(ValueError, match="dimension"):
            search.search([0.0], nearest={"k": 1})
        assert search.search([0.0, 0.0], nearest={"k": 1}, columns=["id"])[
            "id"
        ].to_pylist() == [0]


class _ControlledRef:
    def __init__(self) -> None:
        self.result: Future[Any] = Future()

    def future(self) -> Future[Any]:
        return self.result


class _ControlledActor:
    def __init__(self) -> None:
        self.calls: queue.Queue[Any] = queue.Queue()
        self.killed = False
        self.ready = SimpleNamespace(remote=lambda: ray.put({"version": 1}))
        self.search = SimpleNamespace(remote=self.submit)

    def submit(
        self, query: Any, nearest: Any, k: int, options: Any, fast: bool
    ) -> _ControlledRef:
        ref = _ControlledRef()
        self.calls.put((ray.get(query), ref))
        return ref

    @staticmethod
    def finish(call: Any) -> None:
        query, ref = call
        ref.result.set_result(
            pa.table(
                {
                    "query_index": pa.array(range(len(query)), type=pa.int32()),
                    "id": pa.array([int(item[0]) for item in query], type=pa.int64()),
                    "_distance": pa.array([0.0] * len(query), type=pa.float32()),
                    "_rowid": pa.array([0] * len(query), type=pa.uint64()),
                }
            )
        )


@pytest.fixture
def controlled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    actor = _ControlledActor()
    factory = SimpleNamespace()
    factory.options = lambda **kwargs: factory
    factory.remote = lambda *args: actor
    monkeypatch.setattr(search_mod, "_VectorSearchActor", factory)
    monkeypatch.setattr(
        ray, "kill", lambda *args, **kwargs: setattr(actor, "killed", True)
    )
    ds = _dataset(tmp_path / "controlled.lance")
    return ds, actor


async def _call(actor: _ControlledActor) -> Any:
    return await asyncio.to_thread(actor.calls.get, True, 5)


async def _barrier(search: Any) -> None:
    await asyncio.sleep(0)
    await asyncio.wrap_future(search._schedule(asyncio.sleep(0)))


@pytest.mark.asyncio
async def test_cancel_in_flight(
    controlled: Any,
) -> None:
    ds, actor = controlled
    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=1,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        first = asyncio.create_task(
            search.search_async([0.0, 0.0], nearest={"k": 1}, columns=["id"])
        )
        first_call = await _call(actor)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(
            search.search_async([1.0, 0.0], nearest={"k": 1}, columns=["id"])
        )
        await _barrier(search)
        assert actor.calls.empty()
        actor.finish(first_call)
        second_call = await _call(actor)
        actor.finish(second_call)
        assert (await second)["id"].to_pylist() == [1]


@pytest.mark.asyncio
async def test_close_with_pending_requests(
    controlled: Any,
) -> None:
    ds, actor = controlled
    search = lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=1,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    )
    first = asyncio.create_task(
        search.search_async([0.0, 0.0], nearest={"k": 1}, columns=["id"])
    )
    call = await _call(actor)
    second = asyncio.create_task(search.search_async([1.0, 0.0], nearest={"k": 1}))
    await _barrier(search)
    closing = asyncio.create_task(search.aclose())
    with pytest.raises(RuntimeError, match="closing or closed"):
        await asyncio.wait_for(second, 5)
    assert not actor.killed and not closing.done()
    actor.finish(call)
    assert (await first)["id"].to_pylist() == [0]
    await asyncio.wait_for(closing, 5)
    assert actor.killed
    await search.aclose()
    with pytest.raises(RuntimeError, match="closing or closed"):
        await search.search_async([0.0, 0.0], nearest={"k": 1})


@pytest.mark.asyncio
async def test_large_batch_interleaving(
    controlled: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    ds, actor = controlled
    monkeypatch.setattr(search_mod.VectorSearchSession, "_QUERY_CHUNK_SIZE", 1)
    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=2,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        large = asyncio.create_task(
            search.search_async(
                [[0.0, 0.0], [1.0, 0.0]], nearest={"k": 1}, columns=["id"]
            )
        )
        large_first = await _call(actor)
        small = asyncio.create_task(
            search.search_async([5.0, 0.0], nearest={"k": 1}, columns=["id"])
        )
        small_call = await _call(actor)
        actor.finish(small_call)
        assert (await small)["id"].to_pylist() == [5]
        assert not large.done()
        actor.finish(large_first)
        large_second = await _call(actor)
        actor.finish(large_second)
        result = await large
        assert result["id"].to_pylist() == [0, 1]
        assert result["query_index"].to_pylist() == [0, 1]


def test_actor_death(tmp_path: Path) -> None:
    ds = _dataset(tmp_path / "actor-death.lance")
    with lr.open_vector_search(
        ds, column="vector", actor_options=lr.VectorSearchActorOptions(num_actors=1)
    ) as search:
        ray.kill(search._actors[0], no_restart=True)
        with pytest.raises(ray.exceptions.ActorDiedError):
            search.search([0.0, 0.0], nearest={"k": 1})
        with pytest.raises(RuntimeError, match="actor died"):
            search.search([0.0, 0.0], nearest={"k": 1})


@pytest.mark.asyncio
async def test_shared_request_limit(controlled: Any) -> None:
    ds, actor = controlled
    reads = threading.Event()

    def inputs() -> Any:
        reads.set()
        yield [[1.0, 0.0]]

    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=1,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        direct = asyncio.create_task(
            search.search_async([0.0, 0.0], nearest={"k": 1}, columns=["id"])
        )
        direct_call = await _call(actor)
        stream = search.map_batches(inputs(), nearest={"k": 1}, columns=["id"])
        streamed = asyncio.create_task(asyncio.to_thread(next, stream))
        await _barrier(search)
        assert not reads.is_set()
        actor.finish(direct_call)
        await direct
        stream_call = await _call(actor)
        actor.finish(stream_call)
        result = await streamed
        assert result["id"].to_pylist() == [1]
        assert result["query_index"].to_pylist() == [0]
        stream.close()


@pytest.mark.asyncio
async def test_stream_backpressure(
    controlled: Any,
) -> None:
    ds, actor = controlled
    reads: list[int] = []

    def inputs() -> Any:
        for i in range(3):
            reads.append(i)
            yield [[float(i), 0.0]]

    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=2,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        stream = search.map_batches(inputs(), nearest={"k": 1}, columns=["id"])
        first_result = asyncio.create_task(asyncio.to_thread(next, stream))
        first_call, second_call = await _call(actor), await _call(actor)
        actor.finish(second_call)
        await _barrier(search)
        assert reads == [0, 1] and not first_result.done()
        actor.finish(first_call)
        first = await first_result
        assert first["query_index"].to_pylist() == [0]
        second = await asyncio.to_thread(next, stream)
        assert second["query_index"].to_pylist() == [1]
        third_call = await _call(actor)
        actor.finish(third_call)
        third = await asyncio.to_thread(next, stream)
        assert third["query_index"].to_pylist() == [2]
        stream.close()


def test_input_error(tmp_path: Path) -> None:
    ds = _dataset(tmp_path / "input-error.lance")

    def inputs() -> Any:
        yield [[0.0, 0.0]]
        raise ValueError("input failed")

    with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=1,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        stream = search.map_batches(inputs(), nearest={"k": 1}, columns=["id"])
        assert next(stream)["id"].to_pylist() == [0]
        with pytest.raises(ValueError, match="input failed"):
            next(stream)
        assert search.search([5.0, 0.0], nearest={"k": 1}, columns=["id"])[
            "id"
        ].to_pylist() == [5]


def test_multivector_search_modes(tmp_path: Path) -> None:
    vector_type = pa.list_(pa.list_(pa.float32(), 2))
    ds = lance.write_dataset(
        pa.table(
            {
                "id": [0, 1, 2],
                "vector": pa.array(
                    [
                        [[1.0, 0.0], [0.0, 1.0]],
                        [[1.0, 0.0]],
                        [[-1.0, 0.0]],
                    ],
                    type=vector_type,
                ),
            }
        ),
        tmp_path / "multi-modes.lance",
    )
    ds.create_index("vector", "IVF_FLAT", metric="cosine", num_partitions=1)
    query = [[1.0, 0.0], [0.0, 1.0]]
    with lr.open_vector_search(
        ds, column="vector", actor_options=lr.VectorSearchActorOptions(num_actors=1)
    ) as search:
        indexed = search.search(query, nearest={"k": 3}, columns=["id"])
        flat = search.search(
            query, nearest={"k": 3, "use_index": False}, columns=["id"]
        )
        assert indexed.column_names == flat.column_names == ["id", "_distance"]
        assert flat["id"].to_pylist() == indexed["id"].to_pylist()
        assert flat["_distance"].to_pylist() == pytest.approx(
            indexed["_distance"].to_pylist()
        )
        batch = search.search(
            pa.array([query], type=pa.list_(pa.list_(pa.float32(), 2), 2)),
            nearest={"k": 1},
            columns=["id"],
        )
        assert batch["query_index"].to_pylist() == [0]
        with pytest.raises(ValueError, match="at least one vector"):
            search.search(np.empty((1, 0, 2)), nearest={"k": 1})


def test_session_metric_and_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "metric-snapshot.lance"
    ds = _dataset(path)
    ds.create_index("vector", "IVF_FLAT", metric="l2", num_partitions=1, name="l2_idx")
    with pytest.raises(ValueError, match="does not match"):
        lr.open_vector_search(ds, column="vector", metric="dot", index_name="l2_idx")
    with lr.open_vector_search(
        ds,
        column="vector",
        metric="dot",
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        # An incompatible automatic index must not hide any of the data.
        result = search.search([1.0, 0.0], nearest={"k": 1}, columns=["id"])
        assert result["id"].to_pylist() == [5]
        lance.write_dataset(
            pa.table(
                {
                    "id": [6],
                    "vector": pa.array([[100.0, 0.0]], type=pa.list_(pa.float32(), 2)),
                }
            ),
            path,
            mode="append",
        )
        assert search.search([1.0, 0.0], nearest={"k": 1}, columns=["id"])[
            "id"
        ].to_pylist() == [5]


def test_source_schema_change(tmp_path: Path) -> None:
    ds = _dataset(tmp_path / "mutated-schema.lance")
    with lr.open_vector_search(
        ds, column="vector", actor_options=lr.VectorSearchActorOptions(num_actors=1)
    ) as search:
        original_version = search.version
        ds.add_columns({"later": "id + 1"})
        assert ds.version > original_version
        result = search.search([0.0, 0.0], nearest={"k": 1})
        assert "later" not in result.column_names
        assert search.version == original_version


def test_hamming_batch(tmp_path: Path) -> None:
    ds = lance.write_dataset(
        pa.table(
            {
                "id": [0, 1, 2],
                "vector": pa.array(
                    [[0, 0], [255, 0], [255, 255]], type=pa.list_(pa.uint8(), 2)
                ),
            }
        ),
        tmp_path / "hamming-batch.lance",
    )
    ds.create_index("vector", "IVF_FLAT", metric="hamming", num_partitions=1)
    with lr.open_vector_search(
        ds, column="vector", actor_options=lr.VectorSearchActorOptions(num_actors=1)
    ) as search:
        for use_index in (True, False):
            result = search.search(
                [[0, 0], [255, 255]],
                nearest={"k": 1, "use_index": use_index},
                columns=["id"],
            )
            assert result["id"].to_pylist() == [0, 2]
            assert result["query_index"].to_pylist() == [0, 1]
            assert result["_distance"].to_pylist() == [0.0, 0.0]
        assert (
            search.search(np.empty((0, 2), dtype=np.uint8), nearest={"k": 1}).num_rows
            == 0
        )


@pytest.mark.asyncio
async def test_stream_error_backpressure(controlled: Any) -> None:
    ds, actor = controlled
    third_read = threading.Event()

    def inputs() -> Any:
        yield [[0.0, 0.0]]
        yield [[1.0, 0.0]]
        third_read.set()

    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=2,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        stream = search.map_batches(inputs(), nearest={"k": 1}, columns=["id"])
        first_result = asyncio.create_task(asyncio.to_thread(next, stream))
        first_call, second_call = await _call(actor), await _call(actor)
        second_call[1].result.set_exception(ValueError("shard failed"))
        read_ahead = await asyncio.to_thread(third_read.wait, 0.2)
        actor.finish(first_call)
        assert (await first_result)["id"].to_pylist() == [0]
        with pytest.raises(ValueError, match="shard failed"):
            await asyncio.to_thread(next, stream)
        assert not read_ahead


def test_stream_buffer_reuse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ds = _dataset(tmp_path / "reused-input.lance")
    preparing = threading.Event()
    allow_prepare = threading.Event()
    reused = threading.Event()
    original = search_mod.VectorSearchSession._prepare

    def prepare(self: Any, *args: Any) -> Any:
        preparing.set()
        assert allow_prepare.wait(5)
        return original(self, *args)

    monkeypatch.setattr(search_mod.VectorSearchSession, "_prepare", prepare)

    def inputs() -> Any:
        buffer = np.array([[0.0, 0.0]], dtype=np.float32)
        yield buffer
        buffer[0, 0] = 5.0
        reused.set()
        yield buffer

    with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=2,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        with ThreadPoolExecutor(max_workers=1) as executor:
            results = executor.submit(
                list, search.map_batches(inputs(), nearest={"k": 1}, columns=["id"])
            )
            try:
                assert preparing.wait(5)
                read_ahead = reused.wait(0.2)
            finally:
                allow_prepare.set()
            tables = results.result(timeout=10)
        assert [table["id"].to_pylist() for table in tables] == [[0], [5]]
        assert not read_ahead


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["prepare", "actor"])
async def test_repeated_cancellation(
    controlled: Any, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    ds, actor = controlled
    started, release = threading.Event(), threading.Event()
    original = search_mod.VectorSearchSession._prepare

    def prepare(self: Any, *args: Any) -> Any:
        started.set()
        assert release.wait(5)
        return original(self, *args)

    if stage == "prepare":
        monkeypatch.setattr(search_mod.VectorSearchSession, "_prepare", prepare)
    else:
        release.set()

    async with lr.open_vector_search(
        ds,
        column="vector",
        max_concurrent_requests=1,
        actor_options=lr.VectorSearchActorOptions(num_actors=1),
    ) as search:
        first = asyncio.create_task(
            search.search_async([0.0, 0.0], nearest={"k": 1}, columns=["id"])
        )
        first_call = None
        if stage == "prepare":
            assert await asyncio.to_thread(started.wait, 5)
        else:
            first_call = await _call(actor)

        async def cancel_twice() -> bool:
            slot = next(iter(search._slots))
            assert slot.task is not None
            slot.task.cancel()
            await asyncio.sleep(0)
            slot.task.cancel()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return slot.released

        try:
            released_early = await asyncio.wrap_future(search._schedule(cancel_twice()))
            second = asyncio.create_task(
                search.search_async([1.0, 0.0], nearest={"k": 1}, columns=["id"])
            )
            await _barrier(search)
            submitted_early = not actor.calls.empty()
        finally:
            release.set()
            if first_call is not None and not first_call[1].result.done():
                actor.finish(first_call)
        with pytest.raises(asyncio.CancelledError):
            await first
        second_call = await _call(actor)
        actor.finish(second_call)
        assert (await second)["id"].to_pylist() == [1]
        assert not released_early and not submitted_early
