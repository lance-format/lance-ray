# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Non-native unit coverage; loading this module does not import lance_ray."""

from __future__ import annotations

import importlib.util
import os
import pickle
import sys
from collections.abc import Iterator
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from uuid import UUID

import pytest


@pytest.fixture(scope="session", autouse=True)
def ray_context() -> Iterator[None]:
    """Override conftest's Ray startup for this isolated unit module."""
    yield


@pytest.fixture(scope="module")
def prewarm() -> Iterator[ModuleType]:
    name = "_lance_ray_prewarm_unit"
    path = Path(__file__).parents[1] / "lance_ray" / "prewarm.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        del sys.modules[name]


def segment(
    number: int, fragments: list[int], size: int | None = 10
) -> SimpleNamespace:
    return SimpleNamespace(
        uuid=str(UUID(int=number)), fragment_ids=fragments, size_bytes=size
    )


class FakeDataset:
    def __init__(self) -> None:
        self.uri = "s3://bucket/table.lance"
        self.version = 7
        self.index_type = "Inverted"
        self.segments = [
            segment(3, [2], 20),
            segment(1, [0, 1], 100),
            segment(2, [1], 30),
        ]
        self.calls: list[tuple[str, list[str], bool]] = []
        self.cache = 10
        self.telemetry_error = False
        self.native_error: Exception | None = None
        self.namespace = object()
        self.session_object = SimpleNamespace(index_cache_size_bytes=self.cache_bytes)

    def describe_indices(self) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(
                name="text_fts", index_type=self.index_type, segments=self.segments
            )
        ]

    def get_fragments(self) -> list[SimpleNamespace]:
        return [SimpleNamespace(fragment_id=i) for i in range(4)]

    def session(self) -> SimpleNamespace:
        return self.session_object

    def cache_bytes(self) -> int:
        if self.telemetry_error:
            raise RuntimeError("telemetry unavailable")
        return self.cache

    def prewarm_index(
        self, name: str, *, index_segments: list[str], with_position: bool = False
    ) -> None:
        self.calls.append((name, index_segments, with_position))
        self.cache += 20
        if self.native_error:
            raise self.native_error


@pytest.fixture
def dataset() -> FakeDataset:
    return FakeDataset()


def selected(plan: Any) -> set[str]:
    return {
        value for assignment in plan.assignments for value in assignment.index_segments
    }


def test_all_segments_and_immutable_serializable_plan(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    plan = prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=2)
    assert plan.dataset_uri == dataset.uri
    assert plan.dataset_version == 7
    assert plan.index_name == "text_fts"
    assert plan.with_position is False
    assert plan.scheduling_basis == "bytes"
    assert [a.index_segments for a in plan.assignments] == [
        (str(UUID(int=1)),),
        (str(UUID(int=2)), str(UUID(int=3))),
    ]
    assert [a.estimated_size_bytes for a in plan.assignments] == [100, 50]
    assert plan.uncovered_fragment_ids == ()
    assert dataset.calls == []
    assert pickle.loads(pickle.dumps(plan)) == plan
    assert asdict(plan)["assignments"][0]["worker_id"] == 0
    with pytest.raises(FrozenInstanceError):
        plan.index_name = "changed"


def test_normalizes_and_deduplicates_uuid_generator(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    values = (
        value for value in [UUID(int=1), UUID(int=1).hex.upper(), str(UUID(int=2))]
    )
    plan = prewarm.plan_index_prewarm(dataset, "text_fts", index_segments=values)
    assert plan.assignments[0].index_segments == (str(UUID(int=1)), str(UUID(int=2)))


@pytest.mark.parametrize("selection", ["segments", "fragments"])
def test_empty_generator_skips_native(
    prewarm: ModuleType, dataset: FakeDataset, selection: str
) -> None:
    kwargs = {"index_segments" if selection == "segments" else "fragment_ids": iter(())}
    plan = prewarm.plan_index_prewarm(dataset, "text_fts", **kwargs)
    assert plan.assignments == ()
    report = prewarm.prewarm_index(
        dataset, "text_fts", **{key: iter(()) for key in kwargs}
    )
    assert report.status == "skipped"
    assert report.index_segments == ()
    assert report.fragment_ids == ()
    assert dataset.calls == []


def test_fragment_selection_reports_whole_segment_and_uncovered(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    plan = prewarm.plan_index_prewarm(dataset, "text_fts", fragment_ids=iter([1, 1, 3]))
    assert plan.assignments[0].index_segments == (str(UUID(int=1)), str(UUID(int=2)))
    assert plan.assignments[0].fragment_ids == (0, 1)
    assert plan.uncovered_fragment_ids == (3,)
    report = prewarm.prewarm_index(dataset, "text_fts", fragment_ids=[3])
    assert report.status == "skipped"
    assert report.uncovered_fragment_ids == (3,)
    assert dataset.calls == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"index_segments": [], "fragment_ids": []}, "only one"),
        ({"index_segments": ["bad"]}, "Invalid index segment"),
        ({"index_segments": [1]}, "UUID strings"),
        ({"index_segments": str(UUID(int=1))}, "not a single UUID"),
        ({"index_segments": UUID(int=1)}, "not a single UUID"),
        ({"index_segments": [str(UUID(int=99))]}, "Unknown index segment"),
        ({"fragment_ids": [99]}, "Unknown fragment"),
        ({"fragment_ids": [-1]}, "nonnegative integers"),
        ({"fragment_ids": [True]}, "nonnegative integers"),
        ({"fragment_ids": [1.5]}, "nonnegative integers"),
        ({"fragment_ids": "1"}, "nonnegative integers"),
        ({"num_workers": 0}, "positive integer"),
        ({"num_workers": True}, "positive integer"),
        ({"num_workers": 1.5}, "positive integer"),
        ({"with_position": 1}, "must be a bool"),
    ],
)
def test_invalid_selection(
    prewarm: ModuleType, dataset: FakeDataset, kwargs: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        prewarm.plan_index_prewarm(dataset, "text_fts", **kwargs)
    assert dataset.calls == []


@pytest.mark.parametrize("index_name", ["missing", "", None])
def test_unknown_index(
    prewarm: ModuleType, dataset: FakeDataset, index_name: Any
) -> None:
    with pytest.raises(ValueError):
        prewarm.prewarm_index(dataset, index_name)
    assert dataset.calls == []


def test_with_position_is_inverted_only(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    dataset.index_type = "IVF_PQ"
    with pytest.raises(ValueError, match="INVERTED"):
        prewarm.prewarm_index(dataset, "text_fts", with_position=True)
    assert dataset.calls == []


@pytest.mark.parametrize("bad_size", [None, -1, True, "10"])
def test_consistent_size_fallback_and_stable_disjoint_scheduling(
    prewarm: ModuleType, dataset: FakeDataset, bad_size: Any
) -> None:
    dataset.segments[0].size_bytes = bad_size
    first = prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=2)
    assert first.scheduling_basis == "segments"
    assert [a.index_segments for a in first.assignments] == [
        (str(UUID(int=1)), str(UUID(int=3))),
        (str(UUID(int=2)),),
    ]
    dataset.segments.reverse()
    assert prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=2) == first
    uuids = [u for a in first.assignments for u in a.index_segments]
    assert len(uuids) == len(set(uuids)) == 3
    assert first.assignments[0].estimated_size_bytes is None
    assert selected(first) == {s.uuid for s in dataset.segments}


def test_zero_bytes_and_excess_workers(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    for item in dataset.segments:
        item.size_bytes = 0
    plan = prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=20)
    assert len(plan.assignments) == 3
    assert plan.scheduling_basis == "bytes"
    assert all(a.estimated_size_bytes == 0 for a in plan.assignments)


def test_worker_call_reuses_dataset_session_and_namespace(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    original_session = dataset.session()
    namespace = dataset.namespace
    report = prewarm.prewarm_index(
        dataset,
        "text_fts",
        index_segments=(v for v in [UUID(int=1)]),
        with_position=True,
        expected_version=7,
        worker_id="search-0",
    )
    assert dataset.calls == [("text_fts", [str(UUID(int=1))], True)]
    assert dataset.session() is original_session
    assert dataset.namespace is namespace
    assert report.status == "success"
    assert report.error is None
    assert report.worker_id == "search-0"
    assert report.process_id == os.getpid()
    assert report.dataset_version == 7
    assert report.dataset_uri == dataset.uri
    assert report.fragment_ids == (0, 1)
    assert report.with_position is True
    assert report.cache_bytes_before == 10
    assert report.cache_bytes_after == 30
    assert report.elapsed_seconds >= 0
    with pytest.raises(FrozenInstanceError):
        report.status = "failed"


@pytest.mark.parametrize("version", [6, True, -1, "7"])
def test_version_guard(prewarm: ModuleType, dataset: FakeDataset, version: Any) -> None:
    with pytest.raises(ValueError, match="expected_version"):
        prewarm.prewarm_index(dataset, "text_fts", expected_version=version)
    assert dataset.calls == []


def test_worker_label_validation(prewarm: ModuleType, dataset: FakeDataset) -> None:
    with pytest.raises(ValueError, match="worker_id"):
        prewarm.prewarm_index(dataset, "text_fts", worker_id=5)
    assert dataset.calls == []


def test_legacy_interface_cannot_trigger_full_prewarm(
    prewarm: ModuleType, dataset: FakeDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def old_prewarm(name: str) -> None:
        calls.append(name)

    monkeypatch.setattr(dataset, "prewarm_index", old_prewarm)
    with pytest.raises(NotImplementedError, match="index_segments"):
        prewarm.prewarm_index(dataset, "text_fts")
    assert calls == []


def test_false_position_not_forwarded_to_segment_only_interface(
    prewarm: ModuleType, dataset: FakeDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def segment_only(name: str, *, index_segments: list[str]) -> None:
        calls.append(index_segments)

    monkeypatch.setattr(dataset, "prewarm_index", segment_only)
    prewarm.prewarm_index(dataset, "text_fts", index_segments=[UUID(int=2)])
    assert calls == [[str(UUID(int=2))]]
    with pytest.raises(NotImplementedError, match="with_position"):
        prewarm.prewarm_index(dataset, "text_fts", with_position=True)
    assert len(calls) == 1


def test_missing_methods_and_uninspectable_signature(
    prewarm: ModuleType, dataset: FakeDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dataset, "describe_indices", None)
    with pytest.raises(NotImplementedError, match="describe_indices"):
        prewarm.plan_index_prewarm(dataset, "text_fts")
    monkeypatch.undo()
    monkeypatch.setattr(dataset, "prewarm_index", None)
    with pytest.raises(NotImplementedError, match="prewarm_index"):
        prewarm.prewarm_index(dataset, "text_fts")
    monkeypatch.undo()

    def broken_signature(value: Any) -> None:
        raise ValueError("no signature")

    monkeypatch.setattr(prewarm.inspect, "signature", broken_signature)
    with pytest.raises(NotImplementedError, match="Cannot inspect"):
        prewarm.prewarm_index(dataset, "text_fts")
    assert dataset.calls == []


def test_positional_only_segments_are_rejected(
    prewarm: ModuleType, dataset: FakeDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    def positional(name: str, index_segments: list[str], /) -> None:
        pytest.fail("must not invoke positional interface")

    monkeypatch.setattr(dataset, "prewarm_index", positional)
    with pytest.raises(NotImplementedError, match="index_segments"):
        prewarm.prewarm_index(dataset, "text_fts")


@pytest.mark.parametrize(
    "broken", ["raise", "missing_session", "missing_stat", "invalid_stat"]
)
def test_telemetry_is_optional(
    prewarm: ModuleType,
    dataset: FakeDataset,
    monkeypatch: pytest.MonkeyPatch,
    broken: str,
) -> None:
    if broken == "raise":
        dataset.telemetry_error = True
    elif broken == "missing_session":
        monkeypatch.setattr(dataset, "session", None)
    elif broken == "missing_stat":
        dataset.session_object = SimpleNamespace()
    else:
        dataset.session_object = SimpleNamespace(
            index_cache_size_bytes=lambda: "unknown"
        )
    report = prewarm.prewarm_index(dataset, "text_fts")
    assert report.status == "success"
    assert report.cache_bytes_before is None
    assert report.cache_bytes_after is None
    assert len(dataset.calls) == 1


@pytest.mark.parametrize("broken_telemetry", [False, True])
def test_native_failure_preserves_cause_and_report(
    prewarm: ModuleType, dataset: FakeDataset, broken_telemetry: bool
) -> None:
    failure = OSError("object store read failed")
    dataset.native_error = failure
    dataset.telemetry_error = broken_telemetry
    with pytest.raises(prewarm.IndexPrewarmError) as captured:
        prewarm.prewarm_index(
            dataset, "text_fts", fragment_ids=[0], worker_id="worker-a"
        )
    assert captured.value.__cause__ is failure
    report = captured.value.report
    assert report.status == "failed"
    assert report.error == "OSError: object store read failed"
    assert report.worker_id == "worker-a"
    assert report.index_segments == (str(UUID(int=1)),)
    assert report.fragment_ids == (0, 1)
    assert report.cache_bytes_before == (None if broken_telemetry else 10)
    assert report.cache_bytes_after == (None if broken_telemetry else 30)
    assert report.elapsed_seconds >= 0


def test_error_pickle_roundtrip(prewarm: ModuleType, dataset: FakeDataset) -> None:
    dataset.native_error = OSError("failed")
    with pytest.raises(prewarm.IndexPrewarmError) as captured:
        prewarm.prewarm_index(dataset, "text_fts")
    restored = pickle.loads(pickle.dumps(captured.value))
    assert restored.report == captured.value.report
    assert str(restored) == str(captured.value)


def test_varargs_segments_are_rejected(
    prewarm: ModuleType, dataset: FakeDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    def variadic(name: str, *index_segments: str) -> None:
        pytest.fail("must not invoke variadic positional interface")

    monkeypatch.setattr(dataset, "prewarm_index", variadic)
    with pytest.raises(NotImplementedError, match="index_segments"):
        prewarm.prewarm_index(dataset, "text_fts")


def test_index_without_segments_is_noop(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    dataset.segments = []
    plan = prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=10)
    assert plan.assignments == ()
    assert plan.scheduling_basis == "segments"
    report = prewarm.prewarm_index(dataset, "text_fts")
    assert report.status == "skipped"
    assert dataset.calls == []


def test_byte_plan_stable_under_metadata_reordering(
    prewarm: ModuleType, dataset: FakeDataset
) -> None:
    first = prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=2)
    dataset.segments.reverse()
    assert prewarm.plan_index_prewarm(dataset, "text_fts", num_workers=2) == first
