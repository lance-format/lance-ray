"""Memory-aware compaction must use integer fragment ids and rebuild split tasks."""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import Any, cast

import lance_ray.compaction as compaction
import pytest
from lance.optimize import CompactionOptions


class _FragmentMetadata:
    def __init__(self, fragment_id: int) -> None:
        self.id = fragment_id


class _StoredFragment:
    def __init__(
        self,
        fragment_id: int,
        file_size_bytes: int | None,
        *,
        missing_size: bool = False,
        overlay_size_bytes: int | None = None,
        missing_overlay_size: bool = False,
    ) -> None:
        self.fragment_id = fragment_id
        self._file_size_bytes = file_size_bytes
        self._missing_size = missing_size
        self._overlay_size_bytes = overlay_size_bytes
        self._missing_overlay_size = missing_overlay_size

    def count_rows(self) -> int:
        return 10

    def data_files(self) -> list[SimpleNamespace]:
        if self._missing_size:
            return [SimpleNamespace(file_size_bytes=None)]
        return [SimpleNamespace(file_size_bytes=self._file_size_bytes)]

    @property
    def metadata(self) -> SimpleNamespace:
        overlays: list[SimpleNamespace] = []
        if self._missing_overlay_size or self._overlay_size_bytes is not None:
            overlays.append(
                SimpleNamespace(
                    data_file=SimpleNamespace(
                        file_size_bytes=(
                            None
                            if self._missing_overlay_size
                            else self._overlay_size_bytes
                        )
                    )
                )
            )
        return SimpleNamespace(overlays=overlays)


class _Dataset:
    def __init__(
        self,
        sizes: dict[int, int | None],
        *,
        missing: set[int] | None = None,
        overlay_sizes: dict[int, int] | None = None,
        missing_overlay: set[int] | None = None,
    ) -> None:
        missing = missing or set()
        overlay_sizes = overlay_sizes or {}
        missing_overlay = missing_overlay or set()
        self.fragments = [
            _StoredFragment(
                fragment_id,
                size,
                missing_size=fragment_id in missing,
                overlay_size_bytes=overlay_sizes.get(fragment_id),
                missing_overlay_size=fragment_id in missing_overlay,
            )
            for fragment_id, size in sizes.items()
        ]
        self.requested_ids: list[Any] = []

    def get_fragments(self) -> list[_StoredFragment]:
        return self.fragments

    def get_fragment(self, fragment_id: Any) -> _StoredFragment | None:
        self.requested_ids.append(fragment_id)
        if not isinstance(fragment_id, int):
            raise TypeError(
                "argument 'fragment_id': 'FragmentMetadata' object "
                "cannot be interpreted as an integer"
            )
        for fragment in self.fragments:
            if fragment.fragment_id == fragment_id:
                return fragment
        return None


class _Task:
    def __init__(self, fragments: list[Any], *, read_version: int = 7) -> None:
        self.fragments = fragments
        self.read_version = read_version

    def json(self) -> str:
        return json.dumps(
            {
                "read_version": self.read_version,
                "options": {"target_rows_per_fragment": 1024},
                "task": {
                    "fragments": [
                        {
                            "id": compaction._fragment_id(fragment),
                            "files": [
                                {"path": f"f{compaction._fragment_id(fragment)}.lance"}
                            ],
                            "physical_rows": 10,
                        }
                        for fragment in self.fragments
                    ]
                },
            }
        )


class _Plan:
    def __init__(self, tasks: list[_Task]) -> None:
        self._tasks = tasks

    @property
    def tasks(self) -> list[_Task]:
        return self._tasks

    def num_tasks(self) -> int:
        return len(self._tasks)


def _options() -> CompactionOptions:
    return cast(CompactionOptions, {})


def test_fragment_id_accepts_int_and_metadata() -> None:
    assert compaction._fragment_id(3) == 3
    assert compaction._fragment_id(_FragmentMetadata(9)) == 9
    assert compaction._fragment_id(SimpleNamespace(fragment_id=4)) == 4
    assert compaction._fragment_id(SimpleNamespace(metadata=SimpleNamespace(id=5))) == 5


def test_fragment_id_rejects_unsupported_type() -> None:
    with pytest.raises(TypeError, match="FragmentMetadata"):
        compaction._fragment_id("frag-1")


def test_memory_plan_looks_up_integer_ids_from_fragment_metadata(
    monkeypatch: Any,
) -> None:
    dataset = _Dataset({1: 100, 2: 100})
    original = _Task([_FragmentMetadata(1), _FragmentMetadata(2)])
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )

    tasks, metrics = compaction._create_memory_aware_compaction_plan(
        cast(Any, dataset), _options(), 10_000
    )

    assert len(tasks) == 1
    assert cast(Any, tasks[0]) is original
    assert dataset.requested_ids
    assert all(isinstance(fragment_id, int) for fragment_id in dataset.requested_ids)
    assert set(dataset.requested_ids) == {1, 2}
    assert metrics["tasks_split"] == 0


def test_memory_plan_splits_via_json_preserving_metadata(
    monkeypatch: Any,
) -> None:
    dataset = _Dataset({1: 1_000, 2: 1_000})
    original = _Task([_FragmentMetadata(1), _FragmentMetadata(2)], read_version=7)
    rebuilt: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )

    def from_json(payload: str) -> Any:
        decoded = json.loads(payload)
        assert isinstance(decoded, dict)
        rebuilt.append(decoded)
        return decoded

    monkeypatch.setattr("lance_ray.compaction.CompactionTask.from_json", from_json)

    tasks, metrics = compaction._create_memory_aware_compaction_plan(
        cast(Any, dataset),
        _options(),
        2_500,
    )
    split_tasks = cast(list[dict[str, Any]], tasks)

    assert metrics["tasks_split"] == 1
    assert len(split_tasks) == 2
    assert [task["task"]["fragments"][0]["id"] for task in split_tasks] == [1, 2]
    assert all(item["read_version"] == 7 for item in rebuilt)
    assert all(
        item["options"] == {"target_rows_per_fragment": 1024} for item in rebuilt
    )
    # Full fragment records are preserved, not reduced to bare integer ids.
    assert all(
        "files" in frag and "physical_rows" in frag
        for item in rebuilt
        for frag in item["task"]["fragments"]
    )
    assert all(isinstance(fragment_id, int) for fragment_id in dataset.requested_ids)


def test_memory_plan_accepts_integer_fragment_ids(monkeypatch: Any) -> None:
    dataset = _Dataset({4: 100})
    original = _Task([4])
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )

    tasks, metrics = compaction._create_memory_aware_compaction_plan(
        cast(Any, dataset), _options(), 10_000
    )

    assert len(tasks) == 1
    assert cast(Any, tasks[0]) is original
    assert 4 in dataset.requested_ids
    assert metrics["tasks_split"] == 0


def test_memory_plan_warns_when_single_fragment_exceeds_budget(
    monkeypatch: Any, caplog: pytest.LogCaptureFixture
) -> None:
    dataset = _Dataset({1: 5_000, 2: 100})
    original = _Task([_FragmentMetadata(1), _FragmentMetadata(2)])
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )
    monkeypatch.setattr(
        "lance_ray.compaction.CompactionTask.from_json",
        lambda payload: json.loads(payload),
    )

    with caplog.at_level(logging.WARNING, logger="lance_ray.compaction"):
        tasks, metrics = compaction._create_memory_aware_compaction_plan(
            cast(Any, dataset),
            _options(),
            1_000,
        )

    assert metrics["tasks_split"] == 1
    assert len(tasks) == 2
    assert any("may still exceed" in record.message for record in caplog.records)
    assert any(
        "do not guarantee" in record.message.lower()
        or "cannot guarantee" in record.message.lower()
        or "may still exceed" in record.message
        for record in caplog.records
    )


def test_memory_plan_unknown_file_size_is_not_treated_as_zero(
    monkeypatch: Any, caplog: pytest.LogCaptureFixture
) -> None:
    # Fragment 1 has unknown size; fragment 2 is small. Packing must not treat
    # the unknown size as zero and keep both fragments in one under-budget task.
    dataset = _Dataset({1: None, 2: 100}, missing={1})
    original = _Task([_FragmentMetadata(1), _FragmentMetadata(2)])
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )
    monkeypatch.setattr(
        "lance_ray.compaction.CompactionTask.from_json",
        lambda payload: json.loads(payload),
    )

    with caplog.at_level(logging.WARNING, logger="lance_ray.compaction"):
        tasks, metrics = compaction._create_memory_aware_compaction_plan(
            cast(Any, dataset),
            _options(),
            10_000,
        )

    assert metrics["tasks_split"] == 1
    assert len(tasks) == 2
    assert any("unknown" in record.message.lower() for record in caplog.records)


def test_compact_database_forwards_memory_limit(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    class _Namespace:
        def list_tables(self, request: Any) -> SimpleNamespace:
            assert request.id == ["db"]
            return SimpleNamespace(tables=["t1", "t2"], page_token=None)

    monkeypatch.setattr(
        "lance_ray.compaction.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: _Namespace(),
    )

    def fake_compact_files(**kwargs: Any) -> str:
        calls.append(kwargs)
        return "metrics"

    monkeypatch.setattr("lance_ray.compaction.compact_files", fake_compact_files)

    results = compaction.compact_database(
        database=["db"],
        namespace_impl="dir",
        max_memory_per_task_bytes=1024,
    )

    assert [call["table_id"] for call in calls] == [["db", "t1"], ["db", "t2"]]
    assert all(call["max_memory_per_task_bytes"] == 1024 for call in calls)
    assert results == [
        {"table_id": ["db", "t1"], "metrics": "metrics"},
        {"table_id": ["db", "t2"], "metrics": "metrics"},
    ]


def test_estimate_includes_overlay_file_size() -> None:
    # Base file is 200 bytes. data_files() hides the 2000-byte overlay.
    # The 2x estimate must be (200 + 2000) * 2 = 4400, not 400.
    dataset = _Dataset({1: 200}, overlay_sizes={1: 2000})
    estimate = compaction._estimate_fragment_memory(cast(Any, dataset), [1])

    assert estimate["fragment_details"][0]["file_size_bytes"] == 2200
    assert estimate["estimated_memory_bytes"] == 4400
    assert estimate["all_sizes_known"] is True


def test_overlay_bytes_keep_task_from_looking_under_budget(
    monkeypatch: Any,
) -> None:
    dataset = _Dataset({1: 200}, overlay_sizes={1: 2000})
    original = _Task([_FragmentMetadata(1)])
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )
    monkeypatch.setattr(
        "lance_ray.compaction.CompactionTask.from_json",
        lambda payload: json.loads(payload),
    )

    tasks, metrics = compaction._create_memory_aware_compaction_plan(
        cast(Any, dataset),
        _options(),
        1000,
    )

    assert metrics["tasks_split"] == 1
    assert len(tasks) == 1
    assert cast(Any, tasks[0]) is not original


def test_unknown_overlay_size_is_not_treated_as_zero(
    monkeypatch: Any, caplog: pytest.LogCaptureFixture
) -> None:
    # The base file fits in the budget. The overlay size is unknown, so the
    # fragment must still be split out instead of staying in an under-budget task.
    dataset = _Dataset({1: 100, 2: 100}, missing_overlay={1})
    original = _Task([_FragmentMetadata(1), _FragmentMetadata(2)])
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )
    monkeypatch.setattr(
        "lance_ray.compaction.CompactionTask.from_json",
        lambda payload: json.loads(payload),
    )

    with caplog.at_level(logging.WARNING, logger="lance_ray.compaction"):
        tasks, metrics = compaction._create_memory_aware_compaction_plan(
            cast(Any, dataset),
            _options(),
            10_000,
        )

    assert metrics["tasks_split"] == 1
    assert len(tasks) == 2
    assert any("unknown" in record.message.lower() for record in caplog.records)


def test_split_parses_task_json_once(monkeypatch: Any) -> None:
    fragment_count = 8
    dataset = _Dataset({fragment_id: 1_000 for fragment_id in range(fragment_count)})
    fragments = [
        _FragmentMetadata(fragment_id) for fragment_id in range(fragment_count)
    ]

    class _CountingTask(_Task):
        def __init__(self, task_fragments: list[Any]) -> None:
            super().__init__(task_fragments)
            self.json_calls = 0

        def json(self) -> str:
            self.json_calls += 1
            return super().json()

    original = _CountingTask(fragments)
    index_calls = 0
    real_index = compaction._index_compaction_task

    def counting_index(task: Any) -> tuple[dict[str, Any], dict[int, Any]]:
        nonlocal index_calls
        index_calls += 1
        return real_index(task)

    monkeypatch.setattr(compaction, "_index_compaction_task", counting_index)
    monkeypatch.setattr(
        "lance_ray.compaction.Compaction.plan",
        lambda dataset, options: _Plan([original]),
    )
    monkeypatch.setattr(
        "lance_ray.compaction.CompactionTask.from_json",
        lambda payload: json.loads(payload),
    )

    tasks, metrics = compaction._create_memory_aware_compaction_plan(
        cast(Any, dataset),
        _options(),
        1_500,
    )

    assert metrics["tasks_split"] == 1
    assert len(tasks) == fragment_count
    assert original.json_calls == 1
    assert index_calls == 1


def test_subset_keeps_fragment_order_and_rejects_duplicates(
    monkeypatch: Any,
) -> None:
    payload: dict[str, Any] = {
        "read_version": 7,
        "task": {
            "fragments": [
                {"id": 3, "files": [{"path": "c.lance"}]},
                {"id": 1, "files": [{"path": "a.lance"}]},
                {"id": 2, "files": [{"path": "b.lance"}]},
            ]
        },
    }
    by_id = {int(record["id"]): record for record in payload["task"]["fragments"]}
    monkeypatch.setattr(
        "lance_ray.compaction.CompactionTask.from_json",
        lambda serialized: json.loads(serialized),
    )

    subset = compaction._subset_indexed_compaction_task(
        payload,
        by_id,
        [3, 1, 2],
    )
    decoded = cast(dict[str, Any], subset)
    assert [fragment["id"] for fragment in decoded["task"]["fragments"]] == [3, 1, 2]

    with pytest.raises(ValueError, match="duplicate fragment 1"):
        compaction._subset_indexed_compaction_task(payload, by_id, [1, 2, 1])


def test_estimate_fragment_memory_stores_integer_ids() -> None:
    dataset = _Dataset({8: 50, 9: 50})
    estimate = compaction._estimate_fragment_memory(cast(Any, dataset), [8, 9])
    assert [detail["fragment_id"] for detail in estimate["fragment_details"]] == [8, 9]
    assert all(
        isinstance(detail["fragment_id"], int)
        for detail in estimate["fragment_details"]
    )
