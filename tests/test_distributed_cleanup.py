"""Tests for lance_ray.cleanup."""

import os
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, Optional
from unittest.mock import patch

import lance
import lance_ray as lr
import pyarrow as pa
import pytest
import ray
from lance.dataset import LanceDataset
from lance_ray import cleanup as cleanup_mod

from _ray_test_support import FakeCleanupStats, FakeSweepTask

requires_distributed_cleanup = pytest.mark.skipif(
    not cleanup_mod._supports_distributed_cleanup(),
    reason="installed pylance cannot distribute a cleanup",
)

_TABLE = pa.table({"id": list(range(100)), "value": [f"v{i}" for i in range(100)]})


def _write_versions(uri: str) -> None:
    """Old versions with an index, deletions and an aged orphan, then one more."""
    lance.write_dataset(_TABLE, uri, max_rows_per_file=25)
    lance.write_dataset(_TABLE, uri, mode="overwrite", max_rows_per_file=25)
    dataset = lance.dataset(uri)
    dataset.create_scalar_index("id", "BTREE")
    dataset.delete("id < 10")
    lance.write_dataset(_TABLE, uri, mode="overwrite", max_rows_per_file=25)
    orphan = Path(uri) / "data" / "orphan.lance"
    orphan.write_bytes(b"orphan")
    eight_days_ago = time.time() - timedelta(days=8).total_seconds()
    os.utime(orphan, (eight_days_ago, eight_days_ago))


def _files(uri: str) -> set[str]:
    return {str(path) for path in Path(uri).rglob("*") if path.is_file()}


@pytest.fixture
def uri(tmp_path: Path) -> str:
    uri = str(tmp_path / "dataset")
    _write_versions(uri)
    return uri


def _expected(uri: str) -> tuple[Any, set[str]]:
    """Single-process explanation, and the files left after cleaning by it."""
    explanation = lance.dataset(uri).explain_cleanup_old_versions(
        older_than=timedelta(0), include_files=True, max_files=100_000
    )
    removed = {"/" + file.path for file in explanation.candidate_files}
    assert removed <= _files(uri)
    return explanation, _files(uri) - removed


@requires_distributed_cleanup
def test_matches_single_process_cleanup(uri: str) -> None:
    expected, remaining = _expected(uri)
    assert expected.stats.old_versions == 4
    assert expected.stats.data_files_removed > 0
    assert expected.stats.index_files_removed > 0
    assert expected.stats.deletion_files_removed > 0

    explanation = lr.explain_cleanup_old_versions(
        uri, older_than=timedelta(0), num_workers=2
    )
    assert explanation.stats.bytes_removed == expected.stats.bytes_removed
    assert sorted(f.path for f in explanation.candidate_files) == sorted(
        f.path for f in expected.candidate_files
    )

    stats = lr.cleanup_old_versions(uri, older_than=timedelta(0), num_workers=2)
    assert stats.old_versions == expected.stats.old_versions
    assert stats.data_files_removed == expected.stats.data_files_removed
    assert stats.bytes_removed == expected.stats.bytes_removed
    assert _files(uri) == remaining
    assert lance.dataset(uri).to_table() == _TABLE


@requires_distributed_cleanup
def test_cleanup_with_directory_namespace(tmp_path: Path) -> None:
    import lance_namespace as ln
    from lance_namespace import DescribeTableRequest

    table_id = ["cleanup_table"]
    namespace: dict[str, Any] = {
        "namespace_impl": "dir",
        "namespace_properties": {"root": str(tmp_path)},
    }
    modes: list[Literal["create", "overwrite"]] = ["create", "overwrite"]
    for mode in modes:
        lr.write_lance(
            ray.data.from_arrow(_TABLE), table_id=table_id, mode=mode, **namespace
        )
    location = (
        ln.connect("dir", {"root": str(tmp_path)})
        .describe_table(DescribeTableRequest(id=table_id))
        .location
    )
    assert len(lance.dataset(location).versions()) == 2

    stats = lr.cleanup_old_versions(
        table_id=table_id, older_than=timedelta(0), num_workers=2, **namespace
    )
    assert stats.old_versions == 1
    assert len(lance.dataset(location).versions()) == 1
    assert lance.dataset(location).to_table().sort_by("id") == _TABLE


def _failing_first_attempt(marker_dir: Path) -> Callable[..., Any]:
    """Wraps the sweep handler so each task's first attempt fails."""
    real = cleanup_mod._sweep_handler
    path = str(marker_dir)

    def create(*args: Any) -> Any:
        run = real(*args)

        def handler(task: Any) -> dict[str, Any]:
            marker = os.path.join(path, str(task.index))
            if not os.path.exists(marker):
                open(marker, "w").close()
                return {"status": "error", "error": "injected"}
            result: dict[str, Any] = run(task)
            return result

        return handler

    return create


@requires_distributed_cleanup
def test_failed_task_raises(uri: str, tmp_path: Path) -> None:
    _, remaining = _expected(uri)
    with (
        patch.object(cleanup_mod, "_sweep_handler", _failing_first_attempt(tmp_path)),
        pytest.raises(RuntimeError, match="files these tasks left"),
    ):
        lr.cleanup_old_versions(
            uri, older_than=timedelta(0), num_workers=2, max_task_retries=0
        )
    # The old versions went with the plan; the dataset is intact.
    assert len(lance.dataset(uri).versions()) == 1
    assert lance.dataset(uri).to_table() == _TABLE
    # No version references what the failed tasks left, so only an unverified
    # cleanup removes it before it is seven days old.
    stats = lr.cleanup_old_versions(
        uri, older_than=timedelta(0), delete_unverified=True
    )
    assert stats.old_versions == 0
    assert _files(uri) == remaining


class _FakePlan:
    """Stands in for pylance's ``CleanupPlan`` and records how it is finished."""

    def __init__(self, marker_dir: Path, fail_attempts: dict[int, int], count: int):
        self.read_version = 3
        self.num_old_versions = 2
        self.max_tasks: list[int] = []
        self.finished: list[list[Any]] = []
        self.explained: list[list[Any]] = []
        self._tasks = [
            FakeSweepTask(index, str(marker_dir), fail_attempts.get(index, 0))
            for index in range(count)
        ]

    def tasks(self, max_tasks: int) -> list[FakeSweepTask]:
        self.max_tasks.append(max_tasks)
        return self._tasks

    def finish(self, dataset: Any, results: list[Any]) -> FakeCleanupStats:
        self.finished.append(results)
        return FakeCleanupStats(
            data_files_removed=sum(r.stats.data_files_removed for r in results)
        )

    def finish_explain(self, dataset: Any, results: list[Any]) -> str:
        self.explained.append(results)
        return "explanation"


@pytest.fixture
def fake_plan(uri: str, tmp_path: Path) -> Iterator[Callable[..., _FakePlan]]:
    """Installs a fake ``plan_cleanup_old_versions`` with the real signature, so the
    driver and the Ray workers run against any pylance release."""
    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    patches: list[Any] = []

    def install(
        count: int = 5, fail_attempts: Optional[dict[int, int]] = None
    ) -> _FakePlan:
        plan = _FakePlan(marker_dir, fail_attempts or {}, count)

        def plan_cleanup_old_versions(
            self: LanceDataset,
            older_than: Optional[timedelta] = None,
            retain_versions: Optional[int] = None,
            *,
            delete_unverified: bool = False,
            error_if_tagged_old_versions: bool = True,
            delete_rate_limit: Optional[int] = None,
            versions: Optional[list[int]] = None,
            delete_concurrency: Optional[int] = None,
            explain: bool = False,
            max_files: int = 1000,
        ) -> _FakePlan:
            return plan

        patcher = patch.object(
            LanceDataset,
            "plan_cleanup_old_versions",
            plan_cleanup_old_versions,
            create=True,
        )
        patcher.start()
        patches.append(patcher)
        return plan

    yield install
    for patcher in reversed(patches):
        patcher.stop()


@pytest.mark.parametrize("explain", [False, True])
def test_dispatches_every_task_and_finishes(
    uri: str, fake_plan: Callable[..., _FakePlan], explain: bool
) -> None:
    # Task 0 fails once, so it is retried after the others are dispatched.
    plan = fake_plan(count=5, fail_attempts={0: 1})
    run = lr.explain_cleanup_old_versions if explain else lr.cleanup_old_versions
    kwargs: dict[str, Any] = {} if explain else {"delete_concurrency": 3}
    with patch.object(cleanup_mod, "logger") as log:
        result = run(
            uri, older_than=timedelta(0), num_workers=2, max_task_retries=1, **kwargs
        )

    assert plan.max_tasks == [2 * cleanup_mod._TASKS_PER_WORKER]
    calls, other = (
        (plan.explained, plan.finished) if explain else (plan.finished, plan.explained)
    )
    assert other == []
    assert len(calls) == 1
    # One result per task, in task order.
    assert [r.index for r in calls[0]] == list(range(5))
    if explain:
        assert result == "explanation"
    else:
        assert result.data_files_removed == 5
        assert {r.delete_concurrency for r in calls[0]} == {3}
    # The driver logs progress once per task, with running totals.
    progress = [
        call.args
        for call in log.info.call_args_list
        if call.args[0].startswith("Cleanup task")
    ]
    assert [args[1] for args in progress] == [1, 2, 3, 4, 5]
    assert progress[-1][6] == 5


def test_passes_max_tasks_to_the_plan(
    uri: str, fake_plan: Callable[..., _FakePlan]
) -> None:
    plan = fake_plan(count=3)
    lr.cleanup_old_versions(uri, older_than=timedelta(0), num_workers=2, max_tasks=3)
    assert plan.max_tasks == [3]


@pytest.mark.parametrize("explain", [False, True])
def test_failure_after_retries_does_not_finish(
    uri: str, fake_plan: Callable[..., _FakePlan], explain: bool
) -> None:
    plan = fake_plan(count=3, fail_attempts={1: 2})
    run = lr.explain_cleanup_old_versions if explain else lr.cleanup_old_versions
    message = "Explaining cleanup failed" if explain else "files these tasks left"
    with pytest.raises(RuntimeError, match=message):
        run(uri, older_than=timedelta(0), num_workers=2, max_task_retries=1)
    assert plan.finished == []
    assert plan.explained == []


@pytest.mark.parametrize(
    ("rate", "num_workers", "num_tasks", "expected"),
    [(None, 2, 5, None), (100, 4, 5, 25), (100, 8, 2, 50), (3, 4, 5, 1)],
)
def test_rate_limit_is_split_between_workers(
    uri: str,
    fake_plan: Callable[..., _FakePlan],
    rate: Optional[int],
    num_workers: int,
    num_tasks: int,
    expected: Optional[int],
) -> None:
    plan = fake_plan(count=num_tasks)
    lr.cleanup_old_versions(
        uri, older_than=timedelta(0), num_workers=num_workers, delete_rate_limit=rate
    )
    assert [r.delete_rate_limit for r in plan.finished[0]] == [expected] * num_tasks


@pytest.mark.parametrize("explain", [False, True])
def test_falls_back_without_distributed_cleanup(uri: str, explain: bool) -> None:
    expected, remaining = _expected(uri)
    with patch.object(cleanup_mod, "_supports_distributed_cleanup", return_value=False):
        if explain:
            result = lr.explain_cleanup_old_versions(uri, older_than=timedelta(0))
            assert result.stats.bytes_removed == expected.stats.bytes_removed
            return
        stats = lr.cleanup_old_versions(uri, older_than=timedelta(0))
    assert stats.old_versions == expected.stats.old_versions
    assert _files(uri) == remaining


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"num_workers": 0}, "num_workers"),
        ({"max_tasks": 0}, "max_tasks"),
        ({"max_task_retries": -1}, "max_task_retries"),
    ],
)
def test_rejects_invalid_arguments(
    tmp_path: Path, kwargs: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        lr.cleanup_old_versions(str(tmp_path / "missing"), **kwargs)
