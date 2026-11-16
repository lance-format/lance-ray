import logging
from pathlib import Path
from typing import Any, Optional

import lance
import lance_ray as lr
import pyarrow as pa
import pytest
from lance.optimize import CompactionOptions
from lance_ray import pool as pool_mod
from ray.util.multiprocessing import Pool


def test_init_global_pool_reuses_existing_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str | tuple[str, int, Optional[dict[str, Any]]]] = []

    class FakePool:
        def __init__(
            self, processes: int, ray_remote_args: Optional[dict[str, Any]]
        ) -> None:
            events.append(("init", processes, ray_remote_args))

        def close(self) -> None:
            events.append("close")

        def join(self) -> None:
            events.append("join")

    pool_mod.clear_global_pool()
    monkeypatch.setattr(pool_mod, "Pool", FakePool)

    first_pool = pool_mod.init_global_pool(
        processes=3,
        ray_remote_args={"num_cpus": 2},
    )
    second_pool = pool_mod.init_global_pool(
        processes=5,
        ray_remote_args={"num_cpus": 4},
    )

    assert first_pool is second_pool
    assert events == [("init", 3, {"num_cpus": 2})]

    pool_mod.clear_global_pool(close=True)
    assert events == [
        ("init", 3, {"num_cpus": 2}),
        "close",
        "join",
    ]


def test_init_global_pool_warns_when_existing_pool_size_differs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    events: list[tuple[str, int, Optional[dict[str, Any]]]] = []

    class FakePool:
        def __init__(
            self, processes: int, ray_remote_args: Optional[dict[str, Any]] = None
        ) -> None:
            events.append(("init", processes, ray_remote_args))

    pool_mod.clear_global_pool()
    monkeypatch.setattr(pool_mod, "Pool", FakePool)

    first_pool = pool_mod.init_global_pool(processes=4)
    with caplog.at_level(logging.WARNING, logger="lance_ray.pool"):
        second_pool = pool_mod.init_global_pool(processes=16)

    assert first_pool is second_pool
    assert "requested 16 workers will be ignored" in caplog.text

    pool_mod.clear_global_pool()


def test_get_or_create_pool_warns_when_global_pool_size_differs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakePool:
        processes = 4

    pool_mod.set_global_pool(FakePool())
    try:
        with (
            caplog.at_level(logging.WARNING, logger="lance_ray.pool"),
            pool_mod.get_or_create_pool(processes=16, ray_remote_args=None) as pool,
        ):
            assert pool is pool_mod.get_global_pool()
    finally:
        pool_mod.clear_global_pool()

    assert "global Ray Pool with 4 processes" in caplog.text
    assert "requested 16 workers will be ignored" in caplog.text


def test_get_or_create_pool_warns_when_remote_args_ignored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakePool:
        processes = 4

    pool_mod.set_global_pool(FakePool())
    try:
        with (
            caplog.at_level(logging.WARNING, logger="lance_ray.pool"),
            pool_mod.get_or_create_pool(
                processes=4, ray_remote_args={"num_cpus": 2}
            ) as pool,
        ):
            assert pool is pool_mod.get_global_pool()
    finally:
        pool_mod.clear_global_pool()

    assert "per-call ray_remote_args" in caplog.text
    assert "are ignored" in caplog.text


def test_get_or_create_pool_handles_none_processes_with_global_pool(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakePool:
        processes = 4

    pool_mod.set_global_pool(FakePool())
    try:
        with (
            caplog.at_level(logging.WARNING, logger="lance_ray.pool"),
            pool_mod.get_or_create_pool(processes=None, ray_remote_args=None) as pool,
        ):
            assert pool is pool_mod.get_global_pool()
    finally:
        pool_mod.clear_global_pool()

    # processes=None must not raise and must not emit a size-mismatch warning.
    assert "workers will be ignored" not in caplog.text


def test_set_global_pool_can_clear_without_closing() -> None:
    events: list[str] = []

    class FakePool:
        def close(self) -> None:
            events.append("close")

        def join(self) -> None:
            events.append("join")

    pool = FakePool()
    pool_mod.set_global_pool(pool)
    assert pool_mod.get_global_pool() is pool

    pool_mod.set_global_pool(None)

    assert pool_mod.get_global_pool() is None
    assert events == []


def test_compact_files_and_add_columns_reuse_global_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri = str(tmp_path / "global_pool.lance")
    lance.write_dataset(pa.table({"id": list(range(8))}), uri, max_rows_per_file=2)

    global_pool = pool_mod.init_global_pool(processes=2)

    def no_local_pool(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("a local Pool was created while a global Pool is set")

    # Patch the class itself: modules that imported ``Pool`` by name share it.
    monkeypatch.setattr(Pool, "__init__", no_local_pool)
    try:
        lr.compact_files(
            uri,
            compaction_options=CompactionOptions(target_rows_per_fragment=100),
            num_workers=2,
        )
        assert len(lance.dataset(uri).get_fragments()) == 1

        lr.add_columns(uri, transform={"doubled": "id * 2"}, concurrency=2)
        table = lance.dataset(uri).to_table()
        assert table["doubled"].to_pylist() == [i * 2 for i in range(8)]

        # The operations must leave the global Pool registered and open.
        assert global_pool.map(abs, [-1]) == [1]
        assert pool_mod.get_global_pool() is global_pool
    finally:
        pool_mod.clear_global_pool(close=True)
