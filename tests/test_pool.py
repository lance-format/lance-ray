import logging

from lance_ray import pool as pool_mod


def test_init_global_pool_reuses_existing_pool(monkeypatch):
    events = []

    class FakePool:
        def __init__(self, processes, ray_remote_args):
            events.append(("init", processes, ray_remote_args))

        def close(self):
            events.append("close")

        def join(self):
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


def test_init_global_pool_warns_when_existing_pool_size_differs(monkeypatch, caplog):
    events = []

    class FakePool:
        def __init__(self, processes, ray_remote_args):
            events.append(("init", processes, ray_remote_args))

    pool_mod.clear_global_pool()
    monkeypatch.setattr(pool_mod, "Pool", FakePool)

    first_pool = pool_mod.init_global_pool(processes=4)
    with caplog.at_level(logging.WARNING, logger="lance_ray.pool"):
        second_pool = pool_mod.init_global_pool(processes=16)

    assert first_pool is second_pool
    assert "requested 16 workers will be ignored" in caplog.text

    pool_mod.clear_global_pool()


def test_get_or_create_pool_warns_when_global_pool_size_differs(caplog):
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


def test_set_global_pool_can_clear_without_closing():
    events = []

    class FakePool:
        def close(self):
            events.append("close")

        def join(self):
            events.append("join")

    pool = FakePool()
    pool_mod.set_global_pool(pool)
    assert pool_mod.get_global_pool() is pool

    pool_mod.set_global_pool(None)

    assert pool_mod.get_global_pool() is None
    assert events == []
