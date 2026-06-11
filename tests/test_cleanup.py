"""Tests for old-version cleanup helpers."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import lance_ray as lr
import pytest
from lance_ray.cleanup import _LIST_TABLES_PAGE_SIZE, CLEANUP_STATS_FIELDS


def make_cleanup_stats(**overrides):
    """Build a stand-in for ``lance.lance.CleanupStats`` (attribute access)."""
    values = {
        "bytes_removed": 0,
        "old_versions": 0,
        "data_files_removed": 0,
        "transaction_files_removed": 0,
        "index_files_removed": 0,
        "deletion_files_removed": 0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def cleanup_stats_dict(**overrides):
    """The dict form of ``make_cleanup_stats`` — kept in lock-step with it."""
    return dict(vars(make_cleanup_stats(**overrides)))


class FakeAsyncResult:
    """Stand-in for the ``AsyncResult`` returned by ``Pool.map_async``."""

    def __init__(self, results):
        self._results = results

    def get(self):
        return self._results


def test_cleanup_stats_fields_match_lance_cleanup_stats():
    """Guard that every field we read off ``CleanupStats`` actually exists.

    ``_cleanup_stats_to_dict`` does ``getattr(stats, field)`` for each name in
    ``CLEANUP_STATS_FIELDS``; if Lance renamed or removed one of those
    attributes this would raise ``AttributeError`` at runtime. This asserts the
    field list is a subset of the real attributes so such a drift fails loudly
    here instead. (It intentionally does not assert equality: Lance adding a new
    counter we don't expose is fine and shouldn't break this test.)
    """
    from lance.lance import CleanupStats

    real_fields = {attr for attr in dir(CleanupStats) if not attr.startswith("_")}
    missing = set(CLEANUP_STATS_FIELDS) - real_fields
    assert not missing, f"CLEANUP_STATS_FIELDS not present on CleanupStats: {missing}"


def test_cleanup_old_versions_passes_options_to_lance_dataset(monkeypatch):
    captured = {}
    stats = make_cleanup_stats(
        bytes_removed=10,
        old_versions=2,
        data_files_removed=3,
        transaction_files_removed=4,
        index_files_removed=5,
        deletion_files_removed=6,
    )

    class FakeDataset:
        def __init__(self, uri, **kwargs):
            captured["dataset"] = {"uri": uri, **kwargs}

        def cleanup_old_versions(self, **kwargs):
            captured["cleanup"] = kwargs
            return stats

    monkeypatch.setattr("lance_ray.cleanup.lance.LanceDataset", FakeDataset)

    result = lr.cleanup_old_versions(
        uri="s3://bucket/table.lance",
        older_than=timedelta(days=7),
        retain_versions=3,
        delete_unverified=True,
        error_if_tagged_old_versions=False,
        delete_rate_limit=100,
        storage_options={"region": "us-west-2"},
    )

    assert result is stats
    assert captured["dataset"] == {
        "uri": "s3://bucket/table.lance",
        "storage_options": {"region": "us-west-2"},
    }
    assert captured["cleanup"] == {
        "older_than": timedelta(days=7),
        "retain_versions": 3,
        "delete_unverified": True,
        "error_if_tagged_old_versions": False,
        "delete_rate_limit": 100,
    }


def test_cleanup_old_versions_resolves_namespace_storage_options(monkeypatch):
    captured = {}
    stats = make_cleanup_stats()

    class FakeDataset:
        def __init__(self, uri, **kwargs):
            captured["dataset"] = {"uri": uri, **kwargs}

        def cleanup_old_versions(self, **kwargs):
            return stats

    namespace = MagicMock()
    namespace.describe_table.return_value = SimpleNamespace(
        location="s3://bucket/ns/table.lance",
        storage_options={"secret_access_key": "namespace-secret"},
    )

    monkeypatch.setattr("lance_ray.cleanup.lance.LanceDataset", FakeDataset)
    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr(
        "lance_ray.cleanup.get_namespace_kwargs",
        lambda namespace_impl, namespace_properties, table_id: {
            "namespace_client": "fake-namespace",
            "table_id": table_id,
        },
    )

    result = lr.cleanup_old_versions(
        table_id=["db", "table"],
        storage_options={
            "access_key_id": "user-access",
            "secret_access_key": "user-secret",
        },
        namespace_impl="dir",
        namespace_properties={"root": "/tmp/tables"},
    )

    assert result is stats
    assert captured["dataset"] == {
        "uri": "s3://bucket/ns/table.lance",
        "storage_options": {
            "access_key_id": "user-access",
            "secret_access_key": "namespace-secret",
        },
        "namespace_client": "fake-namespace",
        "table_id": ["db", "table"],
    }
    request = namespace.describe_table.call_args.args[0]
    assert request.id == ["db", "table"]


def test_cleanup_old_versions_missing_namespace_location_raises(monkeypatch):
    namespace = MagicMock()
    namespace.describe_table.return_value = SimpleNamespace(
        location=None,
        storage_options=None,
    )

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )

    with pytest.raises(ValueError, match="dataset location"):
        lr.cleanup_old_versions(
            table_id=["db", "table"],
            namespace_impl="dir",
            namespace_properties={"root": "/tmp/tables"},
        )


def test_cleanup_old_versions_requires_uri_or_namespace():
    with pytest.raises(ValueError, match="Must provide either"):
        lr.cleanup_old_versions()


def test_cleanup_old_versions_rejects_uri_and_namespace_together():
    with pytest.raises(ValueError, match="Cannot provide both"):
        lr.cleanup_old_versions(
            uri="s3://bucket/table.lance",
            table_id=["db", "table"],
            namespace_impl="dir",
        )


@pytest.mark.parametrize("retain_versions", [0, -1])
def test_cleanup_old_versions_rejects_nonpositive_retain_versions(retain_versions):
    # retain_versions=0 triggers a Rust PanicException in Lance core (a
    # BaseException that escapes our worker's `except Exception`); guard early.
    with pytest.raises(ValueError, match="retain_versions.*positive"):
        lr.cleanup_old_versions(
            uri="s3://bucket/table.lance",
            retain_versions=retain_versions,
        )


def test_cleanup_old_versions_uses_safe_defaults(monkeypatch):
    # Pin the safety/policy defaults: a regression flipping delete_unverified to
    # True or error_if_tagged_old_versions to False must fail here.
    captured = {}
    stats = make_cleanup_stats()

    class FakeDataset:
        def __init__(self, uri, **kwargs):
            captured["dataset"] = {"uri": uri, **kwargs}

        def cleanup_old_versions(self, **kwargs):
            captured["cleanup"] = kwargs
            return stats

    monkeypatch.setattr("lance_ray.cleanup.lance.LanceDataset", FakeDataset)

    result = lr.cleanup_old_versions(uri="s3://bucket/table.lance")

    assert result is stats
    assert captured["cleanup"] == {
        "older_than": None,
        "retain_versions": None,
        "delete_unverified": False,
        "error_if_tagged_old_versions": True,
        "delete_rate_limit": None,
    }


def test_cleanup_database_old_versions_empty_database_raises():
    with pytest.raises(ValueError, match="database.*non-empty"):
        lr.cleanup_database_old_versions(database=[], namespace_impl="dir")


def test_cleanup_database_old_versions_missing_namespace_impl_raises():
    with pytest.raises(ValueError, match="namespace_impl.*required"):
        lr.cleanup_database_old_versions(database=["db"], namespace_impl="")


def test_cleanup_database_old_versions_empty_tables_returns_empty_list():
    namespace = MagicMock()
    namespace.list_tables.return_value = SimpleNamespace(tables=[], page_token=None)

    with patch(
        "lance_ray.cleanup.get_or_create_namespace",
        return_value=namespace,
    ):
        assert (
            lr.cleanup_database_old_versions(
                database=["db"],
                namespace_impl="dir",
                namespace_properties={"root": "/tmp/tables"},
            )
            == []
        )


def test_cleanup_database_old_versions_runs_tables_in_pool(monkeypatch):
    captured = {"cleanup_calls": []}
    stats_by_table = {
        ("db", "table_a"): make_cleanup_stats(bytes_removed=1, old_versions=1),
        ("db", "table_b"): make_cleanup_stats(bytes_removed=2, old_versions=2),
    }

    namespace = MagicMock()
    namespace.list_tables.return_value = SimpleNamespace(
        tables=["table_a", "table_b"],
        page_token=None,
    )

    class FakePool:
        def __init__(self, processes, ray_remote_args=None):
            captured["pool"] = {
                "processes": processes,
                "ray_remote_args": ray_remote_args,
            }

        def map_async(self, func, items, chunksize=1):
            captured["map"] = {"items": items, "chunksize": chunksize}
            return FakeAsyncResult([func(item) for item in items])

        def close(self):
            captured["closed"] = True

        def join(self):
            captured["joined"] = True

    def fake_cleanup_old_versions(**kwargs):
        captured["cleanup_calls"].append(kwargs)
        return stats_by_table[tuple(kwargs["table_id"])]

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr("lance_ray.cleanup.Pool", FakePool)
    monkeypatch.setattr(
        "lance_ray.cleanup.cleanup_old_versions",
        fake_cleanup_old_versions,
    )

    results = lr.cleanup_database_old_versions(
        database=["db"],
        namespace_impl="dir",
        namespace_properties={"root": "/tmp/tables"},
        older_than=timedelta(days=3),
        retain_versions=5,
        delete_unverified=True,
        error_if_tagged_old_versions=False,
        delete_rate_limit=50,
        num_workers=8,
        storage_options={"region": "us-west-2"},
        ray_remote_args={"num_cpus": 1},
    )

    assert captured["pool"] == {
        "processes": 2,
        "ray_remote_args": {"num_cpus": 1},
    }
    assert captured["map"] == {
        "items": [["db", "table_a"], ["db", "table_b"]],
        "chunksize": 1,
    }
    assert captured["closed"] is True
    assert captured["joined"] is True
    assert [tuple(call["table_id"]) for call in captured["cleanup_calls"]] == [
        ("db", "table_a"),
        ("db", "table_b"),
    ]
    assert all(
        call["older_than"] == timedelta(days=3) for call in captured["cleanup_calls"]
    )
    assert all(call["retain_versions"] == 5 for call in captured["cleanup_calls"])
    assert all(call["delete_unverified"] is True for call in captured["cleanup_calls"])
    assert all(
        call["error_if_tagged_old_versions"] is False
        for call in captured["cleanup_calls"]
    )
    assert all(call["delete_rate_limit"] == 50 for call in captured["cleanup_calls"])
    assert all(
        call["storage_options"] == {"region": "us-west-2"}
        for call in captured["cleanup_calls"]
    )
    assert all(call["namespace_impl"] == "dir" for call in captured["cleanup_calls"])
    assert all(
        call["namespace_properties"] == {"root": "/tmp/tables"}
        for call in captured["cleanup_calls"]
    )
    assert results == [
        {
            "table_id": ["db", "table_a"],
            "stats": cleanup_stats_dict(bytes_removed=1, old_versions=1),
        },
        {
            "table_id": ["db", "table_b"],
            "stats": cleanup_stats_dict(bytes_removed=2, old_versions=2),
        },
    ]


def test_cleanup_database_old_versions_paginates_tables(monkeypatch):
    captured = {"requests": []}
    stats = make_cleanup_stats(bytes_removed=1, old_versions=1)
    namespace = MagicMock()

    # Three pages, so an intermediate page_token must be threaded through more
    # than one iteration before the loop terminates.
    pages = {
        None: (["table_a"], "p2"),
        "p2": (["table_b"], "p3"),
        "p3": (["table_c"], None),
    }

    def list_tables(request):
        captured["requests"].append(request)
        tables, next_token = pages[request.page_token]
        return SimpleNamespace(tables=tables, page_token=next_token)

    namespace.list_tables.side_effect = list_tables

    class FakePool:
        def __init__(self, processes, ray_remote_args=None):
            pass

        def map_async(self, func, items, chunksize=1):
            return FakeAsyncResult([func(item) for item in items])

        def close(self):
            pass

        def join(self):
            pass

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr("lance_ray.cleanup.Pool", FakePool)
    monkeypatch.setattr(
        "lance_ray.cleanup.cleanup_old_versions",
        lambda **kwargs: stats,
    )

    results = lr.cleanup_database_old_versions(
        database=["db"],
        namespace_impl="dir",
    )

    assert [request.page_token for request in captured["requests"]] == [
        None,
        "p2",
        "p3",
    ]
    assert [request.id for request in captured["requests"]] == [["db"], ["db"], ["db"]]
    assert [request.include_declared for request in captured["requests"]] == [
        False,
        False,
        False,
    ]
    # Every page request uses the configured page size.
    assert [request.limit for request in captured["requests"]] == [
        _LIST_TABLES_PAGE_SIZE
    ] * 3
    assert results == [
        {
            "table_id": ["db", "table_a"],
            "stats": cleanup_stats_dict(bytes_removed=1, old_versions=1),
        },
        {
            "table_id": ["db", "table_b"],
            "stats": cleanup_stats_dict(bytes_removed=1, old_versions=1),
        },
        {
            "table_id": ["db", "table_c"],
            "stats": cleanup_stats_dict(bytes_removed=1, old_versions=1),
        },
    ]


def test_cleanup_database_old_versions_raises_on_table_failure(monkeypatch):
    namespace = MagicMock()
    namespace.list_tables.return_value = SimpleNamespace(
        tables=["table_a", "table_b", "table_c"],
        page_token=None,
    )

    class FakePool:
        def __init__(self, processes, ray_remote_args=None):
            pass

        def map_async(self, func, items, chunksize=1):
            return FakeAsyncResult([func(item) for item in items])

        def close(self):
            pass

        def join(self):
            pass

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr("lance_ray.cleanup.Pool", FakePool)

    def fake_cleanup_old_versions(**kwargs):
        if kwargs["table_id"] == ["db", "table_b"]:
            return make_cleanup_stats()
        raise RuntimeError(f"boom {kwargs['table_id'][-1]}")

    monkeypatch.setattr(
        "lance_ray.cleanup.cleanup_old_versions",
        fake_cleanup_old_versions,
    )

    # The aggregated message lists only the failed tables, by full table_id,
    # in input order, joined by "; ". The successful table_b is omitted.
    with pytest.raises(RuntimeError) as excinfo:
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
        )
    assert str(excinfo.value) == (
        "Cleanup failed: ['db', 'table_a']: boom table_a; "
        "['db', 'table_c']: boom table_c"
    )


def test_cleanup_database_old_versions_invalid_num_workers_raises():
    with pytest.raises(ValueError, match="num_workers.*positive"):
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
            num_workers=0,
        )


@pytest.mark.parametrize("retain_versions", [0, -1])
def test_cleanup_database_old_versions_rejects_nonpositive_retain_versions(
    retain_versions,
):
    with pytest.raises(ValueError, match="retain_versions.*positive"):
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
            retain_versions=retain_versions,
        )


def test_cleanup_database_old_versions_uses_safe_defaults(monkeypatch):
    # Pin the per-table safety/policy defaults AND the num_workers=4 default
    # (observable only with >4 tables, since processes = min(num_workers, n)).
    captured = {"calls": []}
    namespace = MagicMock()
    namespace.list_tables.return_value = SimpleNamespace(
        tables=["t1", "t2", "t3", "t4", "t5"],
        page_token=None,
    )

    class FakePool:
        def __init__(self, processes, ray_remote_args=None):
            captured["processes"] = processes
            captured["ray_remote_args"] = ray_remote_args

        def map_async(self, func, items, chunksize=1):
            return FakeAsyncResult([func(item) for item in items])

        def close(self):
            pass

        def join(self):
            pass

    def fake_cleanup_old_versions(**kwargs):
        captured["calls"].append(kwargs)
        return make_cleanup_stats()

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr("lance_ray.cleanup.Pool", FakePool)
    monkeypatch.setattr(
        "lance_ray.cleanup.cleanup_old_versions",
        fake_cleanup_old_versions,
    )

    lr.cleanup_database_old_versions(database=["db"], namespace_impl="dir")

    assert captured["processes"] == 4
    assert captured["ray_remote_args"] is None
    assert len(captured["calls"]) == 5
    for call in captured["calls"]:
        assert call["older_than"] is None
        assert call["retain_versions"] is None
        assert call["delete_unverified"] is False
        assert call["error_if_tagged_old_versions"] is True
        assert call["delete_rate_limit"] is None
        assert call["storage_options"] is None


def test_cleanup_database_old_versions_namespace_creation_failure_raises(monkeypatch):
    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: None,
    )

    with pytest.raises(RuntimeError, match="Failed to create namespace"):
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
        )


def test_cleanup_database_old_versions_pool_get_failure_raises(monkeypatch):
    namespace = MagicMock()
    namespace.list_tables.return_value = SimpleNamespace(
        tables=["table_a"],
        page_token=None,
    )

    captured = {}

    class FailingAsyncResult:
        def get(self):
            raise RuntimeError("ray unavailable")

    class FakePool:
        def __init__(self, processes, ray_remote_args=None):
            self.closed = False
            self.joined = False
            captured["pool"] = self

        def map_async(self, func, items, chunksize=1):
            return FailingAsyncResult()

        def close(self):
            self.closed = True

        def join(self):
            self.joined = True

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr("lance_ray.cleanup.Pool", FakePool)

    with pytest.raises(
        RuntimeError, match="Failed to complete distributed cleanup"
    ) as excinfo:
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
        )

    # The original cause is preserved via `raise ... from e`.
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == "ray unavailable"

    # The pool must be released even when map_async().get() raises (the finally
    # block), otherwise the Ray pool would leak on failure.
    assert captured["pool"].closed is True
    assert captured["pool"].joined is True
