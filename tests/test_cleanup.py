"""Tests for old-version cleanup helpers."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import lance_ray as lr
import pytest


def make_cleanup_stats(**overrides):
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
    values = {
        "bytes_removed": 0,
        "old_versions": 0,
        "data_files_removed": 0,
        "transaction_files_removed": 0,
        "index_files_removed": 0,
        "deletion_files_removed": 0,
    }
    values.update(overrides)
    return values


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

    class FakeAsyncResult:
        def __init__(self, results):
            self._results = results

        def get(self):
            return self._results

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

    def list_tables(request):
        captured["requests"].append(request)
        if request.page_token is None:
            return SimpleNamespace(tables=["table_a"], page_token="next")
        return SimpleNamespace(tables=["table_b"], page_token=None)

    namespace.list_tables.side_effect = list_tables

    class FakeAsyncResult:
        def __init__(self, results):
            self._results = results

        def get(self):
            return self._results

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

    assert [request.page_token for request in captured["requests"]] == [None, "next"]
    assert [request.id for request in captured["requests"]] == [["db"], ["db"]]
    assert [request.include_declared for request in captured["requests"]] == [
        False,
        False,
    ]
    assert results == [
        {
            "table_id": ["db", "table_a"],
            "stats": cleanup_stats_dict(bytes_removed=1, old_versions=1),
        },
        {
            "table_id": ["db", "table_b"],
            "stats": cleanup_stats_dict(bytes_removed=1, old_versions=1),
        },
    ]


def test_cleanup_database_old_versions_raises_on_table_failure(monkeypatch):
    namespace = MagicMock()
    namespace.list_tables.return_value = SimpleNamespace(
        tables=["table_a", "table_b", "table_c"],
        page_token=None,
    )

    class FakeAsyncResult:
        def __init__(self, results):
            self._results = results

        def get(self):
            return self._results

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

    with pytest.raises(
        RuntimeError,
        match=r"Cleanup failed: .*table_a.*boom table_a.*table_c.*boom table_c",
    ):
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
        )


def test_cleanup_database_old_versions_invalid_num_workers_raises():
    with pytest.raises(ValueError, match="num_workers.*positive"):
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
            num_workers=0,
        )


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

    class FakeAsyncResult:
        def get(self):
            raise RuntimeError("ray unavailable")

    class FakePool:
        def __init__(self, processes, ray_remote_args=None):
            self.closed = False
            self.joined = False

        def map_async(self, func, items, chunksize=1):
            return FakeAsyncResult()

        def close(self):
            self.closed = True

        def join(self):
            self.joined = True

    monkeypatch.setattr(
        "lance_ray.cleanup.get_or_create_namespace",
        lambda namespace_impl, namespace_properties: namespace,
    )
    monkeypatch.setattr("lance_ray.cleanup.Pool", FakePool)

    with pytest.raises(RuntimeError, match="Failed to complete distributed cleanup"):
        lr.cleanup_database_old_versions(
            database=["db"],
            namespace_impl="dir",
        )
