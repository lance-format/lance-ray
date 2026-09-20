"""Tests for LanceDatasource namespace option resolution."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from types import ModuleType, SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from lance_ray import datasource as datasource_mod


def test_read_namespace_uses_described_location_and_storage_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class DescribeTableRequest:
        def __init__(
            self,
            id: list[str],
            version: int | None = None,
            vend_credentials: bool | None = None,
        ) -> None:
            self.id = id
            self.version = version
            self.vend_credentials = vend_credentials

    class FakeNamespace:
        def describe_table(self, request: DescribeTableRequest) -> SimpleNamespace:
            captured["describe_table_request"] = request
            return SimpleNamespace(
                location="s3://bucket1/lance_minio_catalog/schema/my_table32/",
                storage_options=(
                    {
                        "endpoint": "http://localhost:9000",
                        "allow_http": "true",
                        "secret_access_key": "namespace-secret",
                    }
                    if request.vend_credentials
                    else None
                ),
                managed_versioning=True,
            )

    lance_namespace = ModuleType("lance_namespace")
    # Attribute is set on a dynamically created stub module; mypy cannot model it.
    lance_namespace.DescribeTableRequest = DescribeTableRequest  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lance_namespace", lance_namespace)

    import lance

    class FakeScanner:
        def count_rows(self) -> int:
            return 1

    class FakeFragment:
        schema = pa.schema([("value", pa.int64())])
        metadata = SimpleNamespace(id=7)

        def data_files(self) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(path="s3://bucket1/fragment.lance", file_size_bytes=1)
            ]

    class FakeLanceDataset:
        def __init__(self, **kwargs: Any) -> None:
            captured["dataset_kwargs"] = kwargs
            self.uri = kwargs["uri"]
            self.version = kwargs.get("version", 1)
            self.initial_storage_options = kwargs["storage_options"]
            self._ds = SimpleNamespace(serialized_manifest=lambda: b"manifest")

        def get_fragments(self) -> list[FakeFragment]:
            return [FakeFragment()]

        def scanner(self, **kwargs: Any) -> FakeScanner:
            captured["scanner_kwargs"] = kwargs
            return FakeScanner()

    monkeypatch.setattr(lance, "LanceDataset", FakeLanceDataset)

    monkeypatch.setattr(
        datasource_mod,
        "get_or_create_namespace",
        lambda namespace_impl, namespace_properties: FakeNamespace(),
    )
    monkeypatch.setattr(
        datasource_mod,
        "get_namespace_kwargs",
        lambda namespace_impl, namespace_properties, table_id: {
            "namespace_client": "fake-namespace-client",
            "table_id": table_id,
        },
    )

    def read_fragments_with_retry(
        fragment_ids: Any,
        uri: Any,
        version: Any,
        storage_options: Any,
        manifest: Any,
        namespace_impl: Any,
        namespace_properties: Any,
        table_id: Any,
        base_store_params: Any,
        namespace_client_managed_versioning: Any,
        scanner_options: Any,
        retry_params: Any,
        with_metadata: Any,
    ) -> Iterator[Any]:
        captured["worker_args"] = {
            "fragment_ids": fragment_ids,
            "uri": uri,
            "version": version,
            "storage_options": storage_options,
            "manifest": manifest,
            "namespace_impl": namespace_impl,
            "namespace_properties": namespace_properties,
            "table_id": table_id,
            "base_store_params": base_store_params,
            "namespace_client_managed_versioning": namespace_client_managed_versioning,
            "scanner_options": scanner_options,
            "with_metadata": with_metadata,
        }
        return iter(())

    monkeypatch.setattr(
        datasource_mod,
        "_read_fragments_with_retry",
        read_fragments_with_retry,
    )

    datasource = datasource_mod.LanceDatasource(
        table_id=["lance_minio_catalog", "schema", "my_table32"],
        storage_options={
            "access_key_id": "user-access",
            "secret_access_key": "user-secret",
        },
        dataset_options={"version": 3},
        namespace_impl="rest",
        namespace_properties={"uri": "http://catalog.example"},
    )

    assert datasource.lance_dataset.uri == (
        "s3://bucket1/lance_minio_catalog/schema/my_table32/"
    )
    assert captured["describe_table_request"].id == [
        "lance_minio_catalog",
        "schema",
        "my_table32",
    ]
    assert captured["describe_table_request"].version == 3
    assert captured["describe_table_request"].vend_credentials is True
    assert captured["dataset_kwargs"] == {
        "uri": "s3://bucket1/lance_minio_catalog/schema/my_table32/",
        "version": 3,
        "storage_options": {
            "access_key_id": "user-access",
            "secret_access_key": "namespace-secret",
            "endpoint": "http://localhost:9000",
            "allow_http": "true",
        },
        "namespace_client": "fake-namespace-client",
        "table_id": ["lance_minio_catalog", "schema", "my_table32"],
        "namespace_client_managed_versioning": True,
    }
    read_tasks = datasource.get_read_tasks(1)
    assert len(read_tasks) == 1
    assert list(read_tasks[0].read_fn()) == []
    assert captured["worker_args"]["storage_options"] == {
        "access_key_id": "user-access",
        "secret_access_key": "namespace-secret",
        "endpoint": "http://localhost:9000",
        "allow_http": "true",
    }
    assert captured["worker_args"]["fragment_ids"] == [7]
    assert captured["worker_args"]["uri"] == (
        "s3://bucket1/lance_minio_catalog/schema/my_table32/"
    )
    assert captured["worker_args"]["version"] == 3
    assert captured["worker_args"]["namespace_impl"] == "rest"
    assert captured["worker_args"]["namespace_properties"] == {
        "uri": "http://catalog.example"
    }
    assert captured["worker_args"]["table_id"] == [
        "lance_minio_catalog",
        "schema",
        "my_table32",
    ]
    assert captured["worker_args"]["namespace_client_managed_versioning"] is True


def test_direct_uri_read_keeps_minio_storage_options_on_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    import lance

    class FakeScanner:
        def count_rows(self) -> int:
            return 1

    class FakeFragment:
        schema = pa.schema([("value", pa.int64())])
        metadata = SimpleNamespace(id=11)

        def data_files(self) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(path="s3://bucket1/direct-fragment", file_size_bytes=1)
            ]

    class FakeDataset:
        def __init__(self, **kwargs: Any) -> None:
            captured["dataset_kwargs"] = kwargs
            self.uri = kwargs["uri"]
            self.version = 4
            self.initial_storage_options = kwargs["storage_options"]
            self._ds = SimpleNamespace(serialized_manifest=lambda: b"direct-manifest")

        def get_fragments(self) -> list[FakeFragment]:
            return [FakeFragment()]

        def scanner(self, **kwargs: Any) -> FakeScanner:
            captured["scanner_kwargs"] = kwargs
            return FakeScanner()

    def dataset(**kwargs: Any) -> FakeDataset:
        return FakeDataset(**kwargs)

    monkeypatch.setattr(lance, "dataset", dataset)

    def read_fragments_with_retry(
        fragment_ids: Any,
        uri: Any,
        version: Any,
        storage_options: Any,
        manifest: Any,
        namespace_impl: Any,
        namespace_properties: Any,
        table_id: Any,
        base_store_params: Any,
        namespace_client_managed_versioning: Any,
        scanner_options: Any,
        retry_params: Any,
        with_metadata: Any,
    ) -> Iterator[Any]:
        captured["worker_args"] = {
            "fragment_ids": fragment_ids,
            "uri": uri,
            "version": version,
            "storage_options": storage_options,
            "manifest": manifest,
            "namespace_impl": namespace_impl,
            "namespace_properties": namespace_properties,
            "table_id": table_id,
            "base_store_params": base_store_params,
            "namespace_client_managed_versioning": namespace_client_managed_versioning,
            "scanner_options": scanner_options,
            "with_metadata": with_metadata,
        }
        return iter(())

    monkeypatch.setattr(
        datasource_mod,
        "_read_fragments_with_retry",
        read_fragments_with_retry,
    )

    storage_options = {
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "endpoint": "http://localhost:9000",
        "allow_http": "true",
    }
    datasource = datasource_mod.LanceDatasource(
        uri="s3://bucket1/direct_table.lance",
        storage_options=storage_options,
    )

    read_tasks = datasource.get_read_tasks(1)

    assert captured["dataset_kwargs"] == {
        "uri": "s3://bucket1/direct_table.lance",
        "storage_options": storage_options,
    }
    assert len(read_tasks) == 1
    assert list(read_tasks[0].read_fn()) == []
    assert captured["worker_args"]["fragment_ids"] == [11]
    assert captured["worker_args"]["uri"] == "s3://bucket1/direct_table.lance"
    assert captured["worker_args"]["version"] == 4
    assert captured["worker_args"]["storage_options"] == storage_options
    assert captured["worker_args"]["manifest"] == b"direct-manifest"
    assert captured["worker_args"]["namespace_impl"] is None
    assert captured["worker_args"]["namespace_properties"] is None
    assert captured["worker_args"]["table_id"] is None
    assert captured["worker_args"]["namespace_client_managed_versioning"] is False


def test_worker_reconstruction_passes_managed_versioning_to_lance_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    import lance

    class FakeLanceDataset:
        def __init__(self, **kwargs: Any) -> None:
            captured["dataset_kwargs"] = kwargs

    monkeypatch.setattr(lance, "LanceDataset", FakeLanceDataset)
    monkeypatch.setattr(
        datasource_mod,
        "get_namespace_kwargs",
        lambda namespace_impl, namespace_properties, table_id: {
            "namespace_client": "fake-namespace-client",
            "table_id": table_id,
        },
    )
    monkeypatch.setattr(
        datasource_mod,
        "_read_fragments",
        lambda fragment_ids, lance_ds, scanner_options, with_metadata: iter(()),
    )

    result = datasource_mod._read_fragments_with_retry(
        fragment_ids=[7],
        uri="s3://bucket1/lance_minio_catalog/schema/my_table32/",
        version=3,
        storage_options={
            "access_key_id": "user-access",
            "secret_access_key": "namespace-secret",
            "endpoint": "http://localhost:9000",
            "allow_http": "true",
        },
        manifest=b"manifest",
        namespace_impl="rest",
        namespace_properties={"uri": "http://catalog.example"},
        table_id=["lance_minio_catalog", "schema", "my_table32"],
        base_store_params=None,
        namespace_client_managed_versioning=True,
        scanner_options={},
        retry_params={
            "description": "test read",
            "match": [],
            "max_attempts": 1,
            "max_backoff_s": 0,
        },
    )

    assert list(result) == []
    assert captured["dataset_kwargs"] == {
        "uri": "s3://bucket1/lance_minio_catalog/schema/my_table32/",
        "version": 3,
        "storage_options": {
            "access_key_id": "user-access",
            "secret_access_key": "namespace-secret",
            "endpoint": "http://localhost:9000",
            "allow_http": "true",
        },
        "serialized_manifest": b"manifest",
        "namespace_client": "fake-namespace-client",
        "table_id": ["lance_minio_catalog", "schema", "my_table32"],
        "namespace_client_managed_versioning": True,
    }


def test_namespace_describe_only_receives_integer_dataset_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class DescribeTableRequest:
        def __init__(
            self,
            id: list[str],
            version: int | None = None,
            vend_credentials: bool | None = None,
        ) -> None:
            self.id = id
            self.version = version
            self.vend_credentials = vend_credentials

    class FakeNamespace:
        def describe_table(self, request: DescribeTableRequest) -> SimpleNamespace:
            captured["describe_table_request"] = request
            return SimpleNamespace(
                location="s3://bucket1/tagged_table/",
                storage_options=None,
            )

    lance_namespace = ModuleType("lance_namespace")
    # Attribute is set on a dynamically created stub module; mypy cannot model it.
    lance_namespace.DescribeTableRequest = DescribeTableRequest  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lance_namespace", lance_namespace)

    import lance

    class FakeLanceDataset:
        def __init__(self, **kwargs: Any) -> None:
            captured["dataset_kwargs"] = kwargs
            self.uri = kwargs["uri"]
            self.version = kwargs["version"]
            self.initial_storage_options = kwargs["storage_options"]

        def get_fragments(self) -> list[Any]:
            return []

    monkeypatch.setattr(lance, "LanceDataset", FakeLanceDataset)
    monkeypatch.setattr(
        datasource_mod,
        "get_or_create_namespace",
        lambda namespace_impl, namespace_properties: FakeNamespace(),
    )
    monkeypatch.setattr(
        datasource_mod,
        "get_namespace_kwargs",
        lambda namespace_impl, namespace_properties, table_id: {
            "namespace_client": "fake-namespace-client",
            "table_id": table_id,
        },
    )

    datasource = datasource_mod.LanceDatasource(
        table_id=["tagged_table"],
        dataset_options={"version": "release-tag"},
        namespace_impl="rest",
        namespace_properties={"uri": "http://catalog.example"},
    )

    assert datasource.lance_dataset.uri == "s3://bucket1/tagged_table/"
    assert captured["describe_table_request"].version is None
    assert captured["describe_table_request"].vend_credentials is True
    assert captured["dataset_kwargs"]["version"] == "release-tag"
