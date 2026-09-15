"""Tests for update_columns_from."""

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import lance
import lance_ray as lr
import lance_ray.datasource as datasource_module
import lance_ray.io as lance_io
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ray
from lance.dataset import LanceDataset
from lance_ray.datasource import (
    LanceDatasource,
    dataset_identity,
    dataset_identity_digest,
    normalize_dataset_uri,
)
from ray.data import Dataset, Schema
from ray.data.block import DataBatch
from ray.exceptions import RayTaskError

import pandas as pd


@pytest.fixture
def multi_fragment_path(tmp_path: Path) -> Path:
    path = tmp_path / "update_columns.lance"
    data = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Dave"],
            "value": [10, 20, 30, 40],
        }
    )
    lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=2)
    return path


def test_update_columns_from_updates_partial_rows(
    multi_fragment_path: Path,
) -> None:
    def update_selected_rows(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame.loc[frame["id"] == 1, "value"] = 101
        frame.loc[frame["id"] == 3, "value"] = 303
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_selected_rows, batch_format="pandas"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )

    assert result["value"].tolist() == [101, 20, 303, 40]
    assert result["name"].tolist() == ["Alice", "Bob", "Charlie", "Dave"]


def test_update_columns_from_updates_filtered_source_rows(
    multi_fragment_path: Path,
) -> None:
    def update_id_one(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        selected = table.filter(
            pc.equal(
                table.column("id"),
                pa.scalar(1, type=table.schema.field("id").type),
            )
        )
        return selected.set_column(
            selected.schema.get_field_index("value"),
            "value",
            pa.array(
                [101] * selected.num_rows, type=selected.schema.field("value").type
            ),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_id_one, batch_format="pyarrow"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )

    assert result["value"].tolist() == [101, 20, 30, 40]
    assert result["name"].tolist() == ["Alice", "Bob", "Charlie", "Dave"]


def test_update_columns_from_preserves_row_metadata(
    multi_fragment_path: Path,
) -> None:
    metadata_before = (
        lr.read_lance(str(multi_fragment_path), with_metadata=True)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)[["id", "_rowaddr", "_fragid"]]
    )

    def update_id_one(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        selected = table.filter(
            pc.equal(
                table.column("id"),
                pa.scalar(1, type=table.schema.field("id").type),
            )
        )
        return selected.set_column(
            selected.schema.get_field_index("value"),
            "value",
            pa.array(
                [101] * selected.num_rows, type=selected.schema.field("value").type
            ),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_id_one, batch_format="pyarrow"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    metadata_after = (
        lr.read_lance(str(multi_fragment_path), with_metadata=True)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)[["id", "_rowaddr", "_fragid"]]
    )

    assert metadata_after.equals(metadata_before)


def test_update_columns_from_surfaces_stale_version_conflict(
    multi_fragment_path: Path,
) -> None:
    old_version = lance.dataset(str(multi_fragment_path)).version

    def update_id_one(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        selected = table.filter(
            pc.equal(
                table.column("id"),
                pa.scalar(1, type=table.schema.field("id").type),
            )
        )
        return selected.set_column(
            selected.schema.get_field_index("value"),
            "value",
            pa.array(
                [999] * selected.num_rows, type=selected.schema.field("value").type
            ),
        )

    stale_source = lr.read_lance(
        str(multi_fragment_path),
        dataset_options={"version": old_version},
        with_metadata=True,
    ).map_batches(update_id_one, batch_format="pyarrow")

    lance.dataset(str(multi_fragment_path)).update({"value": "101"})
    newer_dataset = lance.dataset(str(multi_fragment_path))

    with pytest.raises(Exception, match="(?i)(conflict|preempted)"):
        lr.update_columns_from(
            str(multi_fragment_path),
            stale_source,
            columns=["value"],
            read_version=old_version,
        )

    latest_dataset = lance.dataset(str(multi_fragment_path))
    assert latest_dataset.version == newer_dataset.version
    assert latest_dataset.to_table().sort_by("id").column("value").to_pylist() == [
        101,
        101,
        101,
        101,
    ]


def test_update_columns_from_defaults_to_source_version_after_rename(
    multi_fragment_path: Path,
) -> None:
    def update_id_one(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        selected = table.filter(
            pc.equal(
                table.column("id"),
                pa.scalar(1, type=table.schema.field("id").type),
            )
        )
        return selected.set_column(
            selected.schema.get_field_index("value"),
            "value",
            pa.array(
                [999] * selected.num_rows, type=selected.schema.field("value").type
            ),
        )

    stale_source = lr.read_lance(
        str(multi_fragment_path),
        with_metadata=True,
    ).map_batches(update_id_one, batch_format="pyarrow")
    stale_source._set_name("caller-owned-name")

    lance.dataset(str(multi_fragment_path)).update({"value": "101"})
    newer_dataset = lance.dataset(str(multi_fragment_path))

    with pytest.raises(Exception, match="(?i)(conflict|preempted)"):
        lr.update_columns_from(
            str(multi_fragment_path),
            stale_source,
            columns=["value"],
        )

    latest_dataset = lance.dataset(str(multi_fragment_path))
    assert latest_dataset.version == newer_dataset.version
    assert latest_dataset.to_table().sort_by("id").column("value").to_pylist() == [
        101,
        101,
        101,
        101,
    ]


def test_update_columns_from_defaults_to_union_source_version(
    multi_fragment_path: Path,
) -> None:
    def update_values(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array([999] * table.num_rows, type=table.schema.field("value").type),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)
    stale_source = (
        source.filter(lambda row: row["id"] <= 2)
        .union(source.filter(lambda row: row["id"] > 2))
        .map_batches(update_values, batch_format="pyarrow")
    )

    lance.dataset(str(multi_fragment_path)).update({"value": "101"})
    newer_dataset = lance.dataset(str(multi_fragment_path))

    with pytest.raises(Exception, match="(?i)(conflict|preempted)"):
        lr.update_columns_from(
            str(multi_fragment_path),
            stale_source,
            columns=["value"],
        )

    latest_dataset = lance.dataset(str(multi_fragment_path))
    assert latest_dataset.version == newer_dataset.version
    assert latest_dataset.to_table().sort_by("id").column("value").to_pylist() == [
        101,
        101,
        101,
        101,
    ]


def test_update_columns_from_rejects_union_of_different_source_versions(
    multi_fragment_path: Path,
) -> None:
    old_source = lr.read_lance(str(multi_fragment_path), with_metadata=True).filter(
        lambda row: row["id"] <= 2
    )

    lance.dataset(str(multi_fragment_path)).update({"value": "101"})
    latest_dataset = lance.dataset(str(multi_fragment_path))
    new_source = lr.read_lance(str(multi_fragment_path), with_metadata=True).filter(
        lambda row: row["id"] > 2
    )
    mixed_source = old_source.union(new_source)

    with pytest.raises(ValueError, match="multiple Lance source versions"):
        lr.update_columns_from(
            str(multi_fragment_path),
            mixed_source,
            columns=["value"],
        )

    result = lance.dataset(str(multi_fragment_path))
    assert result.version == latest_dataset.version
    assert result.to_table().sort_by("id").column("value").to_pylist() == [
        101,
        101,
        101,
        101,
    ]


def test_update_columns_from_requires_version_without_source_lineage(
    multi_fragment_path: Path,
) -> None:
    def update_values(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array([999] * table.num_rows, type=table.schema.field("value").type),
        )

    read_version = lance.dataset(str(multi_fragment_path)).version
    materialized_source = lr.read_lance(
        str(multi_fragment_path), with_metadata=True
    ).materialize()
    source = materialized_source.map_batches(update_values, batch_format="pyarrow")

    with pytest.raises(ValueError, match="'read_version' is required"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        read_version=read_version,
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [999, 999, 999, 999]


def test_update_columns_from_source_lineage_does_not_expose_uri(
    multi_fragment_path: Path,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)
    logical_plan = cast(Any, source)._logical_plan
    source_name = logical_plan.sources()[0].name

    assert str(multi_fragment_path) not in source_name
    assert "lance_ray_source_id=" in source_name

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [10, 20, 30, 40]


def test_dataset_identity_digest_requires_reliable_backend() -> None:
    class FakeLanceDataset:
        uri = "az://container/table"
        _ds = object()

    assert dataset_identity_digest(FakeLanceDataset()) is None
    assert (
        dataset_identity_digest(
            FakeLanceDataset(),
            storage_options={"endpoint": "http://minio:9000"},
        )
        is not None
    )


@pytest.mark.parametrize("scheme", ["s3", "gs"])
def test_dataset_identity_digest_distinguishes_global_cloud_buckets(
    scheme: str,
) -> None:
    class FakeLanceDataset:
        _ds = object()

        def __init__(self, dataset_uri: str) -> None:
            self.uri = dataset_uri

    first = dataset_identity_digest(FakeLanceDataset(f"{scheme}://first/table"))
    second = dataset_identity_digest(FakeLanceDataset(f"{scheme}://second/table"))

    assert first is not None
    assert second is not None
    assert first != second


def test_dataset_identity_excludes_endpoint_credentials() -> None:
    class FakeLanceDataset:
        uri = "s3://bucket/table"
        _ds = object()

    first = dataset_identity(
        FakeLanceDataset(),
        storage_options={
            "endpoint_url": "https://first:secret@minio.example:9000/api?token=one"
        },
    )
    second = dataset_identity(
        FakeLanceDataset(),
        storage_options={
            "endpoint_url": "https://second:rotated@minio.example:9000/api?token=two"
        },
    )

    assert first == second
    assert "first" not in first
    assert "secret" not in first
    assert "token" not in first


def test_datasource_caches_unavailable_source_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeLanceDataset:
        version = 7

    calls = 0

    def unavailable_identity(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(
        datasource_module,
        "dataset_identity_digest",
        unavailable_identity,
    )
    datasource = object.__new__(LanceDatasource)
    datasource._source_provenance = None
    datasource._lance_ds = cast(Any, FakeLanceDataset())
    datasource._storage_options = None

    assert datasource.source_identity is None
    assert datasource.source_identity is None
    assert datasource.source_version == 7
    assert calls == 1


def test_normalize_dataset_uri_treats_file_scheme_as_local_path(
    multi_fragment_path: Path,
) -> None:
    local_uri = str(multi_fragment_path)
    file_uri = f"file://{multi_fragment_path}"

    assert normalize_dataset_uri(file_uri) == normalize_dataset_uri(local_uri)


def test_normalize_dataset_uri_handles_windows_drive_paths() -> None:
    normalized = normalize_dataset_uri("C:\\Data\\Table.lance\\")

    assert normalized == normalize_dataset_uri("c:/data/table.lance")
    assert normalized == normalize_dataset_uri("file:///C:/Data/Table.lance/")


def test_update_columns_from_fails_before_partitioning_without_reliable_identity(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = lance.dataset(str(multi_fragment_path)).version
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)
    monkeypatch.setattr(
        lance_io,
        "_source_provenance_from_dataset_lineage",
        lambda _: (version, None),
    )

    def fail_to_arrow_refs(self: Dataset) -> list[Any]:
        raise AssertionError("partitioning must not start before provenance validation")

    monkeypatch.setattr(Dataset, "to_arrow_refs", fail_to_arrow_refs)

    with pytest.raises(ValueError, match="reliable dataset identity"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_source_version_mismatch(
    multi_fragment_path: Path,
) -> None:
    old_version = lance.dataset(str(multi_fragment_path)).version
    lance.dataset(str(multi_fragment_path)).update({"value": "101"})

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="Source Dataset was read from Lance version"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
            read_version=old_version,
        )


def test_update_columns_from_projects_columns_before_partitioning(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partitioned_column_names: list[list[str]] = []
    original_to_arrow_refs = Dataset.to_arrow_refs

    def capturing_to_arrow_refs(
        self: Dataset,
    ) -> list[Any]:
        schema = self.schema()
        partitioned_column_names.append([] if schema is None else list(schema.names))
        return cast(list[Any], original_to_arrow_refs(self))

    monkeypatch.setattr(Dataset, "to_arrow_refs", capturing_to_arrow_refs)

    def add_unused_and_update(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        updated = table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array([999] * table.num_rows, type=table.schema.field("value").type),
        )
        return updated.append_column(
            "embedding",
            pa.array([[0.0] * 8] * table.num_rows, type=pa.list_(pa.float32(), 8)),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        add_unused_and_update,
        batch_format="pyarrow",
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    assert partitioned_column_names
    assert set(partitioned_column_names[-1]) == {"_fragid", "_rowaddr", "value"}

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [999, 999, 999, 999]


def test_update_columns_from_does_not_use_groupby(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Avoid a fixed groupby signature because Ray Data's public API varies by version.
    def fail_groupby(*args: object, **kwargs: object) -> object:
        raise AssertionError("update_columns_from must not use groupby")

    monkeypatch.setattr(Dataset, "groupby", fail_groupby)

    def update_values(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] += 100
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_values,
        batch_format="pandas",
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [110, 120, 130, 140]


def test_update_columns_from_does_not_consume_source_with_take(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_take(self: Dataset, limit: int = 20) -> list[dict[str, Any]]:
        raise AssertionError(
            "update_columns_from must not consume the source with take"
        )

    monkeypatch.setattr(Dataset, "take", fail_take)

    def update_values(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] += 100
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_values,
        batch_format="pandas",
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [110, 120, 130, 140]


def test_update_columns_from_streams_multiple_refs_for_one_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "one_fragment.lance"
    lance.write_dataset(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6], type=pa.int64()),
                "value": pa.array([10, 20, 30, 40, 50, 60], type=pa.int64()),
            }
        ),
        str(path),
        max_rows_per_file=6,
    )

    def update_values(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] += 100
        return frame

    source = lr.read_lance(
        str(path),
        with_metadata=True,
        override_num_blocks=3,
    ).map_batches(update_values, batch_format="pandas")

    captured_block_refs: list[Any] = []
    original_to_arrow_refs = Dataset.to_arrow_refs

    def capturing_to_arrow_refs(self: Dataset) -> list[Any]:
        refs = original_to_arrow_refs(self)
        captured_block_refs.extend(refs)
        return cast(list[Any], refs)

    monkeypatch.setattr(Dataset, "to_arrow_refs", capturing_to_arrow_refs)

    lr.update_columns_from(
        str(path),
        source,
        columns=["value"],
    )

    assert len(captured_block_refs) >= 2
    result = lance.dataset(str(path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [110, 120, 130, 140, 150, 160]


def test_update_columns_from_rejects_different_source_dataset(
    tmp_path: Path,
) -> None:
    def write_dataset(name: str) -> Path:
        path = tmp_path / name
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"],
                "value": [10, 20, 30, 40],
            }
        )
        lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=2)
        return path

    source_path = write_dataset("source.lance")
    target_path = write_dataset("target.lance")
    source_dataset = lance.dataset(str(source_path))
    target_dataset = lance.dataset(str(target_path))
    assert source_dataset.version == target_dataset.version == 1

    def update_values(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array([999] * table.num_rows, type=table.schema.field("value").type),
        )

    source = lr.read_lance(str(source_path), with_metadata=True).map_batches(
        update_values,
        batch_format="pyarrow",
    )
    target_before = target_dataset.to_table().sort_by("id")

    with pytest.raises(ValueError, match="different Lance dataset"):
        lr.update_columns_from(
            str(target_path),
            source,
            columns=["value"],
        )

    result = lance.dataset(str(target_path))
    assert result.version == target_dataset.version
    assert result.to_table().sort_by("id") == target_before


def test_update_columns_from_accepts_normalized_target_uri(
    multi_fragment_path: Path,
) -> None:
    def update_values(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array([999] * table.num_rows, type=table.schema.field("value").type),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_values,
        batch_format="pyarrow",
    )

    lr.update_columns_from(
        f"{multi_fragment_path}/",
        source,
        columns=["value"],
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [999, 999, 999, 999]


def test_update_columns_from_rejects_union_of_different_source_datasets(
    tmp_path: Path,
) -> None:
    def write_dataset(name: str) -> Path:
        path = tmp_path / name
        data = pd.DataFrame(
            {
                "id": [1, 2],
                "value": [10, 20],
            }
        )
        lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=1)
        return path

    first_path = write_dataset("first.lance")
    second_path = write_dataset("second.lance")
    first_dataset = lance.dataset(str(first_path))
    second_dataset = lance.dataset(str(second_path))
    assert first_dataset.version == second_dataset.version == 1

    mixed_source = lr.read_lance(str(first_path), with_metadata=True).union(
        lr.read_lance(str(second_path), with_metadata=True)
    )

    with pytest.raises(ValueError, match="multiple Lance source datasets"):
        lr.update_columns_from(
            str(first_path),
            mixed_source,
            columns=["value"],
        )

    result = lance.dataset(str(first_path))
    assert result.version == first_dataset.version
    assert result.to_table().sort_by("id") == first_dataset.to_table().sort_by("id")


def test_update_columns_from_updates_multiple_columns(
    multi_fragment_path: Path,
) -> None:
    def update_batch(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame.loc[frame["id"] == 2, "name"] = "Bobby"
        frame.loc[frame["id"] == 4, "name"] = "David"
        frame.loc[frame["id"] == 2, "value"] = 202
        frame.loc[frame["id"] == 4, "value"] = 404
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_batch, batch_format="pandas"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["name", "value"],
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )

    assert result["name"].tolist() == ["Alice", "Bobby", "Charlie", "David"]
    assert result["value"].tolist() == [10, 202, 30, 404]


def test_update_columns_from_ignores_unmatched_rowaddr(
    multi_fragment_path: Path,
) -> None:
    def replace_rowaddr(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        rowaddrs = table.column("_rowaddr").to_pylist()
        values = table.column("value").to_pylist()
        ids = table.column("id").to_pylist()
        for index, row_id in enumerate(ids):
            if row_id == 1:
                frag_id = table.column("_fragid")[index].as_py()
                rowaddrs[index] = (frag_id << 32) | 0xFFFFFFFF
                values[index] = 999
        return table.set_column(
            table.schema.get_field_index("_rowaddr"),
            "_rowaddr",
            pa.array(rowaddrs, type=pa.uint64()),
        ).set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array(values, type=table.schema.field("value").type),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        replace_rowaddr, batch_format="pyarrow"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [10, 20, 30, 40]


def test_update_columns_from_uses_requested_read_version(
    multi_fragment_path: Path,
) -> None:
    def update_selected_rows(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame.loc[frame["id"] == 1, "value"] = 101
        frame.loc[frame["id"] == 3, "value"] = 303
        return frame

    read_version = lance.dataset(str(multi_fragment_path)).version
    source = lr.read_lance(
        str(multi_fragment_path),
        dataset_options={"version": read_version},
        with_metadata=True,
    ).map_batches(update_selected_rows, batch_format="pandas")

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        read_version=read_version,
    )

    result = lance.dataset(str(multi_fragment_path))
    assert result.version == read_version + 1
    assert result.to_table().sort_by("id").column("value").to_pylist() == [
        101,
        20,
        303,
        40,
    ]


def test_update_columns_from_uses_tag_as_read_version(
    multi_fragment_path: Path,
) -> None:
    def update_selected_rows(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        values = table.column("value").to_pylist()
        ids = table.column("id").to_pylist()
        updated_values = [
            202 if row_id == 2 else value
            for row_id, value in zip(ids, values, strict=True)
        ]
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array(updated_values, type=table.schema.field("value").type),
        )

    tag = "update-base"
    tagged_version = lance.dataset(str(multi_fragment_path)).version
    lance.dataset(str(multi_fragment_path)).tags.create(tag, tagged_version)
    source = lr.read_lance(
        str(multi_fragment_path),
        dataset_options={"version": tag},
        with_metadata=True,
    ).map_batches(update_selected_rows, batch_format="pyarrow")

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        read_version=tag,
    )

    result = lance.dataset(str(multi_fragment_path))
    assert result.version == tagged_version + 1
    assert result.to_table().sort_by("id").column("value").to_pylist() == [
        10,
        202,
        30,
        40,
    ]


def test_update_columns_from_does_not_retry_commit_conflict(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)
    commit_calls = 0

    def fail_commit(*args: object, **kwargs: object) -> None:
        nonlocal commit_calls
        commit_calls += 1
        raise RuntimeError("concurrent update conflict")

    monkeypatch.setattr(LanceDataset, "commit", fail_commit)

    with pytest.raises(RuntimeError, match="concurrent update conflict"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )

    assert commit_calls == 1


def test_update_columns_from_commits_rewrite_columns_operation(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    committed_update_modes: list[str] = []
    original_commit = cast(Callable[..., LanceDataset], LanceDataset.commit)

    def capturing_commit(
        base_uri: str | Path | LanceDataset,
        operation: object,
        *args: object,
        **kwargs: object,
    ) -> LanceDataset:
        assert isinstance(operation, lance.LanceOperation.Update)
        committed_update_modes.append(operation.update_mode)
        return original_commit(base_uri, operation, *args, **kwargs)

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)
    monkeypatch.setattr(LanceDataset, "commit", capturing_commit)

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    assert committed_update_modes == ["rewrite_columns"]


def test_update_columns_from_requires_dataset(multi_fragment_path: Path) -> None:
    with pytest.raises(ValueError, match="'ds' must be provided"):
        lr.update_columns_from(
            str(multi_fragment_path),
            columns=["value"],
        )


def test_update_columns_from_requires_rowaddr(multi_fragment_path: Path) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=False)

    with pytest.raises(ValueError, match="_rowaddr"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_requires_columns(multi_fragment_path: Path) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="'columns' must be non-empty"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=[],
        )


def test_update_columns_from_rejects_duplicate_columns(
    multi_fragment_path: Path,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="Duplicate columns"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value", "value"],
        )


def test_update_columns_from_rejects_duplicate_rowaddr(
    multi_fragment_path: Path,
) -> None:
    def duplicate_updates(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        conflicting = table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pc.add(table.column("value"), 1000),
        )
        return pa.concat_tables([table, conflicting])

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        duplicate_updates,
        batch_format="pyarrow",
    )

    with pytest.raises(RayTaskError, match="Duplicate _rowaddr.*fragment"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_null_rowaddr(
    multi_fragment_path: Path,
) -> None:
    def null_first_rowaddr(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        rowaddrs = table.column("_rowaddr").to_pylist()
        rowaddrs[0] = None
        return table.set_column(
            table.schema.get_field_index("_rowaddr"),
            "_rowaddr",
            pa.array(rowaddrs, type=pa.uint64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        null_first_rowaddr,
        batch_format="pyarrow",
    )

    with pytest.raises(RayTaskError, match="Null _rowaddr.*fragment"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_null_rowaddr_without_fragid(
    multi_fragment_path: Path,
) -> None:
    source = ray.data.from_arrow(
        pa.table(
            {
                "_rowaddr": pa.array([None], type=pa.uint64()),
                "value": pa.array([10], type=pa.int64()),
            }
        )
    )
    # Without lineage, an explicit version is required before worker validation.
    read_version = lance.dataset(str(multi_fragment_path)).version

    with pytest.raises(RayTaskError, match="Null _rowaddr"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
            read_version=read_version,
        )


def test_update_columns_from_rejects_non_integer_rowaddr(
    multi_fragment_path: Path,
) -> None:
    def cast_rowaddr_to_string(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("_rowaddr"),
            "_rowaddr",
            pc.cast(table.column("_rowaddr"), pa.string()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        cast_rowaddr_to_string,
        batch_format="pyarrow",
    )

    with pytest.raises(ValueError, match="_rowaddr.*integer"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_update_column_type_mismatch(
    multi_fragment_path: Path,
) -> None:
    def cast_value_to_float64(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pc.cast(table.column("value"), pa.float64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        cast_value_to_float64,
        batch_format="pyarrow",
    )

    with pytest.raises(ValueError, match="type mismatch.*value"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_pandas_object_type_mismatch(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def cast_value_to_object(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] = frame["value"].astype(str)
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        cast_value_to_object,
        batch_format="pandas",
    )
    source_schema = cast(Schema | None, source.schema())
    assert source_schema is not None
    source_types = dict(zip(source_schema.names, source_schema.types, strict=True))
    assert source_types["value"] is object

    original_schema = Dataset.schema
    masked_arrow_schema = False

    def retain_ambiguous_pandas_schema(
        self: Dataset,
        fetch_if_missing: bool = True,
    ) -> Schema | None:
        nonlocal masked_arrow_schema
        schema = cast(Schema | None, original_schema(self, fetch_if_missing))
        if schema is None or "value" not in schema.names:
            return schema
        # Model a lazy Pandas plan whose driver schema remains ambiguous
        # even though the worker receives a concrete Arrow string column.
        masked_arrow_schema = True
        return source_schema

    monkeypatch.setattr(Dataset, "schema", retain_ambiguous_pandas_schema)

    version_before = lance.dataset(str(multi_fragment_path)).version
    with pytest.raises(RayTaskError, match="type mismatch.*value"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )

    assert masked_arrow_schema
    assert lance.dataset(str(multi_fragment_path)).version == version_before


def test_update_columns_from_with_batch_size_one(multi_fragment_path: Path) -> None:
    def update_values(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] += 100
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_values,
        batch_format="pandas",
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        batch_size=1,
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [110, 120, 130, 140]


def test_update_columns_from_forwards_ray_remote_args(
    multi_fragment_path: Path,
) -> None:
    def update_values(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] += 100
        return frame

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_values,
        batch_format="pandas",
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        ray_remote_args={"num_cpus": 1},
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [110, 120, 130, 140]


@pytest.mark.parametrize("batch_size", [0, -1])
def test_update_columns_from_rejects_invalid_batch_size(
    multi_fragment_path: Path,
    batch_size: int,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="'batch_size' must be positive"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
            batch_size=batch_size,
        )


def test_update_columns_from_supports_row_filter(
    multi_fragment_path: Path,
) -> None:
    def update_value(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame["value"] = 101
        return frame

    source = (
        lr.read_lance(str(multi_fragment_path), with_metadata=True)
        .filter(lambda row: row["id"] == 1)
        .map_batches(update_value, batch_format="pandas")
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [101, 20, 30, 40]


def test_update_columns_from_normalizes_signed_integer_metadata(
    multi_fragment_path: Path,
) -> None:
    def cast_metadata_and_update(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        table = table.set_column(
            table.schema.get_field_index("_rowaddr"),
            "_rowaddr",
            pc.cast(table.column("_rowaddr"), pa.int64()),
        ).set_column(
            table.schema.get_field_index("_fragid"),
            "_fragid",
            pc.cast(table.column("_fragid"), pa.int64()),
        )
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pc.add(
                table.column("value"),
                pa.scalar(100, type=table.schema.field("value").type),
            ),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        cast_metadata_and_update,
        batch_format="pyarrow",
    )
    source_schema = source.schema()
    assert source_schema is not None
    source_types = dict(zip(source_schema.names, source_schema.types, strict=True))
    assert source_types["_rowaddr"] == pa.int64()
    assert source_types["_fragid"] == pa.int64()

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [110, 120, 130, 140]


@pytest.mark.parametrize("metadata_column", ["_rowaddr", "_fragid"])
def test_update_columns_from_rejects_negative_signed_metadata(
    multi_fragment_path: Path,
    metadata_column: str,
) -> None:
    def make_first_value_negative(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        values = table.column(metadata_column).to_pylist()
        values[0] = -1
        return table.set_column(
            table.schema.get_field_index(metadata_column),
            metadata_column,
            pa.array(values, type=pa.int64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        make_first_value_negative,
        batch_format="pyarrow",
    )
    version_before = lance.dataset(str(multi_fragment_path)).version

    with pytest.raises(RayTaskError, match="-1"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )

    assert lance.dataset(str(multi_fragment_path)).version == version_before


def test_update_columns_from_derives_missing_fragid(
    multi_fragment_path: Path,
) -> None:
    def drop_fragid_and_update(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        values = table.column("value").to_pylist()
        updated_values = [101 if value == 10 else value for value in values]
        return table.drop_columns(["_fragid"]).set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array(updated_values, type=table.schema.field("value").type),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        drop_fragid_and_update, batch_format="pyarrow"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = (
        lr.read_lance(str(multi_fragment_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [101, 20, 30, 40]


@pytest.mark.parametrize("metadata_column", ["_rowaddr", "_fragid", "_rowid"])
def test_update_columns_from_rejects_metadata_column(
    multi_fragment_path: Path,
    metadata_column: str,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="Metadata columns"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=[metadata_column],
        )


def test_update_columns_from_rejects_rowid_present_in_source(
    multi_fragment_path: Path,
) -> None:
    def add_rowid(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.append_column(
            "_rowid",
            pa.array(range(table.num_rows), type=pa.uint64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        add_rowid,
        batch_format="pyarrow",
    )

    with pytest.raises(ValueError, match="Metadata columns"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["_rowid"],
        )


def test_update_columns_from_rejects_missing_column(
    multi_fragment_path: Path,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="missing requested update columns"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["missing"],
        )


def test_update_columns_from_rejects_column_missing_from_target(
    multi_fragment_path: Path,
) -> None:
    def add_missing_column(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.append_column(
            "missing",
            pa.array([1] * table.num_rows, type=pa.int64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        add_missing_column, batch_format="pyarrow"
    )

    with pytest.raises(ValueError, match="do not exist in target"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["missing"],
        )


def test_update_columns_from_rejects_inconsistent_fragid(
    multi_fragment_path: Path,
) -> None:
    def replace_fragid(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("_fragid"),
            "_fragid",
            pa.array([999] * table.num_rows, type=pa.uint64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        replace_fragid, batch_format="pyarrow"
    )

    with pytest.raises(RayTaskError, match="_fragid.*does not match _rowaddr"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_non_integer_fragid(
    multi_fragment_path: Path,
) -> None:
    def cast_fragid_to_string(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        return table.set_column(
            table.schema.get_field_index("_fragid"),
            "_fragid",
            pc.cast(table.column("_fragid"), pa.string()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        cast_fragid_to_string,
        batch_format="pyarrow",
    )

    with pytest.raises(ValueError, match="_fragid.*integer"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_null_fragid(
    multi_fragment_path: Path,
) -> None:
    def null_first_fragid(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        fragids = table.column("_fragid").to_pylist()
        fragids[0] = None
        return table.set_column(
            table.schema.get_field_index("_fragid"),
            "_fragid",
            pa.array(fragids, type=pa.uint64()),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        null_first_fragid,
        batch_format="pyarrow",
    )

    with pytest.raises(RayTaskError, match="Null _fragid"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_inconsistent_fragid_on_duplicated_rows(
    multi_fragment_path: Path,
) -> None:
    def duplicate_with_other_fragid(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        other_fragids = pc.bit_wise_xor(
            pc.cast(pc.shift_right(table.column("_rowaddr"), 32), pa.uint64()),
            pa.scalar(1, type=pa.uint64()),
        )
        conflicting = table.set_column(
            table.schema.get_field_index("_fragid"),
            "_fragid",
            other_fragids,
        )
        return pa.concat_tables([table, conflicting])

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        duplicate_with_other_fragid,
        batch_format="pyarrow",
    )

    with pytest.raises(RayTaskError, match="_fragid.*does not match _rowaddr"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
        )


def test_update_columns_from_rejects_unknown_fragment_before_update(
    multi_fragment_path: Path,
) -> None:
    dataset = lance.dataset(str(multi_fragment_path))
    read_version = dataset.version
    original = dataset.to_table().sort_by("id")
    valid_frag_id = dataset.get_fragments()[0].metadata.id
    missing_frag_id = (
        max(fragment.metadata.id for fragment in dataset.get_fragments()) + 1
    )
    source = ray.data.from_arrow(
        pa.table(
            {
                "_rowaddr": pa.array(
                    [valid_frag_id << 32, missing_frag_id << 32],
                    type=pa.uint64(),
                ),
                "value": pa.array([999, 888], type=pa.int64()),
            }
        )
    )

    with pytest.raises(ValueError, match="_fragid values from input Dataset"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
            read_version=read_version,
        )

    result = lance.dataset(str(multi_fragment_path))
    assert result.version == read_version
    assert result.to_table().sort_by("id") == original


def test_update_columns_from_worker_failure_does_not_commit_partial_updates(
    multi_fragment_path: Path,
) -> None:
    dataset = lance.dataset(str(multi_fragment_path))
    read_version = dataset.version
    original = dataset.to_table().sort_by("id")
    fragments = dataset.get_fragments()
    first_frag_id = fragments[0].metadata.id
    second_frag_id = fragments[1].metadata.id
    source = ray.data.from_arrow(
        [
            pa.table(
                {
                    "_rowaddr": pa.array(
                        [first_frag_id << 32],
                        type=pa.uint64(),
                    ),
                    "value": pa.array([999], type=pa.int64()),
                }
            ),
            pa.table(
                {
                    "_rowaddr": pa.array(
                        [second_frag_id << 32, second_frag_id << 32],
                        type=pa.uint64(),
                    ),
                    "value": pa.array([888, 777], type=pa.int64()),
                }
            ),
        ]
    )
    assert source.num_blocks() == 2

    with pytest.raises(RayTaskError, match="Duplicate _rowaddr"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
            read_version=read_version,
        )

    result = lance.dataset(str(multi_fragment_path))
    assert result.version == read_version
    assert result.to_table().sort_by("id") == original


def test_read_lance_projected_schema_includes_requested_metadata(
    multi_fragment_path: Path,
) -> None:
    source = lr.read_lance(
        str(multi_fragment_path),
        columns=["value"],
        with_metadata=True,
    )

    schema = source.schema()
    assert schema is not None
    assert schema.names == ["value", "_rowaddr", "_fragid"]


def test_update_columns_from_warns_for_empty_source(
    multi_fragment_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).filter(
        lambda row: False
    )
    version_before = lance.dataset(str(multi_fragment_path)).version

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    assert "No rows to update" in caplog.text
    assert lance.dataset(str(multi_fragment_path)).version == version_before


@pytest.mark.parametrize(
    ("source_columns", "requested_columns", "error_match"),
    [
        pytest.param(
            {"value": pa.array([], type=pa.int64())},
            ["value"],
            "must contain '_rowaddr'",
            id="missing-rowaddr",
        ),
        pytest.param(
            {"_rowaddr": pa.array([], type=pa.uint64())},
            ["missing"],
            "missing requested update columns",
            id="missing-source-column",
        ),
        pytest.param(
            {
                "_rowaddr": pa.array([], type=pa.uint64()),
                "missing": pa.array([], type=pa.int64()),
            },
            ["missing"],
            "do not exist in target",
            id="missing-target-column",
        ),
    ],
)
def test_update_columns_from_validates_empty_source(
    multi_fragment_path: Path,
    source_columns: dict[str, Any],
    requested_columns: list[str],
    error_match: str,
) -> None:
    source = ray.data.from_arrow(pa.table(source_columns))
    version_before = lance.dataset(str(multi_fragment_path)).version

    with pytest.raises(ValueError, match=error_match):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=requested_columns,
        )

    assert lance.dataset(str(multi_fragment_path)).version == version_before


def test_update_columns_from_supports_null_column_updates(
    multi_fragment_path: Path,
) -> None:
    def update_with_null(batch: DataBatch) -> pa.Table:
        table = cast(pa.Table, batch)
        ids = table.column("id").to_pylist()
        values = table.column("value").to_pylist()
        updated_values = [
            None if row_id == 1 else value
            for row_id, value in zip(ids, values, strict=True)
        ]
        return table.set_column(
            table.schema.get_field_index("value"),
            "value",
            pa.array(updated_values, type=table.schema.field("value").type),
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True).map_batches(
        update_with_null, batch_format="pyarrow"
    )

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
    )

    result = lance.dataset(str(multi_fragment_path)).to_table().sort_by("id")
    assert result.column("value").to_pylist() == [None, 20, 30, 40]


def test_update_columns_from_supports_nested_column_updates(tmp_path: Path) -> None:
    path = tmp_path / "nested_columns.lance"
    list_type = pa.list_(pa.int64())
    struct_type = pa.struct([pa.field("score", pa.int64())])
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "items": pa.array([[1], [2, 3]], type=list_type),
            "profile": pa.array([{"score": 1}, {"score": 2}], type=struct_type),
        }
    )
    lance.write_dataset(table, str(path), max_rows_per_file=1)

    def update_nested_columns(batch: DataBatch) -> pa.Table:
        source = cast(pa.Table, batch)
        ids = source.column("id").to_pylist()
        items = source.column("items").to_pylist()
        profiles = source.column("profile").to_pylist()
        item_updates = [
            [10, 11] if row_id == 1 else value
            for row_id, value in zip(ids, items, strict=True)
        ]
        profile_updates = [
            {"score": 101} if row_id == 1 else value
            for row_id, value in zip(ids, profiles, strict=True)
        ]
        return source.set_column(
            source.schema.get_field_index("items"),
            "items",
            pa.array(item_updates, type=list_type),
        ).set_column(
            source.schema.get_field_index("profile"),
            "profile",
            pa.array(profile_updates, type=struct_type),
        )

    source = lr.read_lance(str(path), with_metadata=True).map_batches(
        update_nested_columns,
        batch_format="pyarrow",
    )

    lr.update_columns_from(
        str(path),
        source,
        columns=["items", "profile"],
    )

    result = lance.dataset(str(path)).to_table().sort_by("id")
    assert result.column("items").to_pylist() == [[10, 11], [2, 3]]
    assert result.column("profile").to_pylist() == [
        {"score": 101},
        {"score": 2},
    ]


def test_update_columns_from_empty_source_without_fragid(
    multi_fragment_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = ray.data.from_arrow(
        pa.table(
            {
                "_rowaddr": pa.array([], type=pa.uint64()),
                "value": pa.array([], type=pa.int64()),
            }
        )
    )
    version_before = lance.dataset(str(multi_fragment_path)).version

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        read_version=version_before,
    )

    assert "No rows to update" in caplog.text
    assert lance.dataset(str(multi_fragment_path)).version == version_before


@pytest.mark.parametrize(
    ("namespace_impl", "table_id"),
    [
        (None, None),
        ("dir", None),
        (None, ["table"]),
    ],
)
def test_update_columns_from_requires_uri_or_complete_namespace(
    multi_fragment_path: Path,
    namespace_impl: str | None,
    table_id: list[str] | None,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="Must provide either 'uri'"):
        lr.update_columns_from(
            ds=source,
            columns=["value"],
            namespace_impl=namespace_impl,
            table_id=table_id,
        )


def test_update_columns_from_rejects_uri_and_namespace(
    multi_fragment_path: Path,
) -> None:
    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)

    with pytest.raises(ValueError, match="Cannot provide both 'uri'"):
        lr.update_columns_from(
            str(multi_fragment_path),
            source,
            columns=["value"],
            namespace_impl="dir",
            namespace_properties={"root": str(multi_fragment_path.parent)},
            table_id=["table"],
        )


def test_update_columns_from_namespace_mode(tmp_path: Path) -> None:
    table_id = ["update_columns_namespace"]
    data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )
    lr.write_lance(
        ray.data.from_pandas(data),
        namespace_impl="dir",
        namespace_properties={"root": str(tmp_path)},
        table_id=table_id,
        min_rows_per_file=1,
        max_rows_per_file=2,
    )

    def update_batch(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame.loc[frame["id"] == 2, "value"] = 200
        return frame

    source = lr.read_lance(
        namespace_impl="dir",
        namespace_properties={"root": str(tmp_path)},
        table_id=table_id,
        with_metadata=True,
    ).map_batches(update_batch, batch_format="pandas")

    lr.update_columns_from(
        ds=source,
        columns=["value"],
        namespace_impl="dir",
        namespace_properties={"root": str(tmp_path)},
        table_id=table_id,
    )

    result = (
        lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": str(tmp_path)},
            table_id=table_id,
        )
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [10, 200, 30]


def test_update_columns_from_reads_uri_and_updates_via_namespace(
    tmp_path: Path,
) -> None:
    import lance_namespace as ln
    from lance_namespace import DescribeTableRequest

    table_id = ["update_columns_uri_source"]
    data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )
    lr.write_lance(
        ray.data.from_pandas(data),
        namespace_impl="dir",
        namespace_properties={"root": str(tmp_path)},
        table_id=table_id,
        min_rows_per_file=1,
        max_rows_per_file=2,
    )

    namespace = ln.connect("dir", {"root": str(tmp_path)})
    location = namespace.describe_table(DescribeTableRequest(id=table_id)).location
    assert location is not None

    def update_batch(batch: DataBatch) -> pd.DataFrame:
        frame = cast(pd.DataFrame, batch).copy()
        frame.loc[frame["id"] == 2, "value"] = 200
        return frame

    source = lr.read_lance(location, with_metadata=True).map_batches(
        update_batch,
        batch_format="pandas",
    )

    lr.update_columns_from(
        ds=source,
        columns=["value"],
        namespace_impl="dir",
        namespace_properties={"root": str(tmp_path)},
        table_id=table_id,
    )

    result = (
        lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": str(tmp_path)},
            table_id=table_id,
        )
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert result["value"].tolist() == [10, 200, 30]


def test_update_columns_from_forwards_storage_options(
    multi_fragment_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_options = {"timeout": "60s"}
    opened_storage_options: list[dict[str, str] | None] = []
    committed_storage_options: list[dict[str, str] | None] = []
    original_init = cast(Callable[..., None], LanceDataset.__init__)
    original_commit = cast(Callable[..., LanceDataset], LanceDataset.commit)

    def capturing_init(
        self: LanceDataset,
        uri: str | Path,
        *args: object,
        storage_options: dict[str, str] | None = None,
        **kwargs: object,
    ) -> None:
        opened_storage_options.append(storage_options)
        original_init(self, uri, *args, storage_options=storage_options, **kwargs)

    def capturing_commit(
        base_uri: str | Path | LanceDataset,
        operation: object,
        *args: object,
        storage_options: dict[str, str] | None = None,
        **kwargs: object,
    ) -> LanceDataset:
        committed_storage_options.append(storage_options)
        return original_commit(
            base_uri,
            operation,
            *args,
            storage_options=storage_options,
            **kwargs,
        )

    source = lr.read_lance(str(multi_fragment_path), with_metadata=True)
    monkeypatch.setattr(LanceDataset, "__init__", capturing_init)
    monkeypatch.setattr(LanceDataset, "commit", capturing_commit)

    lr.update_columns_from(
        str(multi_fragment_path),
        source,
        columns=["value"],
        storage_options=storage_options,
    )

    assert storage_options in opened_storage_options
    assert committed_storage_options == [storage_options]
