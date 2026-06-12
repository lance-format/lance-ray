"""Test cases for add_columns_from, merge_columns_from and with_metadata functionality."""

import tempfile
from pathlib import Path
from types import SimpleNamespace

import lance
import lance_ray as lr
import pyarrow as pa
import pytest
import ray

import pandas as pd


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


class TestReadLanceWithMetadata:
    def test_with_metadata_includes_rowaddr_and_fragid(self, temp_dir):
        path = Path(temp_dir) / "metadata_test.lance"
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"],
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        ray_ds = lr.read_lance(str(path), with_metadata=True)
        schema_names = ray_ds.schema().names

        assert "_rowaddr" in schema_names
        assert "_fragid" in schema_names
        assert "id" in schema_names
        assert "name" in schema_names

        df = ray_ds.to_pandas()
        assert len(df) == 4
        assert all(df["_fragid"].isin([0, 1]))

    def test_without_metadata_excludes_rowaddr_and_fragid(self, temp_dir):
        path = Path(temp_dir) / "no_metadata_test.lance"
        data = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path))

        ray_ds = lr.read_lance(str(path), with_metadata=False)
        schema_names = ray_ds.schema().names

        assert "_rowaddr" not in schema_names
        assert "_fragid" not in schema_names

    def test_default_without_metadata(self, temp_dir):
        path = Path(temp_dir) / "default_metadata_test.lance"
        data = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path))

        ray_ds = lr.read_lance(str(path))
        schema_names = ray_ds.schema().names

        assert "_rowaddr" not in schema_names
        assert "_fragid" not in schema_names

    def test_fragid_matches_fragment_ids(self, temp_dir):
        path = Path(temp_dir) / "fragid_test.lance"
        data = pd.DataFrame(
            {
                "id": list(range(10)),
                "val": list(range(10)),
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=3)

        lance_ds = lance.dataset(str(path))
        expected_frag_ids = {f.metadata.id for f in lance_ds.get_fragments()}

        ray_ds = lr.read_lance(str(path), with_metadata=True)
        df = ray_ds.to_pandas()
        actual_frag_ids = set(df["_fragid"].unique())

        assert actual_frag_ids == expected_frag_ids


class TestAddColumnsFrom:
    """Tests for the high-level add_columns_from(uri, transform=...) API."""

    def test_transform_adds_columns_and_preserves_original(self, temp_dir):
        path = Path(temp_dir) / "acf_basic.lance"
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"],
                "score": [85.5, 92.0, 78.5, 88.0],
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        def my_udf(batch):
            return {
                "name_len": [len(x) for x in batch["name"]],
                "double_score": [x * 2 for x in batch["score"]],
            }

        lr.add_columns_from(str(path), transform=my_udf)

        result = lr.read_lance(str(path))
        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert "name_len" in df.columns
        assert df["name_len"].tolist() == [5, 3, 7, 4]
        assert "double_score" in df.columns
        assert df["double_score"].tolist() == [171.0, 184.0, 157.0, 176.0]
        assert df["id"].tolist() == [1, 2, 3, 4]
        assert df["name"].tolist() == ["Alice", "Bob", "Charlie", "Dave"]

    def test_transform_multi_fragment_with_small_batch(self, temp_dir):
        path = Path(temp_dir) / "acf_multi.lance"
        n_rows = 30
        data = pd.DataFrame(
            {
                "id": list(range(n_rows)),
                "value": [x * 10 for x in range(n_rows)],
            }
        )
        lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=7)

        def add_double(batch):
            return {"doubled": [int(x) * 2 for x in batch["value"]]}

        lr.add_columns_from(str(path), transform=add_double, batch_size=8)

        result = (
            lr.read_lance(str(path))
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
        assert result["id"].tolist() == list(range(n_rows))
        assert result["doubled"].tolist() == [x * 20 for x in range(n_rows)]

    def test_namespace_only_resolves_uri(self, temp_dir):
        table_id = ["acf_namespace_only"]
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "value": [10, 20, 30, 40],
            }
        )
        lr.write_lance(
            ray.data.from_pandas(data),
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
            min_rows_per_file=1,
            max_rows_per_file=2,
        )

        def add_double(batch):
            return {"doubled": [int(x) * 2 for x in batch["value"]]}

        lr.add_columns_from(
            transform=add_double,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        result = lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )
        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert df["doubled"].tolist() == [20, 40, 60, 80]

    def test_namespace_storage_options_override_local_options(self, monkeypatch):
        from lance_ray.utils import resolve_namespace_table

        class FakeNamespace:
            def describe_table(self, request):
                assert request.id == ["lance_catalog", "sales", "orders"]
                return SimpleNamespace(
                    location="s3://contacts/raw/lance/sales/orders.lance",
                    storage_options={
                        "endpoint": "http://127.0.0.1:9000",
                        "access_key_id": "namespace-ak",
                        "secret_access_key": "namespace-sk",
                        "allow_http": "true",
                    },
                )

        monkeypatch.setattr(
            "lance_ray.utils.get_or_create_namespace",
            lambda namespace_impl, namespace_properties: FakeNamespace(),
        )

        uri, storage_options = resolve_namespace_table(
            None,
            {"access_key_id": "local-ak", "region": "us-east-1"},
            "rest",
            {"uri": "http://127.0.0.1:9101/lance"},
            ["lance_catalog", "sales", "orders"],
        )

        assert uri == "s3://contacts/raw/lance/sales/orders.lance"
        assert storage_options == {
            "endpoint": "http://127.0.0.1:9000",
            "access_key_id": "namespace-ak",
            "secret_access_key": "namespace-sk",
            "allow_http": "true",
            "region": "us-east-1",
        }

    def test_resolve_namespace_table_prefers_explicit_uri(self, monkeypatch):
        """An explicit uri must not trigger describe_table or pull foreign creds."""
        from lance_ray.utils import resolve_namespace_table

        class ExplodingNamespace:
            def describe_table(self, request):  # pragma: no cover - must not run
                raise AssertionError(
                    "describe_table must not be called when uri is provided"
                )

        monkeypatch.setattr(
            "lance_ray.utils.get_or_create_namespace",
            lambda namespace_impl, namespace_properties: ExplodingNamespace(),
        )

        uri, storage_options = resolve_namespace_table(
            "s3://bucket-a/table-a.lance",
            {"access_key_id": "local-ak"},
            "rest",
            {"uri": "http://127.0.0.1:9101/lance"},
            ["lance_catalog", "sales", "orders"],
        )

        assert uri == "s3://bucket-a/table-a.lance"
        assert storage_options == {"access_key_id": "local-ak"}

    def test_namespace_args_preserved_for_read_lance(self, monkeypatch):
        calls = {}

        class FakeRayDataset:
            def map_batches(self, fn, batch_format=None):
                calls["map_batches"] = {
                    "fn": fn,
                    "batch_format": batch_format,
                }
                return self

        def fake_read_lance(*args, **kwargs):
            calls["read_lance"] = {"args": args, "kwargs": kwargs}
            return FakeRayDataset()

        def fake_merge_columns_from(*args, **kwargs):
            calls["merge_columns_from"] = {"args": args, "kwargs": kwargs}

        monkeypatch.setattr("lance_ray.io.read_lance", fake_read_lance)
        monkeypatch.setattr("lance_ray.io.merge_columns_from", fake_merge_columns_from)

        def add_double(batch):
            return {"doubled": [int(x) * 2 for x in batch["value"]]}

        lr.add_columns_from(
            transform=add_double,
            storage_options={"region": "us-east-1"},
            namespace_impl="rest",
            namespace_properties={"uri": "http://127.0.0.1:9101/lance"},
            table_id=["lance_catalog", "sales", "orders"],
        )

        read_kwargs = calls["read_lance"]["kwargs"]
        assert calls["read_lance"]["args"] == (None,)
        assert read_kwargs["namespace_impl"] == "rest"
        assert read_kwargs["namespace_properties"] == {
            "uri": "http://127.0.0.1:9101/lance"
        }
        assert read_kwargs["table_id"] == ["lance_catalog", "sales", "orders"]
        assert read_kwargs["storage_options"] == {"region": "us-east-1"}
        assert read_kwargs["with_metadata"] is True

        merge_kwargs = calls["merge_columns_from"]["kwargs"]
        assert calls["merge_columns_from"]["args"][0] is None
        assert merge_kwargs["namespace_impl"] == "rest"
        assert merge_kwargs["table_id"] == ["lance_catalog", "sales", "orders"]

    def test_uri_and_namespace_are_mutually_exclusive(self):
        def add_double(batch):
            return {"doubled": [1]}

        with pytest.raises(ValueError, match="Cannot provide both 'uri'"):
            lr.add_columns_from(
                "/tmp/table.lance",
                transform=add_double,
                namespace_impl="dir",
                table_id=["table"],
            )

        with pytest.raises(ValueError, match="Cannot provide both 'uri'"):
            lr.add_columns(
                "/tmp/table.lance",
                transform=add_double,
                namespace_impl="dir",
                table_id=["table"],
            )

        with pytest.raises(ValueError, match="Cannot provide both 'uri'"):
            lr.merge_columns_from(
                "/tmp/table.lance",
                object(),
                namespace_impl="dir",
                table_id=["table"],
            )


class TestMergeColumnsFrom:
    """Tests for the low-level merge_columns_from(uri, ds) API."""

    def test_basic_merge(self, temp_dir):
        path = Path(temp_dir) / "mcf_basic.lance"
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"],
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        ray_ds = lr.read_lance(str(path), with_metadata=True)

        def compute_name_len(batch):
            result = {"name_len": [len(x) for x in batch["name"]]}
            if "_rowaddr" in batch:
                result["_rowaddr"] = batch["_rowaddr"]
            return result

        ray_ds = ray_ds.map_batches(compute_name_len)

        lr.merge_columns_from(str(path), ray_ds)

        result = lr.read_lance(str(path))
        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert "name_len" in df.columns
        assert df["name_len"].tolist() == [5, 3, 7, 4]

    def test_merge_missing_metadata_raises(self, temp_dir):
        path = Path(temp_dir) / "mcf_no_meta.lance"
        data = pd.DataFrame({"id": [1, 2, 3]})
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path))

        ray_ds = lr.read_lance(str(path), with_metadata=False)

        with pytest.raises(ValueError, match="_rowaddr"):
            lr.merge_columns_from(str(path), ray_ds)

    def test_merge_no_new_columns_raises(self, temp_dir):
        path = Path(temp_dir) / "mcf_no_new.lance"
        data = pd.DataFrame({"id": [1, 2, 3]})
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path))

        ray_ds = lr.read_lance(str(path), with_metadata=True)

        with pytest.raises(ValueError, match="No new columns"):
            lr.merge_columns_from(str(path), ray_ds)

    def test_merge_subset_raises_without_flag(self, temp_dir):
        path = Path(temp_dir) / "mcf_subset_raise.lance"
        data = pd.DataFrame({"id": list(range(10))})
        lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=3)

        ray_ds = lr.read_lance(str(path), with_metadata=True)
        ray_ds = ray_ds.filter(lambda r: r["_fragid"] == 0)

        def add_col(batch):
            return {
                "new_col": [int(x) + 100 for x in batch["id"]],
                "_rowaddr": batch["_rowaddr"],
            }

        ray_ds = ray_ds.map_batches(add_col)

        with pytest.raises(ValueError, match="does not cover all fragments"):
            lr.merge_columns_from(str(path), ray_ds)

    def test_merge_subset_allowed(self, temp_dir):
        path = Path(temp_dir) / "mcf_subset_ok.lance"
        data = pd.DataFrame({"id": list(range(9))})
        lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=3)

        ray_ds = lr.read_lance(str(path), with_metadata=True)
        ray_ds = ray_ds.filter(lambda r: r["_fragid"] == 0)

        def add_col(batch):
            return {
                "new_col": [int(x) + 100 for x in batch["id"]],
                "_rowaddr": batch["_rowaddr"],
            }

        ray_ds = ray_ds.map_batches(add_col)
        lr.merge_columns_from(str(path), ray_ds, require_full_coverage=False)

        result = (
            lr.read_lance(str(path))
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
        assert "new_col" in result.columns
        first_frag = result[result["id"] < 3]
        assert first_frag["new_col"].tolist() == [100, 101, 102]
        rest = result[result["id"] >= 3]
        assert rest["new_col"].isna().all()

    def test_merge_shuffled(self, temp_dir):
        path = Path(temp_dir) / "mcf_shuffled.lance"
        n_rows = 30
        data = pd.DataFrame(
            {
                "id": list(range(n_rows)),
                "value": [x * 10 for x in range(n_rows)],
            }
        )
        lance.write_dataset(pa.Table.from_pandas(data), str(path), max_rows_per_file=7)

        ray_ds = lr.read_lance(str(path), with_metadata=True)

        def add_double(batch):
            return {
                "doubled": [int(x) * 2 for x in batch["value"]],
                "_rowaddr": batch["_rowaddr"],
            }

        ray_ds = ray_ds.map_batches(add_double)
        ray_ds = ray_ds.random_shuffle(seed=42)

        lr.merge_columns_from(str(path), ray_ds)

        result = (
            lr.read_lance(str(path))
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
        assert result["id"].tolist() == list(range(n_rows))
        assert result["doubled"].tolist() == [x * 20 for x in range(n_rows)]
