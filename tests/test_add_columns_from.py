"""Test cases for add_columns_from, merge_columns_from and with_metadata functionality."""

import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pyarrow as pa
import pytest

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

    def test_basic_transform(self, temp_dir):
        path = Path(temp_dir) / "acf_basic.lance"
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"],
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        def compute_name_len(batch):
            return {"name_len": [len(x) for x in batch["name"]]}

        lr.add_columns_from(str(path), transform=compute_name_len)

        result = lr.read_lance(str(path))
        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert "name_len" in df.columns
        assert df["name_len"].tolist() == [5, 3, 7, 4]

    def test_transform_numeric(self, temp_dir):
        path = Path(temp_dir) / "acf_numeric.lance"
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "score": [85.5, 92.0, 78.5, 88.0, 95.5],
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=3)

        def double_score(batch):
            return {"double_score": [x * 2 for x in batch["score"]]}

        lr.add_columns_from(str(path), transform=double_score)

        result = lr.read_lance(str(path))
        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert "double_score" in df.columns
        expected = [x * 2 for x in data["score"]]
        assert df["double_score"].tolist() == expected

    def test_transform_preserves_original_data(self, temp_dir):
        path = Path(temp_dir) / "acf_preserve.lance"
        data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path))

        def add_label(batch):
            return {"label": ["user" for _ in batch["id"]]}

        lr.add_columns_from(str(path), transform=add_label)

        result = lr.read_lance(str(path))
        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert "label" in df.columns
        assert df["id"].tolist() == [1, 2, 3]
        assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]
        assert df["label"].tolist() == ["user", "user", "user"]

    def test_transform_large_fragment(self, temp_dir):
        path = Path(temp_dir) / "acf_large.lance"
        n_rows = 5000
        data = pd.DataFrame(
            {
                "id": list(range(n_rows)),
                "value": list(range(n_rows)),
            }
        )
        table = pa.Table.from_pandas(data)
        lance.write_dataset(table, str(path), max_rows_per_file=n_rows)

        def add_squared(batch):
            return {"squared": [int(x) * int(x) for x in batch["id"]]}

        lr.add_columns_from(str(path), transform=add_squared, batch_size=512)

        result = (
            lr.read_lance(str(path))
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
        assert "squared" in result.columns
        assert result["id"].tolist() == list(range(n_rows))
        assert result["squared"].tolist() == [i * i for i in range(n_rows)]

    def test_transform_multi_fragment(self, temp_dir):
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

        lr.add_columns_from(str(path), transform=add_double)

        result = (
            lr.read_lance(str(path))
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
        assert result["id"].tolist() == list(range(n_rows))
        assert result["doubled"].tolist() == [x * 20 for x in range(n_rows)]


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
