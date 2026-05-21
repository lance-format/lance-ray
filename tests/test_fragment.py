"""Test cases for lance_ray.fragment module."""

from pathlib import Path

import lance
import pyarrow as pa
import pytest
import ray
from lance_ray.datasink import LanceFragmentCommitter
from lance_ray.fragment import LanceFragmentWriter


class TestLanceFragmentWriterCommitter:
    """Test cases for LanceFragmentWriter and LanceCommitter."""

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_committer(self, tmp_path: Path):
        """Test fragment writer and committer for large-scale data."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Use fragment writer and committer
        (
            ray.data.range(10)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema), batch_size=5)
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        assert ds.schema == schema

        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))
        assert set(tbl["str"].to_pylist()) == set([f"str-{i}" for i in range(10)])
        # Should have 2 fragments since batch_size=5 and we have 10 rows
        assert len(ds.get_fragments()) == 2

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_with_transform(self, tmp_path: Path):
        """Test fragment writer with custom transform function."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("str", pa.string()),
                pa.field("doubled", pa.int64()),
            ]
        )

        def transform(batch: pa.Table) -> pa.Table:
            """Transform function to add a doubled column."""
            df = batch.to_pandas()
            df["doubled"] = df["id"] * 2
            return pa.Table.from_pandas(df, schema=schema)

        # Use fragment writer with transform
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(
                LanceFragmentWriter(tmp_path, schema=schema, transform=transform),
                batch_size=5,
            )
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 5
        tbl = ds.to_table()
        indices = pa.compute.sort_indices(tbl, sort_keys=[("id", "ascending")])
        tbl_sorted = pa.compute.take(tbl, indices)
        assert tbl_sorted.column("doubled").to_pylist() == [0, 2, 4, 6, 8]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_append_mode(self, tmp_path: Path):
        """Test fragment writer with append mode."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Write initial data
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path, mode="create"))
        )

        # Append more data
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] >= 5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path, mode="append"))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_empty_write(self, tmp_path: Path):
        """Test fragment writer with empty data."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Write empty data (filter everything out)
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] > 10)  # Filter out everything
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Empty write should not create a dataset
        with pytest.raises(ValueError):
            lance.dataset(tmp_path)

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_none_values(self, tmp_path: Path):
        """Test fragment writer with None values."""

        def create_row(row):
            return {
                "id": row["id"],
                "str": None if row["id"] % 2 == 0 else f"str-{row['id']}",
            }

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        (
            ray.data.range(10)
            .map(create_row)
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        str_values = tbl["str"].to_pylist()
        id_values = tbl["id"].to_pylist()
        # Even IDs should have None values
        for id_val, str_val in zip(id_values, str_values, strict=False):
            if id_val % 2 == 0:
                # None values might be represented as None or as 'nan' string
                assert str_val is None or str(str_val) == "nan", (
                    f"ID {id_val} should have None/nan but got {str_val}"
                )
            else:
                assert str_val == f"str-{id_val}", (
                    f"ID {id_val} should have 'str-{id_val}' but got {str_val}"
                )
