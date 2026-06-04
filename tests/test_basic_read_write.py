"""Test cases for lance_ray.io module."""

import os
import sys
import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pyarrow as pa
import pytest
import ray
from ray.data import Dataset

import pandas as pd
from _utils import (
    fragment_write_options_skip_reason,
    missing_fragment_write_options,
)

sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "lance" / "python" / "python")
)


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [85.5, 92.0, 78.5, 88.0, 95.5],
        }
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_dataset(sample_data):
    """Create a Ray Dataset from sample data."""
    return ray.data.from_pandas(sample_data)


class TestWriteLance:
    """Test cases for write_lance function."""

    def test_write_lance_basic(self, sample_dataset, temp_dir):
        """Test basic write functionality."""
        path = Path(temp_dir) / "basic_write.lance"

        lr.write_lance(sample_dataset, str(path))

        assert path.exists()
        assert path.is_dir()

    def test_write_lance_with_schema(self, temp_dir):
        """Test write with explicit schema."""
        path = Path(temp_dir) / "schema_write.lance"

        data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        dataset = ray.data.from_pandas(data)

        schema = pa.schema(
            [pa.field("col1", pa.int64()), pa.field("col2", pa.string())]
        )

        lr.write_lance(dataset, str(path), schema=schema)
        assert path.exists()

    def test_write_lance_invalid_input(self, temp_dir):
        """Test error handling for invalid inputs."""
        path = Path(temp_dir) / "invalid.lance"

        with pytest.raises((ValueError, AttributeError, TypeError)):
            lr.write_lance(None, str(path))  # type: ignore

    def test_write_with_pandas_map_batches(self, temp_dir):
        def map_fn(row):
            return {
                "id": row["id"],
                "name": row["name"],
                "age": row["age"],
                "score": row["score"],
                "extra": None,
            }

        def to_pd(batch: pd.DataFrame):
            return batch

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int32()),
                pa.field("score", pa.float64()),
                pa.field("extra", pa.string()),
            ]
        )
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "score": [85.5, 92.0, 78.5, 88.0, 95.5],
            }
        )
        dataset = ray.data.from_pandas(data)
        lance_dataset_path = os.path.join(temp_dir, "lance_dataset_test.lance")
        processed_ds = dataset.map(map_fn).map_batches(
            lambda batch: to_pd(batch), batch_format="pandas"
        )
        lr.write_lance(
            processed_ds, lance_dataset_path, mode="overwrite", schema=schema
        )
        ds = lance.dataset(lance_dataset_path)
        assert ds.count_rows() == 5
        assert ds.schema == schema
        tbl = ds.to_table()
        assert set(data["name"].tolist()) == set(tbl["name"].to_pylist())


class TestReadLance:
    """Test cases for read_lance function."""

    @pytest.fixture
    def lance_dataset_path(self, sample_dataset, temp_dir):
        """Create a Lance dataset for reading tests."""
        path = Path(temp_dir) / "test_dataset.lance"
        lr.write_lance(sample_dataset, str(path))
        return str(path)

    def test_read_lance_basic(self, lance_dataset_path):
        """Test basic read functionality."""
        dataset = lr.read_lance(lance_dataset_path)

        assert isinstance(dataset, Dataset)

        df = dataset.to_pandas()
        assert len(df) == 5
        assert list(df.columns) == ["id", "name", "age", "score"]

    def test_read_lance_with_columns(self, lance_dataset_path):
        """Test reading specific columns."""
        dataset = lr.read_lance(lance_dataset_path, columns=["id", "name"])

        df = dataset.to_pandas()
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 5

    def test_read_lance_with_filter(self, lance_dataset_path):
        """Test reading with filter."""
        dataset = lr.read_lance(lance_dataset_path, filter="age > 30")

        df = dataset.to_pandas()
        assert len(df) == 3
        assert all(df["age"] > 30)

    def test_read_lance_columns_and_filter(self, lance_dataset_path):
        """Test reading with both columns and filter."""
        dataset = lr.read_lance(
            lance_dataset_path, columns=["name", "age"], filter="age >= 35"
        )

        df = dataset.to_pandas()
        assert list(df.columns) == ["name", "age"]
        assert len(df) == 3
        assert all(df["age"] >= 35)

    def test_read_lance_filter_and_count(self, lance_dataset_path):
        """Test reading filter and count."""
        dataset = lr.read_lance(
            lance_dataset_path, columns=["name", "age"], filter="age >= 35"
        )
        assert dataset.count() == 3

    def test_read_lance_nonexistent_path(self):
        """Test reading from non-existent path."""
        with pytest.raises((FileNotFoundError, OSError, Exception)):
            lr.read_lance("/path/that/does/not/exist")


class TestReadWrite:
    """Integration tests for read and write operations."""

    def test_write_then_read_roundtrip(self, sample_data, temp_dir):
        """Test writing data and then reading it back."""
        path = Path(temp_dir) / "roundtrip.lance"

        # Write original data
        original_dataset = ray.data.from_pandas(sample_data)
        lr.write_lance(original_dataset, str(path))

        # Read it back
        read_dataset = lr.read_lance(str(path))
        read_df = read_dataset.to_pandas()

        # Compare data (sort by id to ensure consistent order)
        original_sorted = sample_data.sort_values("id").reset_index(drop=True)
        read_sorted = read_df.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(original_sorted, read_sorted)

    def test_append_mode(self, sample_data, temp_dir):
        """Test append mode with read verification."""
        path = Path(temp_dir) / "append_test.lance"

        # Write initial data
        initial_dataset = ray.data.from_pandas(sample_data[:3])
        lr.write_lance(initial_dataset, str(path))

        # Append more data
        additional_data = pd.DataFrame(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "age": [50, 55],
                "score": [90.0, 85.0],
            }
        )
        additional_dataset = ray.data.from_pandas(additional_data)
        lr.write_lance(additional_dataset, str(path), mode="append")

        # Read all data
        full_dataset = lr.read_lance(str(path))
        full_df = full_dataset.to_pandas()

        assert len(full_df) == 5  # 3 initial + 2 appended

    def test_overwrite_mode(self, sample_dataset, temp_dir):
        """Test different write modes."""
        path = Path(temp_dir) / "modes_test.lance"

        # Test create mode
        lr.write_lance(sample_dataset, str(path), mode="create")
        assert path.exists()

        # Verify initial row count
        initial_dataset = lr.read_lance(str(path))
        initial_df = initial_dataset.to_pandas()
        assert len(initial_df) == 5

        # Create dataset with 2 additional rows
        additional_data = pd.DataFrame(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "age": [50, 55],
                "score": [90.0, 85.0],
            }
        )
        extended_dataset = ray.data.from_pandas(additional_data)

        # Test overwrite mode with extended dataset
        lr.write_lance(extended_dataset, str(path), mode="overwrite")
        assert path.exists()

        # Verify row count after overwrite
        overwritten_dataset = lr.read_lance(str(path))
        overwritten_df = overwritten_dataset.to_pandas()
        assert len(overwritten_df) == 2  # Should have 2 rows after overwrite

    def test_overwrite_mode_allows_schema_change(self, temp_dir):
        """Overwrite writes fragments with the new schema, not append validation."""
        path = Path(temp_dir) / "overwrite_schema_change.lance"
        original_data = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        lr.write_lance(ray.data.from_pandas(original_data), str(path))

        ray_ds = lr.read_lance(str(path))
        updated_ds = ray_ds.map_batches(
            lambda batch: batch.assign(id_2=batch["id"] * 2),
            batch_format="pandas",
        )

        lr.write_lance(updated_ds, str(path), mode="overwrite")

        result = lr.read_lance(str(path)).to_pandas().sort_values("id")
        assert result["id"].tolist() == [1, 2]
        assert result["id_2"].tolist() == [2, 4]

    def test_stream_overwrite_mode_allows_schema_change(self, temp_dir):
        """Streaming overwrite also writes fragments with the new schema."""
        path = Path(temp_dir) / "stream_overwrite_schema_change.lance"
        original_data = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        lr.write_lance(ray.data.from_pandas(original_data), str(path))

        updated_data = pd.DataFrame({"id": [3, 4], "value": [30, 40], "id_2": [6, 8]})
        lr.write_lance(
            ray.data.from_pandas(updated_data),
            str(path),
            mode="overwrite",
            stream=True,
            batch_size=1,
        )

        result = lr.read_lance(str(path)).to_pandas().sort_values("id")
        assert result["id"].tolist() == [3, 4]
        assert result["id_2"].tolist() == [6, 8]

    def test_append_mode_uses_existing_schema(self, temp_dir):
        """Append keeps the existing schema instead of evolving it."""
        path = Path(temp_dir) / "append_schema_change.lance"
        original_data = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        lr.write_lance(ray.data.from_pandas(original_data), str(path))

        additional_data = pd.DataFrame({"id": [3], "value": [30], "id_2": [6]})
        lr.write_lance(
            ray.data.from_pandas(additional_data),
            str(path),
            mode="append",
        )

        result = lr.read_lance(str(path)).to_pandas().sort_values("id")
        assert result.columns.tolist() == ["id", "value"]
        assert result["id"].tolist() == [1, 2, 3]

    def test_read_lance_with_fragment_ids(self, sample_dataset, temp_dir):
        """Test reading with fragment IDs."""
        path = Path(temp_dir) / "fragment_ids_test.lance"
        lr.write_lance(
            sample_dataset, str(path), min_rows_per_file=1, max_rows_per_file=1
        )
        dataset = lr.read_lance(str(path), fragment_ids=[0, 1])
        assert dataset.count() == 2


class TestAddColumns:
    """Test cases for add_columns function."""

    def test_add_columns_basic(self, sample_dataset, temp_dir):
        """Test basic add columns functionality."""
        path = Path(temp_dir) / "add_columns_test.lance"
        lr.write_lance(
            sample_dataset, str(path), min_rows_per_file=3, max_rows_per_file=3
        )

        def double_score(x: pa.RecordBatch) -> pa.RecordBatch:
            df = x.to_pandas()
            return pa.RecordBatch.from_pandas(
                pd.DataFrame({"new_column": df["score"] * 2}),
                schema=pa.schema([pa.field("new_column", pa.float64())]),
            )

        # Add columns
        lr.add_columns(
            str(path),
            transform=double_score,
            concurrency=2,
        )

        # Read it back
        dataset = lr.read_lance(str(path))
        df = dataset.to_pandas()
        assert df.columns.tolist() == ["id", "name", "age", "score", "new_column"]
        assert (df["new_column"] == df["score"] * 2).all()


class TestNamespaceReadWrite:
    """Test cases for read/write with DirectoryNamespace."""

    def test_write_and_read_with_directory_namespace(self, sample_data, temp_dir):
        """Test write and read using DirectoryNamespace."""
        table_id = ["test_table"]

        original_dataset = ray.data.from_pandas(sample_data)
        lr.write_lance(
            original_dataset,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        read_dataset = lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )
        read_df = read_dataset.to_pandas()

        original_sorted = sample_data.sort_values("id").reset_index(drop=True)
        read_sorted = read_df.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(original_sorted, read_sorted)

    def test_overwrite_with_directory_namespace_allows_schema_change(
        self, sample_data, temp_dir
    ):
        """Namespace overwrite writes fragments with the new schema."""
        table_id = ["schema_change_table"]

        lr.write_lance(
            ray.data.from_pandas(sample_data[["id", "score"]]),
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        ray_ds = lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )
        updated_ds = ray_ds.map_batches(
            lambda batch: batch.assign(id_2=batch["id"] * 2),
            batch_format="pandas",
        )

        lr.write_lance(
            updated_ds,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
            mode="overwrite",
        )

        result = lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        ).to_pandas()
        result = result.sort_values("id").reset_index(drop=True)

        assert result["id"].tolist() == sample_data["id"].tolist()
        assert result["id_2"].tolist() == (sample_data["id"] * 2).tolist()


class TestDatasetOptions:
    """Test cases for dataset options in LanceDataset."""

    def test_dataset_with_version(self, sample_dataset, temp_dir):
        """Test dataset options like version and block size."""
        path = Path(temp_dir) / "dataset_options_test.lance"
        lr.write_lance(sample_dataset, str(path))
        lr.write_lance(sample_dataset, str(path), mode="append")

        ds = lance.dataset(str(path))
        versions = ds.versions()
        assert len(versions) == 2
        assert len(ds) == 10

        dataset = lr.read_lance(
            str(path),
            dataset_options={
                "version": versions[0]["version"],
            },
        )
        assert dataset.count() == 5
        dataset = lr.read_lance(
            str(path),
            dataset_options={
                "version": versions[1]["version"],
            },
        )
        assert dataset.count() == 10


try:
    from lance import DatasetBasePath, blob_array, blob_field
except Exception:

    class _Missing:  # type: ignore[no-redef]
        pass

    DatasetBasePath = _Missing
    blob_array = _Missing
    blob_field = _Missing


class TestMultiBaseLayout:
    """Tests for multi-base layout (multiple DatasetBasePath) support.

    These tests verify that lance-ray correctly handles datasets with
    multiple base paths, including auto-assignment of unique base path
    IDs when the user does not specify them explicitly.

    Background:
        pylance's ``DatasetBasePath`` defaults ``id`` to 0.  When
        ``lance.write_dataset`` is used directly, the Rust layer
        automatically re-assigns duplicate-zero IDs to unique values
        (1, 2, 3, …).  However, lance-ray splits write and commit into
        two steps (``write_fragments`` → ``LanceOperation.Overwrite`` →
        ``LanceDataset.commit``), and the ``commit`` path does **not**
        perform auto-assignment.  Without a fix in lance-ray's
        ``normalize_initial_bases``, multiple bases without explicit IDs
        will all carry ``id=0`` and trigger a Rust-level
        ``Duplicate base path ID 0`` error.
    """

    def test_multiple_initial_bases_without_explicit_id(self, temp_dir):
        """Multiple DatasetBasePath objects without explicit id should not collide.

        When the user provides two (or more) ``DatasetBasePath`` objects
        without specifying ``id``, they both default to 0.  lance-ray
        must auto-assign unique IDs before committing, otherwise the
        Rust layer rejects the transaction with
        ``Duplicate base path ID 0 detected``.
        """
        base1_dir = Path(temp_dir) / "base1"
        base2_dir = Path(temp_dir) / "base2"
        base1_dir.mkdir()
        base2_dir.mkdir()

        uri = str(Path(temp_dir) / "multi_base_no_id.lance")

        table = pa.table(
            {
                "id": [1, 2, 3],
                "value": ["a", "b", "c"],
            }
        )
        ray_ds = ray.data.from_arrow(table)

        lr.write_lance(
            ray_ds,
            uri=uri,
            mode="create",
            initial_bases=[
                DatasetBasePath(path=base1_dir.as_uri(), name="base1"),
                DatasetBasePath(path=base2_dir.as_uri(), name="base2"),
            ],
        )

        ds = lance.dataset(uri)
        assert ds.count_rows() == 3

        base_paths = ds._ds.base_paths()
        assert len(base_paths) >= 2
        base_ids = list(base_paths.keys())
        assert len(set(base_ids)) == len(base_ids), (
            f"Base path IDs must be unique, got: {base_paths}"
        )

    @pytest.mark.skipif(
        bool(missing_fragment_write_options("base_store_params")),
        reason=fragment_write_options_skip_reason("base_store_params"),
    )
    def test_multiple_initial_bases_with_blob_v2(self, temp_dir):
        """Multi-base write/read with blob v2 columns and no explicit IDs.

        This is the full end-to-end scenario: blob data lives across
        multiple base paths, each with potentially different storage
        credentials.  Without auto-assigned IDs the commit fails.
        """
        base1_dir = Path(temp_dir) / "blob_base1"
        base2_dir = Path(temp_dir) / "blob_base2"
        base1_dir.mkdir()
        base2_dir.mkdir()

        uri = str(Path(temp_dir) / "multi_base_blob.lance")

        inline_payload = b"inline-bytes"
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                blob_field("blob", nullable=True),
            ]
        )

        table = pa.table(
            {
                "id": [1, 2],
                "blob": blob_array([inline_payload, None]),
            },
            schema=schema,
        )
        ray_ds = ray.data.from_arrow(table)

        base_store_params = {
            base1_dir.as_uri(): {},
            base2_dir.as_uri(): {},
        }

        lr.write_lance(
            ray_ds,
            uri=uri,
            mode="create",
            schema=schema,
            data_storage_version="2.2",
            base_store_params=base_store_params,
            initial_bases=[
                DatasetBasePath(path=base1_dir.as_uri(), name="blob_base1"),
                DatasetBasePath(path=base2_dir.as_uri(), name="blob_base2"),
            ],
        )

        ds = lance.dataset(uri, base_store_params=base_store_params)
        assert ds.count_rows() == 2

        base_paths = ds._ds.base_paths()
        assert len(base_paths) >= 2
        base_ids = list(base_paths.keys())
        assert len(set(base_ids)) == len(base_ids), (
            f"Base path IDs must be unique, got: {base_paths}"
        )

    def test_explicit_ids_are_preserved(self, temp_dir):
        """When the user provides explicit IDs, they must be preserved."""
        base1_dir = Path(temp_dir) / "explicit_base1"
        base2_dir = Path(temp_dir) / "explicit_base2"
        base1_dir.mkdir()
        base2_dir.mkdir()

        uri = str(Path(temp_dir) / "multi_base_explicit_id.lance")

        table = pa.table(
            {
                "id": [1, 2],
                "value": ["x", "y"],
            }
        )
        ray_ds = ray.data.from_arrow(table)

        lr.write_lance(
            ray_ds,
            uri=uri,
            mode="create",
            initial_bases=[
                DatasetBasePath(path=base1_dir.as_uri(), name="base1", id=5),
                DatasetBasePath(path=base2_dir.as_uri(), name="base2", id=10),
            ],
        )

        ds = lance.dataset(uri)
        assert ds.count_rows() == 2

        base_paths = ds._ds.base_paths()
        assert 5 in base_paths
        assert 10 in base_paths

    def test_mixed_explicit_and_implicit_ids(self, temp_dir):
        """One base with explicit id, one without — no collision."""
        base1_dir = Path(temp_dir) / "mixed_base1"
        base2_dir = Path(temp_dir) / "mixed_base2"
        base1_dir.mkdir()
        base2_dir.mkdir()

        uri = str(Path(temp_dir) / "multi_base_mixed_id.lance")

        table = pa.table(
            {
                "id": [1, 2],
                "value": ["x", "y"],
            }
        )
        ray_ds = ray.data.from_arrow(table)

        lr.write_lance(
            ray_ds,
            uri=uri,
            mode="create",
            initial_bases=[
                DatasetBasePath(path=base1_dir.as_uri(), name="base1", id=3),
                DatasetBasePath(path=base2_dir.as_uri(), name="base2"),
            ],
        )

        ds = lance.dataset(uri)
        assert ds.count_rows() == 2

        base_paths = ds._ds.base_paths()
        assert 3 in base_paths
        base_ids = list(base_paths.keys())
        assert len(set(base_ids)) == len(base_ids), (
            f"Base path IDs must be unique, got: {base_paths}"
        )

    def test_dataset_root_base_gets_id_zero(self, temp_dir):
        """A base with is_dataset_root=True should receive id=0."""
        base1_dir = Path(temp_dir) / "root_base"
        base2_dir = Path(temp_dir) / "extra_base"
        base1_dir.mkdir()
        base2_dir.mkdir()

        uri = str(Path(temp_dir) / "multi_base_root.lance")

        table = pa.table(
            {
                "id": [1, 2],
                "value": ["x", "y"],
            }
        )
        ray_ds = ray.data.from_arrow(table)

        lr.write_lance(
            ray_ds,
            uri=uri,
            mode="create",
            initial_bases=[
                DatasetBasePath(
                    path=base1_dir.as_uri(), name="root", is_dataset_root=True
                ),
                DatasetBasePath(path=base2_dir.as_uri(), name="extra"),
            ],
        )

        ds = lance.dataset(uri)
        assert ds.count_rows() == 2

        base_paths = ds._ds.base_paths()
        assert 0 in base_paths
