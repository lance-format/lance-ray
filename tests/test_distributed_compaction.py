"""Test cases for lance_ray.compaction module."""

import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pytest
import ray
from lance.optimize import CompactionOptions

import pandas as pd


@pytest.fixture(scope="session", autouse=True)
def ray_context():
    """Initialize Ray for testing."""
    # Shutdown Ray if it's already running to avoid conflicts
    if ray.is_initialized():
        ray.shutdown()

    # Initialize Ray with minimal configuration
    ray.init(local_mode=False, ignore_reinit_error=True)
    yield

    # Clean shutdown
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def create_dataset_with_fragments(path, fragment_data):
    """
    Create a Lance dataset with multiple fragments.

    Args:
        path: Path to create the dataset
        fragment_data: List of DataFrames, each becomes a separate fragment
    """
    # Create the dataset with the first fragment
    first_df = fragment_data[0]
    first_ray_ds = ray.data.from_pandas(first_df)
    lr.write_lance(
        first_ray_ds,
        str(path),
        min_rows_per_file=len(first_df),
        max_rows_per_file=len(first_df),
    )

    # Append remaining fragments
    for df in fragment_data[1:]:
        ray_ds = ray.data.from_pandas(df)
        lr.write_lance(
            ray_ds,
            str(path),
            mode="append",
            min_rows_per_file=len(df),
            max_rows_per_file=len(df),
        )

    return lance.dataset(str(path))


class TestDistributedCompaction:
    """Test cases for distributed compaction functionality."""

    def test_basic_compaction(self, temp_dir):
        """
        Test basic compaction that merges two fragments into one.
        """
        dataset_path = Path(temp_dir) / "test_dataset_for_compaction"

        # Create two fragments with 10 rows each
        fragment1 = pd.DataFrame(
            {
                "id": range(1, 11),
                "value": [f"row_{i}" for i in range(1, 11)],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": range(11, 21),
                "value": [f"row_{i}" for i in range(11, 21)],
            }
        )

        dataset = create_dataset_with_fragments(dataset_path, [fragment1, fragment2])

        # Verify we have 2 fragments initially
        assert len(dataset.get_fragments()) == 2, "Should start with 2 fragments"
        assert dataset.count_rows() == 20, "Should have 20 total rows"

        # Configure compaction to merge fragments (target 100 rows per fragment)
        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        # Execute distributed compaction
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=2,
        )

        # Verify compaction metrics
        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

        # Reload dataset and verify final state
        dataset = lance.dataset(str(dataset_path))
        fragments = dataset.get_fragments()

        assert len(fragments) == 1, "Should have 1 fragment after compaction"
        assert fragments[0].count_rows() == 20, "Fragment should have 20 rows"
        assert dataset.count_rows() == 20, "Should still have 20 total rows"

    def test_deletion_compaction(self, temp_dir):
        """
        Test compaction that materializes deletions.
        """
        dataset_path = Path(temp_dir) / "test_dataset_for_deletion_compaction"

        # Create two fragments with 10 rows each
        fragment1 = pd.DataFrame(
            {
                "id": range(1, 11),
                "value": [f"row_{i}" for i in range(1, 11)],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": range(11, 21),
                "value": [f"row_{i}" for i in range(11, 21)],
            }
        )

        dataset = create_dataset_with_fragments(dataset_path, [fragment1, fragment2])

        # Verify initial state
        assert len(dataset.get_fragments()) == 2, "Should start with 2 fragments"
        assert dataset.count_rows() == 20, "Should have 20 total rows"

        # Delete rows where id <= 9 (9 rows total)
        # This leaves 11 rows: id 10 from fragment1, and ids 11-20 from fragment2
        dataset.delete("id <= 9")

        # Reload to see the deletion
        dataset = lance.dataset(str(dataset_path))

        # Should still have 2 fragments (deletions not materialized yet)
        assert len(dataset.get_fragments()) == 2, "Should still have 2 fragments"
        assert dataset.count_rows() == 11, "Should have 11 rows after deletion"

        # Configure compaction to materialize deletions
        compaction_options = CompactionOptions(
            materialize_deletions=True,
            materialize_deletions_threshold=0.5,  # 50% threshold
            num_threads=1,
        )

        # Execute distributed compaction
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=2,
        )

        # Verify compaction metrics
        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

        # Reload dataset and verify final state
        dataset = lance.dataset(str(dataset_path))
        fragments = dataset.get_fragments()

        assert len(fragments) == 1, "Should have 1 fragment after compaction"
        assert fragments[0].count_rows() == 11, "Fragment should have 11 rows"
        assert dataset.count_rows() == 11, "Should have 11 total rows"

    def test_compaction_with_many_fragments(self, temp_dir):
        """Test compaction with many small fragments."""
        dataset_path = Path(temp_dir) / "test_many_fragments_compaction"

        # Create 1000 fragments with 5 rows each
        fragments = [
            pd.DataFrame(
                {
                    "id": range(i * 5, (i + 1) * 5),
                    "value": [f"frag_{i}_row_{j}" for j in range(5)],
                }
            )
            for i in range(1000)
        ]

        dataset = create_dataset_with_fragments(dataset_path, fragments)

        # Verify initial state
        assert len(dataset.get_fragments()) == 1000, "Should start with 1000 fragments"
        assert dataset.count_rows() == 5000, "Should have 5000 total rows"

        # Configure compaction to merge small fragments
        compaction_options = CompactionOptions(
            target_rows_per_fragment=20,
            num_threads=1,
        )

        # Execute distributed compaction with 4 workers
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=4,
        )

        # Verify compaction happened
        assert metrics.fragments_removed == 1000, "Should remove all fragments"
        assert metrics.fragments_added == 250, (
            "Should add 250 fragments as target_rows_per_fragment = 20"
        )

        # Reload dataset and verify data integrity
        dataset = lance.dataset(str(dataset_path))
        assert dataset.count_rows() == 5000, "Should still have 5000 total rows"

    def test_compaction_no_work_needed(self, temp_dir):
        """Test compaction when no work is needed."""
        dataset_path = Path(temp_dir) / "test_no_compaction_needed"

        # Create a single fragment with optimal size
        fragment = pd.DataFrame(
            {
                "id": range(100),
                "value": [f"row_{i}" for i in range(100)],
            }
        )

        dataset = create_dataset_with_fragments(dataset_path, [fragment])

        # Verify initial state
        assert len(dataset.get_fragments()) == 1, "Should start with 1 fragment"

        # Configure compaction with target that matches current state
        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        # Execute distributed compaction
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=2,
        )

        # Should be no-op (returns None when no work is needed)
        assert metrics is None, "Should return None when no compaction work is needed"

    def test_compaction_with_ray_remote_args(self, temp_dir):
        """Test compaction with Ray remote args."""
        dataset_path = Path(temp_dir) / "test_ray_args_compaction"

        # Create two fragments
        fragment1 = pd.DataFrame(
            {
                "id": range(10),
                "value": [f"row_{i}" for i in range(10)],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": range(10, 20),
                "value": [f"row_{i}" for i in range(10, 20)],
            }
        )

        create_dataset_with_fragments(dataset_path, [fragment1, fragment2])

        # Configure compaction
        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        # Execute distributed compaction with Ray options
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=2,
            ray_remote_args={"num_cpus": 1},
        )

        # Verify compaction worked
        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

    def test_compaction_with_storage_options(self, temp_dir):
        """Test compaction with storage options."""
        dataset_path = Path(temp_dir) / "test_storage_options_compaction"

        # Create two fragments
        fragment1 = pd.DataFrame(
            {
                "id": range(10),
                "value": [f"row_{i}" for i in range(10)],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": range(10, 20),
                "value": [f"row_{i}" for i in range(10, 20)],
            }
        )

        create_dataset_with_fragments(dataset_path, [fragment1, fragment2])

        # Configure compaction
        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        # Execute distributed compaction with storage options
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=2,
            storage_options={},  # Empty storage options should work
        )

        # Verify compaction worked
        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

    def test_compaction_auto_adjust_workers(self, temp_dir):
        """Test that num_workers is automatically adjusted if it exceeds task count."""
        dataset_path = Path(temp_dir) / "test_auto_adjust_workers"

        # Create two fragments
        fragment1 = pd.DataFrame(
            {
                "id": range(10),
                "value": [f"row_{i}" for i in range(10)],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": range(10, 20),
                "value": [f"row_{i}" for i in range(10, 20)],
            }
        )

        create_dataset_with_fragments(dataset_path, [fragment1, fragment2])

        # Configure compaction
        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        # Request more workers than tasks
        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=10,  # More than needed
        )

        # Should still work and create the compaction
        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

    def test_compaction_preserves_data(self, temp_dir):
        """Test that compaction preserves all data correctly."""
        dataset_path = Path(temp_dir) / "test_data_preservation"

        # Create fragments with specific data we can verify
        fragment1 = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [95.5, 87.3, 92.1],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": [4, 5, 6],
                "name": ["David", "Eve", "Frank"],
                "score": [88.7, 91.2, 85.9],
            }
        )

        dataset = create_dataset_with_fragments(dataset_path, [fragment1, fragment2])

        # Get original data
        original_data = (
            dataset.to_table().to_pandas().sort_values("id").reset_index(drop=True)
        )

        # Configure and execute compaction
        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        metrics = lr.compact_files(
            uri=str(dataset_path),
            compaction_options=compaction_options,
            num_workers=2,
        )

        # Reload dataset and verify data
        dataset = lance.dataset(str(dataset_path))
        compacted_data = (
            dataset.to_table().to_pandas().sort_values("id").reset_index(drop=True)
        )

        # Verify all data is preserved
        pd.testing.assert_frame_equal(original_data, compacted_data)
        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

    def test_compaction_with_directory_namespace(self, temp_dir):
        """Test compaction using DirectoryNamespace for credentials vending."""
        import lance_namespace as ln

        namespace = ln.connect("dir", {"root": temp_dir})
        table_id = ["compaction_test_table"]

        fragment1 = pd.DataFrame(
            {
                "id": range(1, 11),
                "value": [f"row_{i}" for i in range(1, 11)],
            }
        )
        fragment2 = pd.DataFrame(
            {
                "id": range(11, 21),
                "value": [f"row_{i}" for i in range(11, 21)],
            }
        )

        first_ray_ds = ray.data.from_pandas(fragment1)
        lr.write_lance(
            first_ray_ds,
            namespace=namespace,
            table_id=table_id,
            min_rows_per_file=10,
            max_rows_per_file=10,
        )

        second_ray_ds = ray.data.from_pandas(fragment2)
        lr.write_lance(
            second_ray_ds,
            namespace=namespace,
            table_id=table_id,
            mode="append",
            min_rows_per_file=10,
            max_rows_per_file=10,
        )

        from lance_namespace import DescribeTableRequest

        describe_response = namespace.describe_table(DescribeTableRequest(id=table_id))
        uri = describe_response.location

        dataset = lance.dataset(uri)
        assert len(dataset.get_fragments()) == 2, "Should start with 2 fragments"
        assert dataset.count_rows() == 20, "Should have 20 total rows"

        compaction_options = CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        )

        metrics = lr.compact_files(
            uri=uri,
            compaction_options=compaction_options,
            num_workers=2,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        assert metrics.fragments_removed == 2, "Should remove 2 fragments"
        assert metrics.fragments_added == 1, "Should add 1 fragment"

        dataset = lance.dataset(uri)
        assert len(dataset.get_fragments()) == 1, (
            "Should have 1 fragment after compaction"
        )
        assert dataset.count_rows() == 20, "Should still have 20 total rows"
