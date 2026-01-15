"""Test cases for lance_ray.indexing module."""

import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pytest
import ray
from packaging import version

import pandas as pd


def check_lance_version_compatibility():
    """Check if lance version supports distributed indexing."""
    try:
        lance_version = version.parse(lance.__version__)
        min_required_version = version.parse("0.36.0")
        return lance_version >= min_required_version
    except (AttributeError, Exception):
        return False


# Skip all distributed indexing tests if lance version is incompatible
pytestmark = pytest.mark.skipif(
    not check_lance_version_compatibility(),
    reason="Distributed indexing requires pylance >= 0.36.0. Current version: {}".format(
        getattr(lance, "__version__", "unknown")
    ),
)


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
def text_data():
    """Create sample text data for indexing tests."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "text": [
                "The quick brown fox jumps over the lazy dog",
                "Python is a powerful programming language",
                "Machine learning algorithms are fascinating",
                "Data science requires statistical knowledge",
                "Natural language processing uses text analysis",
                "Distributed computing scales horizontally",
                "Ray framework enables parallel processing",
                "Lance format provides efficient storage",
            ],
            "category": [
                "animals",
                "tech",
                "ml",
                "data",
                "nlp",
                "distributed",
                "ray",
                "storage",
            ],
        }
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def text_dataset(text_data):
    """Create a Ray Dataset from text data."""
    return ray.data.from_pandas(text_data)


@pytest.fixture
def multi_fragment_lance_dataset(text_dataset, temp_dir):
    """Create a Lance dataset with multiple fragments for testing."""
    path = Path(temp_dir) / "multi_fragment_text.lance"
    # Create dataset with multiple fragments (2 rows per fragment)
    lr.write_lance(text_dataset, str(path), min_rows_per_file=2, max_rows_per_file=2)
    return str(path)


def generate_multi_fragment_dataset(tmp_path, num_fragments=4, rows_per_fragment=250):
    """Generate a test dataset with multiple fragments."""
    all_data = []
    for frag_idx in range(num_fragments):
        for row_idx in range(rows_per_fragment):
            row_id = frag_idx * rows_per_fragment + row_idx
            all_data.append(
                {
                    "id": row_id,
                    "text": f"This is test document {row_id} with some sample text content for fragment {frag_idx}",
                    "fragment_id": frag_idx,
                }
            )

    df = pd.DataFrame(all_data)
    dataset = ray.data.from_pandas(df)

    path = Path(tmp_path) / "large_multi_fragment.lance"
    lr.write_lance(
        dataset,
        str(path),
        min_rows_per_file=rows_per_fragment,
        max_rows_per_file=rows_per_fragment,
    )

    return lance.dataset(str(path))


class TestDistributedIndexing:
    """Test cases for distributed indexing functionality."""

    def test_build_distributed_fts_index_basic(self, multi_fragment_lance_dataset):
        """Test basic distributed FTS index building."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Find our index
        text_index = None
        for idx in indices:
            if "text" in idx["name"]:
                text_index = idx
                break

        assert text_index is not None, "Text index not found"
        assert text_index["type"] == "Inverted", (
            f"Expected Inverted index, got {text_index['type']}"
        )

    def test_build_distributed_fts_index_with_name(self, multi_fragment_lance_dataset):
        """Test building distributed index with custom name."""
        dataset_uri = multi_fragment_lance_dataset
        custom_name = "custom_text_index"

        # Build distributed index with custom name
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=custom_name,
            num_workers=2,
        )

        # Verify the index was created with correct name
        indices = updated_dataset.list_indices()
        index_names = [idx["name"] for idx in indices]
        assert custom_name in index_names, (
            f"Custom index name '{custom_name}' not found in {index_names}"
        )

    def test_build_distributed_fts_index_search_functionality(
        self, multi_fragment_lance_dataset
    ):
        """Test that the built index actually works for searching."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
        )

        # Test full-text search functionality
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        # Should find at least one result containing "Python"
        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

        # Verify results contain the search term
        text_results = results.column("text").to_pylist()
        assert any(search_term in text for text in text_results), (
            "Search results don't contain the search term"
        )

    def test_build_distributed_fts_index_fts_type(self, multi_fragment_lance_dataset):
        """Test building distributed FTS index."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed FTS index
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_large_dataset(self, temp_dir):
        """Test distributed indexing on a larger dataset with multiple fragments."""
        # Generate larger dataset
        dataset = generate_multi_fragment_dataset(
            temp_dir, num_fragments=4, rows_per_fragment=50
        )

        # Build distributed index
        updated_dataset = lr.create_scalar_index(
            uri=dataset.uri,
            column="text",
            index_type="INVERTED",
            num_workers=4,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Test search functionality
        search_term = "test"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

    def test_build_distributed_index_invalid_column(self, multi_fragment_lance_dataset):
        """Test error handling for invalid column."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            lr.create_scalar_index(
                uri=dataset_uri,
                column="nonexistent",
                index_type="INVERTED",
                num_workers=2,
            )

    def test_build_distributed_index_invalid_index_type(
        self, multi_fragment_lance_dataset
    ):
        """Test error handling for invalid index type."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(
            ValueError,
            match=r"Index type must be one of \['BTREE', 'BITMAP', 'LABEL_LIST', 'INVERTED', 'FTS', 'NGRAM', 'ZONEMAP'\], not 'INVALID'",
        ):
            lr.create_scalar_index(
                uri=dataset_uri,
                column="text",
                index_type="INVALID",
                num_workers=2,
            )

    def test_build_distributed_index_invalid_num_workers(
        self, multi_fragment_lance_dataset
    ):
        """Test error handling for invalid num_workers."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="num_workers must be positive"):
            lr.create_scalar_index(
                uri=dataset_uri,
                column="text",
                index_type="INVERTED",
                num_workers=0,
            )

    def test_build_distributed_index_empty_column(self, multi_fragment_lance_dataset):
        """Test error handling for empty column name."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="Column name cannot be empty"):
            lr.create_scalar_index(
                uri=dataset_uri,
                column="",
                index_type="INVERTED",
                num_workers=2,
            )

    def test_build_distributed_index_non_string_column(self, temp_dir):
        """Test error handling for non-string column."""
        # Create dataset with non-string column
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "numeric_col": [10, 20, 30, 40],
                "text": ["text1", "text2", "text3", "text4"],
            }
        )
        dataset = ray.data.from_pandas(data)
        path = Path(temp_dir) / "non_string_test.lance"
        lr.write_lance(dataset, str(path), min_rows_per_file=2, max_rows_per_file=2)

        with pytest.raises(TypeError, match="must be string type"):
            lr.create_scalar_index(
                uri=str(path),
                column="numeric_col",
                index_type="INVERTED",
                num_workers=2,
            )

    def test_build_distributed_index_with_ray_remote_args(
        self, multi_fragment_lance_dataset
    ):
        """Test building distributed index with Ray options."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with Ray options
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
            ray_remote_args={"num_cpus": 1},
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_with_storage_options(
        self, multi_fragment_lance_dataset
    ):
        """Test building distributed index with storage options."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with storage options
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
            storage_options={},  # Empty storage options should work
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_with_kwargs(self, multi_fragment_lance_dataset):
        """Test building distributed index with additional kwargs."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with additional kwargs
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
            remove_stop_words=False,  # Additional kwarg for create_scalar_index
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_dataset_object(self, multi_fragment_lance_dataset):
        """Test building distributed index with Lance dataset object instead of URI."""
        dataset = lance.dataset(multi_fragment_lance_dataset)

        # Build distributed index using dataset object
        updated_dataset = lr.create_scalar_index(
            uri=dataset.uri,
            column="text",
            index_type="INVERTED",
            num_workers=2,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_replace_false_existing_index(
        self, multi_fragment_lance_dataset
    ):
        """Test that replace=False raises error when trying to create index with existing name."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "test_replace_false_index"

        # First, create an index
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            num_workers=2,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "Initial index creation failed"

        # Now try to create another index with the same name but replace=False
        # The error might be raised as RuntimeError during distributed processing
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            lr.create_scalar_index(
                uri=dataset_uri,
                column="text",
                index_type="INVERTED",
                name=index_name,
                replace=False,
                num_workers=2,
            )

        # Verify the error message contains information about existing index
        error_msg = str(exc_info.value)
        assert "already exists" in error_msg and index_name in error_msg

    def test_build_distributed_index_replace_true_overwrite_existing(
        self, multi_fragment_lance_dataset
    ):
        """Test that replace=True successfully overwrites existing index."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "test_replace_true_index"

        # First, create an index
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            num_workers=2,
        )

        # Verify the index was created
        initial_indices = updated_dataset.list_indices()
        assert len(initial_indices) > 0, "Initial index creation failed"

        # Find our initial index
        initial_index = None
        for idx in initial_indices:
            if idx["name"] == index_name:
                initial_index = idx
                break
        assert initial_index is not None, "Initial index not found"

        # Now create another index with the same name but replace=True
        updated_dataset = lr.create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            replace=True,
            num_workers=2,
        )

        # Verify the index still exists (should have been replaced)
        final_indices = updated_dataset.list_indices()
        final_index = None
        for idx in final_indices:
            if idx["name"] == index_name:
                final_index = idx
                break

        assert final_index is not None, "Index should still exist after replacement"
        assert final_index["type"] == "Inverted", "Index type should remain Inverted"

        # Test that the replaced index still works for searching
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, (
            f"No results found for search term '{search_term}' after index replacement"
        )

    def test_build_distributed_index_auto_adjust_workers(self, temp_dir):
        """Test that num_workers is automatically adjusted if it exceeds fragment count."""
        # Create dataset with only 2 fragments
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "text": ["text1", "text2", "text3", "text4"],
            }
        )
        dataset = ray.data.from_pandas(data)
        path = Path(temp_dir) / "small_dataset.lance"
        lr.write_lance(dataset, str(path), min_rows_per_file=2, max_rows_per_file=2)

        # Request more workers than fragments
        updated_dataset = lr.create_scalar_index(
            uri=str(path),
            column="text",
            index_type="INVERTED",
            num_workers=10,  # More than the 2 fragments
        )

        # Should still work and create the index
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_distributed_fts_index_new_api(self, temp_dir):
        """
        Test distributed FTS index building using the new API from PR #4578.
        This test demonstrates the new workflow with execute_uncommitted() and merge_index_metadata().
        """
        # Generate test dataset with multiple fragments
        ds = generate_multi_fragment_dataset(
            temp_dir, num_fragments=4, rows_per_fragment=250
        )

        # Test with the new distributed index building function
        updated_dataset = lr.create_scalar_index(
            uri=ds.uri,
            column="text",
            index_type="INVERTED",
            name="new_api_test_idx",
            num_workers=2,
            remove_stop_words=False,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after distributed index creation"

        # Find our index
        our_index = None
        for idx in indices:
            if idx["name"] == "new_api_test_idx":
                our_index = idx
                break

        assert our_index is not None, (
            "Index 'new_api_test_idx' not found in indices list"
        )
        assert our_index["type"] == "Inverted", (
            f"Expected Inverted index, got {our_index['type']}"
        )

        # Test that the index works for searching
        sample_data = updated_dataset.take([0], columns=["text"])
        sample_text = sample_data.column(0)[0].as_py()
        search_word = sample_text.split()[0] if sample_text.split() else "test"

        # Perform a full-text search to verify the index works
        results = updated_dataset.scanner(
            full_text_query=search_word,
            columns=["id", "text"],
        ).to_table()

        print(f"Search for '{search_word}' returned {results.num_rows} results")
        assert results.num_rows > 0, f"No results found for search term '{search_word}'"

    def test_distributed_index_with_index_uuid(self, temp_dir):
        """
        Test distributed index building with explicit fragment UUID handling.
        This tests the new index_uuid parameter from PR #4578.
        """
        # Generate test dataset
        ds = generate_multi_fragment_dataset(
            temp_dir, num_fragments=3, rows_per_fragment=100
        )

        # Test with explicit fragment UUID handling
        updated_dataset = lr.create_scalar_index(
            uri=ds.uri,
            column="text",
            index_type="INVERTED",
            name="index_uuid_test_idx",
            num_workers=2,
        )

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after index creation"

        # Find our index
        our_index = None
        for idx in indices:
            if idx["name"] == "index_uuid_test_idx":
                our_index = idx
                break

        assert our_index is not None, "Index 'index_uuid_test_idx' not found"
        assert our_index["type"] == "Inverted", (
            f"Expected Inverted index, got {our_index['type']}"
        )

    def test_distributed_index_error_handling_new_api(self, temp_dir):
        """
        Test error handling in the new distributed indexing API.
        """
        # Generate test dataset
        ds = generate_multi_fragment_dataset(
            temp_dir, num_fragments=2, rows_per_fragment=50
        )

        # Test with invalid parameters that should be caught by the new API
        with pytest.raises(ValueError, match="Column name cannot be empty"):
            lr.create_scalar_index(
                uri=ds.uri,
                column="",
                index_type="INVERTED",
                num_workers=2,
            )

        # Test with invalid index type
        with pytest.raises(
            ValueError,
            match=r"Index type must be one of \['BTREE', 'BITMAP', 'LABEL_LIST', 'INVERTED', 'FTS', 'NGRAM', 'ZONEMAP'\], not 'INVALID_TYPE'",
        ):
            lr.create_scalar_index(
                uri=ds.uri,
                column="text",
                index_type="INVALID_TYPE",
                num_workers=2,
            )


def check_btree_version_compatibility():
    """Check if lance version supports distributed B-tree indexing (>= 0.37.0)."""
    try:
        lance_version = version.parse(lance.__version__)
        btree_min_version = version.parse("0.37.0")
        return lance_version >= btree_min_version
    except (AttributeError, Exception):
        return False


@pytest.mark.skipif(
    not check_btree_version_compatibility(),
    reason="B-tree indexing requires pylance >= 0.37.0. Current version: {}".format(
        getattr(lance, "__version__", "unknown")
    ),
)
class TestDistributedBTreeIndexing:
    """Distributed BTREE indexing tests using the unified lr.create_scalar_index entrypoint."""

    def test_distributed_btree_index_basic(self, temp_dir):
        """Build a distributed BTREE index and verify search works and type is BTree."""
        ds = generate_multi_fragment_dataset(
            temp_dir, num_fragments=3, rows_per_fragment=500
        )

        updated_dataset = lr.create_scalar_index(
            uri=ds.uri,
            column="id",
            index_type="BTREE",
            name="btree_multiple_fragment_idx",
            replace=False,
            num_workers=3,
        )

        # Verify index
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after distributed BTREE build"

        our_index = None
        for idx in indices:
            if idx["name"] == "btree_multiple_fragment_idx":
                our_index = idx
                break
        assert our_index is not None, "BTREE index not found by name"
        assert our_index["type"] == "BTree", (
            f"Expected BTree index, got {our_index['type']}"
        )

        # Spot-check equality and range queries
        eq_id = 100
        eq_tbl = updated_dataset.scanner(
            filter=f"id = {eq_id}", columns=["id", "text"]
        ).to_table()
        assert eq_tbl.num_rows == 1

        rg_tbl = updated_dataset.scanner(
            filter="id >= 200 AND id < 800",
            columns=["id", "text"],
        ).to_table()
        assert rg_tbl.num_rows > 0

    @pytest.fixture
    def btree_comp_datasets(self, tmp_path):
        """Build two datasets: one with a distributed BTREE index and one without index as baseline."""
        with_index = generate_multi_fragment_dataset(
            tmp_path / "with_index", num_fragments=3, rows_per_fragment=500
        )
        without_index = generate_multi_fragment_dataset(
            tmp_path / "without_index", num_fragments=3, rows_per_fragment=500
        )

        # Build BTREE index on the first dataset using unified API
        with_index = lr.create_scalar_index(
            uri=with_index.uri,
            column="id",
            index_type="BTREE",
            name="btree_comp_idx",
            replace=True,
            num_workers=2,
        )

        return {"with_index": with_index, "without_index": without_index}

    @pytest.mark.parametrize(
        "test_name,filter_expr",
        [
            ("First value", "id = 0"),
            ("Fragment 0 last value", "id = 499"),
            ("Fragment 1 first value", "id = 500"),
            ("Fragment 1 last value", "id = 999"),
            ("Fragment 2 first value", "id = 1000"),
            ("Last value", "id = 1499"),
            ("Fragment 0 middle", "id = 250"),
            ("Fragment 1 middle", "id = 750"),
            ("Fragment 2 middle", "id = 1250"),
            ("Range within fragment 0", "id >= 10 AND id < 20"),
            ("Range within fragment 1", "id >= 510 AND id < 520"),
            ("Range within fragment 2", "id >= 1010 AND id < 1020"),
            ("Cross fragment 0-1", "id >= 495 AND id < 505"),
            ("Cross fragment 1-2", "id >= 995 AND id < 1005"),
            ("Cross all fragments", "id >= 250 AND id < 1250"),
            ("Non-existent small value", "id = -1"),
            ("Non-existent large value", "id = 2000"),
            ("Large range", "id >= 0 AND id < 1500"),
            ("Less than boundary", "id < 500"),
            ("Greater than boundary", "id > 999"),
            ("Less than or equal", "id <= 505"),
            ("Greater than or equal", "id >= 995"),
        ],
    )
    def test_btree_query_results_match_baseline(
        self, btree_comp_datasets, test_name, filter_expr
    ):
        """Compare query results between an indexed dataset and an identical baseline dataset without index."""
        with_index = btree_comp_datasets["with_index"]
        without_index = btree_comp_datasets["without_index"]

        res_idx = with_index.scanner(
            filter=filter_expr, columns=["id", "text"]
        ).to_table()
        res_base = without_index.scanner(
            filter=filter_expr, columns=["id", "text"]
        ).to_table()

        assert res_idx.num_rows == res_base.num_rows, (
            f"Test '{test_name}' failed: indexed returned {res_idx.num_rows} rows, "
            f"baseline returned {res_base.num_rows} rows for filter: {filter_expr}"
        )

        if res_idx.num_rows > 0:
            ids_idx = sorted(res_idx.column("id").to_pylist())
            ids_base = sorted(res_base.column("id").to_pylist())
            assert ids_idx == ids_base, (
                f"Test '{test_name}' failed: indexed and baseline results differ for filter: {filter_expr}"
            )

    def test_distributed_btree_index_many_fragments_many_workers(self, temp_dir):
        """
        Test distributed BTREE index building with many fragments and many workers.

        This test reproduces the scenario reported in the bug where:
        - 5000 fragments with 16 workers resulted in "No partition metadata files found"
        - The test uses a smaller scale (50 fragments, 16 workers) for faster execution
        """
        available_cpus = int(ray.cluster_resources().get("CPU", 4))
        num_workers = min(16, available_cpus)

        ds = generate_multi_fragment_dataset(
            temp_dir, num_fragments=50, rows_per_fragment=100
        )

        fragments = list(ds.get_fragments())
        assert len(fragments) == 50, f"Expected 50 fragments, got {len(fragments)}"

        updated_dataset = lr.create_scalar_index(
            uri=ds.uri,
            column="id",
            index_type="BTREE",
            name="btree_many_workers_idx",
            replace=False,
            num_workers=num_workers,
        )

        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after distributed BTREE build"

        our_index = None
        for idx in indices:
            if idx["name"] == "btree_many_workers_idx":
                our_index = idx
                break
        assert our_index is not None, "BTREE index not found by name"
        assert our_index["type"] == "BTree", (
            f"Expected BTree index, got {our_index['type']}"
        )

        eq_tbl = updated_dataset.scanner(filter="id = 2500", columns=["id"]).to_table()
        assert eq_tbl.num_rows == 1, "Exact match query failed"

        rg_tbl = updated_dataset.scanner(
            filter="id >= 1000 AND id < 4000",
            columns=["id"],
        ).to_table()
        assert rg_tbl.num_rows == 3000, (
            f"Range query returned {rg_tbl.num_rows} rows, expected 3000"
        )

    def test_distributed_btree_index_string_column(self, temp_dir):
        """Test distributed BTREE index on string column (like video_uuid in the bug report)."""
        import uuid as uuid_module

        available_cpus = int(ray.cluster_resources().get("CPU", 4))
        num_workers = min(8, available_cpus)

        all_data = []
        num_fragments = 20
        rows_per_fragment = 100
        for frag_idx in range(num_fragments):
            for row_idx in range(rows_per_fragment):
                row_id = frag_idx * rows_per_fragment + row_idx
                all_data.append(
                    {
                        "id": row_id,
                        "video_uuid": str(uuid_module.uuid4()),
                        "fragment_id": frag_idx,
                    }
                )

        df = pd.DataFrame(all_data)
        dataset = ray.data.from_pandas(df)

        path = Path(temp_dir) / "string_btree_test.lance"
        lr.write_lance(
            dataset,
            str(path),
            min_rows_per_file=rows_per_fragment,
            max_rows_per_file=rows_per_fragment,
        )

        ds = lance.dataset(str(path))
        assert len(list(ds.get_fragments())) == num_fragments

        updated_dataset = lr.create_scalar_index(
            uri=ds.uri,
            column="video_uuid",
            index_type="BTREE",
            name="video_uuid_idx",
            replace=False,
            num_workers=num_workers,
        )

        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after distributed BTREE build"

        our_index = None
        for idx in indices:
            if idx["name"] == "video_uuid_idx":
                our_index = idx
                break
        assert our_index is not None, "String BTREE index not found"
        assert our_index["type"] == "BTree"

        sample_uuid = all_data[500]["video_uuid"]
        result = updated_dataset.scanner(
            filter=f"video_uuid = '{sample_uuid}'",
            columns=["id", "video_uuid"],
        ).to_table()
        assert result.num_rows == 1, (
            f"Expected 1 row for exact UUID match, got {result.num_rows}"
        )


class TestNamespaceIndexing:
    """Test cases for distributed indexing with DirectoryNamespace."""

    def test_distributed_fts_index_with_directory_namespace(self, temp_dir):
        """Test distributed FTS index building using DirectoryNamespace."""
        import lance_namespace as ln

        table_id = ["fts_index_test_table"]

        data = pd.DataFrame(
            {
                "id": range(100),
                "text": [
                    f"This is document {i} with searchable content" for i in range(100)
                ],
            }
        )
        dataset = ray.data.from_pandas(data)
        lr.write_lance(
            dataset,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
            min_rows_per_file=25,
            max_rows_per_file=25,
        )

        # Use namespace params only - create_scalar_index will resolve URI internally
        updated_dataset = lr.create_scalar_index(
            column="text",
            index_type="INVERTED",
            name="fts_namespace_idx",
            num_workers=2,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after distributed FTS build"

        our_index = None
        for idx in indices:
            if idx["name"] == "fts_namespace_idx":
                our_index = idx
                break
        assert our_index is not None, "FTS index not found"
        assert our_index["type"] == "Inverted"

        results = updated_dataset.scanner(
            full_text_query="document",
            columns=["id", "text"],
        ).to_table()
        assert results.num_rows > 0, "FTS search should return results"

    def test_distributed_btree_index_with_directory_namespace(self, temp_dir):
        """Test distributed BTREE index building using DirectoryNamespace."""
        import lance_namespace as ln

        table_id = ["btree_index_test_table"]

        data = pd.DataFrame(
            {
                "id": range(200),
                "value": [f"value_{i}" for i in range(200)],
            }
        )
        dataset = ray.data.from_pandas(data)
        lr.write_lance(
            dataset,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
            min_rows_per_file=50,
            max_rows_per_file=50,
        )

        # Use namespace params only - create_scalar_index will resolve URI internally
        updated_dataset = lr.create_scalar_index(
            column="id",
            index_type="BTREE",
            name="btree_namespace_idx",
            num_workers=2,
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after distributed BTREE build"

        our_index = None
        for idx in indices:
            if idx["name"] == "btree_namespace_idx":
                our_index = idx
                break
        assert our_index is not None, "BTREE index not found"
        assert our_index["type"] == "BTree"

        result = updated_dataset.scanner(filter="id = 100", columns=["id"]).to_table()
        assert result.num_rows == 1, "BTREE index query should return 1 row"
