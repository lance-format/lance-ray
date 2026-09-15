# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Test cases for lance_ray.merge_into (distributed merge_into)."""

import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pyarrow as pa
import pytest
import ray

import pandas as pd


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def create_dataset_with_fragments(path, fragment_data, **write_kwargs):
    """Create a Lance dataset where each DataFrame becomes one fragment."""
    first_df = fragment_data[0]
    lr.write_lance(
        ray.data.from_pandas(first_df),
        str(path),
        min_rows_per_file=len(first_df),
        max_rows_per_file=len(first_df),
        **write_kwargs,
    )
    for df in fragment_data[1:]:
        lr.write_lance(
            ray.data.from_pandas(df),
            str(path),
            mode="append",
            min_rows_per_file=len(df),
            max_rows_per_file=len(df),
            **write_kwargs,
        )
    return lance.dataset(str(path))


def make_fragments(num_fragments, rows_per_fragment):
    """DataFrames with ids 0..N-1 and value 'orig_<id>'."""
    return [
        pd.DataFrame(
            {
                "id": range(i * rows_per_fragment, (i + 1) * rows_per_fragment),
                "value": [
                    f"orig_{j}"
                    for j in range(i * rows_per_fragment, (i + 1) * rows_per_fragment)
                ],
            }
        )
        for i in range(num_fragments)
    ]


def id_to_value(dataset):
    table = dataset.to_table()
    return dict(
        zip(
            table.column("id").to_pylist(),
            table.column("value").to_pylist(),
            strict=False,
        )
    )


class TestMergeInto:
    def test_basic_merge_into(self, temp_dir):
        """Update rows in several fragments and insert new rows atomically."""
        path = Path(temp_dir) / "basic_merge_into"
        dataset = create_dataset_with_fragments(path, make_fragments(3, 10))
        assert len(dataset.get_fragments()) == 3
        version_before = dataset.version

        source = pa.table(
            {
                "id": [5, 15, 25, 100, 101],
                "value": ["new_5", "new_15", "new_25", "new_100", "new_101"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        assert updated.version == version_before + 1, (
            "merge_into must commit exactly one new version"
        )

        dataset = updated
        assert dataset.version == version_before + 1
        assert dataset.count_rows() == 32
        values = id_to_value(dataset)
        assert values[5] == "new_5"
        assert values[15] == "new_15"
        assert values[25] == "new_25"
        assert values[100] == "new_100"
        assert values[101] == "new_101"
        # Untouched rows are preserved.
        assert values[0] == "orig_0"
        assert values[14] == "orig_14"
        assert values[29] == "orig_29"

    def test_merge_into_with_ray_dataset_source(self, temp_dir):
        """The source can be a ray.data.Dataset."""
        path = Path(temp_dir) / "ray_ds_source"
        create_dataset_with_fragments(path, make_fragments(2, 10))

        source = ray.data.from_pandas(
            pd.DataFrame({"id": [3, 13, 50], "value": ["new_3", "new_13", "new_50"]})
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        assert dataset.count_rows() == 21
        values = id_to_value(dataset)
        assert values[3] == "new_3"
        assert values[13] == "new_13"
        assert values[50] == "new_50"

    def test_insert_only_source(self, temp_dir):
        """A source with no matching keys only inserts."""
        path = Path(temp_dir) / "insert_only"
        dataset = create_dataset_with_fragments(path, make_fragments(2, 10))
        version_before = dataset.version

        source = pa.table({"id": [100, 101], "value": ["new_100", "new_101"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        assert dataset.version == version_before + 1
        assert dataset.count_rows() == 22
        values = id_to_value(dataset)
        assert values[100] == "new_100"
        assert values[101] == "new_101"

    def test_update_only_source(self, temp_dir):
        """A source where every key matches only updates."""
        path = Path(temp_dir) / "update_only"
        create_dataset_with_fragments(path, make_fragments(2, 10))

        source = pa.table({"id": [5, 15], "value": ["new_5", "new_15"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        assert dataset.count_rows() == 20
        values = id_to_value(dataset)
        assert values[5] == "new_5"
        assert values[15] == "new_15"

    def test_empty_source_is_noop(self, temp_dir):
        """An empty source commits nothing."""
        path = Path(temp_dir) / "empty_source"
        dataset = create_dataset_with_fragments(path, make_fragments(1, 10))
        version_before = dataset.version

        source = pa.table(
            {"id": pa.array([], pa.int64()), "value": pa.array([], pa.string())}
        )
        updated = lr.merge_into(source, str(path), on="id")

        assert updated.version == version_before
        assert lance.dataset(str(path)).version == updated.version

    def test_more_fragments_than_workers(self, temp_dir):
        """Fragment routing works when touched fragments outnumber workers."""
        path = Path(temp_dir) / "many_fragments"
        dataset = create_dataset_with_fragments(path, make_fragments(8, 5))
        assert len(dataset.get_fragments()) == 8
        version_before = dataset.version

        # One update in every fragment (ids 0, 5, 10, ... 35) + two inserts.
        update_ids = list(range(0, 40, 5))
        source = pa.table(
            {
                "id": update_ids + [1000, 1001],
                "value": [f"new_{i}" for i in update_ids] + ["new_1000", "new_1001"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=3)

        assert updated.version == version_before + 1, (
            "All 8 fragment rewrites must land in one atomic commit"
        )
        dataset = updated
        assert dataset.count_rows() == 42
        values = id_to_value(dataset)
        for i in update_ids:
            assert values[i] == f"new_{i}"
        for i in range(40):
            if i not in update_ids:
                assert values[i] == f"orig_{i}"

    def test_more_partitions_than_workers(self, temp_dir):
        """num_partitions controls layout independently of num_workers."""
        path = Path(temp_dir) / "partitions_vs_workers"
        create_dataset_with_fragments(path, make_fragments(4, 5))

        update_ids = [0, 6, 12, 18]
        source = pa.table(
            {
                "id": update_ids + [500, 501, 502],
                "value": [f"new_{i}" for i in update_ids]
                + ["new_500", "new_501", "new_502"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=2, num_partitions=6)

        dataset = updated
        assert dataset.count_rows() == 23
        values = id_to_value(dataset)
        for i in update_ids:
            assert values[i] == f"new_{i}"
        assert values[500] == "new_500"
        assert values[502] == "new_502"

    def test_merge_into_with_scalar_index(self, temp_dir):
        """The plan phase works with a scalar index on the join key."""
        path = Path(temp_dir) / "with_index"
        dataset = create_dataset_with_fragments(path, make_fragments(3, 10))
        dataset.create_scalar_index("id", index_type="BTREE")

        source = pa.table({"id": [7, 17, 27, 200], "value": ["a", "b", "c", "d"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        values = id_to_value(updated)
        assert values[7] == "a"
        assert values[17] == "b"
        assert values[27] == "c"
        assert values[200] == "d"

    def test_merge_into_with_stable_row_ids(self, temp_dir):
        """_rowaddr-based planning is correct on stable-row-id datasets."""
        path = Path(temp_dir) / "stable_row_ids"
        create_dataset_with_fragments(
            path, make_fragments(2, 10), enable_stable_row_ids=True
        )

        source = pa.table({"id": [4, 14, 300], "value": ["new_4", "new_14", "new_300"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        assert dataset.count_rows() == 21
        values = id_to_value(dataset)
        assert values[4] == "new_4"
        assert values[14] == "new_14"
        assert values[300] == "new_300"

    def test_string_join_keys(self, temp_dir):
        """String keys (including quotes) are escaped correctly in lookups."""
        path = Path(temp_dir) / "string_keys"
        df = pd.DataFrame(
            {"key": ["alpha", "be'ta", "gamma"], "value": ["1", "2", "3"]}
        )
        lr.write_lance(ray.data.from_pandas(df), str(path))

        source = pa.table({"key": ["be'ta", "delta"], "value": ["updated", "inserted"]})
        updated = lr.merge_into(source, str(path), on="key", num_workers=2)

        table = updated.to_table()
        values = dict(
            zip(
                table.column("key").to_pylist(),
                table.column("value").to_pylist(),
                strict=False,
            )
        )
        assert values["be'ta"] == "updated"
        assert values["delta"] == "inserted"

    def test_merge_into_with_directory_namespace(self, temp_dir):
        """Namespace-resolved tables work end to end."""
        import lance_namespace as ln
        from lance_namespace import DescribeTableRequest

        table_id = ["merge_into_test_table"]
        df = pd.DataFrame({"id": range(10), "value": [f"orig_{i}" for i in range(10)]})
        lr.write_lance(
            ray.data.from_pandas(df),
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        source = pa.table({"id": [2, 100], "value": ["new_2", "new_100"]})
        lr.merge_into(
            source,
            on="id",
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
            num_workers=2,
        )

        namespace = ln.connect("dir", {"root": temp_dir})
        location = namespace.describe_table(DescribeTableRequest(id=table_id)).location
        values = id_to_value(lance.dataset(location))
        assert values[2] == "new_2"
        assert values[100] == "new_100"


class TestMergeIntoDedupe:
    def test_dedupe_within_chunk(self, temp_dir):
        """Adjacent duplicates collapse to a single row per key."""
        path = Path(temp_dir) / "dedupe_within_chunk"
        create_dataset_with_fragments(path, make_fragments(1, 10))

        source = pa.table(
            {
                "id": [5, 5, 100, 100],
                "value": ["first_5", "dup_5", "first_100", "dup_100"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=1, num_partitions=1)

        values = id_to_value(updated)
        assert values[5] in {"first_5", "dup_5"}
        assert values[100] in {"first_100", "dup_100"}

    def test_dedupe_across_chunks(self, temp_dir):
        """Duplicates split across plan chunks collapse to one row per key.

        With num_partitions=4 the source table is sliced into 4 chunks, so
        the duplicate pairs (row 0 vs row 19, row 1 vs row 18) land in
        different chunks -- exactly the blind spot dedupe closes. Which copy
        survives is unspecified.
        """
        path = Path(temp_dir) / "dedupe_across_chunks"
        dataset = create_dataset_with_fragments(path, make_fragments(2, 10))
        version_before = dataset.version

        ids = [5, 200] + list(range(300, 316)) + [200, 5]
        values = (
            ["first_5", "first_200"]
            + [f"v_{i}" for i in range(300, 316)]
            + ["dup_200", "dup_5"]
        )
        source = pa.table({"id": ids, "value": values})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2, num_partitions=4)

        assert updated.version == version_before + 1
        dataset = updated
        table = dataset.to_table()
        assert table.column("id").to_pylist().count(5) == 1
        assert table.column("id").to_pylist().count(200) == 1
        got = id_to_value(dataset)
        assert got[5] in {"first_5", "dup_5"}
        assert got[200] in {"first_200", "dup_200"}

    def test_dedupe_with_ray_dataset_source(self, temp_dir):
        """Dedupe works when the source is a ray.data.Dataset."""
        path = Path(temp_dir) / "dedupe_ray_ds"
        create_dataset_with_fragments(path, make_fragments(1, 10))

        source = ray.data.from_pandas(
            pd.DataFrame(
                {"id": [3, 50, 3, 50], "value": ["first_3", "first_50", "b", "c"]}
            )
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        values = id_to_value(updated)
        assert values[3] in {"first_3", "b"}
        assert values[50] in {"first_50", "c"}

    def test_dedupe_noop_on_unique_source(self, temp_dir):
        """A source without duplicates is unchanged by the dedupe pass."""
        path = Path(temp_dir) / "dedupe_unique"
        create_dataset_with_fragments(path, make_fragments(2, 10))

        source = pa.table({"id": [5, 15, 100], "value": ["new_5", "new_15", "new_100"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        values = id_to_value(updated)
        assert values[5] == "new_5"
        assert values[15] == "new_15"
        assert values[100] == "new_100"

    def test_dedupe_string_keys(self, temp_dir):
        """Sort-based dedupe handles string keys across chunks."""
        path = Path(temp_dir) / "dedupe_string_keys"
        df = pd.DataFrame({"key": ["alpha", "beta"], "value": ["1", "2"]})
        lr.write_lance(ray.data.from_pandas(df), str(path))

        source = pa.table(
            {
                "key": ["alpha", "x1", "x2", "x3", "x4", "x5", "x6", "alpha"],
                "value": ["first", "a", "b", "c", "d", "e", "f", "dup"],
            }
        )
        updated = lr.merge_into(
            source, str(path), on="key", num_workers=2, num_partitions=4
        )

        table = updated.to_table()
        values = dict(
            zip(
                table.column("key").to_pylist(),
                table.column("value").to_pylist(),
                strict=False,
            )
        )
        assert values["alpha"] in {"first", "dup"}


class TestMergeIntoMergeOnRead:
    def test_update_writes_deletion_vector_not_rewrite(self, temp_dir):
        """A partial update must never rewrite the fragment's data files."""
        path = Path(temp_dir) / "mor_partial_update"
        dataset = create_dataset_with_fragments(path, make_fragments(2, 10))
        files_before = {
            f.fragment_id: [d.path for d in f.metadata.files]
            for f in dataset.get_fragments()
        }

        source = pa.table({"id": [5, 100], "value": ["new_5", "new_100"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        frags_after = {f.fragment_id: f.metadata for f in dataset.get_fragments()}
        for fragment_id, files in files_before.items():
            assert fragment_id in frags_after, (
                "Partially-updated fragments must survive (merge-on-read)"
            )
            assert [d.path for d in frags_after[fragment_id].files] == files, (
                "Data files must never be rewritten by an update"
            )
        touched = frags_after[5 // 10]  # id 5 lives in the first fragment
        assert touched.deletion_file is not None, (
            "The matched row must be masked by a deletion file"
        )
        values = id_to_value(dataset)
        assert values[5] == "new_5"
        assert values[100] == "new_100"
        assert dataset.count_rows() == 21

    def test_full_fragment_update_removes_fragment(self, temp_dir):
        """Updating every row of a fragment removes it instead of keeping an
        all-dead deletion vector."""
        path = Path(temp_dir) / "mor_full_update"
        dataset = create_dataset_with_fragments(path, make_fragments(2, 5))
        ids_before = {f.fragment_id for f in dataset.get_fragments()}
        first_fragment_id = min(ids_before)

        source = pa.table(
            {"id": list(range(5)), "value": [f"new_{i}" for i in range(5)]}
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        ids_after = {f.fragment_id for f in dataset.get_fragments()}
        assert first_fragment_id not in ids_after, (
            "A fully-updated fragment must be removed, not kept empty"
        )
        assert dataset.count_rows() == 10
        values = id_to_value(dataset)
        assert all(values[i] == f"new_{i}" for i in range(5))
        assert all(values[i] == f"orig_{i}" for i in range(5, 10))


class TestMergeIntoValidation:
    def test_requires_uri_or_namespace(self):
        with pytest.raises(ValueError, match="Must provide either"):
            lr.merge_into(pa.table({"id": [1]}), on="id")

    def test_rejects_uri_and_namespace(self):
        with pytest.raises(ValueError, match="Cannot provide both"):
            lr.merge_into(
                pa.table({"id": [1]}),
                "/tmp/x.lance",
                on="id",
                namespace_impl="dir",
                table_id=["t"],
            )

    def test_rejects_empty_on(self):
        with pytest.raises(ValueError, match="join key"):
            lr.merge_into(pa.table({"id": [1]}), "/tmp/x.lance", on="")

    def test_rejects_unknown_key_column(self, temp_dir):
        path = Path(temp_dir) / "unknown_key"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        source = pa.table({"id": [1], "value": ["x"]})
        with pytest.raises(ValueError, match="not found in target schema"):
            lr.merge_into(source, str(path), on="missing_column")

    def test_rejects_missing_source_columns(self, temp_dir):
        path = Path(temp_dir) / "missing_columns"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        source = pa.table({"id": [1]})  # no "value" column
        with pytest.raises(Exception, match="missing target-table columns"):
            lr.merge_into(source, str(path), on="id")

    def test_rejects_null_source_keys(self, temp_dir):
        path = Path(temp_dir) / "null_keys"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        source = pa.table({"id": [1, None], "value": ["a", "b"]})
        with pytest.raises(Exception, match="null values in join key"):
            lr.merge_into(source, str(path), on="id", num_workers=1)

    def test_rejects_bad_source_type(self, temp_dir):
        path = Path(temp_dir) / "bad_source"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        with pytest.raises(TypeError, match="ray.data.Dataset or a pyarrow.Table"):
            lr.merge_into([{"id": 1}], str(path), on="id")

    def test_rejects_bad_worker_counts(self):
        with pytest.raises(ValueError, match="num_workers"):
            lr.merge_into(pa.table({"id": [1]}), "/tmp/x.lance", on="id", num_workers=0)
        with pytest.raises(ValueError, match="num_partitions"):
            lr.merge_into(pa.table({"id": [1]}), "/tmp/x.lance", on="id", num_partitions=0)
