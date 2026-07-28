"""Test cases for the distributed ``update_columns`` API."""

import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ray
from lance.udf import BatchUDF


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def _write_products(path, rows=6, max_rows_per_file=2):
    """A small multi-fragment dataset: ids 1..rows, alternating status."""
    table = pa.table(
        {
            "id": pa.array(list(range(1, rows + 1)), pa.int32()),
            "price": pa.array(
                [float(i * 10) for i in range(1, rows + 1)], pa.float64()
            ),
            "status": pa.array(["a" if i % 2 else "b" for i in range(1, rows + 1)]),
        }
    )
    lance.write_dataset(table, str(path), max_rows_per_file=max_rows_per_file)
    return table


def _record_batch(data, schema=None) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(data, schema=schema)


def _double_price(batch: pa.RecordBatch) -> pa.RecordBatch:
    return _record_batch({"price": pc.multiply(batch["price"], 2.0)})


def _dataset_fingerprint(path):
    """Version plus every data file, to prove nothing was written."""
    ds = lance.dataset(str(path))
    files = sorted(
        data_file.path
        for fragment in ds.get_fragments()
        for data_file in fragment.data_files()
    )
    return ds.version, files


class TestBasicBehavior:
    def test_namespace_only_updates_columns(self, temp_dir):
        """The driver resolves the namespace once, while workers retain it.

        This guards the internal read path used after update_columns has pinned
        the namespace table location and snapshot.  Public read_lance still
        rejects URI-plus-namespace arguments.
        """
        table_id = ["update_columns_namespace_only"]
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int32()),
                "price": pa.array([10.0, 20.0, 30.0], pa.float64()),
            }
        )
        lr.write_lance(
            ray.data.from_arrow(table),
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
            min_rows_per_file=1,
            max_rows_per_file=2,
        )

        result = lr.update_columns(
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        )

        assert result.rows_updated == 3
        got = lr.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": temp_dir},
            table_id=table_id,
        ).take_all()
        assert sorted(row["price"] for row in got) == [20.0, 40.0, 60.0]

    def test_updates_all_rows_across_fragments(self, temp_dir):
        path = Path(temp_dir) / "all_rows.lance"
        _write_products(path)

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        assert result.version == 2
        assert result.rows_updated == 6

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [20.0, 40.0, 60.0, 80.0, 100.0, 120.0]
        assert got["id"] == [1, 2, 3, 4, 5, 6]

    def test_updates_one_fragment_over_multiple_record_batches(self, temp_dir):
        path = Path(temp_dir) / "multi_batch_fragment.lance"
        _write_products(path, rows=6, max_rows_per_file=6)

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
            batch_size=2,
            concurrency=1,
        )

        assert result.rows_updated == 6
        assert lance.dataset(str(path)).to_table()["price"].to_pylist() == [
            20.0,
            40.0,
            60.0,
            80.0,
            100.0,
            120.0,
        ]

    def test_filter_updates_only_matching_rows(self, temp_dir):
        path = Path(temp_dir) / "filtered.lance"
        _write_products(path)

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            filter="status = 'a'",
            read_columns=["price"],
        )

        assert result.rows_updated == 3
        got = lance.dataset(str(path)).to_table().to_pydict()
        # Only the odd ids (status 'a') doubled; the rest keep their old value.
        assert got["price"] == [20.0, 20.0, 60.0, 40.0, 100.0, 60.0]

    def test_partial_fragment_coverage_is_allowed(self, temp_dir):
        path = Path(temp_dir) / "partial.lance"
        _write_products(path)

        # Only touches rows in the first fragment.
        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            filter="id <= 2",
            read_columns=["price"],
        )

        assert result.rows_updated == 2
        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [20.0, 40.0, 30.0, 40.0, 50.0, 60.0]

    def test_updates_multiple_columns(self, temp_dir):
        path = Path(temp_dir) / "multi_col.lance"
        table = pa.table(
            {
                "id": pa.array([1, 2, 3, 4], pa.int32()),
                "price": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
                "label": pa.array(["w", "x", "y", "z"]),
            }
        )
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        def bump_both(batch: pa.RecordBatch) -> pa.RecordBatch:
            return _record_batch(
                {
                    "price": pc.multiply(batch["price"], 10.0),
                    "label": pc.binary_join_element_wise(batch["label"], "!", ""),
                }
            )

        result = lr.update_columns(
            str(path),
            transform=bump_both,
            columns=["price", "label"],
            read_columns=["price", "label"],
        )

        assert result.rows_updated == 4
        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [10.0, 20.0, 30.0, 40.0]
        assert got["label"] == ["w!", "x!", "y!", "z!"]

    def test_transform_may_read_columns_it_does_not_update(self, temp_dir):
        path = Path(temp_dir) / "aux_read.lance"
        _write_products(path, rows=4)

        def price_from_id(batch: pa.RecordBatch) -> pa.RecordBatch:
            return _record_batch({"price": pc.cast(batch["id"], pa.float64())})

        lr.update_columns(
            str(path),
            transform=price_from_id,
            columns=["price"],
            read_columns=["id"],
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [1.0, 2.0, 3.0, 4.0]

    def test_no_op_when_filter_matches_nothing(self, temp_dir):
        path = Path(temp_dir) / "noop.lance"
        _write_products(path)
        before = lance.dataset(str(path)).version

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            filter="id > 1000",
            read_columns=["price"],
        )

        assert result.rows_updated == 0
        assert result.version == before
        assert lance.dataset(str(path)).version == before


class TestTransformContract:
    def test_transform_does_not_see_metadata_columns(self, temp_dir):
        path = Path(temp_dir) / "hidden_meta.lance"
        _write_products(path, rows=4)

        def assert_no_metadata(batch: pa.RecordBatch) -> pa.RecordBatch:
            assert isinstance(batch, pa.RecordBatch)
            for hidden in ("_rowaddr", "_fragid", "_rowid"):
                assert hidden not in batch.column_names
            return _record_batch({"price": pc.multiply(batch["price"], 3.0)})

        lr.update_columns(
            str(path),
            transform=assert_no_metadata,
            columns=["price"],
            read_columns=["price"],
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [30.0, 60.0, 90.0, 120.0]

    def test_accepts_batch_udf(self, temp_dir):
        path = Path(temp_dir) / "batch_udf.lance"
        _write_products(path, rows=2, max_rows_per_file=2)

        udf = BatchUDF(
            lambda batch: _record_batch({"price": pc.multiply(batch["price"], 4.0)}),
            output_schema=pa.schema([pa.field("price", pa.float64())]),
        )
        lr.update_columns(
            str(path),
            transform=udf,
            columns=["price"],
            read_columns=["price"],
        )

        assert lance.dataset(str(path)).to_table()["price"].to_pylist() == [
            40.0,
            80.0,
        ]

    @pytest.mark.parametrize(
        "bad_transform, match",
        [
            (
                lambda b: _record_batch(
                    {"price": pc.multiply(b["price"], 2.0), "extra": b["price"]}
                ),
                "Unexpected: \\['extra'\\]",
            ),
            (lambda b: _record_batch({"nope": b["price"]}), "missing: \\['price'\\]"),
            (
                lambda b: _record_batch({"price": [1.0] * (b.num_rows * 2)}),
                "changed the row count",
            ),
            (
                lambda b: pa.table({"price": [1.0] * b.num_rows}),
                "must return pa.RecordBatch, got Table",
            ),
            (
                lambda b: {"price": [1.0] * b.num_rows},
                "must return pa.RecordBatch, got dict",
            ),
        ],
    )
    def test_rejects_bad_transform_output(self, temp_dir, bad_transform, match):
        path = Path(temp_dir) / "bad_output.lance"
        _write_products(path, rows=2, max_rows_per_file=2)

        with pytest.raises(Exception, match=match):
            lr.update_columns(
                str(path),
                transform=bad_transform,
                columns=["price"],
                read_columns=["price"],
            )

    def test_transform_output_is_cast_to_the_dataset_type(self, temp_dir):
        """The output type is the dataset's, not whatever the transform built."""
        path = Path(temp_dir) / "cast_output.lance"
        _write_products(path, rows=2, max_rows_per_file=2)

        def int_prices(batch: pa.RecordBatch) -> pa.RecordBatch:
            return _record_batch({"price": pa.array([1, 2], pa.int32())})

        lr.update_columns(
            str(path),
            transform=int_prices,
            columns=["price"],
            read_columns=["price"],
        )

        table = lance.dataset(str(path)).to_table()
        assert table.schema.field("price").type == pa.float64()
        assert table.to_pydict()["price"] == [1.0, 2.0]

    def test_rejects_out_of_range_transform_output(self, temp_dir):
        """The cast is safe=True, so an out-of-range integer raises.

        Note this does *not* generalize to floats: Arrow's safe cast does not
        range-check float narrowing, so a float64 result for a float32 column
        rounds (and overflows to inf) silently.  See the ``columns`` docstring.
        """
        path = Path(temp_dir) / "lossy_output.lance"
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "small": pa.array([1, 2], pa.int8()),
            }
        )
        lance.write_dataset(table, str(path))
        before = _dataset_fingerprint(path)

        def too_big(batch: pa.RecordBatch) -> pa.RecordBatch:
            return _record_batch({"small": pa.array([1000, 2000], pa.int32())})

        with pytest.raises(RuntimeError, match="Integer value 1000"):
            lr.update_columns(
                str(path),
                transform=too_big,
                columns=["small"],
                read_columns=["small"],
            )

        assert _dataset_fingerprint(path) == before

    def test_rejects_nulls_for_a_non_nullable_column(self, temp_dir):
        """A non-nullable target rejects null output rather than writing it."""
        path = Path(temp_dir) / "non_nullable.lance"
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("price", pa.float64(), nullable=False),
            ]
        )
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "price": pa.array([1.0, 2.0], pa.float64()),
            },
            schema=schema,
        )
        lance.write_dataset(table, str(path))
        before = _dataset_fingerprint(path)

        def nullify(batch: pa.RecordBatch) -> pa.RecordBatch:
            return _record_batch({"price": pa.array([1.0, None], pa.float64())})

        with pytest.raises(RuntimeError, match="non-nullable"):
            lr.update_columns(
                str(path),
                transform=nullify,
                columns=["price"],
                read_columns=["price"],
            )

        assert _dataset_fingerprint(path) == before

    def test_updates_a_non_nullable_column(self, temp_dir):
        """Non-null output for a non-nullable target round-trips normally."""
        path = Path(temp_dir) / "non_nullable_ok.lance"
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("price", pa.float64(), nullable=False),
            ]
        )
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "price": pa.array([1.0, 2.0], pa.float64()),
            },
            schema=schema,
        )
        lance.write_dataset(table, str(path))

        lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        got = lance.dataset(str(path)).to_table()
        assert got.schema.field("price").nullable is False
        assert got.to_pydict()["price"] == [2.0, 4.0]


class TestPhysicalCorrectness:
    def test_preserves_row_address_schema_and_field_ids(self, temp_dir):
        path = Path(temp_dir) / "identity.lance"
        _write_products(path)

        before_ds = lance.dataset(str(path))
        before_meta = (
            lr.read_lance(str(path), with_metadata=True)
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
        before_schema = before_ds.schema
        before_field_ids = {f.name(): f.id() for f in before_ds.lance_schema.fields()}

        lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        after_ds = lance.dataset(str(path))
        after_meta = (
            lr.read_lance(str(path), with_metadata=True)
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )

        assert after_ds.schema == before_schema
        assert {f.name(): f.id() for f in after_ds.lance_schema.fields()} == (
            before_field_ids
        )
        assert after_meta["_rowaddr"].tolist() == before_meta["_rowaddr"].tolist()
        assert after_meta["_fragid"].tolist() == before_meta["_fragid"].tolist()

    def test_untouched_columns_keep_their_data_files(self, temp_dir):
        path = Path(temp_dir) / "files.lance"
        _write_products(path, rows=4, max_rows_per_file=4)

        def files_by_field(ds):
            fragment = ds.get_fragments()[0]
            mapping = {}
            for data_file in fragment.data_files():
                for field_id in data_file.fields:
                    mapping[field_id] = data_file.path
            return mapping

        before_ds = lance.dataset(str(path))
        before = files_by_field(before_ds)
        field_ids = {f.name(): f.id() for f in before_ds.lance_schema.fields()}

        lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        after = files_by_field(lance.dataset(str(path)))
        # The rewritten column moved to a new file ...
        assert after[field_ids["price"]] != before[field_ids["price"]]
        # ... while every other column still points at the original file.
        for name in ("id", "status"):
            assert after[field_ids[name]] == before[field_ids[name]]

    def test_time_travel_reads_pre_update_values(self, temp_dir):
        path = Path(temp_dir) / "time_travel.lance"
        _write_products(path, rows=4)
        read_version = lance.dataset(str(path)).version

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        old = lance.dataset(str(path), version=read_version).to_table()
        new = lance.dataset(str(path), version=result.version).to_table()
        assert old["price"].to_pylist() == [10.0, 20.0, 30.0, 40.0]
        assert new["price"].to_pylist() == [20.0, 40.0, 60.0, 80.0]

    def test_repeated_updates_stack(self, temp_dir):
        path = Path(temp_dir) / "repeated.lance"
        _write_products(path, rows=4)

        for _ in range(3):
            lr.update_columns(
                str(path),
                transform=_double_price,
                columns=["price"],
                read_columns=["price"],
            )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [80.0, 160.0, 240.0, 320.0]

    def test_deleted_rows_do_not_misalign_columns(self, temp_dir):
        path = Path(temp_dir) / "deleted.lance"
        _write_products(path, rows=6, max_rows_per_file=3)

        ds = lance.dataset(str(path))
        ds.delete("id = 2")

        lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["id"] == [1, 3, 4, 5, 6]
        assert got["price"] == [20.0, 60.0, 80.0, 100.0, 120.0]

    def test_filter_with_delete_vector_updates_only_matching_live_rows(self, temp_dir):
        """A filtered rewrite must preserve deleted and untouched fragment rows."""
        path = Path(temp_dir) / "deleted_filtered.lance"
        _write_products(path, rows=6, max_rows_per_file=3)

        # The first fragment contains ids 1..3. Delete one row from it, then
        # update only one of its remaining live rows; the second fragment must
        # not be rewritten at all.
        lance.dataset(str(path)).delete("id = 2")

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            filter="id = 1",
            read_columns=["price"],
        )

        assert result.rows_updated == 1
        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["id"] == [1, 3, 4, 5, 6]
        assert got["price"] == [20.0, 30.0, 40.0, 50.0, 60.0]


class TestTransactionBehavior:
    def test_commits_a_rewrite_columns_update(self, temp_dir):
        path = Path(temp_dir) / "txn_shape.lance"
        _write_products(path, rows=4)

        before_ds = lance.dataset(str(path))
        read_version = before_ds.version
        price_field_id = before_ds.lance_schema.field("price").id()

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
        )

        txn = lance.dataset(str(path)).read_transaction(result.version)
        assert type(txn.operation).__name__ == "Update"
        assert txn.operation.update_mode == "rewrite_columns"
        assert list(txn.operation.fields_modified) == [price_field_id]
        assert txn.read_version == read_version

    def test_stale_snapshot_commit_fails(self, temp_dir):
        """A stale Update must never be rebased onto a newer conflicting version."""
        from lance.dataset import Transaction

        path = Path(temp_dir) / "stale.lance"
        _write_products(path, rows=4, max_rows_per_file=4)

        ds = lance.dataset(str(path))
        stale_version = ds.version
        fragment = ds.get_fragments()[0]
        update = pa.table(
            {
                "_rowaddr": pa.array([0, 1, 2, 3], pa.uint64()),
                "price": pa.array([1.0, 1.0, 1.0, 1.0], pa.float64()),
            }
        )
        meta, fields_modified = fragment.update_columns(
            update, left_on="_rowaddr", right_on="_rowaddr"
        )

        # A concurrent writer touches the same fragment first.
        lance.dataset(str(path)).delete("id = 1")

        op = lance.LanceOperation.Update(
            updated_fragments=[meta],
            fields_modified=list(fields_modified),
            fields_for_preserving_frag_bitmap=[],
            update_mode="rewrite_columns",
        )
        with pytest.raises(OSError, match="[Cc]ommit conflict"):
            lance.LanceDataset.commit(
                str(path),
                Transaction(read_version=stale_version, operation=op),
                max_retries=5,
            )

    def test_concurrent_append_is_safely_rebased(self, temp_dir):
        """An Append landing mid-flight must be rebased over, not rejected.

        The append has to happen *after* the fragment rewrite and *before* the
        commit, otherwise the update simply reads the post-append snapshot and
        nothing concurrent is exercised.
        """
        from lance.dataset import Transaction

        path = Path(temp_dir) / "rebase.lance"
        _write_products(path, rows=4, max_rows_per_file=4)

        ds = lance.dataset(str(path))
        read_version = ds.version
        fragment = ds.get_fragments()[0]
        update = pa.table(
            {
                "_rowaddr": pa.array([0, 1, 2, 3], pa.uint64()),
                "price": pa.array([-1.0] * 4, pa.float64()),
            }
        )
        meta, fields_modified = fragment.update_columns(
            update, left_on="_rowaddr", right_on="_rowaddr"
        )

        # Concurrent append on a brand new fragment; no overlap with ours.
        extra = pa.table(
            {
                "id": pa.array([99], pa.int32()),
                "price": pa.array([9.0], pa.float64()),
                "status": pa.array(["c"]),
            }
        )
        lance.write_dataset(extra, str(path), mode="append")
        assert lance.dataset(str(path)).version > read_version

        op = lance.LanceOperation.Update(
            updated_fragments=[meta],
            fields_modified=list(fields_modified),
            fields_for_preserving_frag_bitmap=[],
            update_mode="rewrite_columns",
        )
        txn_uuid = "0198aaaa-bbbb-cccc-dddd-eeeeffff0001"
        committed = lance.LanceDataset.commit(
            str(path),
            Transaction(read_version=read_version, operation=op, uuid=txn_uuid),
            max_retries=5,
        )

        got = committed.to_table().to_pydict()
        # Our column rewrite applied, and the concurrently appended row came
        # through untouched.
        assert got["price"] == [-1.0, -1.0, -1.0, -1.0, 9.0]
        # A caller-fixed UUID also survives Lance's conflict-checked rebase.
        assert committed.read_transaction(committed.version).uuid == txn_uuid


class TestResourceOptions:
    """The fragment-local Ray task must receive the tuning parameters."""

    def test_ray_remote_args_are_applied(self, temp_dir):
        path = Path(temp_dir) / "remote_args.lance"
        _write_products(path, rows=4, max_rows_per_file=2)

        lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
            ray_remote_args={"num_cpus": 1},
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [20.0, 40.0, 60.0, 80.0]

    def test_concurrency_and_batch_size_are_applied(self, temp_dir):
        path = Path(temp_dir) / "concurrency.lance"
        _write_products(path, rows=4, max_rows_per_file=2)

        result = lr.update_columns(
            str(path),
            transform=_double_price,
            columns=["price"],
            read_columns=["price"],
            batch_size=2,
            ray_remote_args={"num_cpus": 1},
            concurrency=1,
        )

        assert result.rows_updated == 4
        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["price"] == [20.0, 40.0, 60.0, 80.0]


class TestNestedFieldIds:
    """Columns whose Lance field has children report *leaf* field ids.

    ``fields_modified`` comes from the rewritten data file, which declares leaf
    ids. Comparing it against the top-level field id makes every ``list``-typed
    column fail — and only after the whole distributed pass has completed.
    """

    def test_updates_a_list_column(self, temp_dir):
        path = Path(temp_dir) / "list_col.lance"
        list_type = pa.list_(pa.int32())
        table = pa.table(
            {
                "id": pa.array([1, 2, 3, 4], pa.int32()),
                "tags": pa.array([[1, 2], [3], [4, 5, 6], []], list_type),
            }
        )
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        def append_marker(batch: pa.RecordBatch) -> pa.RecordBatch:
            return _record_batch(
                {
                    "tags": pa.array(
                        [v + [99] for v in batch["tags"].to_pylist()], list_type
                    )
                }
            )

        result = lr.update_columns(
            str(path),
            transform=append_marker,
            columns=["tags"],
            read_columns=["tags"],
        )

        assert result.rows_updated == 4
        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["tags"] == [[1, 2, 99], [3, 99], [4, 5, 6, 99], [99]]

    def test_fields_modified_uses_leaf_ids(self, temp_dir):
        path = Path(temp_dir) / "leaf_ids.lance"
        list_type = pa.list_(pa.int32())
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "tags": pa.array([[1], [2]], list_type),
            }
        )
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        ds = lance.dataset(str(path))
        tags = ds.lance_schema.field("tags")
        leaf_ids = [child.id() for child in tags.children()]
        # Precondition for this test to mean anything.
        assert leaf_ids and leaf_ids != [tags.id()]

        result = lr.update_columns(
            str(path),
            transform=lambda b: _record_batch(
                {"tags": pa.array([[7]] * b.num_rows, list_type)}
            ),
            columns=["tags"],
            read_columns=["tags"],
        )

        txn = lance.dataset(str(path)).read_transaction(result.version)
        assert list(txn.operation.fields_modified) == leaf_ids

    def test_updates_a_fixed_size_list_column(self, temp_dir):
        path = Path(temp_dir) / "vector_col.lance"
        vec_type = pa.list_(pa.float32(), 2)
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "vec": pa.array([[1.0, 2.0], [3.0, 4.0]], vec_type),
            }
        )
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        lr.update_columns(
            str(path),
            transform=lambda b: _record_batch(
                {
                    "vec": pa.FixedSizeListArray.from_arrays(
                        pa.array([0.5] * (b.num_rows * 2), pa.float32()), 2
                    )
                }
            ),
            columns=["vec"],
            read_columns=["vec"],
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["vec"] == [[0.5, 0.5], [0.5, 0.5]]


class TestDriverSideRejection:
    """Rejections must land before any Ray task writes a file.

    Asserting only the exception type would still pass if these checks moved
    into the fragment worker, since the message propagates out of Ray anyway.
    Pinning the dataset fingerprint is what actually holds the line.
    """

    @pytest.mark.parametrize(
        "columns, read_columns, exc, match",
        [
            (["brand_new"], None, ValueError, "non-existent column"),
            (["_rowaddr"], None, ValueError, "metadata column"),
            (["meta.price"], None, ValueError, "Nested field path"),
            (["price", "price"], None, ValueError, "Duplicate column"),
            ([pa.field("price", pa.float64())], None, TypeError, "as str"),
            ("price", None, TypeError, "not a bare string"),
            ([], None, ValueError, "at least one column"),
            (["price"], ["price", "_rowaddr"], ValueError, "read_columns"),
            (["price"], ["nope"], ValueError, "do not exist"),
        ],
    )
    def test_rejects_without_touching_the_dataset(
        self, temp_dir, columns, read_columns, exc, match
    ):
        path = Path(temp_dir) / "untouched.lance"
        _write_products(path, rows=4, max_rows_per_file=2)
        before = _dataset_fingerprint(path)

        with pytest.raises(exc, match=match):
            lr.update_columns(
                str(path),
                transform=_double_price,
                columns=columns,
                read_columns=read_columns,
            )

        assert _dataset_fingerprint(path) == before

    def test_stable_row_ids_rejected_without_touching_the_dataset(self, temp_dir):
        path = Path(temp_dir) / "stable_untouched.lance"
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "price": pa.array([1.0, 2.0], pa.float64()),
            }
        )
        lance.write_dataset(table, str(path), enable_stable_row_ids=True)
        before = _dataset_fingerprint(path)

        with pytest.raises(NotImplementedError, match="stable row IDs"):
            lr.update_columns(
                str(path),
                transform=_double_price,
                columns=["price"],
            )

        assert _dataset_fingerprint(path) == before


class TestRejectedScenarios:
    def test_rejects_struct_target_column(self, temp_dir):
        path = Path(temp_dir) / "struct_col.lance"
        struct_type = pa.struct([pa.field("v", pa.int32())])
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "meta": pa.array([{"v": 1}, {"v": 2}], struct_type),
            }
        )
        lance.write_dataset(table, str(path))

        with pytest.raises(ValueError, match="nested \\(struct\\) type"):
            lr.update_columns(
                str(path),
                transform=lambda b: b,
                columns=["meta"],
            )

    def test_requires_uri_or_namespace(self, temp_dir):
        with pytest.raises(ValueError, match="Must provide either 'uri'"):
            lr.update_columns(
                transform=_double_price,
                columns=["price"],
            )


class TestBlobInput:
    @staticmethod
    def _write_blob_dataset(path, rows=4):
        blob_field = pa.field(
            "payload",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        )
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                blob_field,
                pa.field("size", pa.int64()),
            ]
        )
        table = pa.table(
            {
                "id": pa.array(list(range(rows)), pa.int32()),
                "payload": pa.array(
                    [b"x" * (i + 1) for i in range(rows)], pa.large_binary()
                ),
                "size": pa.array([0] * rows, pa.int64()),
            },
            schema=schema,
        )
        lance.write_dataset(table, str(path), max_rows_per_file=2)
        return table

    def test_legacy_blob_can_be_read_to_compute_a_plain_column(self, temp_dir):
        path = Path(temp_dir) / "blob_input.lance"
        self._write_blob_dataset(path)

        def payload_size(batch: pa.RecordBatch) -> pa.RecordBatch:
            sizes = [
                len(v) if v is not None else 0 for v in batch["payload"].to_pylist()
            ]
            return _record_batch({"size": pa.array(sizes, pa.int64())})

        lr.update_columns(
            str(path),
            transform=payload_size,
            columns=["size"],
            read_columns=["payload"],
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["size"] == [1, 2, 3, 4]

    def test_default_projection_excludes_blob_columns(self, temp_dir):
        path = Path(temp_dir) / "blob_default.lance"
        self._write_blob_dataset(path)

        def record_projection(batch: pa.RecordBatch) -> pa.RecordBatch:
            # The transform runs in a Ray worker, so the observation has to
            # travel back through the data itself.
            assert "payload" not in batch.column_names
            width = len(batch.column_names)
            return _record_batch(
                {"size": pa.array([width] * batch.num_rows, pa.int64())}
            )

        lr.update_columns(
            str(path),
            transform=record_projection,
            columns=["size"],
        )

        # read_columns=None expands to the non-blob columns only: id + size.
        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["size"] == [2, 2, 2, 2]

    def test_rejects_blob_output(self, temp_dir):
        path = Path(temp_dir) / "blob_output.lance"
        self._write_blob_dataset(path)

        with pytest.raises(ValueError, match="Cannot write blob column 'payload'"):
            lr.update_columns(
                str(path),
                transform=lambda b: b,
                columns=["payload"],
                read_columns=["payload"],
            )

    def test_blob_v2_column_is_readable_and_excluded_by_default(self, temp_dir):
        blob_field = pytest.importorskip("lance").blob_field
        blob_array = pytest.importorskip("lance").blob_array

        path = Path(temp_dir) / "blob_v2.lance"
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                blob_field("payload"),
                pa.field("size", pa.int64()),
            ]
        )
        table = pa.table(
            {
                "id": pa.array([0, 1], pa.int32()),
                "payload": blob_array([b"ab", b"cdef"]),
                "size": pa.array([0, 0], pa.int64()),
            },
            schema=schema,
        )
        lance.write_dataset(
            table,
            str(path),
            max_rows_per_file=2,
            data_storage_version="2.2",  # blob v2 requires file version >= 2.2
        )

        # Blob v2 is the case the default projection exists to protect: it must
        # not be pulled in unless asked for.
        lr.update_columns(
            str(path),
            transform=lambda b: _record_batch(
                {"size": pa.array([len(b.column_names)] * b.num_rows, pa.int64())}
            ),
            columns=["size"],
        )
        assert lance.dataset(str(path)).to_table().to_pydict()["size"] == [2, 2]

        # ... but it is readable when explicitly requested.
        def payload_size(batch: pa.RecordBatch) -> pa.RecordBatch:
            sizes = [
                len(v) if v is not None else 0 for v in batch["payload"].to_pylist()
            ]
            return _record_batch({"size": pa.array(sizes, pa.int64())})

        lr.update_columns(
            str(path),
            transform=payload_size,
            columns=["size"],
            read_columns=["payload"],
        )
        assert lance.dataset(str(path)).to_table().to_pydict()["size"] == [2, 4]

    def test_unrelated_blob_column_does_not_block_updates(self, temp_dir):
        path = Path(temp_dir) / "blob_unrelated.lance"
        self._write_blob_dataset(path)

        lr.update_columns(
            str(path),
            transform=lambda b: _record_batch(
                {"size": pc.cast(pc.multiply(b["id"], 100), pa.int64())}
            ),
            columns=["size"],
            read_columns=["id"],
        )

        got = lance.dataset(str(path)).to_table().to_pydict()
        assert got["size"] == [0, 100, 200, 300]

        # The blob column was not rewritten; read_lance reconstructs the bytes.
        payloads = (
            lr.read_lance(str(path), columns=["id", "payload"])
            .to_pandas()
            .sort_values("id")["payload"]
            .tolist()
        )
        assert payloads == [b"x", b"xx", b"xxx", b"xxxx"]
