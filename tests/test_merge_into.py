# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Test cases for lance_ray.merge_into (distributed merge_into)."""

import collections
import datetime
import tempfile
from decimal import Decimal
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


def id_to_rowid(dataset):
    table = dataset.to_table(columns=["id"], with_row_id=True)
    return dict(
        zip(
            table.column("id").to_pylist(),
            table.column("_rowid").to_pylist(),
            strict=True,
        )
    )


def test_rowaddr_parts_are_local_offsets():
    """delete_rows takes fragment-local offsets, not packed _rowaddr/_rowid."""
    from lance_ray.merge_into import _rowaddr_parts

    assert _rowaddr_parts(0) == (0, 0)
    assert _rowaddr_parts(17) == (0, 17)
    assert _rowaddr_parts((1 << 32) | 5) == (1, 5)
    assert _rowaddr_parts((3 << 32) | 0) == (3, 0)


def test_pack_plan_buckets_omits_empty_owners():
    """Shuffle payloads skip apply owners that received no rows."""
    from lance_ray.merge_into import (
        _OFFSET_COLUMN,
        _ROWID_COLUMN,
        _pack_plan_buckets,
    )

    chunk = pa.table({"id": [10, 20], "value": ["a", "b"]})
    updates: list = [collections.defaultdict(list) for _ in range(4)]
    updates[2][5].append((0, 99, 3))
    inserts: list[list[int]] = [[] for _ in range(4)]
    inserts[1] = [1]
    owners, buckets, bucket_rows = _pack_plan_buckets(
        4, chunk, updates, inserts, _ROWID_COLUMN, _OFFSET_COLUMN
    )
    assert owners == [1, 2]
    assert bucket_rows == [0, 1, 1, 0]
    assert buckets[0]["frags"] == {}
    assert buckets[0]["inserts"].num_rows == 1
    assert _ROWID_COLUMN in buckets[0]["inserts"].column_names
    matched = buckets[1]["frags"][5]
    assert matched.num_rows == 1
    assert matched.column(_OFFSET_COLUMN).to_pylist() == [3]
    assert matched.column(_ROWID_COLUMN).to_pylist() == [99]


def test_helper_column_names_share_taken_set():
    from lance_ray.merge_into import (
        _OFFSET_COLUMN,
        _ROWID_COLUMN,
        _helper_column_names,
    )

    assert _helper_column_names(pa.schema([("id", pa.int64())])) == (
        _ROWID_COLUMN,
        _OFFSET_COLUMN,
    )
    taken_defaults = pa.schema(
        [
            (_ROWID_COLUMN, pa.int64()),
            (_OFFSET_COLUMN, pa.int64()),
        ]
    )
    assert _helper_column_names(taken_defaults) == (
        f"{_ROWID_COLUMN}_2",
        f"{_OFFSET_COLUMN}_2",
    )
    taken_through_2 = pa.schema(
        [
            ("id", pa.int64()),
            (_ROWID_COLUMN, pa.int64()),
            (_OFFSET_COLUMN, pa.int64()),
            (f"{_ROWID_COLUMN}_2", pa.int64()),
            (f"{_OFFSET_COLUMN}_2", pa.int64()),
        ]
    )
    assert _helper_column_names(taken_through_2) == (
        f"{_ROWID_COLUMN}_3",
        f"{_OFFSET_COLUMN}_3",
    )


def test_sql_literal_renders_common_scalars():
    from lance_ray.merge_into import _sql_literal

    assert _sql_literal(True) == "TRUE"
    assert _sql_literal(False, pa.bool_()) == "FALSE"
    assert _sql_literal(7, pa.int64()) == "7"
    assert _sql_literal("O'Brien") == "'O''Brien'"
    assert _sql_literal(datetime.date(2024, 1, 15), pa.date32()) == "DATE '2024-01-15'"
    assert (
        _sql_literal(
            datetime.datetime(2024, 1, 15, 12, 30, 0),
            pa.timestamp("us"),
        )
        == "arrow_cast(1705321800000000, 'Timestamp(Microsecond, None)')"
    )
    assert (
        _sql_literal(Decimal("12.000"), pa.decimal128(5, 3)) == "DECIMAL(5,3) '12.000'"
    )
    assert _sql_literal(b"abc", pa.binary()) == "X'616263'"


@pytest.mark.parametrize(
    ("key_type", "ticks"),
    [
        (pa.timestamp("s"), -1),
        (pa.timestamp("ms"), -1),
        (pa.timestamp("us"), -1),
        (pa.timestamp("ns"), -1),
        (pa.timestamp("ns", tz="Asia/Shanghai"), -1),
        (pa.time32("s"), 1),
        (pa.time32("ms"), 1),
        (pa.time64("us"), 1),
        (pa.time64("ns"), 1),
    ],
)
def test_temporal_key_literals_match_exactly(
    tmp_path: Path, key_type: pa.DataType, ticks: int
) -> None:
    """Lance must resolve the literal to exactly one tick, also with an index."""
    from lance_ray.merge_into import _join_key_values, _sql_literal

    keys = pa.array([ticks, ticks + 1], type=key_type)
    dataset = lance.write_dataset(pa.table({"key": keys}), str(tmp_path / "keys"))
    assert _join_key_values(pa.chunked_array([keys])) == [ticks, ticks + 1]
    assert _join_key_values(pa.chunked_array([keys.dictionary_encode()])) == [
        ticks,
        ticks + 1,
    ]
    predicate = f"key IN ({_sql_literal(keys[0], key_type)})"
    assert (
        dataset.to_table(filter=predicate)
        .column("key")
        .equals(pa.chunked_array([keys.slice(0, 1)]))
    )
    dataset.create_scalar_index("key", index_type="BTREE")
    assert (
        dataset.to_table(filter=predicate)
        .column("key")
        .equals(pa.chunked_array([keys.slice(0, 1)]))
    )
    assert "ScalarIndexQuery" in dataset.scanner(filter=predicate).explain_plan()


def test_nested_join_key_type_is_rejected():
    from lance_ray.merge_into import _raise_unless_supported_join_key

    _raise_unless_supported_join_key("id", pa.int64())
    _raise_unless_supported_join_key("k", pa.dictionary(pa.int32(), pa.string()))
    with pytest.raises(TypeError, match="unsupported type"):
        _raise_unless_supported_join_key("tags", pa.list_(pa.int32()))


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
        """A source where every key matches only updates.

        id 15 lives in fragment 1, so deletion must use the local offset
        (not the packed ``_rowid`` / ``_rowaddr`` value).
        """
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
        """num_partitions only splits the source; apply owners follow num_workers."""
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
        updated = lr.merge_into(
            source, str(path), on="id", num_workers=2, num_partitions=6
        )

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
        """Updated keys keep their logical _rowid; inserts get a new id."""
        path = Path(temp_dir) / "stable_row_ids"
        dataset = create_dataset_with_fragments(
            path, make_fragments(2, 10), enable_stable_row_ids=True
        )
        before_ids = id_to_rowid(dataset)

        source = pa.table({"id": [4, 14, 300], "value": ["new_4", "new_14", "new_300"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        dataset = updated
        assert dataset.count_rows() == 21
        values = id_to_value(dataset)
        assert values[4] == "new_4"
        assert values[14] == "new_14"
        assert values[300] == "new_300"

        after_ids = id_to_rowid(dataset)
        assert after_ids[4] == before_ids[4]
        assert after_ids[14] == before_ids[14]
        assert after_ids[0] == before_ids[0]
        assert after_ids[19] == before_ids[19]
        assert after_ids[300] not in before_ids.values()

    def test_target_one_to_many_updates_all_matches(self, temp_dir):
        """A source key that hits several target rows updates every match."""
        path = Path(temp_dir) / "target_one_to_many"
        create_dataset_with_fragments(
            path,
            [
                pd.DataFrame({"id": [1, 1, 2], "value": ["a", "b", "c"]}),
            ],
        )
        source = pa.table({"id": [1, 3], "value": ["z", "new_3"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        assert updated.count_rows() == 4
        table = updated.to_table()
        pairs = list(
            zip(
                table.column("id").to_pylist(),
                table.column("value").to_pylist(),
                strict=True,
            )
        )
        assert pairs.count((1, "z")) == 2
        assert (2, "c") in pairs
        assert (3, "new_3") in pairs

    def test_target_one_to_many_across_fragments(self, temp_dir):
        """Matches on the same key in different fragments are all updated."""
        path = Path(temp_dir) / "target_one_to_many_frags"
        create_dataset_with_fragments(
            path,
            [
                pd.DataFrame({"id": [1, 2], "value": ["a", "keep_2"]}),
                pd.DataFrame({"id": [1, 3], "value": ["b", "keep_3"]}),
            ],
        )
        source = pa.table({"id": [1], "value": ["z"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        assert updated.count_rows() == 4
        table = updated.to_table()
        pairs = list(
            zip(
                table.column("id").to_pylist(),
                table.column("value").to_pylist(),
                strict=True,
            )
        )
        assert pairs.count((1, "z")) == 2
        assert (2, "keep_2") in pairs
        assert (3, "keep_3") in pairs

    def test_target_one_to_many_preserves_stable_row_ids(self, temp_dir):
        """Join-all keeps every matched logical _rowid."""
        path = Path(temp_dir) / "target_one_to_many_stable"
        dataset = create_dataset_with_fragments(
            path,
            [pd.DataFrame({"id": [1, 1, 2], "value": ["a", "b", "c"]})],
            enable_stable_row_ids=True,
        )
        before_table = dataset.to_table(columns=["id"], with_row_id=True)
        before_ids = [
            rowid
            for key, rowid in zip(
                before_table.column("id").to_pylist(),
                before_table.column("_rowid").to_pylist(),
                strict=True,
            )
            if key == 1
        ]

        source = pa.table({"id": [1], "value": ["z"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=1)

        after_table = updated.to_table(columns=["id", "value"], with_row_id=True)
        after_pairs = list(
            zip(
                after_table.column("id").to_pylist(),
                after_table.column("value").to_pylist(),
                after_table.column("_rowid").to_pylist(),
                strict=True,
            )
        )
        updated_ids = sorted(rowid for key, value, rowid in after_pairs if key == 1)
        assert updated_ids == sorted(before_ids)
        assert all(value == "z" for key, value, _ in after_pairs if key == 1)

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

    def test_insert_when_helper_name_is_user_column(self, temp_dir):
        """Inserts drop the computed helper, not a colliding user field."""
        path = Path(temp_dir) / "helper_insert"
        target = pa.table(
            {
                "id": [1, 2],
                "__merge_into_rowid": [10, 20],
                "value": ["a", "b"],
            }
        )
        lr.write_lance(ray.data.from_arrow(target), str(path))
        source = pa.table(
            {
                "id": [3],
                "__merge_into_rowid": [30],
                "value": ["c"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=1)
        values = id_to_value(updated)
        assert values == {1: "a", 2: "b", 3: "c"}
        rowids = dict(
            zip(
                updated.to_table().column("id").to_pylist(),
                updated.to_table().column("__merge_into_rowid").to_pylist(),
                strict=True,
            )
        )
        assert rowids == {1: 10, 2: 20, 3: 30}

    def test_update_when_helper_names_are_user_columns(self, temp_dir):
        """Matched rows keep user fields that reuse the default helper names."""
        path = Path(temp_dir) / "helper_update"
        target = pa.table(
            {
                "id": [1, 2],
                "__merge_into_rowid": [10, 20],
                "__merge_into_offset": [100, 200],
                "value": ["a", "b"],
            }
        )
        lr.write_lance(ray.data.from_arrow(target), str(path))
        source = pa.table(
            {
                "id": [1],
                "__merge_into_rowid": [11],
                "__merge_into_offset": [101],
                "value": ["A"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=1)
        table = updated.to_table()
        by_id = {
            key: (value, rowid, offset)
            for key, value, rowid, offset in zip(
                table.column("id").to_pylist(),
                table.column("value").to_pylist(),
                table.column("__merge_into_rowid").to_pylist(),
                table.column("__merge_into_offset").to_pylist(),
                strict=True,
            )
        }
        assert by_id[1] == ("A", 11, 101)
        assert by_id[2] == ("b", 20, 200)

    def test_helpers_skip_default_and_suffix_2_user_columns(self, temp_dir):
        """Default helper names and their _2 suffixes can all be user fields."""
        path = Path(temp_dir) / "helper_suffix_3"
        target = pa.table(
            {
                "id": [1],
                "__merge_into_rowid": [10],
                "__merge_into_offset": [100],
                "__merge_into_rowid_2": [12],
                "__merge_into_offset_2": [102],
                "value": ["old"],
            }
        )
        lr.write_lance(ray.data.from_arrow(target), str(path))
        source = pa.table(
            {
                "id": [1, 2],
                "__merge_into_rowid": [11, 21],
                "__merge_into_offset": [101, 201],
                "__merge_into_rowid_2": [13, 23],
                "__merge_into_offset_2": [103, 203],
                "value": ["new", "ins"],
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=1)
        table = updated.to_table()
        by_id = {
            key: (
                value,
                table.column("__merge_into_rowid").to_pylist()[i],
                table.column("__merge_into_offset").to_pylist()[i],
                table.column("__merge_into_rowid_2").to_pylist()[i],
                table.column("__merge_into_offset_2").to_pylist()[i],
            )
            for i, (key, value) in enumerate(
                zip(
                    table.column("id").to_pylist(),
                    table.column("value").to_pylist(),
                    strict=True,
                )
            )
        }
        assert by_id[1] == ("new", 11, 101, 13, 103)
        assert by_id[2] == ("ins", 21, 201, 23, 203)
        assert updated.count_rows() == 2

    def test_date_join_keys(self, temp_dir):
        """Date keys are planned as DATE literals, not remote TypeErrors."""
        path = Path(temp_dir) / "date_keys"
        target = pa.table(
            {
                "event_date": pa.array(
                    [
                        datetime.date(2024, 1, 1),
                        datetime.date(2024, 1, 2),
                        datetime.date(2024, 1, 3),
                    ],
                    type=pa.date32(),
                ),
                "value": ["a", "b", "c"],
            }
        )
        lr.write_lance(ray.data.from_arrow(target), str(path))
        source = pa.table(
            {
                "event_date": pa.array(
                    [datetime.date(2024, 1, 2), datetime.date(2024, 1, 4)],
                    type=pa.date32(),
                ),
                "value": ["B", "D"],
            }
        )
        updated = lr.merge_into(source, str(path), on="event_date", num_workers=2)
        pairs = list(
            zip(
                updated.to_table().column("event_date").to_pylist(),
                updated.to_table().column("value").to_pylist(),
                strict=True,
            )
        )
        assert (datetime.date(2024, 1, 1), "a") in pairs
        assert (datetime.date(2024, 1, 2), "B") in pairs
        assert (datetime.date(2024, 1, 3), "c") in pairs
        assert (datetime.date(2024, 1, 4), "D") in pairs

    @pytest.mark.parametrize("timezone", [None, "Asia/Shanghai"])
    @pytest.mark.parametrize("with_index", [False, True])
    def test_nanosecond_timestamp_join_keys(
        self, tmp_path: Path, timezone: str | None, with_index: bool
    ) -> None:
        """Submicrosecond keys, including before the epoch, update exactly."""
        path = str(tmp_path / "timestamp_keys")
        key_type = pa.timestamp("ns", tz=timezone)
        target = pa.table(
            {
                "key": pa.array([-1001, -1, 0, 1, 1700000000000000001], type=key_type),
                "value": [
                    "keep_negative",
                    "old_negative",
                    "keep_zero",
                    "old",
                    "old_recent",
                ],
            }
        )
        dataset = lance.write_dataset(target, path, max_rows_per_file=2)
        if with_index:
            dataset.create_scalar_index("key", index_type="BTREE")
        version_before = dataset.version
        source = pa.table(
            {
                "key": pa.array(
                    [-1, 1, 1700000000000000001, 1700000000000000002],
                    type=key_type,
                ),
                "value": ["new_negative", "new", "new_recent", "inserted"],
            }
        )
        updated = lr.merge_into(source, path, on="key", num_workers=2)
        result = updated.to_table()
        assert updated.version == version_before + 1
        assert result.num_rows == 6
        assert result.schema.field("key").type == key_type
        assert dict(
            zip(
                result.column("key").cast(pa.int64()).to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            )
        ) == {
            -1001: "keep_negative",
            -1: "new_negative",
            0: "keep_zero",
            1: "new",
            1700000000000000001: "new_recent",
            1700000000000000002: "inserted",
        }

    @pytest.mark.parametrize("with_index", [False, True])
    def test_nanosecond_time_join_keys(self, tmp_path: Path, with_index: bool) -> None:
        """Distinct nanoseconds within one microsecond remain distinct keys."""
        path = str(tmp_path / "time_keys")
        key_type = pa.time64("ns")
        target = pa.table(
            {
                "key": pa.array([0, 1, 2, 86399999999999], type=key_type),
                "value": ["keep", "old_1", "old_2", "old_end_of_day"],
            }
        )
        dataset = lance.write_dataset(target, path, max_rows_per_file=2)
        if with_index:
            dataset.create_scalar_index("key", index_type="BTREE")
        version_before = dataset.version
        source = pa.table(
            {
                "key": pa.array([1, 2, 3, 86399999999999], type=key_type),
                "value": ["new_1", "new_2", "inserted", "new_end_of_day"],
            }
        )
        updated = lr.merge_into(source, path, on="key", num_workers=2)
        result = updated.to_table()
        assert updated.version == version_before + 1
        assert result.num_rows == 5
        assert dict(
            zip(
                result.column("key").cast(pa.int64()).to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            )
        ) == {
            0: "keep",
            1: "new_1",
            2: "new_2",
            3: "inserted",
            86399999999999: "new_end_of_day",
        }

    def test_decimal_and_binary_join_keys(self, temp_dir):
        path = Path(temp_dir) / "decimal_binary"
        dec = pa.decimal128(5, 2)
        target = pa.table(
            {
                "amount": pa.array([Decimal("1.50"), Decimal("2.00")], type=dec),
                "payload": pa.array([b"aa", b"bb"], type=pa.binary()),
                "value": ["keep", "old"],
            }
        )
        lr.write_lance(ray.data.from_arrow(target), str(path))
        source = pa.table(
            {
                "amount": pa.array([Decimal("2.00"), Decimal("3.25")], type=dec),
                "payload": pa.array([b"bb", b"cc"], type=pa.binary()),
                "value": ["new_2", "new_3"],
            }
        )
        by_amount = lr.merge_into(
            source.select(["amount", "payload", "value"]),
            str(path),
            on="amount",
            num_workers=1,
        )
        amounts = dict(
            zip(
                by_amount.to_table().column("amount").to_pylist(),
                by_amount.to_table().column("value").to_pylist(),
                strict=True,
            )
        )
        assert amounts[Decimal("1.50")] == "keep"
        assert amounts[Decimal("2.00")] == "new_2"
        assert amounts[Decimal("3.25")] == "new_3"

        path_bin = Path(temp_dir) / "binary_keys"
        lr.write_lance(ray.data.from_arrow(target), str(path_bin))
        by_payload = lr.merge_into(
            source.select(["amount", "payload", "value"]),
            str(path_bin),
            on="payload",
            num_workers=1,
        )
        payloads = dict(
            zip(
                by_payload.to_table().column("payload").to_pylist(),
                by_payload.to_table().column("value").to_pylist(),
                strict=True,
            )
        )
        assert payloads[b"aa"] == "keep"
        assert payloads[b"bb"] == "new_2"
        assert payloads[b"cc"] == "new_3"

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
        updated = lr.merge_into(
            source, str(path), on="id", num_workers=1, num_partitions=1
        )

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
        updated = lr.merge_into(
            source, str(path), on="id", num_workers=2, num_partitions=4
        )

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


class TestMergeIntoCommitAck:
    def test_operation_visible_after_insert_only(self, temp_dir):
        """New-fragment paths identify a committed insert-only merge."""
        from lance_ray.merge_into import _merge_operation_visible

        path = Path(temp_dir) / "ack_visible_insert"
        dataset = create_dataset_with_fragments(path, make_fragments(1, 10))
        read_version = dataset.version
        baseline_ids = {fragment.fragment_id for fragment in dataset.get_fragments()}

        updated = lr.merge_into(
            pa.table({"id": [100, 101], "value": ["new_100", "new_101"]}),
            str(path),
            on="id",
            num_workers=1,
        )
        new_fragments = [
            fragment.metadata
            for fragment in updated.get_fragments()
            if fragment.fragment_id not in baseline_ids
        ]
        assert new_fragments
        assert _merge_operation_visible(
            updated,
            new_fragments=new_fragments,
            updated_fragments=[],
            removed_fragment_ids=[],
        )
        pinned = lance.dataset(str(path), version=read_version)
        assert not _merge_operation_visible(
            pinned,
            new_fragments=new_fragments,
            updated_fragments=[],
            removed_fragment_ids=[],
        )

    def test_lost_commit_ack_returns_committed_table(self, temp_dir, monkeypatch):
        """A raised commit after a successful PUT must not fail merge_into."""
        path = Path(temp_dir) / "ack_lost"
        create_dataset_with_fragments(path, make_fragments(1, 10))
        real_commit = lance.LanceDataset.commit

        def commit_then_lose_ack(*args, **kwargs):
            real_commit(*args, **kwargs)
            raise RuntimeError("lost ack")

        monkeypatch.setattr(lance.LanceDataset, "commit", commit_then_lose_ack)

        updated = lr.merge_into(
            pa.table({"id": [100], "value": ["new_100"]}),
            str(path),
            on="id",
            num_workers=1,
        )

        assert updated.count_rows() == 11
        assert id_to_value(updated)[100] == "new_100"
        assert lance.dataset(str(path)).count_rows() == 11

    def test_failed_commit_is_not_treated_as_success(self, temp_dir, monkeypatch):
        """A commit that never landed must still raise."""
        path = Path(temp_dir) / "ack_not_visible"
        dataset = create_dataset_with_fragments(path, make_fragments(1, 10))
        version_before = dataset.version

        def commit_fails(*args, **kwargs):
            raise RuntimeError("commit failed")

        monkeypatch.setattr(lance.LanceDataset, "commit", commit_fails)

        with pytest.raises(RuntimeError, match="commit failed"):
            lr.merge_into(
                pa.table({"id": [100], "value": ["new_100"]}),
                str(path),
                on="id",
                num_workers=1,
            )
        assert lance.dataset(str(path)).version == version_before
        assert lance.dataset(str(path)).count_rows() == 10


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

    def test_rejects_nested_join_key_on_driver(self, temp_dir):
        """Nested join keys fail from the target schema, before plan tasks."""
        path = Path(temp_dir) / "nested_key"
        target = pa.table(
            {
                "tags": pa.array([[1], [2]], type=pa.list_(pa.int32())),
                "value": ["a", "b"],
            }
        )
        lr.write_lance(ray.data.from_arrow(target), str(path))
        source = pa.table(
            {
                "tags": pa.array([[1], [3]], type=pa.list_(pa.int32())),
                "value": ["A", "C"],
            }
        )
        with pytest.raises(TypeError, match="unsupported type"):
            lr.merge_into(source, str(path), on="tags", num_workers=1)

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
            lr.merge_into(
                pa.table({"id": [1]}), "/tmp/x.lance", on="id", num_partitions=0
            )
