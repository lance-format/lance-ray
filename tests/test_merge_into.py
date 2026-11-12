# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Test cases for lance_ray.merge_into (distributed merge_into)."""

import collections
import datetime
import tempfile
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn, cast

import lance
import lance_ray as lr
import pyarrow as pa
import pytest
import ray

import pandas as pd


@pytest.fixture
def temp_dir() -> Iterator[str]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def create_dataset_with_fragments(
    path: Path, fragment_data: list[pd.DataFrame], **write_kwargs: Any
) -> lance.LanceDataset:
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


def make_fragments(num_fragments: int, rows_per_fragment: int) -> list[pd.DataFrame]:
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


def id_to_value(dataset: lance.LanceDataset) -> dict[int, str]:
    table = dataset.to_table()
    return dict(
        zip(
            cast(list[int], table.column("id").to_pylist()),
            cast(list[str], table.column("value").to_pylist()),
            strict=False,
        )
    )


def id_to_rowid(dataset: lance.LanceDataset) -> dict[int, int]:
    table = dataset.to_table(columns=["id"], with_row_id=True)
    return dict(
        zip(
            cast(list[int], table.column("id").to_pylist()),
            cast(list[int], table.column("_rowid").to_pylist()),
            strict=True,
        )
    )


def test_rowaddr_parts_are_local_offsets() -> None:
    """delete_rows takes fragment-local offsets, not packed _rowaddr/_rowid."""
    from lance_ray.merge_into import _rowaddr_parts

    assert _rowaddr_parts(0) == (0, 0)
    assert _rowaddr_parts(17) == (0, 17)
    assert _rowaddr_parts((1 << 32) | 5) == (1, 5)
    assert _rowaddr_parts((3 << 32) | 0) == (3, 0)


def test_pack_plan_buckets_omits_empty_owners() -> None:
    """Shuffle payloads skip apply owners that received no rows."""
    from lance_ray.merge_into import (
        _OFFSET_COLUMN,
        _ROWID_COLUMN,
        _pack_plan_buckets,
    )

    chunk = pa.table({"id": [10, 20], "value": ["a", "b"]})
    updates: list[dict[int, list[tuple[int, int, int]]]] = [
        collections.defaultdict(list) for _ in range(4)
    ]
    updates[2][5].append((0, 99, 3))
    inserts: list[list[int]] = [[] for _ in range(4)]
    inserts[1] = [1]
    owners, buckets, bucket_rows = _pack_plan_buckets(
        4,
        chunk,
        updates,
        inserts,
        _ROWID_COLUMN,
        _OFFSET_COLUMN,
        rowid_type=pa.int64(),
    )
    assert owners == [1, 2]
    assert bucket_rows == [0, 1, 1, 0]
    assert buckets[0]["frags"] == {}
    assert buckets[0]["inserts"].num_rows == 1
    assert buckets[0]["inserts"].column(_ROWID_COLUMN).to_pylist() == [None]
    matched = buckets[1]["frags"][5]
    assert matched.num_rows == 1
    assert matched.column(_OFFSET_COLUMN).to_pylist() == [3]
    assert matched.column(_ROWID_COLUMN).to_pylist() == [99]


def test_helper_column_names_share_taken_set() -> None:
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


def test_sql_literal_renders_common_scalars() -> None:
    from lance_ray.merge_into import _sql_literal

    assert _sql_literal(True) == "TRUE"
    assert _sql_literal(False, pa.bool_()) == "FALSE"
    assert _sql_literal(7, pa.int64()) == "7"
    assert _sql_literal(-9223372036854775808, pa.int64()) == (
        "arrow_cast('-9223372036854775808', 'Int64')"
    )
    assert _sql_literal(2**63 - 1, pa.uint64()) == "9223372036854775807"
    assert _sql_literal(2**63, pa.uint64()) == (
        "arrow_cast('9223372036854775808', 'UInt64')"
    )
    assert _sql_literal(2**64 - 1, pa.uint64()) == (
        "arrow_cast('18446744073709551615', 'UInt64')"
    )
    assert _sql_literal("O'Brien") == "'O''Brien'"
    assert _sql_literal(datetime.date(2024, 1, 15), pa.date32()) == "DATE '2024-01-15'"
    assert _sql_literal(2932897, pa.date32()) == "arrow_cast(2932897, 'Date32')"
    assert _sql_literal(253402300800000, pa.date64()) == (
        "arrow_cast(253402300800000, 'Date64')"
    )
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
    assert _sql_literal(1.5, pa.float64()) == "1.5"
    with pytest.raises(ValueError, match="finite"):
        _sql_literal(float("nan"), pa.float64())
    with pytest.raises(ValueError, match="finite"):
        _sql_literal(float("inf"), pa.float32())


def test_sql_decimal_literal_accepts_full_decimal128_precision() -> None:
    """decimal128(38, *) must not inherit the default precision-28 context."""
    from lance_ray.merge_into import _sql_literal

    value = Decimal("1" * 36 + ".25")
    assert _sql_literal(value, pa.decimal128(38, 2)) == (
        "DECIMAL(38,2) '111111111111111111111111111111111111.25'"
    )
    wide = Decimal("9" * 40)
    assert _sql_literal(wide, pa.decimal256(40, 0)) == (
        f"arrow_cast('{wide}', 'Decimal256(40, 0)')"
    )
    assert _sql_literal(Decimal("100"), pa.decimal128(3, -2)) == (
        "arrow_cast(arrow_cast('1', 'Decimal128(3, 0)') * "
        "arrow_cast(arrow_cast('100', 'Decimal128(3, 0)'), 'Decimal128(1, -2)'), "
        "'Decimal128(3, -2)')"
    )
    from decimal import Context, localcontext

    with localcontext(Context(prec=42)):
        full = Decimal(10) ** 39
    rendered = _sql_literal(full, pa.decimal128(38, -2))
    assert rendered == (
        "arrow_cast(arrow_cast('10000000000000000000000000000000000000', "
        "'Decimal128(38, 0)') * arrow_cast(arrow_cast('100', 'Decimal128(3, 0)'), "
        "'Decimal128(1, -2)'), 'Decimal128(38, -2)')"
    )
    with localcontext(Context(prec=160)):
        huge = Decimal("1E76")
    huge_literal = _sql_literal(huge, pa.decimal256(76, -76))
    assert "Decimal256(77" not in huge_literal
    assert huge_literal == (
        "arrow_cast(arrow_cast('1', 'Decimal256(76, 0)') * "
        "arrow_cast(arrow_cast("
        "'1000000000000000000000000000000000000000000000000000000000000000000000000000', "
        "'Decimal256(76, 0)'), 'Decimal256(1, -75)') * "
        "arrow_cast(arrow_cast('10', 'Decimal128(2, 0)'), 'Decimal256(1, -1)'), "
        "'Decimal256(76, -76)')"
    )


def test_sql_identifier_doubles_embedded_backticks() -> None:
    from lance_ray.merge_into import _sql_identifier

    assert _sql_identifier("id") == "`id`"
    assert _sql_identifier("key`name") == "`key``name`"


def test_delete_offsets_are_real_numbers() -> None:
    """delete_rows does int(offset); Arrow scalars are not accepted everywhere."""
    from lance_ray.merge_into import _offsets_for_delete_rows

    offsets = pa.chunked_array(
        [
            pa.array([3], type=pa.int64()),
            pa.array([1], type=pa.int64()),
        ]
    )
    values = _offsets_for_delete_rows(offsets)
    assert [int(value) for value in values] == [3, 1]
    assert not isinstance(values[0], pa.Scalar)


def test_fragment_match_ids_stay_packed_and_release() -> None:
    """Row ids and offsets must not accumulate as Python lists or sets."""
    import tracemalloc

    import numpy as np
    from lance_ray.merge_into import _FragmentMatchIds

    n = 200_000
    batch = 10_000
    matches = _FragmentMatchIds()
    tracemalloc.start()
    for start in range(0, n, batch):
        values = pa.array(np.arange(start, start + batch, dtype=np.int64))
        matches.append(values, values)
    assert len(matches.row_ids) == n // batch
    assert all(isinstance(array, pa.Array) for array in matches.row_ids)
    matches.release_row_ids_after_duplicate_check("fragment 1")
    _current, packed_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    offsets = matches.take_offsets_after_duplicate_check("fragment 1 offsets")

    assert matches.row_ids == []
    assert matches.offsets == []
    assert len(offsets) == n
    assert offsets[0].as_py() == 0
    assert offsets[n - 1].as_py() == n - 1

    tracemalloc.start()
    python_ids: list[int] = []
    for start in range(0, n, batch):
        python_ids.extend(range(start, start + batch))
    python_ids_set = set(python_ids)
    _current, python_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert len(python_ids_set) == n
    # Compare with a list-plus-set of the same integers in this process.
    # An absolute byte cap tracks unrelated allocator noise.
    assert packed_peak < python_peak

    duplicate = _FragmentMatchIds()
    duplicate.append(
        pa.array([1, 2], type=pa.int64()), pa.array([0, 1], type=pa.int64())
    )
    duplicate.append(pa.array([2], type=pa.int64()), pa.array([2], type=pa.int64()))
    with pytest.raises(RuntimeError, match="Duplicate target row ids"):
        duplicate.release_row_ids_after_duplicate_check("fragment 1")


def test_fragment_match_ids_drop_parent_bucket_buffers() -> None:
    """Cross-bucket identities must not keep the deserialized bucket allocation."""
    import sys

    import numpy as np
    from lance_ray.merge_into import _FragmentMatchIds

    n = 100_000
    parent = pa.table(
        {
            "payload": pa.array(["x" * 10] * n),
            "rowid": pa.array(range(n), type=pa.uint64()),
            "offset": pa.array(range(n), type=pa.int64()),
        }
    )
    view = parent.slice(5, 2)
    # pyarrow-stubs types ChunkedArray.chunk() as ChunkedArray, which has no
    # buffers. chunks is list[Array].
    row_array = view.column("rowid").chunks[0]
    offset_array = view.column("offset").chunks[0]
    row_buf = row_array.buffers()[1]
    offset_buf = offset_array.buffers()[1]
    assert row_buf is not None and offset_buf is not None
    assert row_buf.size > row_array.nbytes
    assert offset_buf.size > offset_array.nbytes
    row_refs = sys.getrefcount(row_buf)
    offset_refs = sys.getrefcount(offset_buf)

    matches = _FragmentMatchIds()
    matches.append(view.column("rowid"), view.column("offset"))

    assert sys.getrefcount(row_buf) == row_refs
    assert sys.getrefcount(offset_buf) == offset_refs
    assert matches.row_ids[0].to_pylist() == [5, 6]
    assert matches.offsets[0].to_pylist() == [5, 6]
    for stored in (*matches.row_ids, *matches.offsets):
        data = stored.buffers()[1]
        assert data is not None
        assert data.size == stored.nbytes
        assert data.address != row_buf.address
        assert data.address != offset_buf.address

    root = pa.allocate_buffer(1 << 20)
    memory: np.ndarray[tuple[int], np.dtype[np.int64]] = np.ndarray(
        shape=(1,), dtype=np.int64, buffer=memoryview(root)
    )
    memory[0] = 42
    shared = pa.Array.from_buffers(pa.int64(), 1, [None, root])
    assert shared.nbytes == 8
    shared_buf = shared.buffers()[1]
    assert shared_buf is not None
    assert shared_buf.size == root.size
    root_refs = sys.getrefcount(root)
    matches.append(shared, shared)
    assert sys.getrefcount(root) == root_refs
    assert matches.offsets[1].to_pylist() == [42]
    stored_root = matches.offsets[1].buffers()[1]
    assert stored_root is not None
    assert stored_root.size == 8
    assert stored_root.address != root.address


def test_nonfinite_float_keys_are_rejected() -> None:
    from lance_ray.merge_into import _align_chunk

    nan_rows = pa.table(
        {
            "score": pa.array([1.0, float("nan")], type=pa.float64()),
            "value": pa.array(["a", "b"]),
        }
    )
    with pytest.raises(ValueError, match="finite"):
        _align_chunk(nan_rows, nan_rows.schema, "score")
    inf_rows = pa.table(
        {
            "score": pa.array([float("-inf")], type=pa.float64()),
            "value": pa.array(["a"]),
        }
    )
    with pytest.raises(ValueError, match="finite"):
        _align_chunk(inf_rows, inf_rows.schema, "score")


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("dictionary", [False, True])
def test_nonfinite_float16_keys_are_rejected(bad: float, dictionary: bool) -> None:
    import numpy as np
    from lance_ray.merge_into import _align_chunk

    keys: pa.Array[Any] = pa.array(np.array([1.0, bad], dtype=np.float16))
    if dictionary:
        keys = pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int8()), keys)
    rows = pa.table({"score": keys, "value": ["a", "b"]})
    with pytest.raises(ValueError, match="finite"):
        _align_chunk(rows, rows.schema, "score")


def test_empty_temporal_batches_keep_one_schema() -> None:
    from lance_ray.merge_into import _add_sort_key, _dedupe_source_batch

    empty = pa.table({"ts": pa.array([], type=pa.timestamp("ns"))})
    sort_column = "__merge_into_sort_key"
    keyed = _add_sort_key(empty, on="ts", sort_column=sort_column)
    assert keyed.num_rows == 0
    assert sort_column in keyed.column_names
    assert keyed.column(sort_column).type == pa.int64()
    dropped = _dedupe_source_batch(keyed, on="ts", sort_column=sort_column)
    assert sort_column not in dropped.column_names
    assert dropped.schema == empty.schema
    zero_column = empty.select([])
    untouched = _add_sort_key(zero_column, on="ts", sort_column=sort_column)
    assert untouched.num_rows == 0
    assert untouched.column_names == []


def test_scalar_index_field_names_use_backticks() -> None:
    from lance_ray.merge_into import _has_scalar_index_on

    class _Index:
        field_names = ["`user.id`", "id"]

    class _Dataset:
        def describe_indices(self) -> list[_Index]:
            return [_Index()]

    dataset = cast(lance.LanceDataset, _Dataset())
    assert _has_scalar_index_on(dataset, "user.id")
    assert _has_scalar_index_on(dataset, "id")
    assert not _has_scalar_index_on(dataset, "user")


def test_schema_field_ids_include_nested_leaves() -> None:
    """Index maintenance needs leaf ids, not only top-level field ids."""
    from lance.schema import LanceSchema
    from lance_ray.merge_into import _schema_field_ids

    arrow_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("meta", pa.struct([("text", pa.string())])),
        ]
    )
    schema = LanceSchema.from_pyarrow(arrow_schema)
    top_level = [field.id() for field in schema.fields()]
    all_ids = _schema_field_ids(schema)
    nested = schema.field("meta.text")
    assert nested is not None
    assert all_ids[: len(top_level)] == top_level
    assert nested.id() in all_ids
    assert nested.id() not in top_level


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


def test_nested_join_key_type_is_rejected() -> None:
    from lance_ray.merge_into import _raise_unless_supported_join_key

    _raise_unless_supported_join_key("id", pa.int64())
    _raise_unless_supported_join_key("k", pa.dictionary(pa.int32(), pa.string()))
    with pytest.raises(TypeError, match="unsupported type"):
        _raise_unless_supported_join_key("tags", pa.list_(pa.int32()))


class TestMergeInto:
    def test_basic_merge_into(self, temp_dir: str) -> None:
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

    def test_merge_into_with_ray_dataset_source(self, temp_dir: str) -> None:
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

    def test_insert_only_source(self, temp_dir: str) -> None:
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

    def test_update_only_source(self, temp_dir: str) -> None:
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

    def test_empty_source_is_noop(self, temp_dir: str) -> None:
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

    def test_more_fragments_than_workers(self, temp_dir: str) -> None:
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

    def test_more_partitions_than_workers(self, temp_dir: str) -> None:
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

    def test_merge_into_with_scalar_index(self, temp_dir: str) -> None:
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
        assert updated.to_table(filter="id = 7").column("value").to_pylist() == ["a"]
        assert updated.to_table(filter="id = 200").column("value").to_pylist() == ["d"]
        assert updated.to_table(filter="id = 0").column("value").to_pylist() == [
            "orig_0"
        ]

    @pytest.mark.parametrize("with_index", [False, True])
    def test_merge_into_float_keys(self, temp_dir: str, with_index: bool) -> None:
        """Finite float keys match through a scan and through a BTREE."""
        path = str(Path(temp_dir) / "float_key")
        target = pa.table(
            {
                "score": pa.array([1.5, 0.1, 1e-05], type=pa.float64()),
                "value": ["exact", "tenth", "tiny"],
            }
        )
        dataset = lance.write_dataset(target, path)
        if with_index:
            dataset.create_scalar_index("score", index_type="BTREE")
        updated = lr.merge_into(
            pa.table(
                {
                    "score": pa.array([0.1, 1e-05, 3.5], type=pa.float64()),
                    "value": ["tenth-new", "tiny-new", "inserted"],
                }
            ),
            path,
            on="score",
            num_workers=1,
        )
        rows = updated.to_table().to_pylist()
        by_score = {row["score"]: row["value"] for row in rows}
        assert len(rows) == 4
        assert by_score == {
            1.5: "exact",
            0.1: "tenth-new",
            1e-05: "tiny-new",
            3.5: "inserted",
        }
        assert updated.to_table(filter="score = 0.1").column("value").to_pylist() == [
            "tenth-new"
        ]
        assert updated.to_table(filter="score = 1e-05").column("value").to_pylist() == [
            "tiny-new"
        ]

    def test_dictionary_and_float16_keys_round_trip(self, temp_dir: str) -> None:
        """Dictionary and float16 keys sort through a helper and keep their type."""
        dict_path = str(Path(temp_dir) / "dict_key")
        dict_type = pa.dictionary(pa.int32(), pa.string())
        lance.write_dataset(
            pa.table(
                {
                    "label": pa.array(["keep", "old"], type=dict_type),
                    "value": ["a", "b"],
                }
            ),
            dict_path,
        )
        updated = lr.merge_into(
            pa.table(
                {
                    "label": pa.array(["old", "old", "new"], type=dict_type),
                    "value": ["first", "second", "inserted"],
                }
            ),
            dict_path,
            on="label",
            num_workers=1,
        )
        dict_rows = {
            row["label"]: row["value"] for row in updated.to_table().to_pylist()
        }
        assert dict_rows["keep"] == "a"
        assert dict_rows["old"] in {"first", "second"}
        assert dict_rows["new"] == "inserted"
        assert len(dict_rows) == 3
        assert pa.types.is_dictionary(updated.schema.field("label").type)

        import numpy as np

        float_path = str(Path(temp_dir) / "float16_key")
        # Older PyArrow builds reject Python ints in a float16 array and
        # require numpy.float16 values.
        lance.write_dataset(
            pa.table(
                {
                    "score": pa.array(np.array([1, 2], dtype=np.float16)),
                    "value": ["a", "old"],
                }
            ),
            float_path,
        )
        updated = lr.merge_into(
            pa.table(
                {
                    "score": pa.array(np.array([2, 2, 3], dtype=np.float16)),
                    "value": ["first", "second", "inserted"],
                }
            ),
            float_path,
            on="score",
            num_workers=1,
        )
        scores = {
            float(row["score"]): row["value"] for row in updated.to_table().to_pylist()
        }
        assert scores[1.0] == "a"
        assert scores[2.0] in {"first", "second"}
        assert scores[3.0] == "inserted"
        assert len(scores) == 3
        assert updated.schema.field("score").type == pa.float16()

    def test_decimal256_join_key_round_trips(self, temp_dir: str) -> None:
        """A 40-digit decimal256 key matches through arrow_cast, not DECIMAL()."""
        path = str(Path(temp_dir) / "decimal256")
        decimal_type = pa.decimal256(40, 0)
        amount = Decimal("9" * 40)
        lance.write_dataset(
            pa.table(
                {
                    "amount": pa.array([amount], type=decimal_type),
                    "value": ["old"],
                }
            ),
            path,
        )
        updated = lr.merge_into(
            pa.table(
                {
                    "amount": pa.array([amount], type=decimal_type),
                    "value": ["new"],
                }
            ),
            path,
            on="amount",
            num_workers=1,
        )
        table = updated.to_table()
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == ["new"]
        assert table.column("amount").to_pylist() == [amount]

    def test_backtick_join_column_is_rejected(self, temp_dir: str) -> None:
        """Lance 12 cannot resolve a field whose name contains a backtick."""
        name = "key`name"
        path = str(Path(temp_dir) / "backtick_key")
        lance.write_dataset(
            pa.table({name: pa.array([1], type=pa.int64()), "value": ["old"]}),
            path,
        )
        with pytest.raises(ValueError, match="backtick"):
            lr.merge_into(
                pa.table({name: pa.array([1], type=pa.int64()), "value": ["new"]}),
                path,
                on=name,
                num_workers=1,
            )

    @pytest.mark.parametrize("with_index", [False, True])
    def test_int64_min_join_key(self, temp_dir: str, with_index: bool) -> None:
        """Int64 minimum matches through a typed literal, with or without an index."""
        path = str(Path(temp_dir) / f"int64_min_{with_index}")
        minimum = -9223372036854775808
        lance.write_dataset(
            pa.table(
                {
                    "id": pa.array([minimum, 1], type=pa.int64()),
                    "value": ["old", "keep"],
                }
            ),
            path,
        )
        if with_index:
            lance.dataset(path).create_scalar_index("id", index_type="BTREE")
        updated = lr.merge_into(
            pa.table(
                {
                    "id": pa.array([minimum, 2], type=pa.int64()),
                    "value": ["new", "inserted"],
                }
            ),
            path,
            on="id",
            num_workers=1,
        )
        rows = {row["id"]: row["value"] for row in updated.to_table().to_pylist()}
        assert rows[minimum] == "new"
        assert rows[1] == "keep"
        assert rows[2] == "inserted"
        assert len(rows) == 3

    @pytest.mark.parametrize("with_index", [False, True])
    def test_uint64_join_keys_at_and_above_int64_max(
        self, temp_dir: str, with_index: bool
    ) -> None:
        """UInt64 keys at and above 2**63 match exactly, with or without an index.

        2**63 + 1 is not a Float64 value. A bare SQL number would round onto
        2**63 and either miss this row or update the neighboring key.
        """
        from lance_ray.merge_into import _sql_literal

        path = str(Path(temp_dir) / f"uint64_high_{with_index}")
        boundary = 2**63
        odd = 2**63 + 1
        maximum = 2**64 - 1
        key_type = pa.uint64()
        lance.write_dataset(
            pa.table(
                {
                    "id": pa.array([boundary, odd, 1], type=key_type),
                    "value": ["old-boundary", "old-odd", "keep"],
                }
            ),
            path,
        )
        if with_index:
            lance.dataset(path).create_scalar_index("id", index_type="BTREE")
        updated = lr.merge_into(
            pa.table(
                {
                    "id": pa.array([boundary, odd, maximum], type=key_type),
                    "value": ["new-boundary", "new-odd", "inserted"],
                }
            ),
            path,
            on="id",
            num_workers=1,
        )
        table = updated.to_table()
        ids = table.column("id").to_pylist()
        assert table.num_rows == 4
        assert ids.count(boundary) == 1
        assert ids.count(odd) == 1
        assert ids.count(maximum) == 1
        rows = {row["id"]: row["value"] for row in table.to_pylist()}
        assert rows[boundary] == "new-boundary"
        assert rows[odd] == "new-odd"
        assert rows[1] == "keep"
        assert rows[maximum] == "inserted"
        odd_predicate = f"id = {_sql_literal(odd, key_type)}"
        assert updated.to_table(filter=odd_predicate).column("value").to_pylist() == [
            "new-odd"
        ]

    def test_negative_scale_decimal_keys(self, temp_dir: str) -> None:
        """Negative-scale decimals sort on their coefficient and match exactly."""
        cases = (
            (pa.decimal128(3, -2), Decimal("100"), Decimal("200"), Decimal("300")),
            (
                pa.decimal256(40, -2),
                Decimal(10) ** 41,
                (Decimal(10) ** 39) * 2,
                (Decimal(10) ** 39) * 3,
            ),
        )
        for index, (decimal_type, kept, updated_key, inserted_key) in enumerate(cases):
            path = str(Path(temp_dir) / f"neg_scale_{index}")
            lance.write_dataset(
                pa.table(
                    {
                        "amount": pa.array([kept, updated_key], type=decimal_type),
                        "value": ["keep", "old"],
                    }
                ),
                path,
            )
            merged = lr.merge_into(
                pa.table(
                    {
                        "amount": pa.array(
                            [updated_key, updated_key, inserted_key], type=decimal_type
                        ),
                        "value": ["first", "second", "inserted"],
                    }
                ),
                path,
                on="amount",
                num_workers=1,
            )
            rows = {
                row["amount"]: row["value"] for row in merged.to_table().to_pylist()
            }
            assert rows[kept] == "keep"
            assert rows[updated_key] in {"first", "second"}
            assert rows[inserted_key] == "inserted"
            assert len(rows) == 3
            assert merged.schema.field("amount").type == decimal_type

    @pytest.mark.parametrize("with_index", [False, True])
    @pytest.mark.parametrize(
        "decimal_type",
        [pa.decimal128(38, -2), pa.decimal256(76, -2)],
    )
    def test_full_precision_negative_scale_decimal(
        self, temp_dir: str, with_index: bool, decimal_type: pa.DataType
    ) -> None:
        """A full-precision coefficient must not overflow before the lookup cast."""
        from decimal import Context, localcontext

        if not pa.types.is_decimal(decimal_type):
            raise AssertionError(f"expected a decimal type, got {decimal_type}")
        precision = int(decimal_type.precision)
        with localcontext(Context(prec=precision + 4)):
            kept_coeff = Decimal(1)
            updated_coeff = Decimal(10) ** (precision - 1)
            inserted_coeff = Decimal(2)
            kept = kept_coeff * 100
            updated_key = updated_coeff * 100
            inserted_key = inserted_coeff * 100
        scale0 = (
            pa.decimal256(precision, 0)
            if pa.types.is_decimal256(decimal_type)
            else pa.decimal128(precision, 0)
        )

        def as_stored(coefficients: list[Decimal]) -> Any:
            encoded = pa.array(coefficients, type=scale0)
            return pa.Array.from_buffers(decimal_type, len(encoded), encoded.buffers())

        path = str(Path(temp_dir) / f"full_neg_{precision}_{with_index}")
        lance.write_dataset(
            pa.table(
                {
                    "amount": as_stored([kept_coeff, updated_coeff]),
                    "value": ["keep", "old"],
                }
            ),
            path,
        )
        if with_index:
            lance.dataset(path).create_scalar_index("amount", index_type="BTREE")
        merged = lr.merge_into(
            pa.table(
                {
                    "amount": as_stored([updated_coeff, updated_coeff, inserted_coeff]),
                    "value": ["first", "second", "inserted"],
                }
            ),
            path,
            on="amount",
            num_workers=1,
        )
        rows = {row["amount"]: row["value"] for row in merged.to_table().to_pylist()}
        assert rows[kept] == "keep"
        assert rows[updated_key] in {"first", "second"}
        assert rows[inserted_key] == "inserted"
        assert len(rows) == 3
        assert merged.schema.field("amount").type == decimal_type

    @pytest.mark.parametrize("with_index", [False, True])
    def test_decimal256_scale_minus_76(self, temp_dir: str, with_index: bool) -> None:
        """decimal256(76, -76) matches without a 77-digit scale-0 power of ten."""
        from decimal import Context, localcontext

        decimal_type = pa.decimal256(76, -76)
        with localcontext(Context(prec=160)):
            kept = Decimal("1E76")
            updated_key = Decimal("2E76")
            inserted_key = Decimal("3E76")
        scale0 = pa.decimal256(76, 0)

        def as_stored(values: list[Decimal]) -> Any:
            coefficients = [value.scaleb(decimal_type.scale) for value in values]
            encoded = pa.array(coefficients, type=scale0)
            return pa.Array.from_buffers(decimal_type, len(encoded), encoded.buffers())

        path = str(Path(temp_dir) / f"scale_minus_76_{with_index}")
        lance.write_dataset(
            pa.table(
                {
                    "amount": as_stored([kept, updated_key]),
                    "value": ["keep", "old"],
                }
            ),
            path,
        )
        if with_index:
            lance.dataset(path).create_scalar_index("amount", index_type="BTREE")
        merged = lr.merge_into(
            pa.table(
                {
                    "amount": as_stored([updated_key, updated_key, inserted_key]),
                    "value": ["first", "second", "inserted"],
                }
            ),
            path,
            on="amount",
            num_workers=1,
        )
        rows = {row["amount"]: row["value"] for row in merged.to_table().to_pylist()}
        assert rows[kept] == "keep"
        assert rows[updated_key] in {"first", "second"}
        assert rows[inserted_key] == "inserted"
        assert len(rows) == 3

    def test_dates_outside_python_year_range(self, temp_dir: str) -> None:
        """date32/date64 values outside year 1–9999 survive sort and lookup."""
        path = str(Path(temp_dir) / "wide_dates")
        day = 2932897
        millis = 253402300800000
        lance.write_dataset(
            pa.table(
                {
                    "day": pa.array([day, -719164], type=pa.date32()),
                    "millis": pa.array([millis, 0], type=pa.date64()),
                    "value": ["future", "past"],
                }
            ),
            path,
        )
        updated = lr.merge_into(
            pa.table(
                {
                    "day": pa.array([day, 0], type=pa.date32()),
                    "millis": pa.array([millis, 86_400_000], type=pa.date64()),
                    "value": ["updated", "inserted"],
                }
            ),
            path,
            on="day",
            num_workers=1,
        )
        table = updated.to_table()
        rows = dict(
            zip(
                table.column("day").cast(pa.int32()).to_pylist(),
                table.column("value").to_pylist(),
                strict=True,
            )
        )
        assert rows[day] == "updated"
        assert rows[-719164] == "past"
        assert rows[0] == "inserted"
        assert len(rows) == 3

    def test_uint64_row_ids_above_int64_max(self, temp_dir: str) -> None:
        """Stable row ids at or above 2**63 stay UInt64, and inserts still commit."""
        from lance.fragment import RowIdSequence

        path = str(Path(temp_dir) / "high_row_ids")
        table = pa.table({"id": [1, 2], "value": ["a", "b"]})
        fragments = lance.fragment.write_fragments(
            table, path, mode="create", enable_stable_row_ids=True
        )
        high = 2**63
        fragments[0].row_id_meta = RowIdSequence(
            pa.array([high, high + 1], type=pa.uint64())
        ).to_inline_metadata()
        lance.LanceDataset.commit(
            path,
            lance.LanceOperation.Overwrite(table.schema, fragments),
            enable_stable_row_ids=True,
        )
        updated = lr.merge_into(
            pa.table({"id": [2, 3], "value": ["updated", "inserted"]}),
            path,
            on="id",
            num_workers=1,
        )
        rows = {
            row["id"]: row for row in updated.to_table(with_row_id=True).to_pylist()
        }
        assert rows[1]["_rowid"] == high
        assert rows[1]["value"] == "a"
        assert rows[2]["_rowid"] == high + 1
        assert rows[2]["value"] == "updated"
        assert rows[3]["value"] == "inserted"
        assert rows[3]["_rowid"] not in {high, high + 1}

    def test_arrow_view_source_is_cast_before_ray(self, temp_dir: str) -> None:
        """string_view and binary_view tables are converted before Ray serializes them."""
        path = str(Path(temp_dir) / "views")
        lance.write_dataset(
            pa.table(
                {
                    "label": ["keep", "old"],
                    "payload": [b"aa", b"bb"],
                    "value": ["a", "b"],
                }
            ),
            path,
        )
        source = pa.table(
            {
                "label": pa.array(["old", "old", "new"], type=pa.string_view()),
                "payload": pa.array([b"cc", b"dd", b"ee"], type=pa.binary_view()),
                "value": ["first", "second", "inserted"],
            }
        )
        updated = lr.merge_into(source, path, on="label", num_workers=1)
        rows = {row["label"]: row for row in updated.to_table().to_pylist()}
        assert rows["keep"]["value"] == "a"
        assert rows["old"]["value"] in {"first", "second"}
        assert rows["new"]["value"] == "inserted"
        assert updated.schema.field("label").type == pa.string()
        assert updated.schema.field("payload").type == pa.binary()

    def test_merge_into_bool_keys(self, temp_dir: str) -> None:
        """Boolean keys update the matched row and insert the missing one."""
        bool_path = Path(temp_dir) / "bool_key"
        lance.write_dataset(
            pa.table({"flag": [True], "value": ["yes"]}), str(bool_path)
        )
        updated = lr.merge_into(
            pa.table({"flag": [True, False], "value": ["still", "no"]}),
            str(bool_path),
            on="flag",
            num_workers=1,
        )
        by_flag = {row["flag"]: row["value"] for row in updated.to_table().to_pylist()}
        assert by_flag == {True: "still", False: "no"}

    def test_second_merge_sees_existing_deletion_vector(self, temp_dir: str) -> None:
        """A later merge updates rows already masked by a deletion vector."""
        path = Path(temp_dir) / "second_merge"
        create_dataset_with_fragments(path, make_fragments(2, 5))
        mid = lr.merge_into(
            pa.table({"id": [1, 20], "value": ["v1", "ins"]}),
            str(path),
            on="id",
            num_workers=1,
        )
        assert any(
            fragment.deletion_file is not None for fragment in mid.get_fragments()
        )
        final = lr.merge_into(
            pa.table({"id": [1, 20, 21], "value": ["v2", "ins2", "newer"]}),
            str(path),
            on="id",
            num_workers=1,
        )
        values = id_to_value(final)
        assert values[1] == "v2"
        assert values[20] == "ins2"
        assert values[21] == "newer"
        assert values[0] == "orig_0"
        assert final.count_rows() == 12

    def test_merge_keeps_dataset_storage_version(self, temp_dir: str) -> None:
        """New fragments stay on the target table's data storage version."""
        path = Path(temp_dir) / "storage_version"
        lance.write_dataset(
            pa.table({"id": [1, 2], "value": ["a", "b"]}),
            str(path),
            data_storage_version="2.1",
        )
        updated = lr.merge_into(
            pa.table({"id": [2, 3], "value": ["updated", "new"]}),
            str(path),
            on="id",
            num_workers=1,
        )
        assert updated.data_storage_version == "2.1"
        versions = {
            (data_file.file_major_version, data_file.file_minor_version)
            for fragment in updated.get_fragments()
            for data_file in fragment.data_files()
        }
        assert versions == {(2, 1)}

    def test_merge_into_with_stable_row_ids(self, temp_dir: str) -> None:
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

    def test_stable_row_rewrite_does_not_reuse_nested_index(
        self, temp_dir: str
    ) -> None:
        """A nested-field index must not cover rows rewritten with stable ids."""
        path = Path(temp_dir) / "nested_index_stable"
        meta_type = pa.struct([("text", pa.string())])
        table = pa.table(
            {
                "id": [1, 2, 3, 4],
                "meta": pa.array(
                    [
                        {"text": "old"},
                        {"text": "keep"},
                        {"text": "other"},
                        {"text": "rest"},
                    ],
                    type=meta_type,
                ),
            }
        )
        dataset = lance.write_dataset(
            table, str(path), max_rows_per_file=2, enable_stable_row_ids=True
        )
        dataset.create_scalar_index("meta.text", index_type="BTREE", name="meta_text")
        before_ids = {fragment.fragment_id for fragment in dataset.get_fragments()}

        source = pa.table(
            {
                "id": [1],
                "meta": pa.array([{"text": "updated"}], type=meta_type),
            }
        )
        updated = lr.merge_into(source, str(path), on="id", num_workers=1)
        found = updated.to_table(filter="meta.text = 'updated'")
        assert found.column("id").to_pylist() == [1]
        assert updated.to_table(filter="meta.text = 'keep'").column(
            "id"
        ).to_pylist() == [2]

        new_ids = {
            fragment.fragment_id for fragment in updated.get_fragments()
        } - before_ids
        assert new_ids
        index = next(
            description
            for description in updated.describe_indices()
            if description.name == "meta_text"
        )
        covered = {
            int(fragment_id)
            for segment in index.segments
            for fragment_id in segment.fragment_ids
        }
        assert new_ids.isdisjoint(covered)

    def test_target_one_to_many_updates_all_matches(self, temp_dir: str) -> None:
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

    def test_target_one_to_many_across_fragments(self, temp_dir: str) -> None:
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

    def test_target_one_to_many_preserves_stable_row_ids(self, temp_dir: str) -> None:
        """Join-all keeps every matched logical _rowid."""
        path = Path(temp_dir) / "target_one_to_many_stable"
        dataset = create_dataset_with_fragments(
            path,
            [pd.DataFrame({"id": [1, 1, 2], "value": ["a", "b", "c"]})],
            enable_stable_row_ids=True,
        )
        before_table = dataset.to_table(columns=["id"], with_row_id=True)
        before_ids = [
            cast(int, rowid)
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
        updated_ids = sorted(
            cast(int, rowid) for key, value, rowid in after_pairs if key == 1
        )
        assert updated_ids == sorted(before_ids)
        assert all(value == "z" for key, value, _ in after_pairs if key == 1)

    def test_string_join_keys(self, temp_dir: str) -> None:
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

    def test_insert_when_helper_name_is_user_column(self, temp_dir: str) -> None:
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

    def test_update_when_helper_names_are_user_columns(self, temp_dir: str) -> None:
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

    def test_helpers_skip_default_and_suffix_2_user_columns(
        self, temp_dir: str
    ) -> None:
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

    def test_date_join_keys(self, temp_dir: str) -> None:
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

    def test_decimal_and_binary_join_keys(self, temp_dir: str) -> None:
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

    def test_merge_into_with_directory_namespace(self, temp_dir: str) -> None:
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
        assert isinstance(location, str)
        values = id_to_value(lance.dataset(location))
        assert values[2] == "new_2"
        assert values[100] == "new_100"


class TestMergeIntoDedupe:
    @pytest.mark.parametrize("with_index", [False, True])
    def test_dedupe_nanosecond_time_keys_across_chunks(
        self, tmp_path: Path, with_index: bool
    ) -> None:
        """Time keys retain all ticks through Ray sorting and boundary dedupe."""
        path = str(tmp_path / "dedupe_time_keys")
        key_type = pa.time64("ns")
        dataset = lance.write_dataset(
            pa.table(
                {
                    "key": pa.array([0, 1, 2, 86399999999999], type=key_type),
                    "value": ["keep_0", "old_1", "keep_2", "keep_end_of_day"],
                }
            ),
            path,
            max_rows_per_file=2,
        )
        if with_index:
            dataset.create_scalar_index("key", index_type="BTREE")
        version_before = dataset.version
        # Duplicate update/insert keys start in separate source partitions.
        ticks = [1, 3] + list(range(10, 26)) + [3, 1]
        values = (
            ["first_1", "first_3"]
            + [f"v_{tick}" for tick in range(10, 26)]
            + ["dup_3", "dup_1"]
        )
        source = pa.table({"key": pa.array(ticks, type=key_type), "value": values})
        updated = lr.merge_into(source, path, on="key", num_workers=2, num_partitions=4)
        table = updated.to_table()
        # Python datetime.time cannot represent these nanoseconds: inspect ticks.
        result_ticks = table.column("key").cast(pa.int64()).to_pylist()
        result_values = dict(
            zip(result_ticks, table.column("value").to_pylist(), strict=True)
        )
        assert updated.version == version_before + 1
        assert table.schema == source.schema
        assert table.num_rows == 21
        assert len(result_ticks) == len(set(result_ticks))
        assert result_values[0] == "keep_0"
        assert result_values[1] in {"first_1", "dup_1"}
        assert result_values[2] == "keep_2"
        assert result_values[3] in {"first_3", "dup_3"}
        assert result_values[86399999999999] == "keep_end_of_day"
        assert all(result_values[tick] == f"v_{tick}" for tick in range(10, 26))

    def test_dedupe_within_chunk(self, temp_dir: str) -> None:
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

    def test_dedupe_across_chunks(self, temp_dir: str) -> None:
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

    def test_dedupe_with_ray_dataset_source(self, temp_dir: str) -> None:
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

    def test_dedupe_noop_on_unique_source(self, temp_dir: str) -> None:
        """A source without duplicates is unchanged by the dedupe pass."""
        path = Path(temp_dir) / "dedupe_unique"
        create_dataset_with_fragments(path, make_fragments(2, 10))

        source = pa.table({"id": [5, 15, 100], "value": ["new_5", "new_15", "new_100"]})
        updated = lr.merge_into(source, str(path), on="id", num_workers=2)

        values = id_to_value(updated)
        assert values[5] == "new_5"
        assert values[15] == "new_15"
        assert values[100] == "new_100"

    def test_dedupe_string_keys(self, temp_dir: str) -> None:
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

    def test_dedupe_collapses_keys_equal_only_after_cast(self, temp_dir: str) -> None:
        """String keys that cast to the same integer are one join key."""
        path = Path(temp_dir) / "dedupe_cast_keys"
        lance.write_dataset(
            pa.table({"id": pa.array([100], type=pa.int64()), "value": ["keep"]}),
            str(path),
        )
        source = pa.table(
            {
                "id": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "1"],
                "value": [
                    "from_01",
                    "v2",
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v7",
                    "v8",
                    "v9",
                    "from_1",
                ],
            }
        )
        updated = lr.merge_into(
            source, str(path), on="id", num_workers=2, num_partitions=4
        )
        values = id_to_value(updated)
        assert updated.count_rows() == 10
        assert values[1] in {"from_01", "from_1"}
        assert values[2] == "v2"
        assert values[9] == "v9"
        assert values[100] == "keep"
        assert updated.to_table().column("id").to_pylist().count(1) == 1


class TestMergeIntoMergeOnRead:
    def test_update_writes_deletion_vector_not_rewrite(self, temp_dir: str) -> None:
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

    def test_full_fragment_update_removes_fragment(self, temp_dir: str) -> None:
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
    def test_operation_visible_after_insert_only(self, temp_dir: str) -> None:
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

    def test_lost_commit_ack_returns_committed_table(
        self, temp_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A raised commit after a successful PUT must not fail merge_into."""
        path = Path(temp_dir) / "ack_lost"
        create_dataset_with_fragments(path, make_fragments(1, 10))
        real_commit = lance.LanceDataset.commit

        def commit_then_lose_ack(*args: Any, **kwargs: Any) -> NoReturn:
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

    def test_failed_commit_is_not_treated_as_success(
        self, temp_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A commit that never landed must still raise."""
        path = Path(temp_dir) / "ack_not_visible"
        dataset = create_dataset_with_fragments(path, make_fragments(1, 10))
        version_before = dataset.version

        def commit_fails(*args: Any, **kwargs: Any) -> NoReturn:
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
    @pytest.mark.parametrize("backpressure", [1, 2, -1, None])
    def test_rejects_generator_backpressure_before_target_resolution(
        self, backpressure: int | None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from importlib import import_module

        module = import_module("lance_ray.merge_into")

        def unexpected_resolution(*args: Any, **kwargs: Any) -> NoReturn:
            pytest.fail("Target resolution must not run for unsupported Ray options")

        monkeypatch.setattr(module, "resolve_namespace_table", unexpected_resolution)
        remote_args = {"_generator_backpressure_num_objects": backpressure}
        with pytest.raises(ValueError, match="_generator_backpressure_num_objects"):
            lr.merge_into(
                pa.table({"id": [1]}),
                "/tmp/x.lance",
                on="id",
                ray_remote_args=remote_args,
            )
        assert remote_args == {"_generator_backpressure_num_objects": backpressure}

    def test_requires_uri_or_namespace(self) -> None:
        with pytest.raises(ValueError, match="Must provide either"):
            lr.merge_into(pa.table({"id": [1]}), on="id")

    def test_rejects_uri_and_namespace(self) -> None:
        with pytest.raises(ValueError, match="Cannot provide both"):
            lr.merge_into(
                pa.table({"id": [1]}),
                "/tmp/x.lance",
                on="id",
                namespace_impl="dir",
                table_id=["t"],
            )

    def test_rejects_empty_on(self) -> None:
        with pytest.raises(ValueError, match="join key"):
            lr.merge_into(pa.table({"id": [1]}), "/tmp/x.lance", on="")

    def test_rejects_unknown_key_column(self, temp_dir: str) -> None:
        path = Path(temp_dir) / "unknown_key"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        source = pa.table({"id": [1], "value": ["x"]})
        with pytest.raises(ValueError, match="not found in target schema"):
            lr.merge_into(source, str(path), on="missing_column")

    def test_rejects_nested_join_key_on_driver(self, temp_dir: str) -> None:
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

    def test_rejects_missing_source_columns(self, temp_dir: str) -> None:
        path = Path(temp_dir) / "missing_columns"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        source = pa.table({"id": [1]})  # no "value" column
        with pytest.raises(Exception, match="missing target-table columns"):
            lr.merge_into(source, str(path), on="id")

    def test_rejects_null_source_keys(self, temp_dir: str) -> None:
        path = Path(temp_dir) / "null_keys"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        source = pa.table({"id": [1, None], "value": ["a", "b"]})
        with pytest.raises(Exception, match="null values in join key"):
            lr.merge_into(source, str(path), on="id", num_workers=1)

    def test_rejects_bad_source_type(self, temp_dir: str) -> None:
        path = Path(temp_dir) / "bad_source"
        create_dataset_with_fragments(path, make_fragments(1, 5))
        with pytest.raises(TypeError, match="ray.data.Dataset or a pyarrow.Table"):
            # Deliberately pass an unsupported source to exercise runtime validation.
            lr.merge_into([{"id": 1}], str(path), on="id")  # type: ignore[arg-type]

    def test_rejects_bad_worker_counts(self) -> None:
        with pytest.raises(ValueError, match="num_workers"):
            lr.merge_into(pa.table({"id": [1]}), "/tmp/x.lance", on="id", num_workers=0)
        with pytest.raises(ValueError, match="num_partitions"):
            lr.merge_into(
                pa.table({"id": [1]}), "/tmp/x.lance", on="id", num_partitions=0
            )
