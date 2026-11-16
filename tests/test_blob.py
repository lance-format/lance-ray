"""Blob round-trip tests for lance-ray.

This test ensures that pa.large_binary fields with metadata
{"lance-encoding:blob": "true"} are written and read back with
byte-for-byte fidelity using write_lance and read_lance.
The tests cover PyLance's default file format and explicit format selection
for legacy Blob and native Blob v2 fields.

Tests cover:
- Single blob column round-trip
- Multiple blob columns round-trip
- JPG blob integration test
- Projection and filtering on blob columns
"""

import hashlib
import io
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

# Make local Lance python package available for import
sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "lance" / "python" / "python")
)

import pyarrow as pa
import pytest
import ray

import pandas as pd
from _utils import to_numpy_backed

# Skip cleanly if Lance isn't available in the environment
pytest.importorskip("lance")

import lance
import lance_ray.io as lr


@pytest.fixture
def temp_dir() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as d:
        yield d


def _generate_jpg_bytes() -> bytes:
    """Generate JPEG-like bytes for testing.

    Prefer Pillow to create a real JPEG; fall back to random bytes if
    Pillow or its dependencies are not available.
    """
    try:
        from PIL import Image

        img = Image.new("RGB", (64, 64), color=(255, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        return buf.getvalue()
    except Exception:
        # Fallback: generate pseudo-image bytes without requiring Pillow.
        try:
            import numpy as np

            # 64x64 RGB random bytes
            data = np.random.randint(0, 256, size=(64, 64, 3), dtype="uint8")
            return data.tobytes()
        except Exception:
            # Last resort: os.urandom.
            import os

            return os.urandom(64 * 64 * 3)


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("create_version", [None, "2.1"], ids=["default", "v2.1"])
def test_legacy_blob_uses_default_version(
    tmp_path: Path, stream: bool, create_version: str | None
) -> None:
    path = str(tmp_path / "legacy_blob_default_version.lance")
    schema = pa.schema(
        [pa.field("blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"})]
    )
    data = ray.data.from_arrow(pa.table({"blob": [b"foo", None]}, schema=schema))

    lr.write_lance(data, path, stream=stream, data_storage_version=create_version)
    lr.write_lance(data, path, mode="append", stream=stream)

    dataset = lance.dataset(path)
    expected_version = create_version or "2.2"
    assert dataset.data_storage_version == expected_version
    file_versions = {
        f"{file.file_major_version}.{file.file_minor_version}"
        for fragment in dataset.get_fragments()
        for file in fragment.data_files()
    }
    assert file_versions == {expected_version}
    blobs = dataset.take_blobs("blob", indices=[0, 1, 2, 3])
    assert [blob.read() if blob is not None else None for blob in blobs] == [
        b"foo",
        None,
        b"foo",
        None,
    ]


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    "legacy_input", [False, True], ids=["native-default", "legacy-2.2"]
)
def test_write_blob_v2(tmp_path: Path, stream: bool, legacy_input: bool) -> None:
    path = tmp_path / "blob_v2.lance"
    values = [b"foo", None]
    if legacy_input:
        schema = pa.schema(
            [
                pa.field(
                    "blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                )
            ]
        )
        table = pa.table({"blob": values}, schema=schema)
        lr.write_lance(
            ray.data.from_arrow(table),
            str(path),
            stream=stream,
            data_storage_version="2.2",
        )
    else:
        table = pa.table({"blob": lance.blob_array(values)})
        lr.write_lance(ray.data.from_arrow(table), str(path), stream=stream)

    dataset = lance.dataset(path)
    assert dataset.data_storage_version == "2.2"
    blob_type = dataset.schema.field("blob").type
    assert isinstance(blob_type, pa.ExtensionType)
    assert blob_type.extension_name == "lance.blob.v2"
    blobs = dataset.take_blobs("blob", indices=[0, 1])
    assert [blob.read() if blob is not None else None for blob in blobs] == values


def test_append_legacy_blob_to_blob_v2(tmp_path: Path) -> None:
    path = str(tmp_path / "blob_v2.lance")
    lance.write_dataset(
        pa.table(
            {"id": pa.array([1], type=pa.int32()), "blob": lance.blob_array([b"first"])}
        ),
        path,
    )
    legacy_fields: list[pa.Field[Any]] = [
        pa.field("id", pa.int64()),
        pa.field("blob", pa.large_binary(), metadata={b"lance-encoding:blob": b"true"}),
    ]
    data = pa.table({"id": [2], "blob": [b"second"]}, schema=pa.schema(legacy_fields))

    lr.write_lance(ray.data.from_arrow([pa.table({}), data]), path, mode="append")

    dataset = lance.dataset(path)
    ids = dataset.to_table(columns=["id"]).column("id")
    assert ids.type == pa.int32()
    assert ids.to_pylist() == [1, 2]
    blobs = dataset.take_blobs("blob", indices=[0, 1])
    assert [blob.read() if blob is not None else None for blob in blobs] == [
        b"first",
        b"second",
    ]


@pytest.mark.parametrize("container", ["struct", "list", "large_list"])
def test_append_nested_legacy_blob_to_blob_v2(tmp_path: Path, container: str) -> None:
    def table(values: pa.StructArray) -> pa.Table:
        if container == "list":
            return pa.table(
                {"payload": pa.ListArray.from_arrays([0, len(values)], values)}
            )
        if container == "large_list":
            return pa.table(
                {
                    "payload": pa.LargeListArray.from_arrays(
                        pa.array([0, len(values)], type=pa.int64()), values
                    )
                }
            )
        return pa.table({"payload": values})

    path = str(tmp_path / "nested_blob_v2.lance")
    native = pa.StructArray.from_arrays(
        [pa.array([1], type=pa.int32()), lance.blob_array([b"first"])],
        fields=[pa.field("id", pa.int32()), lance.blob_field("blob")],
    )
    legacy = pa.StructArray.from_arrays(
        [
            pa.array([2, 3], type=pa.int64()),
            pa.array([b"second", None], type=pa.large_binary()),
        ],
        fields=[
            pa.field("id", pa.int64()),
            pa.field(
                "blob", pa.large_binary(), metadata={b"lance-encoding:blob": b"true"}
            ),
        ],
    )

    lance.write_dataset(table(native), path, data_storage_version="2.2")
    lr.write_lance(ray.data.from_arrow(table(legacy)), path, mode="append")

    payload = (
        lance.dataset(path)
        .scanner(blob_handling="all_binary")
        .to_table()
        .column("payload")
        .combine_chunks()
    )
    if isinstance(payload, pa.ListArray | pa.LargeListArray):
        assert payload.offsets.to_pylist() == [0, 1, 3]
        payload = payload.values
    assert isinstance(payload, pa.StructArray)
    assert payload.field("id").type == pa.int32()
    assert payload.field("id").to_pylist() == [1, 2, 3]
    assert payload.field("blob").to_pylist() == [b"first", b"second", None]


def test_single_blob_roundtrip(temp_dir: str) -> None:
    """Test single blob column round-trip."""
    path = Path(temp_dir) / "single_blob_roundtrip.lance"

    # Prepare arrow table with a blob column and an id column
    blob_values = [b"foo", b"bar", b"", None, b"\x00\x01\x02"]
    ids = pa.array([0, 1, 2, 3, 4], pa.int64())

    schema_fields: list[pa.Field[Any]] = [
        pa.field(
            "blob",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        ),
        pa.field("id", pa.int64()),
    ]
    schema = pa.schema(schema_fields)
    table = pa.table(
        [pa.array(blob_values, type=pa.large_binary()), ids], schema=schema
    )

    # Write via lance-ray
    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=schema, data_storage_version="2.1")

    # Read back via lance-ray
    ds_read = lr.read_lance(str(path))
    df = ds_read.to_pandas().pipe(to_numpy_backed)

    # Sort by id to ensure consistent order
    df_sorted = df.sort_values("id").reset_index(drop=True)

    # Compare bytes, preserving None
    expected = blob_values
    actual = df_sorted["blob"].tolist()
    assert actual == expected


def test_multi_blob_roundtrip(temp_dir: str) -> None:
    """Test multiple blob columns round-trip."""
    path = Path(temp_dir) / "multi_blob_roundtrip.lance"

    # Construct rows covering None, empty bytes, and non-empty bytes across both blob columns
    blob1_values = [b"foo", None, b"", b"alpha", b"\x00\x01"]
    blob2_values = [b"bar", b"baz", b"qux", None, b"\x02\x03\x04"]
    ids = pa.array([0, 1, 2, 3, 4], pa.int64())

    schema_fields: list[pa.Field[Any]] = [
        pa.field("blob1", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        pa.field("blob2", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        pa.field("id", pa.int64()),
    ]
    schema = pa.schema(schema_fields)
    table = pa.table(
        [
            pa.array(blob1_values, type=pa.large_binary()),
            pa.array(blob2_values, type=pa.large_binary()),
            ids,
        ],
        schema=schema,
    )

    # Write via lance-ray
    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=table.schema, data_storage_version="2.1")

    # Read back via lance-ray
    ds_read = lr.read_lance(str(path))
    df = ds_read.to_pandas().pipe(to_numpy_backed)

    # Sort by id to ensure consistent order
    df_sorted = df.sort_values("id").reset_index(drop=True)

    # Compare bytes, preserving None
    assert df_sorted["blob1"].tolist() == blob1_values
    assert df_sorted["blob2"].tolist() == blob2_values


@pytest.mark.lance_integration
def test_jpg_blob_integration(
    temp_dir: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """JPG blob integration test with real image data."""
    # Prepare asset bytes
    jpg_bytes = _generate_jpg_bytes()

    # Build Arrow schema with blob + string + int
    schema_fields: list[pa.Field[Any]] = [
        pa.field("blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        pa.field("name", pa.string()),
        pa.field("id", pa.int64()),
    ]
    schema = pa.schema(schema_fields)

    # Construct a small table with repeated JPG bytes and varied name/id
    names = pa.array(["img_a", "img_b", "img_c"], type=pa.string())
    ids = pa.array([1, 2, 3], type=pa.int64())
    blobs = pa.array([jpg_bytes, jpg_bytes, jpg_bytes], type=pa.large_binary())
    table = pa.table([blobs, names, ids], schema=schema)

    # Write via lance-ray
    path = Path(temp_dir) / "jpg_blob_integration.lance"
    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=schema, data_storage_version="2.1")

    # Read back via lance-ray
    ds_read = lr.read_lance(str(path))
    df = ds_read.to_pandas()
    df_sorted = df.sort_values("id").reset_index(drop=True)

    # Validate blob bytes and other columns
    actual_blobs = df_sorted["blob"].tolist()
    actual_names = df_sorted["name"].tolist()
    actual_ids = df_sorted["id"].tolist()

    assert actual_blobs == [jpg_bytes, jpg_bytes, jpg_bytes]
    assert actual_names == ["img_a", "img_b", "img_c"]
    assert actual_ids == [1, 2, 3]

    # Print diagnostics: lengths and SHA-256 digests
    dig = hashlib.sha256(jpg_bytes).hexdigest()[:16]
    print(f"jpg length={len(jpg_bytes)}, sha256[:16]={dig}, rows={len(df_sorted)}")
    for i, b in enumerate(actual_blobs):
        print(
            f"row {i}: blob_len={0 if b is None else len(b)}, name={actual_names[i]}, id={actual_ids[i]}"
        )

    # Ensure prints happened
    captured = capsys.readouterr()
    assert "jpg length=" in captured.out


def test_blob_projection_and_filter(temp_dir: str) -> None:
    """Test projection and filtering on blob columns."""
    path = Path(temp_dir) / "blob_projection_filter.lance"

    blob_values = [b"a", b"b", b"c", b"d"]
    ids = pa.array([10, 11, 12, 13], pa.int64())

    schema_fields: list[pa.Field[Any]] = [
        pa.field(
            "blob",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        ),
        pa.field("id", pa.int64()),
    ]
    schema = pa.schema(schema_fields)
    table = pa.table(
        [pa.array(blob_values, type=pa.large_binary()), ids], schema=schema
    )

    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=schema, data_storage_version="2.1")

    # Read only blob column with a filter
    ds_read = lr.read_lance(str(path), columns=["blob"], filter="id >= 12")
    df = ds_read.to_pandas()
    assert df.columns.tolist() == ["blob"]

    # Expected values for id >= 12 -> ["c", "d"]
    assert df["blob"].tolist() == [b"c", b"d"]


def test_multi_blob_projection_and_filter(temp_dir: str) -> None:
    """Test projection and filtering on multiple blob columns."""
    path = Path(temp_dir) / "multi_blob_projection_filter.lance"

    # Construct rows covering None, empty bytes, and non-empty bytes across both blob columns
    blob1_values = [b"foo", None, b"", b"alpha", b"\x00\x01"]
    blob2_values = [b"bar", b"baz", b"qux", None, b"\x02\x03\x04"]
    ids = pa.array([0, 1, 2, 3, 4], pa.int64())

    schema_fields: list[pa.Field[Any]] = [
        pa.field("blob1", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        pa.field("blob2", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        pa.field("id", pa.int64()),
    ]
    schema = pa.schema(schema_fields)
    table = pa.table(
        [
            pa.array(blob1_values, type=pa.large_binary()),
            pa.array(blob2_values, type=pa.large_binary()),
            ids,
        ],
        schema=schema,
    )

    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=table.schema, data_storage_version="2.1")

    # Read only blob columns with a filter on id
    ds_read = lr.read_lance(str(path), columns=["blob1", "blob2"], filter="id >= 2")
    df = ds_read.to_pandas().pipe(to_numpy_backed)
    assert df.columns.tolist() == ["blob1", "blob2"]

    # Expected values for id >= 2 -> index 2,3,4
    expected_blob1 = [blob1_values[i] for i in [2, 3, 4]]
    expected_blob2 = [blob2_values[i] for i in [2, 3, 4]]
    assert df["blob1"].tolist() == expected_blob1
    assert df["blob2"].tolist() == expected_blob2


def test_stream_copy_basic_local(temp_dir: str) -> None:
    """Basic streaming copy on local filesystem (batch_size=1) with legacy blob data."""
    import lance

    src_path = Path(temp_dir) / "src_small_legacy_blob.lance"
    dst_path = Path(temp_dir) / "dst_small_legacy_blob_copy.lance"

    # Build Arrow schema with a legacy blob column + some regular columns
    schema_fields: list[pa.Field[Any]] = [
        pa.field(
            "blob",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        ),
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("val", pa.float64()),
    ]
    schema = pa.schema(schema_fields)

    blob_values = [b"x", None, b"", b"y"]
    ids = pa.array([1, 2, 3, 4], pa.int64())
    names = pa.array(["a", "b", "c", "d"], pa.string())
    vals = pa.array([10.0, 20.0, 30.0, 40.0], pa.float64())
    table = pa.table(
        [
            pa.array(blob_values, type=pa.large_binary()),
            ids,
            names,
            vals,
        ],
        schema=schema,
    )

    # Write source with legacy data storage version
    ds_src_arrow = ray.data.from_arrow(table)
    lr.write_lance(
        ds_src_arrow, str(src_path), schema=schema, data_storage_version="2.1"
    )

    ds_src = ray.data.from_arrow(table)

    lr.write_lance(
        ds_src, str(dst_path), stream=True, batch_size=1, data_storage_version="2.1"
    )

    src = lance.dataset(str(src_path))
    dst = lance.dataset(str(dst_path))
    assert src.count_rows() == dst.count_rows()
    assert src.schema == dst.schema

    src_df = (
        ray.data.from_arrow(table).to_pandas().sort_values("id").reset_index(drop=True)
    )
    dst_df = (
        lr.read_lance(str(dst_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(src_df, dst_df)


def test_stream_copy_resume_local(temp_dir: str) -> None:
    """Resume streaming copy with legacy blob data: write first 2 rows then append the rest."""

    src_path = Path(temp_dir) / "src_resume_legacy_blob.lance"
    dst_path = Path(temp_dir) / "dst_resume_legacy_blob_copy.lance"

    # Legacy blob schema
    schema_fields: list[pa.Field[Any]] = [
        pa.field("blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("val", pa.float64()),
    ]
    schema = pa.schema(schema_fields)

    blob_values = [b"m", b"n", b"o", None, b"p"]
    ids = pa.array([1, 2, 3, 4, 5], pa.int64())
    names = pa.array(["a", "b", "c", "d", "e"], pa.string())
    vals = pa.array([10.0, 20.0, 30.0, 40.0, 50.0], pa.float64())
    table = pa.table(
        [pa.array(blob_values, type=pa.large_binary()), ids, names, vals], schema=schema
    )

    # Write source as legacy format
    ds_src_arrow = ray.data.from_arrow(table)
    lr.write_lance(
        ds_src_arrow, str(src_path), schema=schema, data_storage_version="2.1"
    )

    # Pre-create destination with first 2 rows in the legacy blob format.
    table_first2 = table.slice(0, 2)
    lr.write_lance(
        ray.data.from_arrow(table_first2),
        str(dst_path),
        schema=schema,
        data_storage_version="2.1",
    )

    ds_src = ray.data.from_arrow(table)

    lr.write_lance(
        ds_src,
        str(dst_path),
        stream=True,
        batch_size=2,
        resume_rows=2,
        mode="append",
        data_storage_version="2.1",
    )

    src_df = (
        ray.data.from_arrow(table).to_pandas().sort_values("id").reset_index(drop=True)
    )
    dst_df = (
        lr.read_lance(str(dst_path))
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(src_df, dst_df)
