"""Blob round-trip tests for lance-ray.

This test ensures that pa.large_binary fields with metadata
{"lance-encoding:blob": "true"} are written and read back with
byte-for-byte fidelity using write_lance and read_lance.

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
from pathlib import Path

# Make local Lance python package available for import
sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "lance" / "python" / "python")
)

import pyarrow as pa
import pytest
import ray

import pandas as pd

# Skip cleanly if Lance isn't available in the environment
pytest.importorskip("lance")

import lance_ray.io as lr


@pytest.fixture(scope="session", autouse=True)
def ray_context():
    # Initialize Ray for testing
    if ray.is_initialized():
        ray.shutdown()
    ray.init(local_mode=False, ignore_reinit_error=True)
    yield
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def temp_dir():
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


def test_single_blob_roundtrip(temp_dir):
    """Test single blob column round-trip."""
    path = Path(temp_dir) / "single_blob_roundtrip.lance"

    # Prepare arrow table with a blob column and an id column
    blob_values = [b"foo", b"bar", b"", None, b"\x00\x01\x02"]
    ids = pa.array([0, 1, 2, 3, 4], pa.int64())

    schema = pa.schema(
        [
            pa.field(
                "blob",
                pa.large_binary(),
                metadata={"lance-encoding:blob": "true"},
            ),
            pa.field("id", pa.int64()),
        ]
    )
    table = pa.table(
        [pa.array(blob_values, type=pa.large_binary()), ids], schema=schema
    )

    # Write via lance-ray
    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=schema)

    # Read back via lance-ray
    ds_read = lr.read_lance(str(path))
    df = ds_read.to_pandas()

    # Sort by id to ensure consistent order
    df_sorted = df.sort_values("id").reset_index(drop=True)

    # Compare bytes, preserving None
    expected = blob_values
    actual = df_sorted["blob"].tolist()
    assert actual == expected


def test_multi_blob_roundtrip(temp_dir):
    """Test multiple blob columns round-trip."""
    path = Path(temp_dir) / "multi_blob_roundtrip.lance"

    # Construct rows covering None, empty bytes, and non-empty bytes across both blob columns
    blob1_values = [b"foo", None, b"", b"alpha", b"\x00\x01"]
    blob2_values = [b"bar", b"baz", b"qux", None, b"\x02\x03\x04"]
    ids = pa.array([0, 1, 2, 3, 4], pa.int64())

    schema = pa.schema(
        [
            pa.field(
                "blob1", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
            pa.field(
                "blob2", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
            pa.field("id", pa.int64()),
        ]
    )
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
    lr.write_lance(ds_ray, str(path), schema=table.schema)

    # Read back via lance-ray
    ds_read = lr.read_lance(str(path))
    df = ds_read.to_pandas()

    # Sort by id to ensure consistent order
    df_sorted = df.sort_values("id").reset_index(drop=True)

    # Compare bytes, preserving None
    assert df_sorted["blob1"].tolist() == blob1_values
    assert df_sorted["blob2"].tolist() == blob2_values


@pytest.mark.lance_integration
def test_jpg_blob_integration(temp_dir, capsys):
    """JPG blob integration test with real image data."""
    # Prepare asset bytes
    jpg_bytes = _generate_jpg_bytes()

    # Build Arrow schema with blob + string + int
    schema = pa.schema(
        [
            pa.field(
                "blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
            pa.field("name", pa.string()),
            pa.field("id", pa.int64()),
        ]
    )

    # Construct a small table with repeated JPG bytes and varied name/id
    names = pa.array(["img_a", "img_b", "img_c"], type=pa.string())
    ids = pa.array([1, 2, 3], type=pa.int64())
    blobs = pa.array([jpg_bytes, jpg_bytes, jpg_bytes], type=pa.large_binary())
    table = pa.table([blobs, names, ids], schema=schema)

    # Write via lance-ray
    path = Path(temp_dir) / "jpg_blob_integration.lance"
    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=schema)

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


def test_blob_projection_and_filter(temp_dir):
    """Test projection and filtering on blob columns."""
    path = Path(temp_dir) / "blob_projection_filter.lance"

    blob_values = [b"a", b"b", b"c", b"d"]
    ids = pa.array([10, 11, 12, 13], pa.int64())

    schema = pa.schema(
        [
            pa.field(
                "blob",
                pa.large_binary(),
                metadata={"lance-encoding:blob": "true"},
            ),
            pa.field("id", pa.int64()),
        ]
    )
    table = pa.table(
        [pa.array(blob_values, type=pa.large_binary()), ids], schema=schema
    )

    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=schema)

    # Read only blob column with a filter
    ds_read = lr.read_lance(str(path), columns=["blob"], filter="id >= 12")
    df = ds_read.to_pandas()
    assert df.columns.tolist() == ["blob"]

    # Expected values for id >= 12 -> ["c", "d"]
    assert df["blob"].tolist() == [b"c", b"d"]


def test_multi_blob_projection_and_filter(temp_dir):
    """Test projection and filtering on multiple blob columns."""
    path = Path(temp_dir) / "multi_blob_projection_filter.lance"

    # Construct rows covering None, empty bytes, and non-empty bytes across both blob columns
    blob1_values = [b"foo", None, b"", b"alpha", b"\x00\x01"]
    blob2_values = [b"bar", b"baz", b"qux", None, b"\x02\x03\x04"]
    ids = pa.array([0, 1, 2, 3, 4], pa.int64())

    schema = pa.schema(
        [
            pa.field(
                "blob1", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
            pa.field(
                "blob2", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
            pa.field("id", pa.int64()),
        ]
    )
    table = pa.table(
        [
            pa.array(blob1_values, type=pa.large_binary()),
            pa.array(blob2_values, type=pa.large_binary()),
            ids,
        ],
        schema=schema,
    )

    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(ds_ray, str(path), schema=table.schema)

    # Read only blob columns with a filter on id
    ds_read = lr.read_lance(str(path), columns=["blob1", "blob2"], filter="id >= 2")
    df = ds_read.to_pandas()
    assert df.columns.tolist() == ["blob1", "blob2"]

    # Expected values for id >= 2 -> index 2,3,4
    expected_blob1 = [blob1_values[i] for i in [2, 3, 4]]
    expected_blob2 = [blob2_values[i] for i in [2, 3, 4]]
    assert df["blob1"].tolist() == expected_blob1
    assert df["blob2"].tolist() == expected_blob2


def test_stream_copy_basic_local(temp_dir):
    """Basic streaming copy on local filesystem (batch_size=1) with legacy blob data."""
    import lance

    src_path = Path(temp_dir) / "src_small_legacy_blob.lance"
    dst_path = Path(temp_dir) / "dst_small_legacy_blob_copy.lance"

    # Build Arrow schema with a legacy blob column + some regular columns
    schema = pa.schema(
        [
            pa.field(
                "blob",
                pa.large_binary(),
                metadata={"lance-encoding:blob": "true"},
            ),
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("val", pa.float64()),
        ]
    )

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
    lr.write_lance(ds_src_arrow, str(src_path), schema=schema)

    ds_src = ray.data.from_arrow(table)

    lr.write_lance(ds_src, str(dst_path), stream=True, batch_size=1)

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


def test_stream_copy_resume_local(temp_dir):
    """Resume streaming copy with legacy blob data: write first 2 rows then append the rest."""

    src_path = Path(temp_dir) / "src_resume_legacy_blob.lance"
    dst_path = Path(temp_dir) / "dst_resume_legacy_blob_copy.lance"

    # Legacy blob schema
    schema = pa.schema(
        [
            pa.field(
                "blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("val", pa.float64()),
        ]
    )

    blob_values = [b"m", b"n", b"o", None, b"p"]
    ids = pa.array([1, 2, 3, 4, 5], pa.int64())
    names = pa.array(["a", "b", "c", "d", "e"], pa.string())
    vals = pa.array([10.0, 20.0, 30.0, 40.0, 50.0], pa.float64())
    table = pa.table(
        [pa.array(blob_values, type=pa.large_binary()), ids, names, vals], schema=schema
    )

    # Write source as legacy format
    ds_src_arrow = ray.data.from_arrow(table)
    lr.write_lance(ds_src_arrow, str(src_path), schema=schema)

    # Pre-create destination with first 2 rows (default stable format)
    table_first2 = table.slice(0, 2)
    lr.write_lance(ray.data.from_arrow(table_first2), str(dst_path), schema=schema)

    ds_src = ray.data.from_arrow(table)

    lr.write_lance(
        ds_src,
        str(dst_path),
        stream=True,
        batch_size=2,
        resume_rows=2,
        mode="append",
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
