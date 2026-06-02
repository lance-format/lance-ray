"""Blob v2 integration tests for lance-ray.

These tests verify that:

- lance-ray can read datasets that use the blob v2 extension type
  (``lance.blob.v2``) and exposes them to Ray as LargeBinary bytes.
- Projection and filtering on blob v2 columns work as expected.
- Underlying ``LanceDataset.take_blobs`` supports both ``ids`` and
  ``indices`` selectors for blob v2 columns.
- ``target_bases`` controls which registered base path receives appended
  fragment data.
- ``allow_external_blob_outside_bases`` permits external blob URIs that
  do not fall under any registered base path.
"""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

import pyarrow as pa
import pytest
import ray
from ray.exceptions import RayTaskError

from _utils import (
    fragment_write_options_skip_reason,
    missing_fragment_write_options,
)

# Prefer the local Lance Python package when running in a monorepo layout
sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "lance" / "python" / "python")
)

lance = pytest.importorskip("lance")

try:
    from lance import Blob, DatasetBasePath, blob_array, blob_field
except Exception:  # pragma: no cover - guarded by importorskip
    pytest.skip(
        "blob v2 API is not available in this Lance build", allow_module_level=True
    )

import lance_ray.io as lr  # noqa: E402
from lance_ray.datasink import LanceFragmentCommitter  # noqa: E402
from lance_ray.fragment import LanceFragmentWriter  # noqa: E402


def _build_blob_v2_table(
    tmp_path: Path,
) -> tuple[pa.Table, bytes, bytes, Path, Path]:
    """Construct a small blob v2 table for testing.

    Returns the table, inline payload bytes, external payload bytes, the
    external base path, and the path to the external blob file.
    """

    inline_payload = b"inline-bytes"
    external_payload = b"external-bytes"
    external_base = tmp_path / "external_base"
    external_base.mkdir()
    external_path = external_base / "external_blob.bin"
    external_path.write_bytes(external_payload)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            blob_field("blob"),
        ]
    )

    sig = inspect.signature(Blob.from_uri)
    blob_kwargs: dict[str, object] = {}
    if "position" in sig.parameters and "size" in sig.parameters:
        blob_kwargs = {"position": 0, "size": len(external_payload)}

    table = pa.table(
        {
            "id": [1, 2, 3],
            "blob": blob_array(
                [
                    inline_payload,
                    Blob.from_uri(
                        external_path.as_uri(),
                        **blob_kwargs,
                    ),
                    None,
                ]
            ),
        },
        schema=schema,
    )
    return table, inline_payload, external_payload, external_base, external_path


def _initial_bases(external_base: Path) -> list[DatasetBasePath]:
    return [
        DatasetBasePath(
            external_base.as_uri(),
            name="external_blob_base",
            is_dataset_root=False,
            id=1,
        )
    ]


def _build_external_only_blob_v2_table(
    tmp_path: Path,
    filename: str,
    payload: bytes = b"hello",
) -> tuple[pa.Table, bytes, Path]:
    blob_path = tmp_path / filename
    blob_path.write_bytes(payload)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            blob_field("blob"),
        ]
    )

    sig = inspect.signature(Blob.from_uri)
    blob_kwargs: dict[str, object] = {}
    if "position" in sig.parameters and "size" in sig.parameters:
        blob_kwargs = {"position": 0, "size": len(payload)}

    table = pa.table(
        {
            "id": [1],
            "blob": blob_array([Blob.from_uri(blob_path.as_uri(), **blob_kwargs)]),
        },
        schema=schema,
    )
    return table, payload, blob_path


def test_blob_v2_roundtrip_with_projection_and_filter(tmp_path: Path) -> None:
    """Round-trip blob v2 columns through write_lance/read_lance.

    The Ray-facing schema should expose blob v2 columns as LargeBinary bytes,
    and projection/filter should not break blob reconstruction.
    """

    table, inline_payload, external_payload, external_base, _ = _build_blob_v2_table(
        tmp_path
    )
    path = tmp_path / "blob_v2_roundtrip.lance"

    ds_ray = ray.data.from_arrow(table)
    lr.write_lance(
        ds_ray,
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        initial_bases=_initial_bases(external_base),
    )

    # Full read: expect bytes + None in id order
    try:
        df = (
            lr.read_lance(str(path))
            .to_pandas()
            .sort_values("id")
            .reset_index(drop=True)
        )
    except RayTaskError as exc:  # pragma: no cover - guarded by Lance version
        # Older Lance builds may not be able to decode experimental blob v2
        # files written with data_storage_version="2.2".
        msg = str(exc)
        if "Packed struct fixed child exceeds row bounds" in msg:
            pytest.skip(
                "Current Lance build cannot decode blob v2 packed encoding; "
                "skipping blob v2 roundtrip test.",
            )
        raise
    assert df["blob"].tolist() == [inline_payload, external_payload, None]

    # Projection + filter on the blob column
    df_proj = (
        lr.read_lance(str(path), columns=["blob"], filter="id >= 2")
        .to_pandas()
        .reset_index(drop=True)
    )
    assert df_proj.columns.tolist() == ["blob"]
    assert df_proj["blob"].tolist() == [external_payload, None]


def test_blob_v2_take_blobs_ids_and_indices(tmp_path: Path) -> None:
    """Validate blob v2 take_blobs semantics for ids and indices selectors.

    The bytes read via ``take_blobs`` should match what lance-ray exposes
    through ``read_lance`` for non-null rows.
    """

    table, inline_payload, external_payload, external_base, _ = _build_blob_v2_table(
        tmp_path
    )
    path = tmp_path / "blob_v2_take_blobs.lance"

    write_sig = inspect.signature(lance.write_dataset)
    write_kwargs: dict[str, object] = {"data_storage_version": "2.2"}
    write_kwargs["initial_bases"] = _initial_bases(external_base)
    if "allow_external_blob_outside_bases" in write_sig.parameters:
        write_kwargs["allow_external_blob_outside_bases"] = True

    ds = lance.write_dataset(
        table,
        path,
        **write_kwargs,
    )

    expected = [inline_payload, external_payload]

    # indices selector
    blobs_by_indices = ds.take_blobs("blob", indices=[0, 1])
    values_by_indices: list[bytes] = []
    for bf in blobs_by_indices:
        with bf as f:
            values_by_indices.append(f.read())
    assert values_by_indices == expected

    # ids selector (row ids are stable within a snapshot)
    row_ids = ds.to_table(columns=[], with_row_id=True).column("_rowid").to_pylist()
    blobs_by_ids = ds.take_blobs("blob", ids=row_ids[:2])
    values_by_ids: list[bytes] = []
    for bf in blobs_by_ids:
        with bf as f:
            values_by_ids.append(f.read())
    assert values_by_ids == expected


@pytest.mark.skipif(
    bool(
        missing_fragment_write_options(
            "external_blob_mode",
            "allow_external_blob_outside_bases",
        )
    ),
    reason=fragment_write_options_skip_reason(
        "external_blob_mode",
        "allow_external_blob_outside_bases",
    ),
)
def test_blob_v2_reference_outside_bases_write_lance(tmp_path: Path) -> None:
    """write_lance can opt into absolute external blob references."""
    table, payload, _ = _build_external_only_blob_v2_table(
        tmp_path,
        "external_reference.bin",
    )
    path = tmp_path / "blob_v2_external_reference.lance"

    lr.write_lance(
        ray.data.from_arrow(table),
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        external_blob_mode="reference",
        allow_external_blob_outside_bases=True,
    )

    df = lr.read_lance(str(path)).to_pandas().sort_values("id").reset_index(drop=True)
    assert df["blob"].tolist() == [payload]


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.skipif(
    bool(
        missing_fragment_write_options(
            "external_blob_mode",
            "allow_external_blob_outside_bases",
        )
    ),
    reason=fragment_write_options_skip_reason(
        "external_blob_mode",
        "allow_external_blob_outside_bases",
    ),
)
def test_blob_v2_external_blob_ingest_write_lance(
    tmp_path: Path,
    stream: bool,
) -> None:
    """write_lance can ingest external blobs into Lance-managed storage."""
    table, payload, blob_path = _build_external_only_blob_v2_table(
        tmp_path,
        f"external_ingest_{stream}.bin",
    )
    path = tmp_path / f"blob_v2_external_ingest_{stream}.lance"

    stream_kwargs = {"stream": stream}
    if stream:
        stream_kwargs["batch_size"] = 1

    lr.write_lance(
        ray.data.from_arrow(table),
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        external_blob_mode="ingest",
        **stream_kwargs,
    )

    blob_path.unlink()

    df = lr.read_lance(str(path)).to_pandas().sort_values("id").reset_index(drop=True)
    assert df["blob"].tolist() == [payload]


@pytest.mark.skipif(
    bool(
        missing_fragment_write_options(
            "external_blob_mode",
            "allow_external_blob_outside_bases",
        )
    ),
    reason=fragment_write_options_skip_reason(
        "external_blob_mode",
        "allow_external_blob_outside_bases",
    ),
)
def test_blob_v2_reference_outside_bases_manual_fragment_writer(
    tmp_path: Path,
) -> None:
    """Manual fragment writes can opt into base-outside external references."""
    table, payload, _ = _build_external_only_blob_v2_table(
        tmp_path,
        "manual_external_reference.bin",
    )
    path = tmp_path / "blob_v2_manual_external_reference.lance"

    (
        ray.data.from_arrow(table)
        .map_batches(
            LanceFragmentWriter(
                str(path),
                schema=table.schema,
                data_storage_version="2.2",
                external_blob_mode="reference",
                allow_external_blob_outside_bases=True,
            ),
            batch_size=1,
            batch_format="pyarrow",
        )
        .write_datasink(LanceFragmentCommitter(str(path)))
    )

    df = lr.read_lance(str(path)).to_pandas().sort_values("id").reset_index(drop=True)
    assert df["blob"].tolist() == [payload]


@pytest.mark.skipif(
    bool(missing_fragment_write_options("base_store_params")),
    reason=fragment_write_options_skip_reason("base_store_params"),
)
def test_blob_v2_reference_multi_base_all_lance_ray_paths(tmp_path: Path) -> None:
    """Read/write BlobV2 references through all Lance-Ray base-store paths."""
    table, inline_payload, external_payload, external_base, _ = _build_blob_v2_table(
        tmp_path
    )
    base_store_params = {external_base.as_uri(): {}}
    initial_bases = _initial_bases(external_base)

    # Non-streaming write_lance + full/projection/filter/fragment reads.
    path = tmp_path / "blob_v2_multi_base_non_stream.lance"
    lr.write_lance(
        ray.data.from_arrow(table),
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        base_store_params=base_store_params,
        initial_bases=initial_bases,
    )

    df = (
        lr.read_lance(str(path), base_store_params=base_store_params)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert df["blob"].tolist() == [inline_payload, external_payload, None]

    df_proj = (
        lr.read_lance(
            str(path),
            columns=["blob"],
            filter="id >= 2",
            fragment_ids=[0],
            base_store_params=base_store_params,
        )
        .to_pandas()
        .reset_index(drop=True)
    )
    assert df_proj["blob"].tolist() == [external_payload, None]

    # Streaming write_lance.
    stream_path = tmp_path / "blob_v2_multi_base_stream.lance"
    lr.write_lance(
        ray.data.from_arrow(table),
        str(stream_path),
        schema=table.schema,
        data_storage_version="2.2",
        stream=True,
        batch_size=2,
        base_store_params=base_store_params,
        initial_bases=initial_bases,
    )
    stream_df = (
        lr.read_lance(str(stream_path), base_store_params=base_store_params)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert stream_df["blob"].tolist() == [inline_payload, external_payload, None]

    # Manual fragment writer / committer path.
    manual_path = tmp_path / "blob_v2_multi_base_manual.lance"
    (
        ray.data.from_arrow(table)
        .map_batches(
            LanceFragmentWriter(
                str(manual_path),
                schema=table.schema,
                data_storage_version="2.2",
                initial_bases=initial_bases,
            ),
            batch_size=2,
            batch_format="pyarrow",
        )
        .write_datasink(
            LanceFragmentCommitter(
                str(manual_path),
                base_store_params=base_store_params,
                initial_bases=initial_bases,
            )
        )
    )
    manual_df = (
        lr.read_lance(str(manual_path), base_store_params=base_store_params)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert manual_df["blob"].tolist() == [inline_payload, external_payload, None]


def _build_multi_base_blob_v2_table(
    tmp_path: Path,
) -> tuple[pa.Table, bytes, bytes, Path, Path]:
    """Construct a blob v2 table with two external base directories.

    Returns the table, inline payload, packed payload, base_a path, and
    base_b path.  Two separate base directories are created so that
    ``target_bases`` can choose which one receives appended data.
    """
    inline_payload = b"inline-bytes"
    packed_payload = b"packed-bytes" * 100

    base_a = tmp_path / "base_a"
    base_b = tmp_path / "base_b"
    base_a.mkdir()
    base_b.mkdir()

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            blob_field("blob"),
        ]
    )

    table = pa.table(
        {
            "id": [1, 2],
            "blob": blob_array(
                [
                    inline_payload,
                    packed_payload,
                ]
            ),
        },
        schema=schema,
    )
    return table, inline_payload, packed_payload, base_a, base_b


def _multi_initial_bases(base_a: Path, base_b: Path) -> list[DatasetBasePath]:
    return [
        DatasetBasePath(
            base_a.as_uri(),
            name="base_a",
            is_dataset_root=False,
            id=1,
        ),
        DatasetBasePath(
            base_b.as_uri(),
            name="base_b",
            is_dataset_root=False,
            id=2,
        ),
    ]


@pytest.mark.skipif(
    bool(missing_fragment_write_options("base_store_params", "target_bases")),
    reason=fragment_write_options_skip_reason("base_store_params", "target_bases"),
)
def test_blob_v2_create_with_initial_bases_and_target_bases(tmp_path: Path) -> None:
    """Create can route initial fragment data to a named base path."""
    table, inline_payload, packed_payload, base_a, base_b = (
        _build_multi_base_blob_v2_table(tmp_path)
    )
    initial_bases = _multi_initial_bases(base_a, base_b)
    base_store_params = {base_a.as_uri(): {}, base_b.as_uri(): {}}

    path = tmp_path / "blob_v2_create_target_bases.lance"

    # Phase 1: Create — write dataset with target_bases=["base_b"] to route initial data to base_b only
    lr.write_lance(
        ray.data.from_arrow(table),
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        base_store_params=base_store_params,
        initial_bases=initial_bases,
        target_bases=["base_b"],
    )

    # Phase 2: Verify file routing — data files should only exist in base_b
    b_lance_files = set(base_b.rglob("*.lance"))
    a_lance_files = set(base_a.rglob("*.lance"))
    root_lance_files = (
        set((path / "data").rglob("*.lance")) if (path / "data").exists() else set()
    )
    assert b_lance_files, "target_bases did not route initial data files to base_b"
    assert not a_lance_files, (
        f"Data files unexpectedly written to base_a: {a_lance_files}"
    )
    assert not root_lance_files, (
        f"Data files unexpectedly written to dataset root: {root_lance_files}"
    )

    # Phase 3: Verify data integrity — read back and confirm all blobs are correct
    df = (
        lr.read_lance(str(path), base_store_params=base_store_params)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert df["blob"].tolist() == [inline_payload, packed_payload]


@pytest.mark.skipif(
    bool(missing_fragment_write_options("base_store_params", "target_bases")),
    reason=fragment_write_options_skip_reason("base_store_params", "target_bases"),
)
def test_blob_v2_target_bases_multi_base_routing(tmp_path: Path) -> None:
    """target_bases can route data across multiple base paths.

    Creates a dataset with two registered bases, then performs two
    append writes: one with ``target_bases=["base_a", "base_b"]``
    (multi-element candidate pool) and one with
    ``target_bases=["base_a"]`` (precise routing).  This verifies
    that:

    1. Multi-element target_bases routes data to the candidate pool
       (at least one of the named bases receives new files).
    2. Single-element target_bases routes data to the exact base.
    3. Data written to different bases is readable as a single
       coherent dataset.
    """
    table, inline_payload, packed_payload, base_a, base_b = (
        _build_multi_base_blob_v2_table(tmp_path)
    )
    initial_bases = _multi_initial_bases(base_a, base_b)
    base_store_params = {base_a.as_uri(): {}, base_b.as_uri(): {}}

    path = tmp_path / "blob_v2_multi_base_routing.lance"

    # Phase 1: Create — create dataset without target_bases (default routing)
    lr.write_lance(
        ray.data.from_arrow(table),
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        base_store_params=base_store_params,
        initial_bases=initial_bases,
    )

    a_lance_after_create = set(base_a.rglob("*.lance"))
    b_lance_after_create = set(base_b.rglob("*.lance"))
    root_lance_after_create = (
        set((path / "data").rglob("*.lance")) if (path / "data").exists() else set()
    )

    # Phase 2: Append — multi-element target_bases routing (candidate pool: base_a and base_b)
    append_payload_multi = b"multi-target-bytes" * 100
    append_table_multi = pa.table(
        {
            "id": [3],
            "blob": blob_array([append_payload_multi]),
        },
        schema=table.schema,
    )

    lr.write_lance(
        ray.data.from_arrow(append_table_multi),
        str(path),
        mode="append",
        base_store_params=base_store_params,
        target_bases=["base_a", "base_b"],
    )

    a_lance_after_multi = set(base_a.rglob("*.lance"))
    b_lance_after_multi = set(base_b.rglob("*.lance"))
    root_lance_after_multi = (
        set((path / "data").rglob("*.lance")) if (path / "data").exists() else set()
    )
    new_in_a_or_b = (a_lance_after_multi - a_lance_after_create) | (
        b_lance_after_multi - b_lance_after_create
    )
    new_in_root = root_lance_after_multi - root_lance_after_create
    assert new_in_a_or_b, (
        "target_bases=['base_a', 'base_b'] did not route data files to either base"
    )
    assert not new_in_root, (
        f"Data files unexpectedly written to dataset root: {new_in_root}"
    )

    # Phase 3: Append — single-element target_bases precise routing (base_a only)
    append_payload_a = b"routed-to-base-a" * 100
    append_table_a = pa.table(
        {
            "id": [4],
            "blob": blob_array([append_payload_a]),
        },
        schema=table.schema,
    )

    lr.write_lance(
        ray.data.from_arrow(append_table_a),
        str(path),
        mode="append",
        base_store_params=base_store_params,
        target_bases=["base_a"],
    )

    a_lance_after_precise = set(base_a.rglob("*.lance"))
    new_in_a = a_lance_after_precise - a_lance_after_multi
    assert new_in_a, "target_bases=['base_a'] did not route data files to base_a"

    # Phase 4: End-to-end data integrity verification (cross-base transparent read)
    df = (
        lr.read_lance(str(path), base_store_params=base_store_params)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert df["blob"].tolist() == [
        inline_payload,
        packed_payload,
        append_payload_multi,
        append_payload_a,
    ]


@pytest.mark.skipif(
    bool(missing_fragment_write_options("base_store_params", "target_bases")),
    reason=fragment_write_options_skip_reason("base_store_params", "target_bases"),
)
def test_blob_v2_append_with_target_bases_stream(tmp_path: Path) -> None:
    """Streaming append with target_bases should route data to the named base.

    Same as test_blob_v2_target_bases_multi_base_routing but using
    ``stream=True`` so that each batch is committed as a separate
    fragment, verifying the streaming write path also respects
    target_bases.
    """
    table, inline_payload, packed_payload, base_a, base_b = (
        _build_multi_base_blob_v2_table(tmp_path)
    )
    initial_bases = _multi_initial_bases(base_a, base_b)
    base_store_params = {base_a.as_uri(): {}, base_b.as_uri(): {}}

    path = tmp_path / "blob_v2_target_bases_stream.lance"

    # Phase 1: Create — create dataset without target_bases (default routing)
    lr.write_lance(
        ray.data.from_arrow(table),
        str(path),
        schema=table.schema,
        data_storage_version="2.2",
        base_store_params=base_store_params,
        initial_bases=initial_bases,
    )

    # Phase 2: Append — streaming write with target_bases=["base_b"] and batch_size=1
    append_payload = b"stream-appended-bytes" * 100
    append_table = pa.table(
        {
            "id": [3],
            "blob": blob_array([append_payload]),
        },
        schema=table.schema,
    )

    b_lance_before = set(base_b.rglob("*.lance"))
    root_data_before = (
        set((path / "data").rglob("*.lance")) if (path / "data").exists() else set()
    )

    lr.write_lance(
        ray.data.from_arrow(append_table),
        str(path),
        mode="append",
        stream=True,
        batch_size=1,
        base_store_params=base_store_params,
        target_bases=["base_b"],
    )

    # Phase 3: Verify file routing — new data files should only appear in base_b
    b_lance_after = set(base_b.rglob("*.lance"))
    root_data_after = (
        set((path / "data").rglob("*.lance")) if (path / "data").exists() else set()
    )
    new_in_b = b_lance_after - b_lance_before
    new_in_root = root_data_after - root_data_before
    assert new_in_b, "target_bases did not route data files to base_b"
    assert not new_in_root, (
        f"Data files unexpectedly written to dataset root: {new_in_root}"
    )

    # Phase 4: End-to-end data integrity verification (cross-base transparent read)
    df = (
        lr.read_lance(str(path), base_store_params=base_store_params)
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )
    assert df["blob"].tolist() == [inline_payload, packed_payload, append_payload]
