"""Test cases for lance_ray.fragment module."""

import warnings
from pathlib import Path

import lance
import lance_ray.io as lr
import pyarrow as pa
import pytest
import ray
from lance_ray.datasink import LanceDatasink, LanceFragmentCommitter
from lance_ray.fragment import LanceFragmentWriter


def _legacy_write_fragments(reader, uri, *, schema=None):
    return []


def _write_fragments_with_external_blob_options(
    reader,
    uri,
    *,
    external_blob_mode="reference",
    allow_external_blob_outside_bases=False,
):
    return []


def test_fragment_writer_external_blob_options_fail_fast(monkeypatch, tmp_path: Path):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )

    with pytest.raises(RuntimeError, match="external_blob_mode.*write_fragments"):
        LanceFragmentWriter(
            str(tmp_path),
            data_storage_version="stable",
            external_blob_mode="ingest",
        )

    with pytest.raises(
        RuntimeError,
        match="allow_external_blob_outside_bases.*write_fragments",
    ):
        LanceFragmentWriter(
            str(tmp_path),
            data_storage_version="stable",
            allow_external_blob_outside_bases=True,
        )


def test_datasink_external_blob_options_fail_fast(monkeypatch, tmp_path: Path):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )

    with pytest.raises(RuntimeError, match="external_blob_mode.*write_fragments"):
        LanceDatasink(str(tmp_path), external_blob_mode="ingest")


def test_write_lance_external_blob_options_fail_fast(monkeypatch, tmp_path: Path):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )

    with pytest.raises(RuntimeError, match="external_blob_mode.*write_fragments"):
        lr.write_lance(object(), str(tmp_path), external_blob_mode="ingest")


def test_base_store_params_fail_fast_when_fragment_api_unsupported(
    monkeypatch,
    tmp_path: Path,
):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )
    base_store_params = {tmp_path.as_uri(): {}}

    with pytest.raises(RuntimeError, match="base_store_params.*write_fragments"):
        LanceFragmentWriter(
            str(tmp_path),
            data_storage_version="stable",
            base_store_params=base_store_params,
        )

    with pytest.raises(RuntimeError, match="base_store_params.*write_fragments"):
        LanceDatasink(str(tmp_path), base_store_params=base_store_params)

    with pytest.raises(RuntimeError, match="base_store_params.*write_fragments"):
        lr.write_lance(object(), str(tmp_path), base_store_params=base_store_params)


def test_target_bases_fail_fast_when_fragment_api_unsupported(
    monkeypatch,
    tmp_path: Path,
):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )
    target_bases = ["archive"]

    with pytest.raises(RuntimeError, match="target_bases.*write_fragments"):
        LanceFragmentWriter(
            str(tmp_path),
            data_storage_version="stable",
            target_bases=target_bases,
        )

    with pytest.raises(RuntimeError, match="target_bases.*write_fragments"):
        LanceDatasink(str(tmp_path), target_bases=target_bases)

    with pytest.raises(RuntimeError, match="target_bases.*write_fragments"):
        lr.write_lance(object(), str(tmp_path), target_bases=target_bases)


def test_allow_external_blob_outside_bases_ignored_for_ingest(
    monkeypatch,
    tmp_path: Path,
):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _write_fragments_with_external_blob_options,
    )

    with pytest.warns(UserWarning, match="will be ignored"):
        writer = LanceFragmentWriter(
            str(tmp_path),
            data_storage_version="stable",
            external_blob_mode="ingest",
            allow_external_blob_outside_bases=True,
        )

    assert writer.allow_external_blob_outside_bases is False


def test_unsupported_ingest_with_allow_external_blob_outside_bases_does_not_warn(
    monkeypatch,
    tmp_path: Path,
):
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="external_blob_mode.*write_fragments"):
            LanceFragmentWriter(
                str(tmp_path),
                data_storage_version="stable",
                external_blob_mode="ingest",
                allow_external_blob_outside_bases=True,
            )

    assert not any("will be ignored" in str(warning.message) for warning in caught)


class TestLanceFragmentWriterCommitter:
    """Test cases for LanceFragmentWriter and LanceCommitter."""

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_committer(self, tmp_path: Path):
        """Test fragment writer and committer for large-scale data."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Use fragment writer and committer
        (
            ray.data.range(10)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema), batch_size=5)
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        assert ds.schema == schema

        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))
        assert set(tbl["str"].to_pylist()) == set([f"str-{i}" for i in range(10)])
        # Should have 2 fragments since batch_size=5 and we have 10 rows
        assert len(ds.get_fragments()) == 2

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_with_transform(self, tmp_path: Path):
        """Test fragment writer with custom transform function."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("str", pa.string()),
                pa.field("doubled", pa.int64()),
            ]
        )

        def transform(batch: pa.Table) -> pa.Table:
            """Transform function to add a doubled column."""
            df = batch.to_pandas()
            df["doubled"] = df["id"] * 2
            return pa.Table.from_pandas(df, schema=schema)

        # Use fragment writer with transform
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(
                LanceFragmentWriter(tmp_path, schema=schema, transform=transform),
                batch_size=5,
            )
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 5
        tbl = ds.to_table()
        indices = pa.compute.sort_indices(tbl, sort_keys=[("id", "ascending")])
        tbl_sorted = pa.compute.take(tbl, indices)
        assert tbl_sorted.column("doubled").to_pylist() == [0, 2, 4, 6, 8]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_append_mode(self, tmp_path: Path):
        """Test fragment writer with append mode."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Write initial data
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path, mode="create"))
        )

        # Append more data
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] >= 5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path, mode="append"))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_overwrite_mode_schema_change(self, tmp_path: Path):
        """Writer+committer overwrite evolves the schema when the SAME mode is
        passed to both (the matching mode is required; see class docs)."""
        # Initial dataset with two columns.
        (
            ray.data.range(4)
            .map(lambda x: {"id": x["id"], "value": x["id"] * 10})
            .map_batches(LanceFragmentWriter(tmp_path))
            .write_datasink(LanceFragmentCommitter(tmp_path, mode="create"))
        )

        # Overwrite with an added column, passing mode="overwrite" to BOTH the
        # writer (assigns new field ids) and the committer (Overwrite op).
        (
            ray.data.range(4)
            .map(lambda x: {"id": x["id"], "value": x["id"] * 10, "id_2": x["id"] * 2})
            .map_batches(LanceFragmentWriter(tmp_path, mode="overwrite"))
            .write_datasink(LanceFragmentCommitter(tmp_path, mode="overwrite"))
        )

        ds = lance.dataset(tmp_path)
        assert ds.schema.names == ["id", "value", "id_2"]
        tbl = ds.to_table().sort_by("id")
        assert tbl["id"].to_pylist() == [0, 1, 2, 3]
        assert tbl["id_2"].to_pylist() == [0, 2, 4, 6]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_empty_write(self, tmp_path: Path):
        """Test fragment writer with empty data."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Write empty data (filter everything out)
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] > 10)  # Filter out everything
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Empty write should not create a dataset
        with pytest.raises(ValueError):
            lance.dataset(tmp_path)

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_none_values(self, tmp_path: Path):
        """Test fragment writer with None values."""

        def create_row(row):
            return {
                "id": row["id"],
                "str": None if row["id"] % 2 == 0 else f"str-{row['id']}",
            }

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        (
            ray.data.range(10)
            .map(create_row)
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceFragmentCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        str_values = tbl["str"].to_pylist()
        id_values = tbl["id"].to_pylist()
        # Even IDs should have None values
        for id_val, str_val in zip(id_values, str_values, strict=False):
            if id_val % 2 == 0:
                # None values might be represented as None or as 'nan' string
                assert str_val is None or str(str_val) == "nan", (
                    f"ID {id_val} should have None/nan but got {str_val}"
                )
            else:
                assert str_val == f"str-{id_val}", (
                    f"ID {id_val} should have 'str-{id_val}' but got {str_val}"
                )
