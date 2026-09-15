"""Test cases for lance_ray.fragment module."""

import warnings
from pathlib import Path
from typing import Any, Optional, cast

import lance
import lance_ray.io as lr
import pyarrow as pa
import pytest
import ray
from lance_ray.datasink import LanceDatasink, LanceFragmentCommitter
from lance_ray.fragment import LanceFragmentWriter, write_fragment


def _legacy_write_fragments(
    reader: Any, uri: Any, *, schema: Optional[pa.Schema] = None
) -> list[Any]:
    return []


def _write_fragments_with_external_blob_options(
    reader: Any,
    uri: Any,
    *,
    external_blob_mode: str = "reference",
    allow_external_blob_outside_bases: bool = False,
) -> list[Any]:
    return []


def test_fragment_writer_external_blob_options_fail_fast(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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


def test_datasink_external_blob_options_fail_fast(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )

    with pytest.raises(RuntimeError, match="external_blob_mode.*write_fragments"):
        LanceDatasink(str(tmp_path), external_blob_mode="ingest")


def test_write_lance_external_blob_options_fail_fast(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )

    with pytest.raises(RuntimeError, match="external_blob_mode.*write_fragments"):
        lr.write_lance(cast(Any, object()), str(tmp_path), external_blob_mode="ingest")


def test_base_store_params_fail_fast_when_fragment_api_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import lance.fragment as lance_fragment

    monkeypatch.setattr(
        lance_fragment,
        "write_fragments",
        _legacy_write_fragments,
    )
    base_store_params: dict[str, dict[str, Any]] = {tmp_path.as_uri(): {}}

    with pytest.raises(RuntimeError, match="base_store_params.*write_fragments"):
        LanceFragmentWriter(
            str(tmp_path),
            data_storage_version="stable",
            base_store_params=base_store_params,
        )

    with pytest.raises(RuntimeError, match="base_store_params.*write_fragments"):
        LanceDatasink(str(tmp_path), base_store_params=base_store_params)

    with pytest.raises(RuntimeError, match="base_store_params.*write_fragments"):
        lr.write_lance(
            cast(Any, object()), str(tmp_path), base_store_params=base_store_params
        )


def test_target_bases_fail_fast_when_fragment_api_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
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
        lr.write_lance(cast(Any, object()), str(tmp_path), target_bases=target_bases)


@pytest.mark.parametrize("target_all_bases", [True, False])
def test_target_all_bases_fail_fast_when_fragment_api_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target_all_bases: bool,
) -> None:
    import lance.fragment as lance_fragment

    monkeypatch.setattr(lance_fragment, "write_fragments", _legacy_write_fragments)
    uri = str(tmp_path)

    with pytest.raises(RuntimeError, match="target_all_bases.*write_fragments"):
        LanceFragmentWriter(
            uri, data_storage_version="stable", target_all_bases=target_all_bases
        )
    with pytest.raises(RuntimeError, match="target_all_bases.*write_fragments"):
        LanceDatasink(uri, target_all_bases=target_all_bases)
    with pytest.raises(RuntimeError, match="target_all_bases.*write_fragments"):
        lr.write_lance(cast(Any, object()), uri, target_all_bases=target_all_bases)
    with pytest.raises(RuntimeError, match="target_all_bases.*write_fragments"):
        write_fragment([pa.table({"id": [1]})], uri, target_all_bases=target_all_bases)


@pytest.mark.parametrize("target_all_bases", [None, True, False])
def test_fragment_writer_target_all_bases_forwarded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target_all_bases: Optional[bool],
) -> None:
    import lance.fragment as lance_fragment

    omitted = object()
    received: list[object] = []

    def capture(
        reader: Any, uri: str, *, target_all_bases: object = omitted, **kwargs: Any
    ) -> list[Any]:
        received.append(target_all_bases)
        return []

    monkeypatch.setattr(lance_fragment, "write_fragments", capture)
    writer = LanceFragmentWriter(
        str(tmp_path), data_storage_version="stable", target_all_bases=target_all_bases
    )
    writer(pa.table({"id": [1]}))

    assert len(received) == 1
    assert received[0] is (omitted if target_all_bases is None else target_all_bases)


def test_allow_external_blob_outside_bases_ignored_for_ingest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
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
    def test_fragment_writer_committer(self, tmp_path: Path) -> None:
        """Test fragment writer and committer for large-scale data."""
        schema_fields: list[pa.Field[Any]] = [
            pa.field("id", pa.int64()),
            pa.field("str", pa.string()),
        ]
        schema = pa.schema(schema_fields)

        # Use fragment writer and committer
        (
            ray.data.range(10)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(
                LanceFragmentWriter(str(tmp_path), schema=schema), batch_size=5
            )
            .write_datasink(LanceFragmentCommitter(str(tmp_path)))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        assert ds.schema == schema

        tbl = ds.to_table()
        assert sorted(cast("list[int]", tbl["id"].to_pylist())) == list(range(10))
        assert set(tbl["str"].to_pylist()) == set([f"str-{i}" for i in range(10)])
        # Should have 2 fragments since batch_size=5 and we have 10 rows
        assert len(ds.get_fragments()) == 2

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_committer_enables_stable_row_ids(
        self, tmp_path: Path
    ) -> None:
        schema_fields: list[pa.Field[Any]] = [pa.field("id", pa.int64())]
        schema = pa.schema(schema_fields)

        (
            ray.data.range(10)
            .map_batches(
                LanceFragmentWriter(
                    str(tmp_path),
                    schema=schema,
                    enable_stable_row_ids=True,
                ),
                batch_size=5,
            )
            .write_datasink(
                LanceFragmentCommitter(
                    str(tmp_path),
                    enable_stable_row_ids=True,
                )
            )
        )

        dataset = lance.dataset(tmp_path)
        assert dataset.has_stable_row_ids
        before_table = dataset.to_table(columns=["id"], with_row_id=True)
        before = dict(
            zip(
                before_table["id"].to_pylist(),
                before_table["_rowid"].to_pylist(),
                strict=True,
            )
        )

        dataset.optimize.compact_files(target_rows_per_fragment=10)
        compacted = lance.dataset(tmp_path)
        after_table = compacted.to_table(columns=["id"], with_row_id=True)
        after = dict(
            zip(
                after_table["id"].to_pylist(),
                after_table["_rowid"].to_pylist(),
                strict=True,
            )
        )

        assert after == before

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_with_transform(self, tmp_path: Path) -> None:
        """Test fragment writer with custom transform function."""
        schema_fields: list[pa.Field[Any]] = [
            pa.field("id", pa.int64()),
            pa.field("str", pa.string()),
            pa.field("doubled", pa.int64()),
        ]
        schema = pa.schema(schema_fields)

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
                LanceFragmentWriter(str(tmp_path), schema=schema, transform=transform),
                batch_size=5,
            )
            .write_datasink(LanceFragmentCommitter(str(tmp_path)))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 5
        tbl = ds.to_table()
        indices = pa.compute.sort_indices(tbl, sort_keys=[("id", "ascending")])
        tbl_sorted = pa.compute.take(tbl, indices)
        assert tbl_sorted.column("doubled").to_pylist() == [0, 2, 4, 6, 8]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_append_mode(self, tmp_path: Path) -> None:
        """Test fragment writer with append mode."""
        schema_fields: list[pa.Field[Any]] = [
            pa.field("id", pa.int64()),
            pa.field("str", pa.string()),
        ]
        schema = pa.schema(schema_fields)

        # Write initial data
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(str(tmp_path), schema=schema))
            .write_datasink(LanceFragmentCommitter(str(tmp_path), mode="create"))
        )

        # Append more data
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] >= 5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(str(tmp_path), schema=schema))
            .write_datasink(LanceFragmentCommitter(str(tmp_path), mode="append"))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        assert sorted(cast("list[int]", tbl["id"].to_pylist())) == list(range(10))

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_empty_write(self, tmp_path: Path) -> None:
        """Test fragment writer with empty data."""
        schema_fields: list[pa.Field[Any]] = [
            pa.field("id", pa.int64()),
            pa.field("str", pa.string()),
        ]
        schema = pa.schema(schema_fields)

        # Write empty data (filter everything out)
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] > 10)  # Filter out everything
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(str(tmp_path), schema=schema))
            .write_datasink(LanceFragmentCommitter(str(tmp_path)))
        )

        # Empty write should not create a dataset
        with pytest.raises(ValueError):
            lance.dataset(tmp_path)

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_none_values(self, tmp_path: Path) -> None:
        """Test fragment writer with None values."""

        def create_row(row: dict[str, Any]) -> dict[str, Any]:
            return {
                "id": row["id"],
                "str": None if row["id"] % 2 == 0 else f"str-{row['id']}",
            }

        schema_fields: list[pa.Field[Any]] = [
            pa.field("id", pa.int64()),
            pa.field("str", pa.string()),
        ]
        schema = pa.schema(schema_fields)

        (
            ray.data.range(10)
            .map(create_row)
            .map_batches(LanceFragmentWriter(str(tmp_path), schema=schema))
            .write_datasink(LanceFragmentCommitter(str(tmp_path)))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        str_values = tbl["str"].to_pylist()
        id_values = tbl["id"].to_pylist()
        # Even IDs should have None values
        for id_val, str_val in zip(
            cast("list[int]", id_values), str_values, strict=False
        ):
            if id_val % 2 == 0:
                # None values might be represented as None or as 'nan' string
                assert str_val is None or str(str_val) == "nan", (
                    f"ID {id_val} should have None/nan but got {str_val}"
                )
            else:
                assert str_val == f"str-{id_val}", (
                    f"ID {id_val} should have 'str-{id_val}' but got {str_val}"
                )
