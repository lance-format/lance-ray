"""Tests for LanceDatasource.estimate_inmemory_data_size."""

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import lance
import lance_ray as lr
import numpy as np
import pyarrow as pa
import pytest
from lance_ray.datasource import LanceDatasource

ROWS = 4000
VECTOR_DIM = 32
FRAGMENTS = 20
ROWS_PER_FRAGMENT = ROWS // FRAGMENTS


@pytest.fixture
def wide_table_path(tmp_path: Path) -> Iterator[str]:
    """A table with a fixed-width, a variable-width and a wide vector column."""
    rng = np.random.default_rng(0)
    table = pa.table(
        {
            "id": pa.array(np.arange(ROWS, dtype=np.int64)),
            "name": pa.array(["x" * int(n) for n in rng.integers(1, 100, ROWS)]),
            "vec": pa.FixedSizeListArray.from_arrays(
                pa.array(rng.random(ROWS * VECTOR_DIM).astype(np.float32)), VECTOR_DIM
            ),
        }
    )
    path = str(tmp_path / "wide")
    lance.write_dataset(table, path, max_rows_per_file=ROWS_PER_FRAGMENT)
    yield path


def _actual_bytes(path: str, columns: Any = None) -> int:
    return int(lance.dataset(path).to_table(columns=columns).nbytes)


def _estimate(path: str, **kwargs: Any) -> int:
    estimate = LanceDatasource(path, **kwargs).estimate_inmemory_data_size()
    assert estimate is not None
    return estimate


def _file_sizes(path: str) -> int:
    return sum(
        data_file.file_size_bytes
        for fragment in lance.dataset(path).get_fragments()
        for data_file in fragment.data_files()
    )


class TestInMemoryEstimate:
    def test_fixed_width_projection_is_exact(self, wide_table_path: str) -> None:
        estimate = _estimate(wide_table_path, columns=["id"])

        assert estimate == _actual_bytes(wide_table_path, ["id"])

    def test_projection_does_not_count_unread_columns(
        self, wide_table_path: str
    ) -> None:
        narrow = _estimate(wide_table_path, columns=["id"])

        assert narrow * 10 < _estimate(wide_table_path)

    @pytest.mark.parametrize(
        "columns", [["name"], ["id", "name"], ["vec", "name"], None]
    )
    def test_variable_width_columns_are_sampled(
        self, wide_table_path: str, columns: Any
    ) -> None:
        estimate = _estimate(wide_table_path, columns=columns)

        assert estimate == pytest.approx(
            _actual_bytes(wide_table_path, columns), rel=0.1
        )

    def test_fixed_width_types_are_sized_from_the_schema(self, tmp_path: Path) -> None:
        """No row data is read when every column has a fixed width."""
        table = pa.table(
            {
                "i8": pa.array(np.arange(ROWS, dtype=np.int8)),
                "flag": pa.array(np.arange(ROWS) % 2 == 0),
                "ts": pa.array(np.arange(ROWS), type=pa.timestamp("us")),
                "dec": pa.array([1] * ROWS, type=pa.decimal128(20, 2)),
                "point": pa.StructArray.from_arrays(
                    [
                        pa.array(np.zeros(ROWS)),
                        pa.array(np.zeros(ROWS, dtype=np.int32)),
                    ],
                    names=["x", "n"],
                ),
                "pair": pa.FixedSizeListArray.from_arrays(
                    pa.array(np.zeros(ROWS * 2, dtype=np.int16)), 2
                ),
            }
        )
        path = str(tmp_path / "fixed")
        lance.write_dataset(table, path, max_rows_per_file=ROWS_PER_FRAGMENT)

        def no_scan(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("a fixed-width estimate must not scan")

        with patch.object(lance.LanceDataset, "scanner", no_scan):
            estimate = _estimate(path)

        assert estimate == _actual_bytes(path)

    def test_fragment_ids_limit_the_rows_counted(self, wide_table_path: str) -> None:
        fragments = lance.dataset(wide_table_path).get_fragments()
        fragment_ids = [f.fragment_id for f in fragments][:3]

        estimate = _estimate(wide_table_path, columns=["id"], fragment_ids=fragment_ids)

        assert estimate == 3 * ROWS_PER_FRAGMENT * 8

    def test_deleted_rows_are_not_counted(self, wide_table_path: str) -> None:
        # Every fragment keeps half of its rows, behind a deletion file.
        lance.dataset(wide_table_path).delete("id % 2 = 0")

        estimate = _estimate(wide_table_path, columns=["id"])

        assert estimate == (ROWS // 2) * 8

    def test_row_id_and_metadata_columns_are_counted(
        self, wide_table_path: str
    ) -> None:
        plain = _estimate(wide_table_path, columns=["id"])

        with_row_id = _estimate(
            wide_table_path, columns=["id"], scanner_options={"with_row_id": True}
        )
        with_metadata = _estimate(wide_table_path, columns=["id"], with_metadata=True)

        assert with_row_id - plain == ROWS * 8
        assert with_metadata - plain == ROWS * 16

    def test_read_lance_plans_with_the_projected_estimate(
        self, wide_table_path: str
    ) -> None:
        original = LanceDatasource.estimate_inmemory_data_size
        seen: list[Any] = []

        def spy(self: LanceDatasource) -> Any:
            seen.append(original(self))
            return seen[-1]

        with patch.object(LanceDatasource, "estimate_inmemory_data_size", spy):
            lr.read_lance(wide_table_path, columns=["id"])

        assert seen == [_actual_bytes(wide_table_path, ["id"])]


class TestFileSizeFallback:
    """Reads whose size cannot be derived keep the file-size estimate."""

    def test_expression_projection(self, wide_table_path: str) -> None:
        estimate = _estimate(
            wide_table_path, scanner_options={"columns": {"doubled": "id * 2"}}
        )

        assert estimate == _file_sizes(wide_table_path)

    def test_blob_column(self, tmp_path: Path) -> None:
        """A blob column is read back as bytes, which a sample would not show."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field(
                    "blob",
                    pa.large_binary(),
                    metadata={"lance-encoding:blob": "true"},
                ),
            ]
        )
        table = pa.table(
            {"id": [1, 2, 3], "blob": [b"a" * 1000, b"b" * 2000, b"c" * 3000]},
            schema=schema,
        )
        path = str(tmp_path / "blobs")
        lance.write_dataset(table, path, data_storage_version="2.1")

        assert _estimate(path, columns=["blob"]) == _file_sizes(path)
        assert _estimate(path) == _file_sizes(path)
        assert _estimate(path, columns=["id"]) == 3 * 8

    def test_sampling_failure_does_not_fail_the_read(
        self, wide_table_path: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        def broken_scanner(*args: Any, **kwargs: Any) -> Any:
            raise OSError(f"cannot read {wide_table_path}")

        with (
            patch.object(lance.LanceDataset, "scanner", broken_scanner),
            caplog.at_level(logging.DEBUG, logger="lance_ray.datasource"),
        ):
            estimate = _estimate(wide_table_path, columns=["name"])

        assert estimate == _file_sizes(wide_table_path)
        assert wide_table_path not in caplog.text
