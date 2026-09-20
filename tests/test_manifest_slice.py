"""Manifest slicing: unit tests for the splicer and end-to-end checks that
read / append / add_columns in ``manifest_mode="slice"`` match ``"full"``."""

import lance
import lance_ray as lr
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ray
from lance_ray.datasink import LanceDatasink, LanceFragmentCommitter
from lance_ray.datasource import LanceDatasource
from lance_ray.fragment import LanceFragmentWriter
from lance_ray.manifest_slice import (
    ManifestSlicer,
    _read_varint,
    field_id_witness,
)
from ray import cloudpickle


@pytest.fixture(scope="module", autouse=True)
def ray_context():
    ray.init(ignore_reinit_error=True)
    yield


@pytest.fixture
def dataset_uri(tmp_path):
    """Three fragments (ids 0, 1, 2) of 10 rows each."""
    uri = str(tmp_path / "sliced.lance")
    for i in range(3):
        table = pa.table(
            {
                "id": pa.array(range(i * 10, i * 10 + 10)),
                "v": [f"s{j}" for j in range(10)],
            }
        )
        lance.write_dataset(table, uri, mode="append" if i else "create")
    return uri


def slicer_of(dataset):
    return ManifestSlicer(dataset._ds.serialized_manifest())


def open_slice(dataset, fragment_ids):
    return lance.LanceDataset(
        dataset.uri,
        version=dataset.version,
        serialized_manifest=slicer_of(dataset).slice(fragment_ids),
    )


def sorted_rows(table_or_rows):
    """Rows as dicts ordered by id, from a pyarrow Table or a Ray Dataset."""
    if isinstance(table_or_rows, pa.Table):
        return table_or_rows.sort_by("id").to_pylist()
    return sorted(table_or_rows.take_all(), key=lambda row: row["id"])


class TestManifestSlicer:
    def test_slice_is_a_valid_manifest_view(self, dataset_uri):
        ds = lance.dataset(dataset_uri)
        slicer = slicer_of(ds)

        assert sorted(slicer._fragments) == [0, 1, 2]  # id 0 is omitted on the wire
        assert len(slicer.header) < len(ds._ds.serialized_manifest()) / 2

        everything = open_slice(ds, [0, 1, 2])
        assert everything.version == ds.version
        assert everything.schema == ds.schema
        assert sorted_rows(everything.to_table()) == sorted_rows(ds.to_table())

        only_one = open_slice(ds, [1])
        assert [f.fragment_id for f in only_one.get_fragments()] == [1]
        assert sorted_rows(only_one.to_table()) == sorted_rows(
            ds.get_fragment(1).to_table()
        )

        header_only = open_slice(ds, [])
        assert header_only.get_fragments() == []
        assert header_only.schema == ds.schema

    def test_slice_rejects_other_version(self, dataset_uri):
        ds = lance.dataset(dataset_uri)
        with pytest.raises(ValueError, match="version"):
            lance.LanceDataset(
                ds.uri,
                version=ds.version - 1,
                serialized_manifest=slicer_of(ds).slice([0]),
            )


def top_level_fields(manifest: bytes) -> list[int]:
    """Field number of every top-level protobuf record in a serialized manifest."""
    buf, pos, fields = memoryview(manifest), 0, []
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        value, pos = _read_varint(buf, pos)  # the varint itself, or a record length
        if key & 7 == 2:
            pos += value
        fields.append(key >> 3)
    return fields


def test_manifest_format_is_the_one_the_slicer_was_written_for(dataset_uri):
    """Fails when a pylance upgrade changes the Manifest layout
    (lance/protos/table.proto). Review ManifestSlicer before updating this."""
    ds = lance.dataset(dataset_uri)
    fields = top_level_fields(ds._ds.serialized_manifest())

    # Reviewed against pylance 5.0.0-beta.6+ve.23: fields 1-21, 17 is reserved.
    assert set(fields) <= set(range(1, 22)) - {17}
    # Field 2 is the fragment list, one record per fragment.
    assert fields.count(2) == len(ds.get_fragments())


def drop_column_held_by_one_fragment(uri):
    """Leave a dropped column's field id alive only in fragment 0's data file.

    Fragment 0 gets one new data file holding ``keep`` and ``gone``; dropping
    ``gone`` keeps that file (it still serves ``keep``), so the dropped field
    id survives in fragment 0's metadata and nowhere else. Returns that id.
    """
    ds = lance.dataset(uri)
    meta, schema = ds.get_fragment(0).merge_columns(
        lambda b: pa.record_batch(
            [pc.add(b.column("id"), 1000), pc.add(b.column("id"), 2000)],
            names=["keep", "gone"],
        ),
        columns=["id"],
    )
    ds = lance.LanceDataset.commit(
        ds,
        lance.LanceOperation.Merge(
            [meta] + [f.metadata for f in ds.get_fragments()[1:]], schema
        ),
        read_version=ds.version,
    )
    dropped_id = schema.field("gone").id()
    ds.drop_columns(["gone"])
    return dropped_id


class TestFieldIdWitness:
    def test_no_witness_when_schema_holds_max_id(self, dataset_uri):
        assert field_id_witness(lance.dataset(dataset_uri)) == []

    def test_witness_is_fragment_holding_dropped_id(self, dataset_uri):
        dropped_id = drop_column_held_by_one_fragment(dataset_uri)
        ds = lance.dataset(dataset_uri)

        assert ds.max_field_id == dropped_id
        assert field_id_witness(ds) == [0]
        # Without the witness a slice under-counts and would hand the dropped
        # id to the next new column.
        assert open_slice(ds, [1]).max_field_id < dropped_id
        assert open_slice(ds, [1, 0]).max_field_id == dropped_id


@pytest.mark.parametrize("manifest_mode", ["full", "slice"])
class TestEndToEnd:
    def test_read_lance(self, dataset_uri, manifest_mode):
        expected = sorted_rows(lance.dataset(dataset_uri).to_table())
        got = lr.read_lance(
            dataset_uri, override_num_blocks=2, manifest_mode=manifest_mode
        )
        assert sorted_rows(got) == expected

        filtered = lr.read_lance(
            dataset_uri,
            columns=["id"],
            filter="id >= 25",
            override_num_blocks=3,
            manifest_mode=manifest_mode,
        )
        assert sorted_rows(filtered) == [{"id": i} for i in range(25, 30)]

    def test_read_lance_with_scalar_index(self, dataset_uri, manifest_mode):
        lance.dataset(dataset_uri).create_scalar_index("id", index_type="BTREE")
        got = lr.read_lance(
            dataset_uri,
            filter="id = 17",
            override_num_blocks=3,
            manifest_mode=manifest_mode,
        )
        assert sorted_rows(got) == [{"id": 17, "v": "s7"}]

    def test_write_lance_append(self, dataset_uri, manifest_mode):
        before = lance.dataset(dataset_uri)
        new_rows = ray.data.from_arrow(
            pa.table({"id": pa.array(range(100, 106)), "v": ["n"] * 6})
        ).repartition(2)
        lr.write_lance(
            new_rows,
            dataset_uri,
            mode="append",
            min_rows_per_file=1,
            max_rows_per_file=3,
            manifest_mode=manifest_mode,
        )

        after = lance.dataset(dataset_uri)
        assert after.version == before.version + 1
        assert after.count_rows() == 36
        assert sorted_rows(after.to_table(filter="id >= 100")) == [
            {"id": i, "v": "n"} for i in range(100, 106)
        ]

    def test_fragment_writer_append(self, dataset_uri, manifest_mode):
        writer = LanceFragmentWriter(
            dataset_uri,
            schema=lance.dataset(dataset_uri).schema,
            manifest_mode=manifest_mode,
        )
        assert (writer._manifest_header is not None) == (manifest_mode == "slice")
        (
            ray.data.from_arrow(pa.table({"id": pa.array([200, 201]), "v": ["w", "w"]}))
            .map_batches(writer, batch_size=1)
            .write_datasink(LanceFragmentCommitter(dataset_uri, mode="append"))
        )
        assert lance.dataset(dataset_uri).count_rows() == 32

    def test_add_columns_after_dropping_a_column(self, dataset_uri, manifest_mode):
        """The schema-evolution trap: drop a column that only some fragments
        hold, then add one. The new column must get a fresh field id, so old
        data never resurfaces under the new name."""
        dropped_id = drop_column_held_by_one_fragment(dataset_uri)

        def negate(batch: pa.RecordBatch) -> pa.RecordBatch:
            return pa.record_batch([pc.multiply(batch.column("id"), -1)], names=["neg"])

        lr.add_columns(
            dataset_uri,
            transform=negate,
            read_columns=["id"],
            concurrency=2,
            manifest_mode=manifest_mode,
        )

        ds = lance.dataset(dataset_uri)
        assert ds.lance_schema.field("neg").id() > dropped_id
        assert sorted_rows(ds.to_table(columns=["id", "neg"])) == [
            {"id": i, "neg": -i} for i in range(30)
        ]


def test_append_tasks_write_through_the_manifest_header(dataset_uri):
    """The append test above would also pass if tasks fell back to opening the
    dataset by URI, so fail the write task unless it got the header."""

    class ProbeDatasink(LanceDatasink):
        def write(self, blocks, ctx):
            import lance_ray.datasink as datasink_module

            real = datasink_module.write_fragment

            def probe(stream, dest, **kwargs):
                assert isinstance(dest, lance.LanceDataset), "opened by URI"
                assert dest.version == self.read_version
                assert dest.get_fragments() == []
                return real(stream, dest, **kwargs)

            datasink_module.write_fragment = probe
            try:
                return super().write(blocks, ctx)
            finally:
                datasink_module.write_fragment = real

    rows = ray.data.from_arrow(pa.table({"id": pa.array([300]), "v": ["p"]}))
    rows.write_datasink(ProbeDatasink(dataset_uri, mode="append"))
    assert lance.dataset(dataset_uri).count_rows() == 31


class TestWriterVersionPinning:
    """A slice-mode LanceFragmentWriter writes against the version it pinned
    when constructed; the commit must be checked against that version."""

    @staticmethod
    def append_through(writer, uri, table):
        (
            ray.data.from_arrow(table)
            .map_batches(writer, batch_size=None)
            .write_datasink(LanceFragmentCommitter(uri, mode="append"))
        )

    def test_append_is_rejected_if_table_was_overwritten(self, dataset_uri):
        writer = LanceFragmentWriter(dataset_uri)
        overwritten = pa.table({"price": pa.array([1]), "owner": ["x"]})
        lance.write_dataset(overwritten, dataset_uri, mode="overwrite")

        # Same types as the new columns: without the version check this commits
        # and the row reads back as {"price": 777, "owner": "secret"}.
        with pytest.raises(OSError, match="Overwrite"):
            self.append_through(
                writer, dataset_uri, pa.table({"id": pa.array([777]), "v": ["secret"]})
            )
        assert lance.dataset(dataset_uri).to_table() == overwritten

    def test_append_survives_a_concurrent_append(self, dataset_uri):
        writer = LanceFragmentWriter(dataset_uri)
        theirs = pa.table({"id": pa.array([50]), "v": ["theirs"]})
        lance.write_dataset(theirs, dataset_uri, mode="append")

        self.append_through(
            writer, dataset_uri, pa.table({"id": pa.array([60]), "v": ["mine"]})
        )
        ds = lance.dataset(dataset_uri)
        assert ds.count_rows() == 32
        assert sorted_rows(ds.to_table(filter="id >= 50")) == [
            {"id": 50, "v": "theirs"},
            {"id": 60, "v": "mine"},
        ]


def test_read_tasks_carry_only_their_fragments(dataset_uri):
    def task_sizes(manifest_mode):
        source = LanceDatasource(uri=dataset_uri, manifest_mode=manifest_mode)
        return [len(cloudpickle.dumps(t)) for t in source.get_read_tasks(3)]

    full, sliced = task_sizes("full"), task_sizes("slice")
    assert len(full) == len(sliced) == 3
    assert all(s < f for s, f in zip(sliced, full, strict=True))
