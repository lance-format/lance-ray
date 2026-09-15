"""Test cases for lance_ray.fragment module."""

import io
import tempfile
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Optional, cast

import lance
import lance_ray.io as lr
import pyarrow as pa
import pytest
import ray
from lance.fragment import FragmentMetadata
from lance_ray.datasink import LanceDatasink, LanceFragmentCommitter
from lance_ray.fragment import LanceFragmentWriter, write_fragment

import pandas as pd

REPLAY_THRESHOLD_ENV = "LANCE_RAY_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES"


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


@pytest.mark.parametrize("failure_after_batches", [1, 5, 10])
@pytest.mark.parametrize("failures", [1, 2])
@pytest.mark.parametrize("explicit_max_attempts", [False, True])
@pytest.mark.usefixtures("replay_storage", "replay_files")
def test_write_fragment_retry_replays_complete_input(
    monkeypatch: pytest.MonkeyPatch,
    failure_after_batches: int,
    failures: int,
    explicit_max_attempts: bool,
) -> None:
    import lance.fragment as lance_fragment

    batch_rows = 4_096
    expected_ids = list(range(batch_rows * 10))
    attempts: list[list[int]] = []
    readers: list[pa.RecordBatchReader] = []
    generated_batches: list[int] = []

    def input_blocks() -> Iterator[pa.Table]:
        for batch_index in range(10):
            generated_batches.append(batch_index)
            yield pa.table(
                {"id": range(batch_index * batch_rows, (batch_index + 1) * batch_rows)}
            )

    def partially_failing_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        readers.append(reader)
        ids: list[int] = []
        attempts.append(ids)
        for batch_index, batch in enumerate(reader, start=1):
            ids.extend(cast("list[int]", batch.column("id").to_pylist()))
            if len(attempts) <= failures and batch_index == failure_after_batches:
                raise RuntimeError("LanceError(IO): injected write failure")
        return [FragmentMetadata(id=0, files=[], physical_rows=len(ids))]

    monkeypatch.setattr(lance_fragment, "write_fragments", partially_failing_write)
    retry_params: dict[str, Any] = {
        "description": "write lance fragments",
        "match": ["LanceError(IO)"],
        "max_backoff_s": 0,
    }
    if explicit_max_attempts:
        retry_params["max_attempts"] = failures + 1

    result = write_fragment(
        input_blocks(), "memory://retry-replay", retry_params=retry_params
    )

    assert len(attempts) == failures + 1
    assert (
        attempts[:-1] == [expected_ids[: failure_after_batches * batch_rows]] * failures
    )
    assert attempts[-1] == expected_ids
    assert generated_batches == list(range(10))
    assert len({id(reader) for reader in readers}) == failures + 1
    assert sum(fragment.num_rows for fragment, _ in result) == len(expected_ids)


@pytest.fixture
def replay_disk_files(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[IO[bytes]]]:
    """Track disk handles independently of their owning spool objects."""
    temporary_file = tempfile.TemporaryFile
    files: list[IO[bytes]] = []

    def tracked_file(*args: Any, **kwargs: Any) -> IO[bytes]:
        file: IO[bytes] = temporary_file(*args, **kwargs)
        files.append(file)
        return file

    monkeypatch.setattr(tempfile, "TemporaryFile", tracked_file)
    yield files
    assert all(file.closed for file in files)


@pytest.fixture
def replay_files(
    monkeypatch: pytest.MonkeyPatch, replay_disk_files: list[IO[bytes]]
) -> Iterator[list[tempfile.SpooledTemporaryFile[bytes]]]:
    spooled_file = tempfile.SpooledTemporaryFile
    files: list[tempfile.SpooledTemporaryFile[bytes]] = []

    def tracked_spool(
        *args: Any, **kwargs: Any
    ) -> tempfile.SpooledTemporaryFile[bytes]:
        file: tempfile.SpooledTemporaryFile[bytes] = spooled_file(*args, **kwargs)
        files.append(file)
        return file

    def unexpected_fileno(_self: tempfile.SpooledTemporaryFile[bytes]) -> int:
        raise AssertionError("fileno() forces rollover and must not be used")

    monkeypatch.setattr(spooled_file, "fileno", unexpected_fileno)
    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", tracked_spool)
    yield files
    assert all(file.closed for file in files)


@pytest.fixture(params=[128 * 1024 * 1024, 0], ids=["memory", "disk"])
def replay_storage(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> int:
    threshold = cast(int, request.param)
    monkeypatch.setenv(REPLAY_THRESHOLD_ENV, str(threshold))
    return threshold


@pytest.mark.parametrize("threshold", [None, "0", "1024", "8192"])
def test_write_fragment_spool_storage_and_replay(
    monkeypatch: pytest.MonkeyPatch,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    replay_disk_files: list[IO[bytes]],
    threshold: Optional[str],
) -> None:
    import lance.fragment as lance_fragment

    monkeypatch.delenv(REPLAY_THRESHOLD_ENV, raising=False)
    if threshold is not None:
        monkeypatch.setenv(REPLAY_THRESHOLD_ENV, threshold)
    expected_threshold = int(threshold) if threshold is not None else 134_217_728
    states: list[bool] = []
    attempts: list[list[int]] = []
    new_stream = pa.ipc.new_stream

    def input_blocks() -> Iterator[pa.Table]:
        for start in range(0, 192, 64):
            yield pa.table({"id": range(start, start + 64)})
            # Inspect stdlib state only in tests, without forcing fileno().
            states.append(vars(replay_files[0])["_rolled"])

    def checked_stream(
        sink: "tempfile.SpooledTemporaryFile[bytes]", schema: pa.Schema
    ) -> pa.RecordBatchStreamWriter:
        # Zero must roll before the IPC writer can emit even its header.
        assert vars(sink)["_rolled"] == (expected_threshold == 0)
        return new_stream(cast(io.IOBase, sink), schema)

    def retry_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        assert len(states) == 3  # All input is staged before any target write.
        ids = cast("list[int]", reader.read_all()["id"].to_pylist())
        attempts.append(ids)
        if len(attempts) == 1:
            raise RuntimeError("injected write failure")
        return [FragmentMetadata(id=0, files=[], physical_rows=len(ids))]

    monkeypatch.setattr(pa.ipc, "new_stream", checked_stream)
    monkeypatch.setattr(lance_fragment, "write_fragments", retry_write)
    result = write_fragment(
        input_blocks(),
        "memory://spool-storage",
        retry_params={"description": "write", "max_attempts": 2, "max_backoff_s": 0},
    )
    assert result[0][0].num_rows == 192
    assert attempts == [list(range(192))] * 2
    assert len(replay_files) == 1
    assert vars(replay_files[0])["_max_size"] == expected_threshold
    expected_states = {
        0: [True, True, True],
        1024: [False, True, True],
        8192: [False, False, False],
        134_217_728: [False, False, False],
    }
    assert states == expected_states[expected_threshold]
    assert len(replay_disk_files) == int(states[-1])


def test_write_fragment_reads_threshold_per_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    replay_disk_files: list[IO[bytes]],
) -> None:
    for index, threshold in enumerate(["8192", "0", None]):
        if threshold is None:
            monkeypatch.delenv(REPLAY_THRESHOLD_ENV, raising=False)
        else:
            monkeypatch.setenv(REPLAY_THRESHOLD_ENV, threshold)
        write_fragment(
            [pa.table({"id": [index]})],
            str(tmp_path / f"call-{index}.lance"),
            retry_params={"description": "write", "max_attempts": 2},
        )
    assert [vars(file)["_max_size"] for file in replay_files] == [8192, 0, 134_217_728]
    assert [vars(file)["_rolled"] for file in replay_files] == [False, True, False]
    assert len(replay_disk_files) == 1


@pytest.mark.parametrize("threshold", ["-1", "", "invalid", "1.5"])
def test_write_fragment_rejects_invalid_threshold(
    monkeypatch: pytest.MonkeyPatch,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    threshold: str,
) -> None:
    import lance.fragment as lance_fragment

    def unexpected_write(*_args: Any, **_kwargs: Any) -> list[FragmentMetadata]:
        raise AssertionError("invalid configuration must not reach the writer")

    monkeypatch.setenv(REPLAY_THRESHOLD_ENV, threshold)
    monkeypatch.setattr(lance_fragment, "write_fragments", unexpected_write)
    with pytest.raises(ValueError, match=REPLAY_THRESHOLD_ENV):
        write_fragment(
            [pa.table({"id": [1]})],
            "memory://invalid-threshold",
            retry_params={"description": "write", "max_attempts": 2},
        )
    assert replay_files == []
    assert (
        write_fragment(
            [],
            "memory://empty",
            retry_params={"description": "write", "max_attempts": 2},
        )
        == []
    )
    assert replay_files == []


@pytest.mark.parametrize("threshold", [0, 1024], ids=["immediate", "automatic"])
@pytest.mark.parametrize("failure", ["create", "copy"])
def test_write_fragment_rollover_failure_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    replay_disk_files: list[IO[bytes]],
    threshold: int,
    failure: str,
) -> None:
    import lance.fragment as lance_fragment

    error = OSError("injected rollover failure")
    temporary_file = tempfile.TemporaryFile

    class FailingCopy(io.BufferedWriter):
        def write(self, data: Any) -> int:
            raise error

    def failing_file(*args: Any, **kwargs: Any) -> IO[bytes]:
        if failure == "create":
            raise error
        file = temporary_file(*args, **kwargs)
        return FailingCopy(file)

    def unexpected_write(*_args: Any, **_kwargs: Any) -> list[FragmentMetadata]:
        raise AssertionError("failed rollover must not reach the destination writer")

    monkeypatch.setenv(REPLAY_THRESHOLD_ENV, str(threshold))
    monkeypatch.setattr(tempfile, "TemporaryFile", failing_file)
    monkeypatch.setattr(lance_fragment, "write_fragments", unexpected_write)
    with pytest.raises(OSError, match="injected rollover failure"):
        write_fragment(
            (pa.table({"id": range(64)}) for _ in range(3)),
            "memory://rollover-failure",
            retry_params={"description": "write", "max_attempts": 3},
        )
    assert len(replay_files) == 1
    assert replay_files[0].closed
    assert len(replay_disk_files) == (1 if failure == "copy" else 0)
    assert all(file.closed for file in replay_disk_files)


@pytest.mark.parametrize("max_attempts", [1, 2])
@pytest.mark.parametrize("result_kind", ["early", "short", "long"])
@pytest.mark.usefixtures("replay_storage")
def test_write_fragment_rejects_incomplete_result(
    monkeypatch: pytest.MonkeyPatch,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    max_attempts: int,
    result_kind: str,
) -> None:
    import lance.fragment as lance_fragment

    generated: list[int] = []
    calls = 0

    def input_blocks() -> Iterator[pa.Table]:
        for value in range(4):
            generated.append(value)
            yield pa.table({"id": [value]})

    def incomplete_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        if result_kind == "early":
            rows = next(reader).num_rows
        else:
            rows = sum(batch.num_rows for batch in reader)
            rows += -1 if result_kind == "short" else 1
        return [FragmentMetadata(id=0, files=[], physical_rows=rows)]

    monkeypatch.setattr(lance_fragment, "write_fragments", incomplete_write)
    wrote = {"early": 1, "short": 3, "long": 5}[result_kind]
    with pytest.raises(RuntimeError, match=f"expected 4, wrote {wrote}"):
        write_fragment(
            input_blocks(),
            "memory://incomplete-result",
            retry_params={
                "description": "write lance fragments",
                "max_attempts": max_attempts,
                "max_backoff_s": 0,
            },
        )

    assert calls == 1  # Integrity errors must not be retried, even with match=None.
    assert generated == list(range(4))
    assert len(replay_files) == (1 if max_attempts > 1 else 0)
    assert all(file.closed for file in replay_files)


@pytest.mark.parametrize("explicit_max_attempts", [False, True])
def test_write_fragment_single_attempt_remains_streaming(
    monkeypatch: pytest.MonkeyPatch, explicit_max_attempts: bool
) -> None:
    import lance.fragment as lance_fragment

    generated: list[int] = []

    def input_blocks() -> Iterator[pa.Table]:
        for value in range(4):
            generated.append(value)
            yield pa.table({"id": [value]})

    def unexpected_spool(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("single-attempt writes must not create a replay file")

    def streaming_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        assert generated == [0]  # Only schema inference may look ahead.
        for value in range(4):
            assert next(reader).column("id").to_pylist() == [value]
            assert generated == list(range(value + 1))
        return [FragmentMetadata(id=0, files=[], physical_rows=4)]

    monkeypatch.setenv(REPLAY_THRESHOLD_ENV, "invalid")
    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", unexpected_spool)
    monkeypatch.setattr(tempfile, "TemporaryFile", unexpected_spool)
    monkeypatch.setattr(lance_fragment, "write_fragments", streaming_write)
    retry_params = (
        {"description": "write lance fragments", "max_attempts": 1}
        if explicit_max_attempts
        else None
    )
    result = write_fragment(
        input_blocks(), "memory://streaming", retry_params=retry_params
    )
    assert result[0][0].num_rows == 4


@pytest.mark.parametrize(
    ("message", "expected_attempts"),
    [("LanceError(IO): injected failure", 3), ("invalid write argument", 1)],
)
@pytest.mark.usefixtures("replay_storage")
def test_write_fragment_failure_closes_replay(
    monkeypatch: pytest.MonkeyPatch,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    message: str,
    expected_attempts: int,
) -> None:
    import lance.fragment as lance_fragment

    calls = 0
    error = RuntimeError(message)

    def failing_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        assert next(reader).column("id").to_pylist() == [0, 1]
        raise error

    monkeypatch.setattr(lance_fragment, "write_fragments", failing_write)
    with pytest.raises(RuntimeError) as exc_info:
        write_fragment(
            [pa.table({"id": [0, 1]}), pa.table({"id": [2, 3]})],
            "memory://write-failure",
            retry_params={
                "description": "write lance fragments",
                "match": ["LanceError(IO)"],
                "max_attempts": 3,
                "max_backoff_s": 0,
            },
        )
    assert exc_info.value is error
    assert calls == expected_attempts
    assert len(replay_files) == 1
    assert replay_files[0].closed


@pytest.mark.parametrize("failure", ["input", "conversion", "ipc"])
@pytest.mark.usefixtures("replay_storage")
def test_write_fragment_spool_failure_closes_replay(
    monkeypatch: pytest.MonkeyPatch,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    failure: str,
) -> None:
    import lance.fragment as lance_fragment
    from lance_ray.pandas import pd_to_arrow

    calls = 0
    converted_blocks = 0
    new_stream = pa.ipc.new_stream
    error = OSError("injected spooling failure")

    def input_blocks() -> Iterator[pa.Table]:
        yield pa.table({"id": [0, 1]})
        if failure == "input":
            raise error
        yield pa.table({"id": [2, 3]})

    def failing_converter(
        block: pa.Table | pd.DataFrame | dict[str, Any], schema: Optional[pa.Schema]
    ) -> pa.Table:
        nonlocal converted_blocks
        converted_blocks += 1
        if converted_blocks == 2:
            raise error
        return pd_to_arrow(block, schema)

    @contextmanager
    def failing_stream(
        sink: IO[bytes], schema: pa.Schema
    ) -> Iterator[pa.RecordBatchStreamWriter]:
        with new_stream(cast(io.IOBase, sink), schema) as writer:
            yield writer
        # A flush/close error must also prevent any destination write.
        raise error

    def unexpected_write(*_args: Any, **_kwargs: Any) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        raise AssertionError("failed spooling must not reach the destination writer")

    monkeypatch.setattr(lance_fragment, "write_fragments", unexpected_write)
    if failure == "ipc":
        monkeypatch.setattr(pa.ipc, "new_stream", failing_stream)
    elif failure == "conversion":
        monkeypatch.setattr("lance_ray.fragment.pd_to_arrow", failing_converter)
    with pytest.raises(OSError) as exc_info:
        write_fragment(
            input_blocks(),
            "memory://spool-failure",
            retry_params={
                "description": "write lance fragments",
                "max_attempts": 3,
                "max_backoff_s": 0,
            },
        )
    assert exc_info.value is error
    assert calls == 0
    assert len(replay_files) == 1
    assert replay_files[0].closed


@pytest.mark.parametrize("max_attempts", [1, 2])
@pytest.mark.parametrize(
    "input_kind", ["arrow", "pandas", "dict", "zero_rows", "empty"]
)
@pytest.mark.usefixtures("replay_storage")
def test_write_fragment_input_compatibility(
    tmp_path: Path,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    max_attempts: int,
    input_kind: str,
) -> None:
    schema = pa.schema([pa.field("id", pa.int64())], metadata={b"source": b"test"})
    table = pa.table({"id": [0, 1, 2, 3]}, schema=schema)
    blocks: list[pa.Table | pd.DataFrame | dict[str, Any]]
    if input_kind == "pandas":
        blocks = [table.to_pandas()]
    elif input_kind == "dict":
        blocks = [table.to_pydict()]
    elif input_kind == "zero_rows":
        blocks = [table.slice(0, 0)]
    elif input_kind == "empty":
        blocks = []
    else:
        blocks = [table]
    result = write_fragment(
        iter(blocks),
        str(tmp_path / "input.lance"),
        schema=schema,
        retry_params={
            "description": "write lance fragments",
            "max_attempts": max_attempts,
        },
    )
    expected_rows = 0 if input_kind in {"zero_rows", "empty"} else 4
    assert sum(fragment.num_rows for fragment, _ in result) == expected_rows
    assert all(
        result_schema.equals(schema, check_metadata=True) for _, result_schema in result
    )
    assert len(replay_files) == (1 if max_attempts > 1 and blocks else 0)
    assert all(file.closed for file in replay_files)


@pytest.mark.parametrize("via_datasink", [False, True])
@pytest.mark.parametrize("failure_after_batches", [5, 10])
@pytest.mark.usefixtures("replay_storage")
def test_write_fragment_retry_commits_all_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    replay_files: list[tempfile.SpooledTemporaryFile[bytes]],
    via_datasink: bool,
    failure_after_batches: int,
) -> None:
    import lance.fragment as lance_fragment

    real_write = lance_fragment.write_fragments
    attempts: list[int] = []
    uri = str(tmp_path / "retry.lance")
    schema = pa.schema([pa.field("id", pa.int64())])

    def injected_write(
        reader: pa.RecordBatchReader, uri: str, **kwargs: Any
    ) -> list[FragmentMetadata]:
        if not attempts:
            table = pa.Table.from_batches(
                [next(reader) for _ in range(failure_after_batches)]
            )
            partial = real_write(table, uri, return_transaction=False, **kwargs)
            attempts.append(sum(fragment.num_rows for fragment in partial))
            raise RuntimeError("LanceError(IO): injected after partial file write")
        result = real_write(reader, uri, return_transaction=False, **kwargs)
        attempts.append(sum(fragment.num_rows for fragment in result))
        return result

    monkeypatch.setattr(lance_fragment, "write_fragments", injected_write)
    blocks = (
        pa.table({"id": range(i * 4_096, (i + 1) * 4_096)}, schema=schema)
        for i in range(10)
    )
    if via_datasink:
        # Keep the Datasink's default attempt count and error matching.
        monkeypatch.setattr(
            LanceDatasink, "WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS", 0
        )
        sink = LanceDatasink(uri, schema=schema)
        sink.on_write_start()
        write_return = sink.write(blocks, None)
        sink.on_write_complete([write_return])
    else:
        fragments = write_fragment(
            blocks,
            uri,
            schema=schema,
            retry_params={
                "description": "write lance fragments",
                "match": ["LanceError(IO)"],
                "max_attempts": 2,
                "max_backoff_s": 0,
            },
        )
        lance.LanceDataset.commit(
            uri, lance.LanceOperation.Overwrite(schema, [f for f, _ in fragments])
        )

    dataset = lance.dataset(uri)
    assert attempts == [failure_after_batches * 4_096, 40_960]
    assert dataset.count_rows() == 40_960
    assert dataset.to_table()["id"].to_pylist() == list(range(40_960))
    assert len(replay_files) == 1
    assert replay_files[0].closed


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
