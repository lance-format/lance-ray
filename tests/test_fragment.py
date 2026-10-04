"""Test cases for lance_ray.fragment module."""

import inspect
import tempfile
import warnings
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, Optional, cast
from unittest.mock import patch

import lance
import lance_ray.io as lr
import pyarrow as pa
import pytest
import ray
from lance.fragment import FragmentMetadata
from lance_ray.datasink import LanceDatasink, LanceFragmentCommitter
from lance_ray.fragment import (
    FragmentBlock,
    FragmentStream,
    FragmentStreamFactory,
    LanceFragmentWriter,
    write_fragment,
)

import pandas as pd


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


@pytest.fixture
def no_replay_staging(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_staging(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("write retries must not stage an IPC stream")

    monkeypatch.setattr(tempfile, "TemporaryFile", unexpected_staging)
    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", unexpected_staging)
    monkeypatch.setattr(pa.ipc, "new_stream", unexpected_staging)
    monkeypatch.setattr(pa.ipc, "open_stream", unexpected_staging)


@pytest.fixture
def closed_readers(monkeypatch: pytest.MonkeyPatch) -> list[pa.RecordBatchReader]:
    import lance_ray.fragment as fragment_module

    closed: list[pa.RecordBatchReader] = []

    class TrackedReaders:
        @staticmethod
        @contextmanager
        def from_batches(
            schema: pa.Schema, batches: Iterable[pa.RecordBatch]
        ) -> Iterator[pa.RecordBatchReader]:
            with pa.RecordBatchReader.from_batches(schema, batches) as reader:
                try:
                    yield reader
                finally:
                    closed.append(reader)

    # Keep the real Arrow class intact for Lance's runtime type checks.
    arrow = SimpleNamespace(**vars(pa))
    arrow.RecordBatchReader = TrackedReaders
    monkeypatch.setattr(fragment_module, "pa", arrow)
    return closed


class ReplayableBlocks:
    def __init__(self, tables: list[pa.Table]) -> None:
        self.tables = tables
        self.closed = 0

    def __iter__(self) -> Iterator[pa.Table]:
        try:
            yield from self.tables
        finally:
            self.closed += 1


@pytest.mark.parametrize("input_kind", ["list", "tuple", "iterable", "factory"])
@pytest.mark.parametrize("failure_after_batches", [1, 4])
@pytest.mark.parametrize("failures", [1, 2])
@pytest.mark.usefixtures("no_replay_staging")
def test_write_fragment_retry_replays_complete_input(
    monkeypatch: pytest.MonkeyPatch,
    closed_readers: list[pa.RecordBatchReader],
    input_kind: str,
    failure_after_batches: int,
    failures: int,
) -> None:
    tables = [pa.table({"id": [i]}) for i in range(4)]
    replayable = ReplayableBlocks(tables)
    stream: FragmentStream | FragmentStreamFactory
    streams: dict[str, FragmentStream | FragmentStreamFactory] = {
        "list": tables,
        "tuple": tuple(tables),
        "iterable": replayable,
        "factory": replayable.__iter__,
    }
    stream = streams[input_kind]
    attempts: list[list[int]] = []
    readers: list[pa.RecordBatchReader] = []

    def failing_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        readers.append(reader)
        assert closed_readers == readers[:-1]
        ids: list[int] = []
        attempts.append(ids)
        for batch in reader:
            ids.extend(cast("list[int]", batch.column("id").to_pylist()))
            if len(attempts) <= failures and len(ids) == failure_after_batches:
                raise RuntimeError("LanceError(IO): injected write failure")
        return [FragmentMetadata(id=0, files=[], physical_rows=len(ids))]

    monkeypatch.setattr("lance.fragment.write_fragments", failing_write)
    params = {
        "description": "write",
        "match": ["LanceError(IO)"],
        "max_attempts": failures + 1,
        "max_backoff_s": 0,
    }
    original_params = params.copy()
    result = write_fragment(stream, "memory://retry", retry_params=params)
    assert attempts == [list(range(failure_after_batches))] * failures + [
        list(range(4))
    ]
    assert len({id(reader) for reader in readers}) == failures + 1
    assert closed_readers == readers
    assert sum(fragment.num_rows for fragment, _ in result) == 4
    assert params == original_params
    if input_kind in {"iterable", "factory"}:
        assert replayable.closed == failures + 1


@pytest.mark.parametrize("empty", [False, True])
def test_write_fragment_rejects_iterator_before_consumption(empty: bool) -> None:
    consumed = False

    def blocks() -> Generator[pa.Table, None, None]:
        nonlocal consumed
        consumed = True
        if not empty:
            yield pa.table({"id": [1]})

    source = blocks()
    with pytest.raises(TypeError, match="stream factory.*one-shot Iterator"):
        write_fragment(
            source,
            "memory://rejected",
            retry_params={"description": "write", "max_attempts": 2},
        )
    assert not consumed
    source.close()


@pytest.mark.parametrize("max_attempts", [1, 2])
@pytest.mark.parametrize("result_kind", ["early", "short", "long"])
@pytest.mark.usefixtures("no_replay_staging")
def test_write_fragment_rejects_incomplete_result(
    monkeypatch: pytest.MonkeyPatch,
    closed_readers: list[pa.RecordBatchReader],
    max_attempts: int,
    result_kind: str,
) -> None:
    generated: list[int] = []
    calls = 0

    def blocks() -> Generator[pa.Table, None, None]:
        for i in range(4):
            generated.append(i)
            yield pa.table({"id": [i]})

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

    monkeypatch.setattr("lance.fragment.write_fragments", incomplete_write)
    wrote = {"early": 1, "short": 3, "long": 5}[result_kind]
    with pytest.raises(RuntimeError, match=f"expected 4, wrote {wrote}"):
        write_fragment(
            blocks,
            "memory://incomplete",
            retry_params={
                "description": "write",
                "max_attempts": max_attempts,
                "max_backoff_s": 0,
            },
        )
    assert calls == 1  # Row-count mismatches are outside retry, even with match=None.
    assert generated == list(range(4))
    assert len(closed_readers) == 1


@pytest.mark.parametrize("as_factory", [False, True])
@pytest.mark.parametrize(
    "retry_params",
    [None, {"description": "write"}, {"description": "write", "max_attempts": 1}],
)
@pytest.mark.usefixtures("no_replay_staging")
def test_write_fragment_single_attempt_remains_streaming(
    monkeypatch: pytest.MonkeyPatch,
    as_factory: bool,
    retry_params: Optional[dict[str, Any]],
) -> None:
    generated: list[int] = []
    factory_calls = 0

    def blocks() -> Generator[pa.Table, None, None]:
        nonlocal factory_calls
        factory_calls += 1
        for value in range(4):
            generated.append(value)
            yield pa.table({"id": [value]})

    def streaming_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        assert generated == [0]  # Only schema inference may look ahead.
        for value in range(4):
            assert next(reader).column("id").to_pylist() == [value]
            assert generated == list(range(value + 1))
        return [FragmentMetadata(id=0, files=[], physical_rows=4)]

    monkeypatch.setattr("lance.fragment.write_fragments", streaming_write)
    result = write_fragment(
        blocks if as_factory else blocks(),
        "memory://streaming",
        retry_params=retry_params,
    )
    assert result[0][0].num_rows == 4
    assert factory_calls == 1


@pytest.mark.parametrize("failure_after_batches", [1, 4])
def test_write_fragment_omitted_attempts_does_not_retry(
    monkeypatch: pytest.MonkeyPatch,
    failure_after_batches: int,
) -> None:
    generated: list[int] = []
    closed: list[bool] = []
    error = RuntimeError("LanceError(IO): injected failure")
    params = {"description": "write", "match": ["LanceError(IO)"], "max_backoff_s": 0}
    original_params = params.copy()
    calls = 0

    def blocks() -> Generator[pa.Table, None, None]:
        try:
            for i in range(4):
                generated.append(i)
                yield pa.table({"id": [i]})
        finally:
            closed.append(True)

    def failing_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        for _ in range(failure_after_batches):
            next(reader)
        raise error

    monkeypatch.setattr("lance.fragment.write_fragments", failing_write)
    source = blocks()
    with pytest.raises(RuntimeError) as exc_info:
        write_fragment(source, "memory://failure", retry_params=params)
    assert exc_info.value is error
    assert calls == 1
    assert generated == list(range(failure_after_batches))
    assert params == original_params
    assert closed == []  # A directly supplied iterator belongs to the caller.
    source.close()
    assert closed == [True]


@pytest.mark.parametrize(
    ("message", "attempts"),
    [("LanceError(IO): injected failure", 3), ("invalid write argument", 1)],
)
def test_write_fragment_failure_closes_attempt_resources(
    monkeypatch: pytest.MonkeyPatch,
    closed_readers: list[pa.RecordBatchReader],
    message: str,
    attempts: int,
) -> None:
    replayable = ReplayableBlocks([pa.table({"id": [0]}), pa.table({"id": [1]})])
    error = RuntimeError(message)
    calls = 0

    def failing_write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        assert next(reader).column("id").to_pylist() == [0]
        raise error

    monkeypatch.setattr("lance.fragment.write_fragments", failing_write)
    with pytest.raises(RuntimeError) as exc_info:
        write_fragment(
            replayable,
            "memory://failure",
            retry_params={
                "description": "write",
                "match": ["LanceError(IO)"],
                "max_attempts": 3,
                "max_backoff_s": 0,
            },
        )
    assert exc_info.value is error
    assert calls == attempts
    assert replayable.closed == attempts
    assert len(closed_readers) == attempts


@pytest.mark.parametrize("failure", ["factory", "first", "input", "conversion"])
def test_write_fragment_input_failure(
    monkeypatch: pytest.MonkeyPatch,
    closed_readers: list[pa.RecordBatchReader],
    failure: str,
) -> None:
    from lance_ray.pandas import pd_to_arrow

    error = ValueError("invalid input")
    closed: list[bool] = []
    factory_calls = 0

    def blocks() -> Generator[pa.Table, None, None]:
        try:
            if failure == "first":
                raise error
            yield pa.table({"id": [0]})
            if failure == "input":
                raise error
            yield pa.table({"id": [1]})
        finally:
            closed.append(True)

    def factory() -> Iterator[pa.Table]:
        nonlocal factory_calls
        factory_calls += 1
        if failure == "factory":
            raise error
        return blocks()

    def convert(block: FragmentBlock, schema: Optional[pa.Schema]) -> pa.Table:
        if failure == "conversion":
            raise error
        return pd_to_arrow(block, schema)

    def consume(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        list(reader)
        raise AssertionError("invalid input should not complete")

    monkeypatch.setattr("lance_ray.fragment.pd_to_arrow", convert)
    monkeypatch.setattr("lance.fragment.write_fragments", consume)
    with pytest.raises(ValueError) as exc_info:
        write_fragment(
            factory,
            "memory://failure",
            retry_params={
                "description": "write",
                "max_attempts": 2,
                "match": ["LanceError(IO)"],
                "max_backoff_s": 0,
            },
        )
    assert exc_info.value is error
    assert factory_calls == 1
    assert closed == ([] if failure == "factory" else [True])
    assert len(closed_readers) == (1 if failure in {"input", "conversion"} else 0)


@pytest.mark.parametrize("empty_schema", [False, True])
def test_write_fragment_closes_empty_factory(
    monkeypatch: pytest.MonkeyPatch,
    empty_schema: bool,
) -> None:
    closed: list[bool] = []
    calls = 0

    def blocks() -> Generator[pa.Table, None, None]:
        nonlocal calls
        calls += 1
        try:
            if empty_schema:
                yield pa.table({})
        finally:
            closed.append(True)

    def unexpected_write(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("empty input must not reach the writer")

    monkeypatch.setattr("lance.fragment.write_fragments", unexpected_write)
    assert (
        write_fragment(
            blocks,
            "memory://empty",
            retry_params={
                "description": "write",
                "max_attempts": 2,
                "max_backoff_s": 0,
            },
        )
        == []
    )
    assert calls == 1
    assert closed == [True]


def test_write_fragment_retries_factory_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def factory() -> Iterator[pa.Table]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient source error")
        return iter([pa.table({"id": [0, 1]})])

    def write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        assert reader.read_all()["id"].to_pylist() == [0, 1]
        return [FragmentMetadata(id=0, files=[], physical_rows=2)]

    monkeypatch.setattr("lance.fragment.write_fragments", write)
    result = write_fragment(
        factory,
        "memory://factory-retry",
        retry_params={
            "description": "write",
            "match": ["transient source error"],
            "max_attempts": 2,
            "max_backoff_s": 0,
        },
    )
    assert calls == 2
    assert result[0][0].num_rows == 2


@pytest.mark.parametrize("explicit_schema", [False, True])
def test_write_fragment_recreates_schema_and_counter(
    monkeypatch: pytest.MonkeyPatch,
    explicit_schema: bool,
) -> None:
    # Different integer widths expose stale inferred schemas, while logical IDs agree.
    schemas = [pa.schema([("id", pa.int32())]), pa.schema([("id", pa.int64())])]
    calls = 0
    factories = 0

    def factory() -> Iterator[pa.Table]:
        nonlocal factories
        schema = schemas[factories]
        factories += 1
        return iter([pa.table({"id": [0, 1]}, schema=schema)])

    def write(
        reader: pa.RecordBatchReader, _uri: str, **kwargs: Any
    ) -> list[FragmentMetadata]:
        nonlocal calls
        expected = schemas[1] if explicit_schema else schemas[calls]
        assert reader.schema == kwargs["schema"] == expected
        assert reader.read_all()["id"].to_pylist() == [0, 1]
        calls += 1
        if calls == 1:
            raise OSError("retry")
        return [FragmentMetadata(id=0, files=[], physical_rows=2)]

    monkeypatch.setattr("lance.fragment.write_fragments", write)
    result = write_fragment(
        factory,
        "memory://schema",
        schema=schemas[1] if explicit_schema else None,
        retry_params={"description": "write", "max_attempts": 2, "max_backoff_s": 0},
    )
    assert factories == calls == 2
    assert result[0][1] == schemas[1]


@pytest.mark.parametrize("max_attempts", [1, 2])
@pytest.mark.parametrize("explicit_schema", [False, True])
@pytest.mark.parametrize(
    "input_kind", ["arrow", "pandas", "dict", "zero_rows", "empty", "no_columns"]
)
def test_write_fragment_input_compatibility(
    tmp_path: Path,
    max_attempts: int,
    explicit_schema: bool,
    input_kind: str,
) -> None:
    schema = pa.schema([pa.field("id", pa.int64())], metadata={b"source": b"test"})
    table = pa.table({"id": [0, 1, 2, 3]}, schema=schema)
    blocks: list[FragmentBlock]
    if input_kind == "pandas":
        blocks = [table.to_pandas()]
    elif input_kind == "dict":
        blocks = [table.to_pydict()]
    elif input_kind == "zero_rows":
        blocks = [table.slice(0, 0)]
    elif input_kind == "empty":
        blocks = []
    elif input_kind == "no_columns":
        blocks = [pa.table({})]
        schema = pa.schema([])
    else:
        blocks = [table]
    result = write_fragment(
        blocks,
        str(tmp_path / "input.lance"),
        schema=schema if explicit_schema else None,
        retry_params={"description": "write", "max_attempts": max_attempts},
    )
    expected_rows = 0 if input_kind in {"zero_rows", "empty", "no_columns"} else 4
    assert sum(fragment.num_rows for fragment, _ in result) == expected_rows
    if explicit_schema:
        assert all(s.equals(schema, check_metadata=True) for _, s in result)


@pytest.mark.parametrize("max_attempts", [1, 10])
def test_datasink_reuses_block_objects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    max_attempts: int,
) -> None:
    tables = [pa.table({"id": [i]}) for i in range(4)]
    generated: list[int] = []

    def blocks() -> Generator[pa.Table, None, None]:
        for i, table in enumerate(tables):
            generated.append(i)
            yield table

    source = blocks()

    def write(
        stream: FragmentStream | FragmentStreamFactory, _uri: str, **kwargs: Any
    ) -> list[tuple[FragmentMetadata, pa.Schema]]:
        assert kwargs["retry_params"]["max_attempts"] == max_attempts
        if max_attempts == 1:
            assert stream is source
            assert generated == []
        else:
            assert generated == list(range(4))
            assert callable(stream)
            for _ in range(2):
                assert all(a is b for a, b in zip(stream(), tables, strict=True))
        return []

    monkeypatch.setattr("lance_ray.datasink.write_fragment", write)
    sink = LanceDatasink(str(tmp_path))
    if max_attempts == 1:
        sink._retry_params["max_attempts"] = 1
    assert sink.write(source, None) == []
    source.close()


@pytest.mark.parametrize("output_kind", ["table", "list", "generator"])
@pytest.mark.usefixtures("no_replay_staging")
def test_fragment_writer_repeats_transform(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    output_kind: str,
) -> None:
    original = pa.table({"id": [0, 1, 2, 3]})
    transforms: list[pa.Table] = []
    calls = 0

    def transform(table: pa.Table) -> pa.Table | Iterable[pa.Table]:
        transforms.append(table)
        if output_kind == "table":
            return table
        blocks = [table.slice(i, 1) for i in range(4)]
        return blocks if output_kind == "list" else (block for block in blocks)

    def write(
        reader: pa.RecordBatchReader, _uri: str, **_kwargs: Any
    ) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        if calls == 1:
            next(reader)
            raise OSError("retry")
        assert reader.read_all()["id"].to_pylist() == [0, 1, 2, 3]
        return [FragmentMetadata(id=0, files=[], physical_rows=4)]

    monkeypatch.setattr("lance.fragment.write_fragments", write)
    writer = LanceFragmentWriter(
        str(tmp_path),
        transform=transform,
        use_legacy_format=None,
        retry_params={"description": "write", "max_attempts": 2, "max_backoff_s": 0},
    )
    result = writer(original)
    assert result.num_rows == 1
    assert len(transforms) == calls == 2
    assert all(table is original for table in transforms)


@pytest.mark.parametrize("failure_after_batches", [5, 10])
@pytest.mark.usefixtures("no_replay_staging")
def test_write_fragment_retry_commits_all_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_after_batches: int,
) -> None:
    import lance.fragment as lance_fragment

    real_write = lance_fragment.write_fragments
    attempts: list[int] = []
    uri = str(tmp_path / "retry.lance")
    schema = pa.schema([pa.field("id", pa.int64())])

    def injected_write(
        reader: pa.RecordBatchReader,
        uri: str,
        *,
        return_transaction: Literal[False] = False,
        **kwargs: Any,
    ) -> list[FragmentMetadata]:
        if not attempts:
            table = pa.Table.from_batches(
                [next(reader) for _ in range(failure_after_batches)]
            )
            partial = real_write(
                table, uri, return_transaction=return_transaction, **kwargs
            )
            attempts.append(sum(fragment.num_rows for fragment in partial))
            raise RuntimeError("LanceError(IO): injected after partial file write")
        result = real_write(
            reader, uri, return_transaction=return_transaction, **kwargs
        )
        attempts.append(sum(fragment.num_rows for fragment in result))
        return result

    def blocks() -> Generator[pa.Table, None, None]:
        for i in range(10):
            yield pa.table({"id": range(i * 4_096, (i + 1) * 4_096)}, schema=schema)

    monkeypatch.setattr(lance_fragment, "write_fragments", injected_write)
    fragments = write_fragment(
        blocks,
        uri,
        schema=schema,
        retry_params={
            "description": "write",
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


@pytest.mark.parametrize("entry_point", ["write_lance", "fragment_writer"])
def test_retry_through_ray_commits_all_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    entry_point: str,
) -> None:
    @contextmanager
    def _fail_first_real_write() -> Iterator[list[int]]:
        """Install failure injection inside the worker executing the write."""
        import lance.fragment as lance_fragment

        real_write = lance_fragment.write_fragments
        attempts: list[int] = []

        def injected_write(
            reader: pa.RecordBatchReader,
            uri: str,
            *,
            return_transaction: Literal[False] = False,
            **kwargs: Any,
        ) -> list[FragmentMetadata]:
            if not attempts:
                # Slice even a single-batch input to leave a genuinely partial file.
                partial_input = next(reader).slice(0, 1)
                partial = real_write(
                    partial_input, uri, return_transaction=return_transaction, **kwargs
                )
                attempts.append(sum(fragment.num_rows for fragment in partial))
                raise RuntimeError("LanceError(IO): injected after partial file write")
            result = real_write(
                reader, uri, return_transaction=return_transaction, **kwargs
            )
            attempts.append(sum(fragment.num_rows for fragment in result))
            return result

        with patch.object(lance_fragment, "write_fragments", injected_write):
            yield attempts
        # Avoid pytest's assertion rewriting in code serialized to workers,
        # which may only have the runtime dependencies installed.
        if len(attempts) != 2 or attempts[0] != 1 or attempts[1] <= attempts[0]:
            raise AssertionError(
                f"Expected a partial write then a full retry: {attempts}"
            )

    class FailingOnceDatasink(LanceDatasink):
        WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 0

        def write(
            self, blocks: Iterable[pa.Table | pd.DataFrame], ctx: Any
        ) -> list[tuple[bytes, bytes]]:
            with _fail_first_real_write():
                return super().write(blocks, ctx)

    class FailingOnceFragmentWriter(LanceFragmentWriter):
        def __call__(self, batch: FragmentBlock) -> pa.Table:
            with _fail_first_real_write():
                return super().__call__(batch)

    uri = str(tmp_path / "ray-retry.lance")
    data = ray.data.range(40_960, override_num_blocks=2)
    if entry_point == "write_lance":
        # The driver sends the subclass to Ray; its override patches inside each worker.
        monkeypatch.setattr(lr, "LanceDatasink", FailingOnceDatasink)
        lr.write_lance(data, uri, min_rows_per_file=20_480)
    else:

        def transform(table: pa.Table) -> Iterator[pa.Table]:
            for i in range(0, table.num_rows, 4_096):
                yield table.slice(i, 4_096)

        writer = FailingOnceFragmentWriter(
            uri,
            transform=transform,
            use_legacy_format=None,
            retry_params={
                "description": "write",
                "match": ["LanceError(IO)"],
                "max_attempts": 2,
                "max_backoff_s": 0,
            },
        )
        data.map_batches(
            writer, batch_format="pyarrow", batch_size=20_480
        ).write_datasink(LanceFragmentCommitter(uri))
    dataset = lance.dataset(uri)
    assert dataset.count_rows() == 40_960
    # Ray task completion order is not guaranteed; compare the complete ID multiset.
    assert sorted(cast("list[int]", dataset.to_table()["id"].to_pylist())) == list(
        range(40_960)
    )


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


@pytest.mark.parametrize("target_all_bases", [True, False])
def test_target_bases_and_target_all_bases_are_mutually_exclusive(
    tmp_path: Path, target_all_bases: bool
) -> None:
    uri = str(tmp_path)
    target_bases = ["archive"]
    match = "'target_bases' and 'target_all_bases' are mutually exclusive"

    with pytest.raises(ValueError, match=match):
        LanceFragmentWriter(
            uri,
            data_storage_version="stable",
            target_bases=target_bases,
            target_all_bases=target_all_bases,
        )
    with pytest.raises(ValueError, match=match):
        LanceDatasink(uri, target_bases=target_bases, target_all_bases=target_all_bases)
    for stream in [False, True]:
        with pytest.raises(ValueError, match=match):
            lr.write_lance(
                cast(Any, object()),
                uri,
                target_bases=target_bases,
                target_all_bases=target_all_bases,
                stream=stream,
            )
    with pytest.raises(ValueError, match=match):
        write_fragment(
            [pa.table({"id": [1]})],
            uri,
            target_bases=target_bases,
            target_all_bases=target_all_bases,
        )


@pytest.mark.parametrize("target_bases", [None, []])
@pytest.mark.parametrize("target_all_bases", [None, True, False])
def test_fragment_writer_target_all_bases_forwarded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target_bases: Optional[list[str]],
    target_all_bases: Optional[bool],
) -> None:
    import lance.fragment as lance_fragment

    omitted = object()
    received: list[tuple[Optional[list[str]], object]] = []

    def capture(
        reader: pa.RecordBatchReader,
        _uri: str,
        *,
        target_bases: Optional[list[str]] = None,
        target_all_bases: object = omitted,
        **_kwargs: Any,
    ) -> list[FragmentMetadata]:
        received.append((target_bases, target_all_bases))
        rows = sum(batch.num_rows for batch in reader)
        return [FragmentMetadata(id=0, files=[], physical_rows=rows)]

    monkeypatch.setattr(lance_fragment, "write_fragments", capture)
    writer = LanceFragmentWriter(
        str(tmp_path),
        data_storage_version="stable",
        target_bases=target_bases,
        target_all_bases=target_all_bases,
    )
    writer(pa.table({"id": [1]}))

    assert len(received) == 1
    assert received[0][0] == target_bases
    assert received[0][1] is (omitted if target_all_bases is None else target_all_bases)


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


@pytest.mark.parametrize("max_attempts", [1, 2])
def test_write_fragment_ingest_warning_points_to_caller(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    max_attempts: int,
) -> None:
    calls = 0

    def write(
        reader: pa.RecordBatchReader,
        _uri: str,
        *,
        external_blob_mode: str = "reference",
        allow_external_blob_outside_bases: bool = False,
        **_kwargs: Any,
    ) -> list[FragmentMetadata]:
        nonlocal calls
        calls += 1
        assert external_blob_mode == "ingest"
        assert allow_external_blob_outside_bases is False
        rows = sum(batch.num_rows for batch in reader)
        if calls < max_attempts:
            raise OSError("retry write")
        return [FragmentMetadata(id=0, files=[], physical_rows=rows)]

    monkeypatch.setattr("lance.fragment.write_fragments", write)
    with pytest.warns(UserWarning, match="will be ignored") as caught:
        call_lineno = inspect.stack()[0].lineno + 1
        write_fragment(
            [pa.table({"id": [0]})],
            str(tmp_path / "warning.lance"),
            external_blob_mode="ingest",
            allow_external_blob_outside_bases=True,
            retry_params={
                "description": "write",
                "match": ["retry write"],
                "max_attempts": max_attempts,
                "max_backoff_s": 0,
            },
        )

    assert calls == max_attempts
    assert len(caught) == max_attempts
    for warning in caught:
        assert warning.filename == __file__
        assert warning.lineno == call_lineno


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
