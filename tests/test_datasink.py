import pickle

import pyarrow as pa
import pytest

from lance_ray.datasink import _BaseLanceDatasink


class _DummyDatasink(_BaseLanceDatasink):
    def get_name(self) -> str:
        return "dummy"

    def write(self, blocks, ctx):  # pragma: no cover - not used in tests
        return []


def test_collect_serialized_payloads_with_extra_metadata(monkeypatch):
    """Ensure extra fields in Ray write_result do not break payload parsing."""

    ds = _DummyDatasink(uri="dummy", schema=pa.schema([pa.field("c", pa.int64())]))
    monkeypatch.setattr(ds, "_drain_fragment_payloads", lambda: [])

    fragment = pickle.dumps({"id": 1})
    schema = pickle.dumps(pa.schema([pa.field("c", pa.int64())]))

    write_result = [[(fragment, schema, b"ignored_stats")]]

    payloads = ds._collect_serialized_payloads(write_result)

    assert payloads == [(fragment, schema)]

