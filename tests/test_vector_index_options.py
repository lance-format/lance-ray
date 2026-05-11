"""Tests for distributed vector index option handling."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


def _load_index_module_with_stubs():
    """Load lance_ray.index when the native pylance extension is unavailable."""

    repo_root = Path(__file__).resolve().parents[1]
    package = ModuleType("lance_ray")
    package.__path__ = [str(repo_root / "lance_ray")]

    lance = ModuleType("lance")
    lance.__version__ = "6.0.0"

    lance_dataset = ModuleType("lance.dataset")
    lance_dataset.Index = type("Index", (), {})
    lance_dataset.IndexConfig = type("IndexConfig", (), {})
    lance_dataset.LanceDataset = object

    lance_indices = ModuleType("lance.indices")
    lance_indices.IndicesBuilder = object

    ray = ModuleType("ray")
    ray_util = ModuleType("ray.util")
    ray_multiprocessing = ModuleType("ray.util.multiprocessing")
    ray_multiprocessing.Pool = object

    sys.modules["lance_ray"] = package
    sys.modules["lance"] = lance
    sys.modules["lance.dataset"] = lance_dataset
    sys.modules["lance.indices"] = lance_indices
    sys.modules["ray"] = ray
    sys.modules["ray.util"] = ray_util
    sys.modules["ray.util.multiprocessing"] = ray_multiprocessing

    spec = importlib.util.spec_from_file_location(
        "lance_ray.index",
        repo_root / "lance_ray" / "index.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["lance_ray.index"] = module
    spec.loader.exec_module(module)
    return module


try:
    from lance_ray import index as index_mod
except ImportError:  # pragma: no cover - environment dependent
    index_mod = _load_index_module_with_stubs()


class _FakeSchema:
    def field(self, column):
        if column != "vector":
            raise KeyError(column)
        return SimpleNamespace(name=column)


class _FakeFragment:
    def __init__(self, fragment_id, rows):
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self):
        return self._rows


class _FakeSegmentBuilder:
    def with_index_type(self, index_type):
        self.index_type = index_type
        return self

    def with_segments(self, segments):
        self.segments = segments
        return self

    def build_all(self):
        return ["merged_segment"]


class _FakeDataset:
    uri = "memory://fake"
    schema = _FakeSchema()

    def get_fragments(self):
        return [_FakeFragment(0, 100), _FakeFragment(1, 100)]

    def count_rows(self):
        return 200

    def create_index_segment_builder(self):
        return _FakeSegmentBuilder()

    def commit_existing_index_segments(self, **kwargs):
        self.commit_kwargs = kwargs
        return self


def test_create_index_uses_sample_rate_for_global_training(monkeypatch):
    """The public sample_rate option should drive both IVF and PQ training."""

    captured = {}
    fake_dataset = _FakeDataset()

    class FakeIndicesBuilder:
        dimension = 16

        def __init__(self, dataset, column):
            captured["builder_dataset"] = dataset
            captured["builder_column"] = column

        def train_ivf(self, **kwargs):
            captured["train_ivf"] = kwargs
            return SimpleNamespace(centroids="ivf_centroids", num_partitions=4)

        def train_pq(self, ivf_model, **kwargs):
            captured["train_pq_ivf_model"] = ivf_model
            captured["train_pq"] = kwargs
            return SimpleNamespace(codebook="pq_codebook", num_subvectors=4)

    def fake_handle_vector_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        return [
            {
                "status": "success",
                "fragment_ids": [0, 1],
                "segment_index": "segment",
            }
        ]

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(index_mod, "IndicesBuilder", FakeIndicesBuilder)
    monkeypatch.setattr(index_mod, "LanceDataset", lambda *args, **kwargs: fake_dataset)
    monkeypatch.setattr(
        index_mod,
        "_handle_vector_fragment_index",
        fake_handle_vector_fragment_index,
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset = index_mod.create_index(
        uri=fake_dataset,
        column="vector",
        index_type="IVF_PQ",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        num_sub_vectors=4,
        sample_rate=8,
    )

    assert updated_dataset is fake_dataset
    assert captured["train_ivf"]["sample_rate"] == 8
    assert captured["train_pq"]["sample_rate"] == 8
    assert captured["fragment_handler_kwargs"]["ivf_centroids"] == "ivf_centroids"
    assert captured["fragment_handler_kwargs"]["pq_codebook"] == "pq_codebook"
    assert "sample_rate" not in captured["fragment_handler_kwargs"]


def test_create_index_rejects_non_positive_sample_rate(monkeypatch):
    """Invalid sample rates should fail before training starts."""

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)

    with pytest.raises(ValueError, match="sample_rate must be positive, got 0"):
        index_mod.create_index(
            uri=_FakeDataset(),
            column="vector",
            index_type="IVF_PQ",
            sample_rate=0,
        )
