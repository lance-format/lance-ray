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
    ray.ObjectRef = type("ObjectRef", (), {})
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


class _FakeField:
    def __init__(self, name, field_type=None):
        self.name = name
        self.type = field_type or index_mod.pa.float32()


class _FakeLanceField:
    def id(self):
        return 7


class _FakeLanceSchema:
    def field(self, column):
        if column not in {"value", "text"}:
            raise KeyError(column)
        return _FakeLanceField()


class _FakeSchema:
    def field(self, column):
        if column == "vector":
            return _FakeField(column)
        if column == "value":
            return _FakeField(column, index_mod.pa.int64())
        if column == "text":
            return _FakeField(column, index_mod.pa.string())
        else:
            raise KeyError(column)

    def __iter__(self):
        return iter(
            [
                _FakeField("vector"),
                _FakeField("value", index_mod.pa.int64()),
                _FakeField("text", index_mod.pa.string()),
            ]
        )


class _FakeFragment:
    def __init__(self, fragment_id, rows):
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self):
        return self._rows


class _FakeDataset:
    uri = "memory://fake"
    schema = _FakeSchema()
    lance_schema = _FakeLanceSchema()
    version = 1

    def get_fragments(self):
        return [_FakeFragment(0, 100), _FakeFragment(1, 100)]

    def count_rows(self):
        return 200

    def describe_indices(self):
        return []

    def create_scalar_index(self, **kwargs):
        self.scalar_index_kwargs = kwargs

    def create_index_uncommitted(self, **kwargs):
        self.vector_index_kwargs = kwargs
        return "segment"

    def commit_existing_index_segments(self, **kwargs):
        self.commit_kwargs = kwargs
        return self


def test_map_async_with_pool_closes_and_joins_pool(monkeypatch):
    """The Ray Pool should be joined after close so actors finish cleanup."""

    events = []

    class FakeAsyncResult:
        def get(self):
            events.append("get")
            return [{"status": "success"}]

    class FakePool:
        def __init__(self, processes, ray_remote_args):
            events.append(("init", processes, ray_remote_args))

        def map_async(self, fragment_handler, fragment_batches, chunksize):
            events.append(("map_async", fragment_batches, chunksize))
            return FakeAsyncResult()

        def close(self):
            events.append("close")

        def join(self):
            events.append("join")

    def create_fragment_handler():
        events.append("create_handler")
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    # _map_async_with_pool now goes through get_or_create_pool, which constructs
    # and closes/joins the Pool in lance_ray.pool. Patch it there and make sure
    # no global Pool is configured so the local close-and-join path is taken.
    pool_mod = sys.modules[index_mod.get_or_create_pool.__module__]
    monkeypatch.setattr(pool_mod, "_GLOBAL_POOL", None)
    monkeypatch.setattr(pool_mod, "Pool", FakePool)

    assert index_mod._map_async_with_pool(
        create_fragment_handler=create_fragment_handler,
        fragment_batches=[[0, 1]],
        num_workers=2,
        ray_remote_args={"num_cpus": 1},
        error_prefix="failed",
    ) == [{"status": "success"}]
    assert events == [
        ("init", 2, {"num_cpus": 1}),
        "create_handler",
        ("map_async", [[0, 1]], 1),
        "get",
        "close",
        "join",
    ]


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

    def fake_put_vector_index_artifacts(ivf_centroids, pq_codebook):
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", "pq_ref"

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
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
    monkeypatch.setattr(
        index_mod,
        "_put_vector_index_artifacts_in_object_store",
        fake_put_vector_index_artifacts,
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
    assert captured["put_artifacts"] == ("ivf_centroids", "pq_codebook")
    assert captured["fragment_handler_kwargs"]["ivf_centroids"] == "ivf_ref"
    assert captured["fragment_handler_kwargs"]["pq_codebook"] == "pq_ref"
    assert "sample_rate" not in captured["fragment_handler_kwargs"]
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


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


@pytest.mark.parametrize("index_type", ["BTREE", "BITMAP", "INVERTED", "FTS"])
def test_create_scalar_index_uses_segment_path(monkeypatch, index_type):
    """Migrated scalar indexes should use Lance's segment workflow."""

    captured = {"loads": []}
    fake_dataset = _FakeDataset()

    def fake_lance_dataset(*args, **kwargs):
        captured["loads"].append(kwargs)
        return fake_dataset

    def fake_handle_scalar_segment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {
            "status": "success",
            "fragment_ids": fragment_ids,
            "segment_index": "segment",
        }

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [0, 1],
                "segment_index": "segment",
            }
        ]

    monkeypatch.setattr(index_mod, "LanceDataset", fake_lance_dataset)
    monkeypatch.setattr(
        index_mod,
        "_handle_scalar_segment_index",
        fake_handle_scalar_segment_index,
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    column = "text" if index_type in {"INVERTED", "FTS"} else "value"
    updated_dataset = index_mod.create_scalar_index(
        uri="memory://fake",
        column=column,
        index_type=index_type,
        num_workers=2,
        block_size=4096,
    )

    assert updated_dataset is fake_dataset
    assert [load["block_size"] for load in captured["loads"]] == [4096, 4096]
    assert captured["fragment_handler_kwargs"]["index_type"] == index_type
    assert captured["fragment_handler_kwargs"]["block_size"] == 4096
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


def test_create_index_passes_block_size_to_loads_and_handler(monkeypatch):
    """The vector index path should use block_size for driver and worker loads."""

    captured = {"loads": []}
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

    def fake_lance_dataset(*args, **kwargs):
        captured["loads"].append(kwargs)
        return fake_dataset

    def fake_handle_vector_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_put_vector_index_artifacts(ivf_centroids, pq_codebook):
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", "pq_ref"

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [0, 1],
                "segment_index": "segment",
            }
        ]

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(index_mod, "IndicesBuilder", FakeIndicesBuilder)
    monkeypatch.setattr(index_mod, "LanceDataset", fake_lance_dataset)
    monkeypatch.setattr(
        index_mod,
        "_handle_vector_fragment_index",
        fake_handle_vector_fragment_index,
    )
    monkeypatch.setattr(
        index_mod,
        "_put_vector_index_artifacts_in_object_store",
        fake_put_vector_index_artifacts,
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset = index_mod.create_index(
        uri="memory://fake",
        column="vector",
        index_type="IVF_PQ",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        num_sub_vectors=4,
        block_size=8192,
    )

    assert updated_dataset is fake_dataset
    assert [load["block_size"] for load in captured["loads"]] == [8192, 8192]
    assert captured["fragment_handler_kwargs"]["block_size"] == 8192
    assert captured["put_artifacts"] == ("ivf_centroids", "pq_codebook")
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


def test_fragment_handlers_pass_block_size_to_dataset_load(monkeypatch):
    """Worker-side scalar and vector handlers should load datasets with block_size."""

    captured = {"loads": []}
    fake_dataset = _FakeDataset()

    def fake_lance_dataset(*args, **kwargs):
        captured["loads"].append(kwargs)
        return fake_dataset

    monkeypatch.setattr(index_mod, "LanceDataset", fake_lance_dataset)

    scalar_handler = index_mod._handle_fragment_index(
        dataset_uri="memory://fake",
        column="value",
        index_type="LABEL_LIST",
        name="value_idx",
        index_uuid="scalar-index",
        replace=False,
        train=True,
        block_size=4096,
    )
    vector_handler = index_mod._handle_vector_fragment_index(
        dataset_uri="memory://fake",
        column="vector",
        index_type="IVF_PQ",
        name="vector_idx",
        index_uuid="vector-index",
        replace=False,
        metric="l2",
        num_partitions=4,
        num_sub_vectors=4,
        ivf_centroids="ivf_centroids",
        pq_codebook="pq_codebook",
        block_size=8192,
    )

    assert scalar_handler([0])["status"] == "success"
    assert vector_handler([0])["status"] == "success"
    assert [load["block_size"] for load in captured["loads"]] == [4096, 8192]


def test_vector_fragment_handler_resolves_shared_artifact_refs(monkeypatch):
    """Workers should dereference shared training artifacts before Lance calls."""

    class FakeObjectRef:
        def __init__(self, value):
            self.value = value

    fake_dataset = _FakeDataset()
    captured = {"gets": []}

    def fake_get(ref):
        captured["gets"].append(ref)
        return ref.value

    monkeypatch.setattr(index_mod.ray, "ObjectRef", FakeObjectRef, raising=False)
    monkeypatch.setattr(index_mod.ray, "get", fake_get, raising=False)
    monkeypatch.setattr(index_mod, "LanceDataset", lambda *args, **kwargs: fake_dataset)

    ivf_ref = FakeObjectRef("ivf_centroids")
    pq_ref = FakeObjectRef("pq_codebook")
    vector_handler = index_mod._handle_vector_fragment_index(
        dataset_uri="memory://fake",
        column="vector",
        index_type="IVF_PQ",
        name="vector_idx",
        index_uuid="vector-index",
        replace=False,
        metric="l2",
        num_partitions=4,
        num_sub_vectors=4,
        ivf_centroids=ivf_ref,
        pq_codebook=pq_ref,
    )

    assert vector_handler([0])["status"] == "success"
    assert captured["gets"] == [ivf_ref, pq_ref]
    assert fake_dataset.vector_index_kwargs["ivf_centroids"] == "ivf_centroids"
    assert fake_dataset.vector_index_kwargs["pq_codebook"] == "pq_codebook"
