"""Tests for distributed vector index option handling."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import numpy as np
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
        if column != "value":
            raise KeyError(column)
        return _FakeLanceField()


class _FakeSchema:
    def field(self, column):
        if column == "vector":
            return _FakeField(column)
        if column == "value":
            return _FakeField(column, index_mod.pa.int64())
        else:
            raise KeyError(column)

    def __iter__(self):
        return iter(
            [
                _FakeField("vector"),
                _FakeField("value", index_mod.pa.int64()),
            ]
        )


class _FakeFragment:
    def __init__(self, fragment_id, rows):
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self):
        return self._rows


class _FakeSegmentBuilder:
    def __init__(self):
        self.build_all_calls = 0
        self.index_type = None
        self.segments = None

    def with_index_type(self, index_type):
        self.index_type = index_type
        return self

    def with_segments(self, segments):
        self.segments = segments
        return self

    def build_all(self):
        self.build_all_calls += 1
        return ["merged_segment"]


class _FakeDataset:
    uri = "memory://fake"
    schema = _FakeSchema()
    lance_schema = _FakeLanceSchema()
    version = 1

    def __init__(self):
        self.segment_builder_calls = 0
        self.last_segment_builder = None
        self.commit_kwargs = None

    def get_fragments(self):
        return [_FakeFragment(0, 100), _FakeFragment(1, 100)]

    def count_rows(self):
        return 200

    def list_indices(self):
        return []

    def create_scalar_index(self, **kwargs):
        self.scalar_index_kwargs = kwargs

    def create_index_segment_builder(self):
        self.segment_builder_calls += 1
        self.last_segment_builder = _FakeSegmentBuilder()
        return self.last_segment_builder

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

    monkeypatch.setattr(index_mod, "Pool", FakePool)

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


def test_create_index_uses_provided_vector_training_artifacts(monkeypatch):
    """Provided IVF/PQ artifacts should be reused without driver-side training."""

    captured = {}
    fake_dataset = _FakeDataset()

    class FailingIndicesBuilder:
        def __init__(self, dataset, column):
            raise AssertionError("provided artifacts should skip training")

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
    monkeypatch.setattr(index_mod, "IndicesBuilder", FailingIndicesBuilder)
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
        uri="memory://fake",
        column="vector",
        index_type="IVF_PQ",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        num_sub_vectors=4,
        ivf_centroids="provided_ivf",
        pq_codebook="provided_pq",
    )

    assert updated_dataset is fake_dataset
    assert captured["put_artifacts"] == ("provided_ivf", "provided_pq")
    assert captured["fragment_handler_kwargs"]["ivf_centroids"] == "ivf_ref"
    assert captured["fragment_handler_kwargs"]["pq_codebook"] == "pq_ref"


def test_create_index_uses_distributed_vector_training(monkeypatch):
    """Distributed training mode should produce shared artifacts before workers build."""

    captured = {}
    fake_dataset = _FakeDataset()

    class FailingIndicesBuilder:
        def __init__(self, dataset, column):
            raise AssertionError(
                "distributed mode should not use driver IndicesBuilder"
            )

    def fake_distributed_training(**kwargs):
        captured["distributed_training"] = kwargs
        return "dist_ivf", "dist_pq", 4, 2

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
    monkeypatch.setattr(index_mod, "IndicesBuilder", FailingIndicesBuilder)
    monkeypatch.setattr(index_mod, "LanceDataset", lambda *args, **kwargs: fake_dataset)
    monkeypatch.setattr(
        index_mod,
        "_train_vector_index_artifacts_distributed",
        fake_distributed_training,
    )
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
        num_sub_vectors=2,
        sample_rate=8,
        training_mode="distributed",
    )

    assert updated_dataset is fake_dataset
    assert captured["distributed_training"]["dataset_uri"] == "memory://fake"
    assert captured["distributed_training"]["index_type"] == "IVF_PQ"
    assert captured["distributed_training"]["num_workers"] == 2
    assert sorted(sum(captured["distributed_training"]["fragment_batches"], [])) == [
        0,
        1,
    ]
    assert captured["distributed_training"]["sample_rate"] == 8
    assert captured["put_artifacts"] == ("dist_ivf", "dist_pq")
    assert captured["fragment_handler_kwargs"]["ivf_centroids"] == "ivf_ref"
    assert captured["fragment_handler_kwargs"]["pq_codebook"] == "pq_ref"


def test_create_index_distributed_training_reuses_provided_ivf(monkeypatch):
    """Distributed PQ training should preserve caller-provided IVF centroids."""

    captured = {}
    fake_dataset = _FakeDataset()

    class FailingIndicesBuilder:
        def __init__(self, dataset, column):
            raise AssertionError(
                "distributed mode should not use driver IndicesBuilder"
            )

    def fake_distributed_training(**kwargs):
        captured["distributed_training"] = kwargs
        return kwargs["ivf_centroids"], "dist_pq", 4, 2

    def fake_handle_vector_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_put_vector_index_artifacts(ivf_centroids, pq_codebook):
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", "pq_ref"

    def fake_map_async_with_pool(**kwargs):
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [0, 1],
                "segment_index": "segment",
            }
        ]

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(index_mod, "IndicesBuilder", FailingIndicesBuilder)
    monkeypatch.setattr(index_mod, "LanceDataset", lambda *args, **kwargs: fake_dataset)
    monkeypatch.setattr(
        index_mod,
        "_train_vector_index_artifacts_distributed",
        fake_distributed_training,
    )
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
        num_sub_vectors=2,
        ivf_centroids="provided_ivf",
        training_mode="distributed",
    )

    assert updated_dataset is fake_dataset
    assert captured["distributed_training"]["ivf_centroids"] == "provided_ivf"
    assert captured["distributed_training"]["pq_codebook"] is None
    assert captured["put_artifacts"] == ("provided_ivf", "dist_pq")
    assert captured["fragment_handler_kwargs"]["ivf_centroids"] == "ivf_ref"
    assert captured["fragment_handler_kwargs"]["pq_codebook"] == "pq_ref"


def test_distributed_training_uses_shard_actors_and_pq_sample_size(monkeypatch):
    """Distributed training should keep samples in actors and size PQ sampling."""

    samples_by_fragment = {
        (0,): np.asarray(
            [[0.0, 0.0, 0.0, 0.0], [0.2, 0.1, 0.0, 0.0]], dtype=np.float32
        ),
        (1,): np.asarray(
            [[10.0, 10.0, 10.0, 10.0], [10.2, 10.1, 10.0, 10.0]],
            dtype=np.float32,
        ),
    }
    requested_sample_sizes = []

    class FakeDataset:
        def count_rows(self):
            return 4

    class FakeResultRef:
        def __init__(self, value):
            self.value = value

    class FakeActorMethod:
        def __init__(self, fn):
            self.fn = fn

        def remote(self, *args):
            return FakeResultRef(self.fn(*args))

    class FakeActorHandle:
        def __init__(self, actor):
            self._actor = actor

        def __getattr__(self, name):
            attr = getattr(self._actor, name)
            if callable(attr):
                return FakeActorMethod(attr)
            return attr

    class FakeRemoteActorClass:
        def __init__(self, actor_cls):
            self.actor_cls = actor_cls

        def options(self, **kwargs):
            return self

        def remote(self, *args):
            return FakeActorHandle(self.actor_cls(*args))

    def fake_remote(target):
        if target is not index_mod._VectorTrainingShard:
            raise AssertionError("distributed training should use shard actors")
        return FakeRemoteActorClass(target)

    def fake_load_vector_training_sample(
        dataset_uri,
        column,
        fragment_ids,
        sample_size,
        storage_options,
        block_size,
        namespace_impl,
        namespace_properties,
        table_id,
    ):
        requested_sample_sizes.append(sample_size)
        return samples_by_fragment[tuple(fragment_ids)]

    def fake_get(value):
        if isinstance(value, list):
            return [item.value for item in value]
        return value.value

    fake_ray = SimpleNamespace(
        remote=fake_remote,
        get=fake_get,
        ObjectRef=type("FakeObjectRef", (), {}),
    )

    monkeypatch.setattr(index_mod, "ray", fake_ray)
    monkeypatch.setattr(
        index_mod,
        "_load_vector_training_sample",
        fake_load_vector_training_sample,
    )
    monkeypatch.setattr(
        index_mod, "LanceDataset", lambda *args, **kwargs: FakeDataset()
    )

    ivf_centroids, pq_codebook, num_partitions, num_sub_vectors = (
        index_mod._train_vector_index_artifacts_distributed(
            dataset_uri="memory://fake",
            column="vector",
            index_type="IVF_PQ",
            metric="l2",
            num_partitions=2,
            num_sub_vectors=2,
            sample_rate=4,
            fragment_batches=[[0], [1]],
            num_workers=2,
            storage_options={},
            block_size=None,
            namespace_impl=None,
            namespace_properties=None,
            table_id=None,
            ray_remote_args=None,
            max_iters=1,
            tolerance=0.0,
        )
    )

    assert len(ivf_centroids) == 2
    assert len(pq_codebook) == 512
    assert num_partitions == 2
    assert num_sub_vectors == 2
    assert requested_sample_sizes == [512, 512]


def test_create_index_rejects_unknown_vector_training_mode(monkeypatch):
    """Vector training mode should be explicit and validated before training."""

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(
        index_mod, "LanceDataset", lambda *args, **kwargs: _FakeDataset()
    )

    with pytest.raises(ValueError, match="training_mode must be one of"):
        index_mod.create_index(
            uri="memory://fake",
            column="vector",
            index_type="IVF_FLAT",
            training_mode="elsewhere",
        )


def test_distributed_training_rejects_unsupported_metric():
    """Distributed KMeans currently supports only l2 and cosine distances."""

    with pytest.raises(
        ValueError,
        match="distributed vector training currently supports metric 'l2' or 'cosine'",
    ):
        index_mod._train_vector_index_artifacts_distributed(
            dataset_uri="memory://fake",
            column="vector",
            index_type="IVF_FLAT",
            metric="dot",
            num_partitions=2,
            num_sub_vectors=None,
            sample_rate=4,
            fragment_batches=[[0], [1]],
            num_workers=2,
            storage_options={},
            block_size=None,
            namespace_impl=None,
            namespace_properties=None,
            table_id=None,
            ray_remote_args=None,
        )


def test_vector_training_sample_uses_reservoir_sampling(monkeypatch):
    """Worker-side sampling should not only use the beginning of each shard."""

    vectors = np.arange(20, dtype=np.float32).reshape(10, 2)
    vector_values = index_mod.pa.array(vectors.reshape(-1), type=index_mod.pa.float32())
    vector_array = index_mod.pa.FixedSizeListArray.from_arrays(vector_values, 2)
    batches = [
        index_mod.pa.record_batch([vector_array.slice(0, 2)], names=["vector"]),
        index_mod.pa.record_batch([vector_array.slice(2, 4)], names=["vector"]),
        index_mod.pa.record_batch([vector_array.slice(6, 4)], names=["vector"]),
    ]

    class FakeScanner:
        def to_batches(self):
            return iter(batches)

    class FakeDataset:
        def get_fragment(self, fragment_id):
            return SimpleNamespace(fragment_id=fragment_id)

        def scanner(self, columns, fragments):
            return FakeScanner()

    monkeypatch.setattr(
        index_mod, "LanceDataset", lambda *args, **kwargs: FakeDataset()
    )

    sample = index_mod._load_vector_training_sample(
        dataset_uri="memory://fake",
        column="vector",
        fragment_ids=[0],
        sample_size=4,
        storage_options={},
        block_size=None,
        namespace_impl=None,
        namespace_properties=None,
        table_id=None,
    )

    assert sample.shape == (4, 2)
    assert sample[:, 0].max() > vectors[:4, 0].max()


def test_combine_kmeans_partials_keeps_empty_centroids():
    """Distributed KMeans reduce should not collapse empty clusters to zero."""

    previous = np.asarray(
        [
            [1.0, 1.0],
            [10.0, 10.0],
        ],
        dtype=np.float32,
    )
    partials = [
        {
            "sums": np.asarray([[4.0, 8.0], [0.0, 0.0]], dtype=np.float64),
            "counts": np.asarray([4, 0], dtype=np.int64),
            "loss": 3.5,
        }
    ]

    centroids, loss = index_mod._combine_kmeans_partials(partials, previous)

    np.testing.assert_allclose(centroids, [[1.0, 2.0], [10.0, 10.0]])
    assert loss == 3.5


def test_combine_pq_partials_keeps_empty_codewords():
    """Distributed PQ reduce should keep old codewords for empty assignments."""

    previous = np.asarray(
        [
            [[1.0, 1.0], [10.0, 10.0]],
            [[2.0, 2.0], [20.0, 20.0]],
        ],
        dtype=np.float32,
    )
    partials = [
        {
            "sums": np.asarray(
                [[[4.0, 8.0], [0.0, 0.0]], [[0.0, 0.0], [10.0, 20.0]]],
                dtype=np.float64,
            ),
            "counts": np.asarray([[4, 0], [0, 2]], dtype=np.int64),
            "loss": 2.0,
        }
    ]

    codebooks, loss = index_mod._combine_pq_partials(partials, previous)

    np.testing.assert_allclose(
        codebooks, [[[1.0, 2.0], [10.0, 10.0]], [[2.0, 2.0], [5.0, 10.0]]]
    )
    assert loss == 2.0


def test_create_index_commits_worker_segments_by_default(monkeypatch):
    """Vector index builds should directly commit worker segments by default."""

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

    def fake_handle_vector_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [0],
                "segment_index": "segment_0",
            },
            {
                "status": "success",
                "fragment_ids": [1],
                "segment_index": "segment_1",
            },
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
        lambda ivf_centroids, pq_codebook: (ivf_centroids, pq_codebook),
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset = index_mod.create_index(
        uri="memory://fake",
        column="vector",
        index_type="IVF_FLAT",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
    )

    assert updated_dataset is fake_dataset
    assert fake_dataset.segment_builder_calls == 0
    assert fake_dataset.commit_kwargs["segments"] == ["segment_0", "segment_1"]
    assert "segment_native" not in captured["fragment_handler_kwargs"]


def test_create_index_uses_driver_finalize_when_segment_native_disabled(monkeypatch):
    """Vector index builds should use driver finalize when segment-native is disabled."""

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

    def fake_handle_vector_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [0],
                "segment_index": "segment_0",
            },
            {
                "status": "success",
                "fragment_ids": [1],
                "segment_index": "segment_1",
            },
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
        lambda ivf_centroids, pq_codebook: (ivf_centroids, pq_codebook),
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset = index_mod.create_index(
        uri="memory://fake",
        column="vector",
        index_type="IVF_FLAT",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        segment_native=False,
    )

    assert updated_dataset is fake_dataset
    assert fake_dataset.segment_builder_calls == 1
    assert fake_dataset.last_segment_builder.index_type == "IVF_FLAT"
    assert fake_dataset.last_segment_builder.segments == ["segment_0", "segment_1"]
    assert fake_dataset.last_segment_builder.build_all_calls == 1
    assert fake_dataset.commit_kwargs["segments"] == ["merged_segment"]
    assert "segment_native" not in captured["fragment_handler_kwargs"]


def test_create_index_filters_vector_fragments(monkeypatch):
    """The vector path should distribute only requested fragment IDs."""

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

    def fake_handle_vector_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [1],
                "segment_index": "segment_1",
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
        lambda ivf_centroids, pq_codebook: (ivf_centroids, pq_codebook),
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset = index_mod.create_index(
        uri="memory://fake",
        column="vector",
        index_type="IVF_FLAT",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        fragment_ids=[1],
    )

    assert updated_dataset is fake_dataset
    assert captured["map_kwargs"]["fragment_batches"] == [[1]]
    assert captured["map_kwargs"]["num_workers"] == 1
    assert "fragment_ids" not in captured["fragment_handler_kwargs"]
    assert fake_dataset.commit_kwargs["segments"] == ["segment_1"]


def test_create_index_rejects_invalid_vector_fragment_ids(monkeypatch):
    """The vector path should fail before training when fragment IDs are invalid."""

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(
        index_mod, "LanceDataset", lambda *args, **kwargs: _FakeDataset()
    )

    with pytest.raises(ValueError, match=r"Fragment IDs \{99\} do not exist"):
        index_mod.create_index(
            uri="memory://fake",
            column="vector",
            index_type="IVF_FLAT",
            fragment_ids=[99],
        )


def test_create_index_rejects_empty_vector_fragment_ids(monkeypatch):
    """The vector path should reject empty fragment selection before training."""

    class FailingIndicesBuilder:
        def __init__(self, dataset, column):
            raise AssertionError("fragment_ids should be validated before training")

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(
        index_mod, "LanceDataset", lambda *args, **kwargs: _FakeDataset()
    )
    monkeypatch.setattr(index_mod, "IndicesBuilder", FailingIndicesBuilder)

    with pytest.raises(ValueError, match="fragment_ids cannot be empty"):
        index_mod.create_index(
            uri="memory://fake",
            column="vector",
            index_type="IVF_FLAT",
            fragment_ids=[],
        )


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


def test_check_pylance_version_rejects_old_vector_api(monkeypatch):
    """Vector segment APIs require the pylance version declared by the package."""

    monkeypatch.setattr(index_mod.lance, "__version__", "0.39.106", raising=False)

    with pytest.raises(RuntimeError, match=r"pylance >= 7\.0\.0b7"):
        index_mod._check_pylance_version()


def test_create_scalar_index_passes_block_size_to_loads_and_handler(monkeypatch):
    """The scalar index path should use block_size whenever it loads a dataset."""

    captured = {"loads": []}
    fake_dataset = _FakeDataset()

    def fake_lance_dataset(*args, **kwargs):
        captured["loads"].append(kwargs)
        return fake_dataset

    def fake_handle_fragment_index(**kwargs):
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {
            "status": "success",
            "fragment_ids": fragment_ids,
            "fields": [7],
        }

    def fake_map_async_with_pool(**kwargs):
        captured["map_kwargs"] = kwargs
        kwargs["create_fragment_handler"]()
        return [
            {
                "status": "success",
                "fragment_ids": [0, 1],
                "fields": [7],
            }
        ]

    monkeypatch.setattr(index_mod, "LanceDataset", fake_lance_dataset)
    monkeypatch.setattr(
        index_mod,
        "_handle_fragment_index",
        fake_handle_fragment_index,
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)
    monkeypatch.setattr(index_mod, "merge_index_metadata_compat", lambda *a, **k: None)
    monkeypatch.setattr(index_mod, "Index", SimpleNamespace)
    monkeypatch.setattr(
        index_mod.lance,
        "LanceDataset",
        SimpleNamespace(commit=lambda *args, **kwargs: fake_dataset),
        raising=False,
    )
    monkeypatch.setattr(
        index_mod.lance,
        "LanceOperation",
        SimpleNamespace(CreateIndex=SimpleNamespace),
        raising=False,
    )

    updated_dataset = index_mod.create_scalar_index(
        uri="memory://fake",
        column="value",
        index_type="BTREE",
        num_workers=2,
        block_size=4096,
    )

    assert updated_dataset is fake_dataset
    assert [load["block_size"] for load in captured["loads"]] == [4096, 4096]
    assert captured["fragment_handler_kwargs"]["block_size"] == 4096


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
        index_type="BTREE",
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
