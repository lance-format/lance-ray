"""Tests for distributed vector index option handling."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest


def _load_index_module_with_stubs() -> ModuleType:
    """Load lance_ray.index when the native pylance extension is unavailable."""

    repo_root = Path(__file__).resolve().parents[1]
    package = ModuleType("lance_ray")
    package.__path__ = [str(repo_root / "lance_ray")]

    lance = ModuleType("lance")
    lance.__version__ = "6.0.0"  # type: ignore[attr-defined]

    lance_dataset = ModuleType("lance.dataset")
    lance_dataset.Index = type("Index", (), {})  # type: ignore[attr-defined]
    lance_dataset.IndexConfig = type("IndexConfig", (), {})  # type: ignore[attr-defined]
    lance_dataset.LanceDataset = object  # type: ignore[attr-defined]

    lance_indices = ModuleType("lance.indices")
    lance_indices.IndicesBuilder = object  # type: ignore[attr-defined]

    ray = ModuleType("ray")
    ray.ObjectRef = type("ObjectRef", (), {})  # type: ignore[attr-defined]
    ray_util = ModuleType("ray.util")
    ray_multiprocessing = ModuleType("ray.util.multiprocessing")
    ray_multiprocessing.Pool = object  # type: ignore[attr-defined]

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
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["lance_ray.index"] = module
    spec.loader.exec_module(module)
    return module


try:
    from lance_ray import index as index_mod
except ImportError:  # pragma: no cover - environment dependent
    index_mod = _load_index_module_with_stubs()


class _FakeField:
    def __init__(self, name: str, field_type: Any = None) -> None:
        self.name = name
        self.type = field_type or pa.float32()


class _FakeLanceField:
    def id(self) -> int:
        return 7


class _FakeLanceSchema:
    def field(self, column: str) -> "_FakeLanceField":
        if column not in {"value", "text", "labels", "event_date"}:
            raise KeyError(column)
        return _FakeLanceField()


class _FakeSchema:
    def field(self, column: str) -> "_FakeField":
        if column == "vector":
            return _FakeField(column)
        if column == "value":
            return _FakeField(column, pa.int64())
        if column == "text":
            return _FakeField(column, pa.string())
        if column == "labels":
            return _FakeField(column, pa.list_(pa.string()))
        if column == "event_date":
            return _FakeField(column, pa.date32())
        else:
            raise KeyError(column)

    def __iter__(self) -> Any:
        return iter(
            [
                _FakeField("vector"),
                _FakeField("value", pa.int64()),
                _FakeField("text", pa.string()),
                _FakeField("labels", pa.list_(pa.string())),
                _FakeField("event_date", pa.date32()),
            ]
        )


class _FakeFragment:
    def __init__(self, fragment_id: int, rows: int) -> None:
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self) -> int:
        return self._rows


class _FakeDataset:
    uri = "memory://fake"
    schema = _FakeSchema()
    lance_schema = _FakeLanceSchema()
    version = 1

    def get_fragments(self) -> list["_FakeFragment"]:
        return [_FakeFragment(0, 100), _FakeFragment(1, 100)]

    def count_rows(self) -> int:
        return 200

    def describe_indices(self) -> list[Any]:
        return []

    def create_scalar_index(self, **kwargs: Any) -> None:
        self.scalar_index_kwargs = kwargs

    def create_index_uncommitted(self, **kwargs: Any) -> str:
        self.vector_index_kwargs = kwargs
        return "segment"

    def commit_existing_index_segments(self, **kwargs: Any) -> "_FakeDataset":
        self.commit_kwargs = kwargs
        return self


def test_map_async_with_pool_closes_and_joins_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Ray Pool should be joined after close so actors finish cleanup."""

    events: list[Any] = []

    class FakeAsyncResult:
        def get(self) -> Any:
            events.append("get")
            return [{"status": "success"}]

    class FakePool:
        def __init__(self, processes: int, ray_remote_args: Any) -> None:
            events.append(("init", processes, ray_remote_args))

        def map_async(
            self, fragment_handler: Any, fragment_batches: Any, chunksize: int
        ) -> Any:
            events.append(("map_async", fragment_batches, chunksize))
            return FakeAsyncResult()

        def close(self) -> None:
            events.append("close")

        def join(self) -> None:
            events.append("join")

    def create_fragment_handler() -> Any:
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


def test_create_index_passes_global_training_options_to_segment_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Global training options must reach driver training and segment builds."""

    captured: dict[str, Any] = {}
    fake_dataset: Any = _FakeDataset()

    class FakeIndicesBuilder:
        dimension = 16

        def __init__(self, dataset: Any, column: str) -> None:
            captured["builder_dataset"] = dataset
            captured["builder_column"] = column

        def train_ivf(self, **kwargs: Any) -> Any:
            captured["train_ivf"] = kwargs
            return SimpleNamespace(centroids="ivf_centroids", num_partitions=4)

        def train_pq(self, ivf_model: Any, **kwargs: Any) -> Any:
            captured["train_pq_ivf_model"] = ivf_model
            captured["train_pq"] = kwargs
            return SimpleNamespace(codebook="pq_codebook", num_subvectors=4)

    def fake_put_vector_index_artifacts(ivf_centroids: Any, pq_codebook: Any) -> Any:
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", "pq_ref"

    def fake_map_async_with_pool(**kwargs: Any) -> Any:
        captured["map_kwargs"] = kwargs
        fragment_handler = kwargs["create_fragment_handler"]()
        return [fragment_handler([0, 1])]

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)
    monkeypatch.setattr(index_mod, "IndicesBuilder", FakeIndicesBuilder)
    monkeypatch.setattr(index_mod, "LanceDataset", lambda *args, **kwargs: fake_dataset)
    monkeypatch.setattr(
        index_mod,
        "_put_vector_index_artifacts_in_object_store",
        fake_put_vector_index_artifacts,
    )
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset: Any = index_mod.create_index(
        uri=fake_dataset,
        column="vector",
        index_type="IVF_PQ",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        num_sub_vectors=4,
        num_bits=4,
        sample_rate=8,
        max_iters=3,
    )

    assert updated_dataset is fake_dataset
    assert captured["train_ivf"]["sample_rate"] == 8
    assert captured["train_ivf"]["max_iters"] == 3
    assert captured["train_pq"]["sample_rate"] == 8
    assert captured["train_pq"]["num_bits"] == 4
    assert captured["train_pq"]["max_iters"] == 3
    assert captured["put_artifacts"] == ("ivf_centroids", "pq_codebook")
    assert fake_dataset.vector_index_kwargs["ivf_centroids"] == "ivf_ref"
    assert fake_dataset.vector_index_kwargs["pq_codebook"] == "pq_ref"
    assert fake_dataset.vector_index_kwargs["sample_rate"] == 8
    assert fake_dataset.vector_index_kwargs["num_bits"] == 4
    assert fake_dataset.vector_index_kwargs["max_iters"] == 3
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


def test_create_index_supports_ivf_rq(monkeypatch: pytest.MonkeyPatch) -> None:
    """IVF_RQ should build and share a RaBitQ model when one is not provided."""

    captured: dict[str, Any] = {}
    fake_dataset: Any = _FakeDataset()

    class FakeIndicesBuilder:
        dimension = 16

        def __init__(self, dataset: Any, column: str) -> None:
            captured["builder_dataset"] = dataset
            captured["builder_column"] = column

        def train_ivf(self, **kwargs: Any) -> Any:
            captured["train_ivf"] = kwargs
            return SimpleNamespace(centroids="ivf_centroids", num_partitions=4)

        def train_pq(self, ivf_model: Any, **kwargs: Any) -> None:
            captured["train_pq"] = kwargs
            raise AssertionError("IVF_RQ should not train a PQ codebook")

    def fake_handle_vector_fragment_index(**kwargs: Any) -> Any:
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {
            "status": "success",
            "fragment_ids": fragment_ids,
            "segment_index": "segment",
        }

    def fake_put_vector_index_artifacts(ivf_centroids: Any, pq_codebook: Any) -> Any:
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", None

    def fake_build_rabitq_model(*, dimension: int, num_bits: int) -> str:
        captured["build_rabitq_model"] = {
            "dimension": dimension,
            "num_bits": num_bits,
        }
        return "auto-rq-model"

    def fake_map_async_with_pool(**kwargs: Any) -> Any:
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
    monkeypatch.setattr(index_mod, "_build_rabitq_model", fake_build_rabitq_model)
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset: Any = index_mod.create_index(
        uri=fake_dataset,
        column="vector",
        index_type="IVF_RQ",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        sample_rate=8,
        num_bits=2,
    )

    assert updated_dataset is fake_dataset
    assert captured["train_ivf"]["sample_rate"] == 8
    assert "train_pq" not in captured
    assert captured["build_rabitq_model"] == {"dimension": 16, "num_bits": 2}
    assert captured["put_artifacts"] == ("ivf_centroids", None)
    assert captured["fragment_handler_kwargs"]["index_type"] == "IVF_RQ"
    assert captured["fragment_handler_kwargs"]["ivf_centroids"] == "ivf_ref"
    assert captured["fragment_handler_kwargs"]["pq_codebook"] is None
    assert captured["fragment_handler_kwargs"]["num_bits"] == 2
    assert captured["fragment_handler_kwargs"]["rabitq_model"] == "auto-rq-model"
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


def test_create_index_uses_provided_ivf_rq_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller-provided RaBitQ model should be shared without rebuilding it."""

    captured: dict[str, Any] = {}
    fake_dataset: Any = _FakeDataset()

    class FakeIndicesBuilder:
        dimension = 16

        def __init__(self, dataset: Any, column: str) -> None:
            captured["builder_dataset"] = dataset
            captured["builder_column"] = column

        def train_ivf(self, **kwargs: Any) -> Any:
            captured["train_ivf"] = kwargs
            return SimpleNamespace(centroids="ivf_centroids", num_partitions=4)

        def train_pq(self, ivf_model: Any, **kwargs: Any) -> None:
            raise AssertionError("IVF_RQ should not train a PQ codebook")

    def fake_handle_vector_fragment_index(**kwargs: Any) -> Any:
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {
            "status": "success",
            "fragment_ids": fragment_ids,
            "segment_index": "segment",
        }

    def fake_put_vector_index_artifacts(ivf_centroids: Any, pq_codebook: Any) -> Any:
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", None

    def fake_build_rabitq_model(**kwargs: Any) -> None:
        raise AssertionError("provided rabitq_model should be reused")

    def fake_map_async_with_pool(**kwargs: Any) -> Any:
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
    monkeypatch.setattr(index_mod, "_build_rabitq_model", fake_build_rabitq_model)
    monkeypatch.setattr(index_mod, "_map_async_with_pool", fake_map_async_with_pool)

    updated_dataset: Any = index_mod.create_index(
        uri=fake_dataset,
        column="vector",
        index_type="IVF_RQ",
        name="vector_idx",
        num_workers=2,
        num_partitions=4,
        sample_rate=8,
        rabitq_model="shared-rq-model",
    )

    assert updated_dataset is fake_dataset
    assert captured["fragment_handler_kwargs"]["rabitq_model"] == "shared-rq-model"
    assert captured["fragment_handler_kwargs"]["index_type"] == "IVF_RQ"
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


def test_create_index_rejects_non_positive_sample_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid sample rates should fail before training starts."""

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)

    with pytest.raises(ValueError, match="sample_rate must be positive, got 0"):
        index_mod.create_index(
            uri=cast(Any, _FakeDataset()),
            column="vector",
            index_type="IVF_PQ",
            sample_rate=0,
        )


def test_create_index_rejects_invalid_num_segments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid segment counts should fail before training starts."""

    monkeypatch.setattr(index_mod, "_check_pylance_version", lambda: None)

    with pytest.raises(ValueError, match="num_segments must be positive, got 0"):
        index_mod.create_index(
            uri=cast(Any, _FakeDataset()),
            column="vector",
            index_type="IVF_PQ",
            num_segments=0,
        )


@pytest.mark.parametrize(
    ("index_type", "column"),
    [
        ("BTREE", "value"),
        ("BTREE", "event_date"),
        ("BITMAP", "value"),
        ("INVERTED", "text"),
        ("FTS", "text"),
        ("NGRAM", "text"),
        ("BLOOMFILTER", "value"),
        ("RTREE", "value"),
        ("LABEL_LIST", "labels"),
        ("ZONEMAP", "event_date"),
    ],
)
def test_create_scalar_index_uses_segment_path(
    monkeypatch: pytest.MonkeyPatch, index_type: str, column: str
) -> None:
    """Migrated scalar indexes should use Lance's segment workflow."""

    captured: dict[str, Any] = {"loads": []}
    fake_dataset: Any = _FakeDataset()

    def fake_lance_dataset(*args: Any, **kwargs: Any) -> Any:
        captured["loads"].append(kwargs)
        return fake_dataset

    def fake_handle_scalar_segment_index(**kwargs: Any) -> Any:
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {
            "status": "success",
            "fragment_ids": fragment_ids,
            "segment_index": "segment",
        }

    def fake_map_async_with_pool(**kwargs: Any) -> Any:
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

    updated_dataset: Any = index_mod.create_scalar_index(
        uri="memory://fake",
        column=column,
        index_type=cast(Any, index_type),
        num_workers=2,
        block_size=4096,
    )

    assert updated_dataset is fake_dataset
    assert [load["block_size"] for load in captured["loads"]] == [4096, 4096]
    assert captured["fragment_handler_kwargs"]["index_type"] == index_type
    assert captured["fragment_handler_kwargs"]["block_size"] == 4096
    assert fake_dataset.commit_kwargs["segments"] == ["segment"]


def test_create_label_list_index_rejects_non_list_column() -> None:
    """LABEL_LIST should reject invalid columns before Ray workers start."""

    with pytest.raises(TypeError, match="must be list or large list type"):
        index_mod.create_scalar_index(
            uri=cast(Any, _FakeDataset()),
            column="value",
            index_type="LABEL_LIST",
        )


def test_create_index_passes_block_size_to_loads_and_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The vector index path should use block_size for driver and worker loads."""

    captured: dict[str, Any] = {"loads": []}
    fake_dataset: Any = _FakeDataset()

    class FakeIndicesBuilder:
        dimension = 16

        def __init__(self, dataset: Any, column: str) -> None:
            captured["builder_dataset"] = dataset
            captured["builder_column"] = column

        def train_ivf(self, **kwargs: Any) -> Any:
            captured["train_ivf"] = kwargs
            return SimpleNamespace(centroids="ivf_centroids", num_partitions=4)

        def train_pq(self, ivf_model: Any, **kwargs: Any) -> Any:
            captured["train_pq_ivf_model"] = ivf_model
            captured["train_pq"] = kwargs
            return SimpleNamespace(codebook="pq_codebook", num_subvectors=4)

    def fake_lance_dataset(*args: Any, **kwargs: Any) -> Any:
        captured["loads"].append(kwargs)
        return fake_dataset

    def fake_handle_vector_fragment_index(**kwargs: Any) -> Any:
        captured["fragment_handler_kwargs"] = kwargs
        return lambda fragment_ids: {"status": "success", "fragment_ids": fragment_ids}

    def fake_put_vector_index_artifacts(ivf_centroids: Any, pq_codebook: Any) -> Any:
        captured["put_artifacts"] = (ivf_centroids, pq_codebook)
        return "ivf_ref", "pq_ref"

    def fake_map_async_with_pool(**kwargs: Any) -> Any:
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

    updated_dataset: Any = index_mod.create_index(
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


def test_fragment_handlers_pass_block_size_to_dataset_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker-side scalar and vector handlers should load datasets with block_size."""

    captured: dict[str, Any] = {"loads": []}
    fake_dataset: Any = _FakeDataset()

    def fake_lance_dataset(*args: Any, **kwargs: Any) -> Any:
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
        ivf_centroids=cast(Any, "ivf_centroids"),
        pq_codebook=cast(Any, "pq_codebook"),
        block_size=8192,
    )

    assert scalar_handler([0])["status"] == "success"
    assert vector_handler([0])["status"] == "success"
    assert [load["block_size"] for load in captured["loads"]] == [4096, 8192]


def test_vector_fragment_handler_resolves_shared_artifact_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Workers should dereference shared training artifacts before Lance calls."""

    class FakeObjectRef:
        def __init__(self, value: Any) -> None:
            self.value = value

    fake_dataset: Any = _FakeDataset()
    captured: dict[str, Any] = {"gets": []}

    def fake_get(ref: Any) -> Any:
        captured["gets"].append(ref)
        return ref.value

    # ``index_mod.ray`` may be the stub module installed by
    # ``_load_index_module_with_stubs``, so patch it rather than the real one.
    monkeypatch.setattr(index_mod.ray, "ObjectRef", FakeObjectRef, raising=False)  # type: ignore[attr-defined]
    monkeypatch.setattr(index_mod.ray, "get", fake_get, raising=False)  # type: ignore[attr-defined]
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
        ivf_centroids=cast(Any, ivf_ref),
        pq_codebook=cast(Any, pq_ref),
    )

    assert vector_handler([0])["status"] == "success"
    assert captured["gets"] == [ivf_ref, pq_ref]
    assert fake_dataset.vector_index_kwargs["ivf_centroids"] == "ivf_centroids"
    assert fake_dataset.vector_index_kwargs["pq_codebook"] == "pq_codebook"
