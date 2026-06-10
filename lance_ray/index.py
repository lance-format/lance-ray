# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import math
import uuid
from collections.abc import Callable
from typing import Any, Literal, Optional, TypeAlias, Union

import lance
import numpy as np
import pyarrow as pa
import ray
from lance.dataset import Index, IndexConfig, LanceDataset
from lance.indices import IndicesBuilder
from packaging import version
from ray.util.multiprocessing import Pool

from .utils import (
    get_namespace_kwargs,
    get_or_create_namespace,
    validate_uri_or_namespace,
)

logger = logging.getLogger(__name__)


_VectorIndexArtifact: TypeAlias = (
    pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None
)
_VectorIndexArtifactRef: TypeAlias = _VectorIndexArtifact | ray.ObjectRef
_VectorIndexArtifactRefs: TypeAlias = tuple[
    _VectorIndexArtifactRef, _VectorIndexArtifactRef
]


def _dataset_load_kwargs(
    storage_options: Optional[dict[str, Any]],
    namespace_kwargs: dict[str, Any],
    block_size: Optional[int],
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "storage_options": storage_options,
        **namespace_kwargs,
    }
    if block_size is not None:
        kwargs["block_size"] = block_size
    return kwargs


def _distribute_fragments_balanced(
    fragments: list[Any], num_workers: int, logger: logging.Logger
) -> list[list[int]]:
    """Distribute fragments across workers using a balanced algorithm.

    This function implements a greedy algorithm that assigns fragments to the
    worker with the currently smallest total workload, helping to balance the
    processing time across workers.

    Parameters
    ----------
    fragments : list
        List of Lance fragment objects.
    num_workers : int
        Number of workers to distribute fragments across.
    logger : logging.Logger
        Logger instance for debugging information.

    Returns
    -------
    list[list[int]]
        Each inner list contains fragment IDs for one worker.
    """
    if not fragments:
        return [[] for _ in range(num_workers)]

    fragment_info: list[dict[str, int]] = []
    for fragment in fragments:
        try:
            # Try to get fragment size information
            # fragment.count_rows() gives us the number of rows in the fragment
            row_count = fragment.count_rows()
            fragment_info.append({"id": fragment.fragment_id, "size": row_count})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Could not get size for fragment %s: %s. Using fragment_id as size estimate.",
                fragment.fragment_id,
                exc,
            )
            fragment_info.append(
                {"id": fragment.fragment_id, "size": fragment.fragment_id}
            )

    # Sort fragments by size in descending order (largest first)
    # This helps with better load balancing using the greedy algorithm
    fragment_info.sort(key=lambda x: x["size"], reverse=True)

    worker_batches: list[list[int]] = [[] for _ in range(num_workers)]
    worker_workloads = [0] * num_workers

    # Greedy assignment: assign each fragment to the worker with minimum workload
    for frag_info in fragment_info:
        # Find the worker with the minimum current workload
        min_workload_idx = min(range(num_workers), key=lambda i: worker_workloads[i])
        worker_batches[min_workload_idx].append(frag_info["id"])
        worker_workloads[min_workload_idx] += frag_info["size"]

    total_size = sum(info["size"] for info in fragment_info)
    logger.info("Fragment distribution statistics:")
    logger.info("  Total fragments: %d", len(fragment_info))
    logger.info("  Total size: %d", total_size)
    logger.info("  Workers: %d", num_workers)

    for i, (batch, workload) in enumerate(
        zip(worker_batches, worker_workloads, strict=False)
    ):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info(
            "  Worker %d: %d fragments, workload: %d (%.1f%%)",
            i,
            len(batch),
            workload,
            percentage,
        )

    non_empty_batches = [batch for batch in worker_batches if batch]
    return non_empty_batches


def _map_async_with_pool(
    create_fragment_handler: Callable[[], Any],
    fragment_batches: list[list[int]],
    *,
    num_workers: int,
    ray_remote_args: Optional[dict[str, Any]],
    error_prefix: str,
) -> list[dict[str, Any]]:
    """Run fragment tasks in a Ray-backed multiprocessing Pool.

    This helper encapsulates the common Pool.map_async + get + error wrapping
    logic so that both scalar and vector distributed index builders can share
    the same implementation.
    """
    pool = Pool(processes=num_workers, ray_remote_args=ray_remote_args)
    try:
        fragment_handler = create_fragment_handler()
        rst_futures = pool.map_async(
            fragment_handler,
            fragment_batches,
            chunksize=1,
        )
        results = rst_futures.get()
    except Exception as exc:  # pragma: no cover - exercised via integration tests
        raise RuntimeError(f"{error_prefix}: {exc}") from exc
    finally:
        pool.close()
        pool.join()

    return results


def _is_ray_object_ref(value: Any) -> bool:
    object_ref_type = getattr(ray, "ObjectRef", None)
    return object_ref_type is not None and isinstance(value, object_ref_type)


def _ray_put_index_artifact(value: Any) -> _VectorIndexArtifactRef:
    if value is None or _is_ray_object_ref(value):
        return value
    return ray.put(value)


def _ray_get_index_artifact(value: Any) -> _VectorIndexArtifact:
    if _is_ray_object_ref(value):
        return ray.get(value)
    return value


def _put_vector_index_artifacts_in_object_store(
    ivf_centroids: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
    pq_codebook: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
) -> _VectorIndexArtifactRefs:
    return (
        _ray_put_index_artifact(ivf_centroids),
        _ray_put_index_artifact(pq_codebook),
    )


def _artifact_len(artifact: Any) -> Optional[int]:
    if artifact is None or _is_ray_object_ref(artifact):
        return None
    try:
        return len(artifact)
    except TypeError:
        return None


def _infer_num_partitions_from_artifact(
    ivf_centroids: _VectorIndexArtifactRef,
) -> Optional[int]:
    return _artifact_len(ivf_centroids)


def _infer_num_sub_vectors_from_artifact(
    pq_codebook: _VectorIndexArtifactRef,
) -> Optional[int]:
    codebook_len = _artifact_len(pq_codebook)
    if codebook_len is not None and codebook_len % 256 == 0:
        return codebook_len // 256
    return None


def _vectors_to_numpy(batch_or_table: Any, column: str) -> np.ndarray:
    values = batch_or_table[column]
    if hasattr(values, "combine_chunks"):
        values = values.combine_chunks()

    if hasattr(values, "to_numpy_ndarray"):
        vectors = values.to_numpy_ndarray()
    elif pa.types.is_fixed_size_list(values.type):
        child_offset = values.offset * values.type.list_size
        child_length = len(values) * values.type.list_size
        child_values = values.values.slice(child_offset, child_length)
        vectors = child_values.to_numpy(zero_copy_only=False).reshape(
            len(values), values.type.list_size
        )
    else:
        vectors = np.asarray(values.to_pylist())

    vectors = np.asarray(vectors, dtype=np.float32).reshape(len(values), -1)
    if vectors.size == 0:
        return vectors
    finite_mask = np.isfinite(vectors).all(axis=1)
    return vectors[finite_mask]


def _vector_artifact_to_numpy(artifact: Any) -> np.ndarray:
    if artifact is None:
        raise ValueError("vector artifact cannot be None")
    if hasattr(artifact, "combine_chunks") or hasattr(artifact, "type"):
        table = pa.table({"_artifact": artifact})
        return _vectors_to_numpy(table, "_artifact")
    vectors = np.asarray(artifact, dtype=np.float32)
    return vectors.reshape(len(vectors), -1)


def _create_fixed_size_vector_array(vectors: np.ndarray) -> pa.FixedSizeListArray:
    vectors = np.asarray(vectors, dtype=np.float32)
    if vectors.ndim != 2:
        raise ValueError(f"Expected 2-D vector data, got shape {vectors.shape}")
    flat = pa.array(vectors.reshape(-1), type=pa.float32())
    return pa.FixedSizeListArray.from_arrays(flat, vectors.shape[1])


def _load_vector_training_sample(
    dataset_uri: str,
    column: str,
    fragment_ids: list[int],
    sample_size: int,
    storage_options: Optional[dict[str, Any]],
    block_size: Optional[int],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> np.ndarray:
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    dataset = LanceDataset(
        dataset_uri,
        **_dataset_load_kwargs(storage_options, namespace_kwargs, block_size),
    )
    fragments = [dataset.get_fragment(fragment_id) for fragment_id in fragment_ids]
    scanner = dataset.scanner(columns=[column], fragments=fragments)

    reservoir: np.ndarray | None = None
    rows_seen = 0
    reservoir_size = 0
    rng = np.random.default_rng(0)
    for batch in scanner.to_batches():
        vectors = _vectors_to_numpy(batch, column)
        if vectors.size == 0:
            continue

        if reservoir is None:
            reservoir = np.empty((sample_size, vectors.shape[1]), dtype=np.float32)

        for vector in vectors:
            rows_seen += 1
            if reservoir_size < sample_size:
                reservoir[reservoir_size] = vector
                reservoir_size += 1
                continue
            replacement_index = int(rng.integers(0, rows_seen))
            if replacement_index < sample_size:
                reservoir[replacement_index] = vector

    if reservoir is None:
        return np.empty((0, 0), dtype=np.float32)
    return reservoir[:reservoir_size].astype(np.float32, copy=True)


class _VectorTrainingShard:
    def __init__(
        self,
        dataset_uri: str,
        column: str,
        fragment_ids: list[int],
        sample_size: int,
        storage_options: Optional[dict[str, Any]],
        block_size: Optional[int],
        namespace_impl: Optional[str],
        namespace_properties: Optional[dict[str, str]],
        table_id: Optional[list[str]],
    ):
        self.vectors = _load_vector_training_sample(
            dataset_uri,
            column,
            fragment_ids,
            sample_size,
            storage_options,
            block_size,
            namespace_impl,
            namespace_properties,
            table_id,
        )

    def metadata(self) -> dict[str, int]:
        return _sample_metadata_remote(self.vectors)

    def initial_vectors(self, max_vectors: int) -> np.ndarray:
        return _take_initial_vectors_remote(self.vectors, max_vectors)

    def kmeans_partial(self, centroids: np.ndarray, metric: str) -> dict[str, Any]:
        return _kmeans_partial(self.vectors, centroids, metric)

    def initial_residuals(
        self,
        ivf_centroids: np.ndarray,
        metric: str,
        num_sub_vectors: int,
        max_vectors: int,
    ) -> np.ndarray:
        return _take_initial_residuals_remote(
            self.vectors, ivf_centroids, metric, num_sub_vectors, max_vectors
        )

    def pq_partial(
        self, ivf_centroids: np.ndarray, codebooks: np.ndarray, metric: str
    ) -> dict[str, Any]:
        return _pq_kmeans_partial(self.vectors, ivf_centroids, codebooks, metric)


def _normalize_metric_for_training(metric: str) -> str:
    metric_lower = metric.lower()
    if metric_lower in {"l2", "euclidean"}:
        return "l2"
    if metric_lower == "cosine":
        return "cosine"
    raise ValueError(
        "distributed vector training currently supports metric 'l2' or 'cosine', "
        f"got {metric!r}"
    )


def _normalize_rows(vectors: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return vectors / norms


def _assign_to_centroids(
    vectors: np.ndarray, centroids: np.ndarray, metric: str
) -> tuple[np.ndarray, np.ndarray]:
    if len(vectors) == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float32)

    metric = _normalize_metric_for_training(metric)
    if metric == "cosine":
        vectors_for_distance = _normalize_rows(vectors)
        centroids_for_distance = _normalize_rows(centroids)
    else:
        vectors_for_distance = vectors
        centroids_for_distance = centroids

    chunk_size = max(1, min(4096, len(vectors_for_distance)))
    assignments: list[np.ndarray] = []
    distances: list[np.ndarray] = []
    centroid_norms = np.sum(centroids_for_distance * centroids_for_distance, axis=1)
    for offset in range(0, len(vectors_for_distance), chunk_size):
        chunk = vectors_for_distance[offset : offset + chunk_size]
        dists = (
            np.sum(chunk * chunk, axis=1, keepdims=True)
            + centroid_norms[None, :]
            - 2.0 * chunk @ centroids_for_distance.T
        )
        dists = np.maximum(dists, 0.0)
        chunk_assignments = np.argmin(dists, axis=1)
        assignments.append(chunk_assignments.astype(np.int64))
        distances.append(
            dists[np.arange(len(chunk)), chunk_assignments].astype(np.float32)
        )

    return np.concatenate(assignments), np.concatenate(distances)


def _initial_centroids_from_samples(samples: list[np.ndarray], k: int) -> np.ndarray:
    non_empty_samples = [sample for sample in samples if sample.size > 0]
    if not non_empty_samples:
        raise ValueError("distributed vector training found no sample vectors")

    vectors = np.concatenate(non_empty_samples, axis=0)
    if len(vectors) >= k:
        indices = np.linspace(0, len(vectors) - 1, k, dtype=np.int64)
        return vectors[indices].astype(np.float32, copy=True)

    repeats = math.ceil(k / len(vectors))
    return np.tile(vectors, (repeats, 1))[:k].astype(np.float32, copy=True)


def _kmeans_partial(
    vectors: np.ndarray, centroids: np.ndarray, metric: str
) -> dict[str, Any]:
    if vectors.size == 0:
        return {
            "sums": np.zeros_like(centroids, dtype=np.float64),
            "counts": np.zeros(len(centroids), dtype=np.int64),
            "loss": 0.0,
        }

    assignments, distances = _assign_to_centroids(vectors, centroids, metric)
    sums = np.zeros_like(centroids, dtype=np.float64)
    counts = np.bincount(assignments, minlength=len(centroids)).astype(np.int64)
    np.add.at(sums, assignments, vectors)
    return {"sums": sums, "counts": counts, "loss": float(np.sum(distances))}


def _sample_metadata_remote(vectors: np.ndarray) -> dict[str, int]:
    if vectors.size == 0:
        return {"num_rows": 0, "dimension": 0}
    return {"num_rows": len(vectors), "dimension": vectors.shape[1]}


def _take_initial_vectors_remote(vectors: np.ndarray, max_vectors: int) -> np.ndarray:
    if vectors.size == 0 or max_vectors <= 0:
        dimension = vectors.shape[1] if vectors.ndim == 2 else 0
        return np.empty((0, dimension), dtype=np.float32)
    if len(vectors) <= max_vectors:
        return vectors.astype(np.float32, copy=True)
    indices = np.linspace(0, len(vectors) - 1, max_vectors, dtype=np.int64)
    return vectors[indices].astype(np.float32, copy=True)


def _combine_kmeans_partials(
    partials: list[dict[str, Any]], previous_centroids: np.ndarray
) -> tuple[np.ndarray, float]:
    sums = np.sum([partial["sums"] for partial in partials], axis=0)
    counts = np.sum([partial["counts"] for partial in partials], axis=0)
    next_centroids = previous_centroids.astype(np.float32, copy=True)
    populated = counts > 0
    next_centroids[populated] = (sums[populated] / counts[populated, None]).astype(
        np.float32
    )
    loss = float(sum(partial["loss"] for partial in partials))
    return next_centroids, loss


def _run_distributed_kmeans(
    shards: list[Any],
    initial_centroids: np.ndarray,
    *,
    k: int,
    metric: str,
    max_iters: int,
    tolerance: float,
    ray_remote_args: Optional[dict[str, Any]],
) -> np.ndarray:
    if len(initial_centroids) != k:
        raise ValueError(
            f"initial_centroids length must match k, got {len(initial_centroids)} and {k}"
        )
    centroids = initial_centroids.astype(np.float32, copy=True)

    previous_loss: Optional[float] = None
    for _ in range(max_iters):
        partial_refs = [
            shard.kmeans_partial.remote(centroids, metric) for shard in shards
        ]
        partials = ray.get(partial_refs)
        next_centroids, loss = _combine_kmeans_partials(partials, centroids)
        if previous_loss is not None:
            delta = abs(previous_loss - loss)
            if delta <= tolerance * max(previous_loss, 1.0):
                centroids = next_centroids
                break
        previous_loss = loss
        centroids = next_centroids

    if metric == "cosine":
        centroids = _normalize_rows(centroids).astype(np.float32)
    return centroids


def _default_num_sub_vectors(dimension: int) -> int:
    if dimension % 16 == 0:
        return max(1, dimension // 16)
    if dimension % 8 == 0:
        return max(1, dimension // 8)
    raise ValueError(
        "num_sub_vectors must be provided when vector dimension is not divisible "
        f"by 8 or 16, got dimension={dimension}"
    )


def _compute_residuals(
    vectors: np.ndarray, ivf_centroids: np.ndarray, metric: str
) -> np.ndarray:
    assignments, _ = _assign_to_centroids(vectors, ivf_centroids, metric)
    return vectors - ivf_centroids[assignments]


def _take_initial_residuals_remote(
    vectors: np.ndarray,
    ivf_centroids: np.ndarray,
    metric: str,
    num_sub_vectors: int,
    max_vectors: int,
) -> np.ndarray:
    if vectors.size == 0 or max_vectors <= 0:
        return np.empty((0, num_sub_vectors, 0), dtype=np.float32)
    residuals = _compute_residuals(vectors, ivf_centroids, metric)
    residuals = _take_initial_vectors_remote(residuals, max_vectors)
    return residuals.reshape(len(residuals), num_sub_vectors, -1).astype(
        np.float32,
        copy=False,
    )


def _initial_pq_codebooks_from_residuals(
    residual_samples: list[np.ndarray],
    pq_centroids: int,
) -> np.ndarray:
    non_empty_samples = [sample for sample in residual_samples if sample.size > 0]
    if not non_empty_samples:
        raise ValueError("distributed PQ training found no sample vectors")

    num_sub_vectors = non_empty_samples[0].shape[1]
    sub_vectors = np.concatenate(non_empty_samples, axis=0)
    codebooks = []
    for subvector_index in range(num_sub_vectors):
        codebooks.append(
            _initial_centroids_from_samples(
                [sub_vectors[:, subvector_index, :]], pq_centroids
            )
        )
    return np.stack(codebooks).astype(np.float32)


def _pq_kmeans_partial(
    vectors: np.ndarray,
    ivf_centroids: np.ndarray,
    codebooks: np.ndarray,
    metric: str,
) -> dict[str, Any]:
    num_sub_vectors, pq_centroids, sub_vector_dim = codebooks.shape
    sums = np.zeros_like(codebooks, dtype=np.float64)
    counts = np.zeros((num_sub_vectors, pq_centroids), dtype=np.int64)
    if vectors.size == 0:
        return {"sums": sums, "counts": counts, "loss": 0.0}

    residuals = _compute_residuals(vectors, ivf_centroids, metric)
    sub_vectors = residuals.reshape(len(residuals), num_sub_vectors, sub_vector_dim)
    loss = 0.0
    for subvector_index in range(num_sub_vectors):
        assignments, distances = _assign_to_centroids(
            sub_vectors[:, subvector_index, :],
            codebooks[subvector_index],
            metric,
        )
        counts[subvector_index] = np.bincount(
            assignments, minlength=pq_centroids
        ).astype(np.int64)
        np.add.at(
            sums[subvector_index], assignments, sub_vectors[:, subvector_index, :]
        )
        loss += float(np.sum(distances))
    return {"sums": sums, "counts": counts, "loss": loss}


def _combine_pq_partials(
    partials: list[dict[str, Any]], previous_codebooks: np.ndarray
) -> tuple[np.ndarray, float]:
    sums = np.sum([partial["sums"] for partial in partials], axis=0)
    counts = np.sum([partial["counts"] for partial in partials], axis=0)
    next_codebooks = previous_codebooks.astype(np.float32, copy=True)
    populated = counts > 0
    next_codebooks[populated] = (sums[populated] / counts[populated][:, None]).astype(
        np.float32
    )
    loss = float(sum(partial["loss"] for partial in partials))
    return next_codebooks, loss


def _run_distributed_pq_training(
    shards: list[Any],
    initial_residual_samples: list[np.ndarray],
    *,
    ivf_centroids: np.ndarray,
    metric: str,
    num_sub_vectors: int,
    max_iters: int,
    tolerance: float,
    ray_remote_args: Optional[dict[str, Any]],
) -> np.ndarray:
    codebooks = _initial_pq_codebooks_from_residuals(
        initial_residual_samples,
        pq_centroids=256,
    )
    previous_loss: Optional[float] = None
    for _ in range(max_iters):
        partial_refs = [
            shard.pq_partial.remote(ivf_centroids, codebooks, metric)
            for shard in shards
        ]
        partials = ray.get(partial_refs)
        next_codebooks, loss = _combine_pq_partials(partials, codebooks)
        if previous_loss is not None:
            delta = abs(previous_loss - loss)
            if delta <= tolerance * max(previous_loss, 1.0):
                codebooks = next_codebooks
                break
        previous_loss = loss
        codebooks = next_codebooks
    return codebooks


def _train_vector_index_artifacts_distributed(
    *,
    dataset_uri: str,
    column: str,
    index_type: str,
    metric: str,
    num_partitions: Optional[int],
    num_sub_vectors: Optional[int],
    sample_rate: int,
    fragment_batches: list[list[int]],
    num_workers: int,
    storage_options: Optional[dict[str, Any]],
    block_size: Optional[int],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    ray_remote_args: Optional[dict[str, Any]],
    ivf_centroids: _VectorIndexArtifact = None,
    pq_codebook: _VectorIndexArtifact = None,
    max_iters: int = 50,
    tolerance: float = 1e-4,
) -> tuple[_VectorIndexArtifact, _VectorIndexArtifact, int, Optional[int]]:
    metric = _normalize_metric_for_training(metric)
    pq_index_types = {"IVF_PQ", "IVF_HNSW_PQ"}
    needs_pq = index_type in pq_index_types
    ivf_centroids_np = None

    dataset = LanceDataset(
        dataset_uri,
        **_dataset_load_kwargs(
            storage_options,
            get_namespace_kwargs(namespace_impl, namespace_properties, table_id),
            block_size,
        ),
    )
    num_rows = dataset.count_rows()

    if ivf_centroids is not None:
        ivf_centroids_np = _vector_artifact_to_numpy(ivf_centroids)
        if num_partitions is None:
            num_partitions = len(ivf_centroids_np)
        elif len(ivf_centroids_np) != num_partitions:
            raise ValueError(
                "num_partitions must match provided ivf_centroids length, got "
                f"num_partitions={num_partitions}, centroids={len(ivf_centroids_np)}"
            )
    elif num_partitions is None:
        num_partitions = max(1, min(num_rows, round(math.sqrt(num_rows))))

    ivf_sample_size = 0 if ivf_centroids is not None else num_partitions * sample_rate
    pq_sample_size = 0
    if needs_pq and pq_codebook is None:
        pq_sample_size = 256 * sample_rate

    # IVF and PQ have different sample-size requirements; PQ needs enough
    # residual samples for 256 codewords per subvector.
    sample_size = max(ivf_sample_size, pq_sample_size, num_workers)
    sample_size_per_batch = max(1, math.ceil(sample_size / len(fragment_batches)))
    shard_actor = ray.remote(_VectorTrainingShard)
    if ray_remote_args:
        shard_actor = shard_actor.options(**ray_remote_args)

    # Keep sampled vectors inside one actor per shard. The driver only pulls
    # scalar metadata, bounded initialization slices, and KMeans partials.
    shards = [
        shard_actor.remote(
            dataset_uri,
            column,
            fragment_batch,
            sample_size_per_batch,
            storage_options,
            block_size,
            namespace_impl,
            namespace_properties,
            table_id,
        )
        for fragment_batch in fragment_batches
    ]

    # Dimension discovery stays on shard actors; no full sample reaches driver.
    sample_metadata = ray.get([shard.metadata.remote() for shard in shards])
    dimension = next(
        (item["dimension"] for item in sample_metadata if item["num_rows"]), None
    )
    if dimension is None:
        raise ValueError("distributed vector training found no vectors to train on")
    if ivf_centroids_np is not None and ivf_centroids_np.shape[1] != dimension:
        raise ValueError(
            "provided ivf_centroids dimension does not match vector column, got "
            f"centroids={ivf_centroids_np.shape[1]}, column={dimension}"
        )

    if needs_pq:
        if pq_codebook is not None and num_sub_vectors is None:
            num_sub_vectors = _infer_num_sub_vectors_from_artifact(pq_codebook)
        if num_sub_vectors is None:
            num_sub_vectors = _default_num_sub_vectors(dimension)
        if num_sub_vectors <= 0:
            raise ValueError(f"num_sub_vectors must be positive, got {num_sub_vectors}")
        if dimension % num_sub_vectors != 0:
            raise ValueError(
                "num_sub_vectors must divide vector dimension, got "
                f"dimension={dimension}, num_sub_vectors={num_sub_vectors}"
            )

    if ivf_centroids_np is None:
        logger.info(
            "Training IVF centroids with distributed KMeans: partitions=%d, samples=%d",
            num_partitions,
            sample_size,
        )
        initial_sample_size = max(1, math.ceil(num_partitions / len(shards)))
        initial_samples = ray.get(
            [shard.initial_vectors.remote(initial_sample_size) for shard in shards]
        )
        initial_centroids = _initial_centroids_from_samples(
            initial_samples, num_partitions
        )
        ivf_centroids_np = _run_distributed_kmeans(
            shards,
            initial_centroids,
            k=num_partitions,
            metric=metric,
            max_iters=max_iters,
            tolerance=tolerance,
            ray_remote_args=ray_remote_args,
        )
        ivf_centroids = _create_fixed_size_vector_array(ivf_centroids_np)

    pq_codebook_artifact = pq_codebook
    if needs_pq:
        assert num_sub_vectors is not None
        if pq_codebook_artifact is None:
            logger.info(
                "Training PQ codebook with distributed KMeans: subvectors=%d",
                num_sub_vectors,
            )
            residual_sample_size = max(1, math.ceil(256 / len(shards)))
            # PQ needs only a bounded residual sample to seed codebooks; KMeans
            # iterations still use all per-shard samples through shard actors.
            initial_residual_samples = ray.get(
                [
                    shard.initial_residuals.remote(
                        ivf_centroids_np,
                        metric,
                        num_sub_vectors,
                        residual_sample_size,
                    )
                    for shard in shards
                ]
            )
            pq_codebooks_np = _run_distributed_pq_training(
                shards,
                initial_residual_samples,
                ivf_centroids=ivf_centroids_np,
                metric=metric,
                num_sub_vectors=num_sub_vectors,
                max_iters=max_iters,
                tolerance=tolerance,
                ray_remote_args=ray_remote_args,
            )
            pq_codebook_artifact = _create_fixed_size_vector_array(
                pq_codebooks_np.reshape(-1, pq_codebooks_np.shape[-1])
            )

    return ivf_centroids, pq_codebook_artifact, num_partitions, num_sub_vectors


def _handle_fragment_index(
    dataset_uri: str,
    column: str,
    index_type: str | IndexConfig,
    name: str,
    index_uuid: str,
    replace: bool,
    train: bool,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    **kwargs: Any,
):
    """Create a fragment handler closure for scalar index builds.

    The returned callable can be used with :func:`Pool.map_async` to build
    indices for specific fragments.
    """

    def func(fragment_ids: list[int]) -> dict[str, Any]:
        try:
            if not fragment_ids:
                raise ValueError("fragment_ids cannot be empty")

            for fragment_id in fragment_ids:
                if fragment_id < 0 or fragment_id > 0xFFFFFFFF:
                    raise ValueError(f"Invalid fragment_id: {fragment_id}")

            namespace_kwargs = get_namespace_kwargs(
                namespace_impl, namespace_properties, table_id
            )

            # Load dataset
            dataset = LanceDataset(
                dataset_uri,
                **_dataset_load_kwargs(storage_options, namespace_kwargs, block_size),
            )

            available_fragments = {f.fragment_id for f in dataset.get_fragments()}
            invalid_fragments = set(fragment_ids) - available_fragments
            if invalid_fragments:
                raise ValueError(f"Fragment IDs {invalid_fragments} do not exist")

            logger.info(
                "Building distributed scalar index for fragments %s using create_scalar_index",
                fragment_ids,
            )

            dataset.create_scalar_index(
                column=column,
                index_type=index_type,
                name=name,
                replace=replace,
                train=train,
                index_uuid=index_uuid,
                fragment_ids=fragment_ids,
                **kwargs,
            )

            lance_field = dataset.lance_schema.field(column)
            if lance_field is None:
                raise KeyError(f"{column} not found in schema")
            field_id = lance_field.id()

            logger.info(
                "Fragment scalar index created successfully for fragments %s",
                fragment_ids,
            )

            return {
                "status": "success",
                "fragment_ids": fragment_ids,
                "fields": [field_id],
                "uuid": index_uuid,
            }

        except Exception as exc:  # pragma: no cover - exercised via integration tests
            logger.error(
                "Fragment scalar index task failed for fragments %s: %s",
                fragment_ids,
                exc,
            )
            return {
                "status": "error",
                "fragment_ids": fragment_ids,
                "error": str(exc),
            }

    return func


def merge_index_metadata_compat(dataset, index_id, index_type, **kwargs):
    """Call ``merge_index_metadata`` with backwards compatible signature."""
    try:
        return dataset.merge_index_metadata(
            index_id, index_type, batch_readhead=kwargs.get("batch_readhead")
        )
    except TypeError:
        return dataset.merge_index_metadata(index_id)


def create_scalar_index(
    uri: Optional[str] = None,
    *,
    column: str,
    index_type: Literal["BTREE"]
    | Literal["BITMAP"]
    | Literal["LABEL_LIST"]
    | Literal["INVERTED"]
    | Literal["FTS"]
    | Literal["NGRAM"]
    | Literal["ZONEMAP"]
    | IndexConfig,
    table_id: Optional[list[str]] = None,
    name: Optional[str] = None,
    replace: bool = True,
    train: bool = True,
    fragment_ids: Optional[list[int]] = None,
    index_uuid: Optional[str] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
    """Build scalar indices with Ray in a distributed workflow.

    Args:
        uri: The URI of the Lance dataset to build index on. Either uri OR
            (namespace_impl + table_id) must be provided.
        column: Column name to index.
        index_type: Type of index to build ("BTREE", "BITMAP", "LABEL_LIST",
            "INVERTED", "FTS", "NGRAM", "ZONEMAP") or IndexConfig object.
        table_id: The table identifier as a list of strings. Must be provided
            together with namespace_impl.
        name: Name of the index (generated if None).
        replace: Whether to replace existing index with the same name (default: True).
        train: Whether to train the index (default: True).
        fragment_ids: Optional list of fragment IDs to build index on.
        index_uuid: Optional fragment UUID for distributed indexing.
        num_workers: Number of Ray workers to use (keyword-only).
        storage_options: Storage options for the dataset (keyword-only).
        block_size: Block size in bytes to use when loading the dataset (keyword-only).
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with table_id for resolving the dataset location and
            credentials vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id.
        ray_remote_args: Options for Ray tasks (e.g., num_cpus, resources) (keyword-only).
        **kwargs: Additional arguments to pass to create_scalar_index.

    Returns:
        Updated Lance dataset with the index created.

    Raises:
        ValueError: If input parameters are invalid.
        TypeError: If column type is not string.
        RuntimeError: If index building fails or pylance version is incompatible.
    """
    # Check pylance version compatibility
    try:
        lance_version = version.parse(lance.__version__)
        min_required_version = version.parse("0.36.0")

        if lance_version < min_required_version:
            raise RuntimeError(
                "Distributed indexing requires pylance >= 0.36.0, but found "
                f"{lance.__version__}. The distribute-related interfaces are "
                "not available in older versions. Please upgrade pylance by "
                "running: pip install --upgrade pylance"
            )

        logger.info("Pylance version check passed: %s >= 0.36.0", lance.__version__)

    except AttributeError as err:  # pragma: no cover - defensive
        raise RuntimeError(
            "Cannot determine pylance version. Distributed indexing requires "
            "pylance >= 0.36.0. Please upgrade pylance by running: "
            "pip install --upgrade pylance"
        ) from err

    index_id = str(uuid.uuid4())
    logger.info("Starting distributed scalar index build with ID: %s", index_id)

    # Validate uri or namespace params
    validate_uri_or_namespace(uri, namespace_impl, table_id)

    if not column:
        raise ValueError("Column name cannot be empty")

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")

    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    if isinstance(index_type, str):
        valid_index_types = [
            "BTREE",
            "BITMAP",
            "LABEL_LIST",
            "INVERTED",
            "FTS",
            "NGRAM",
            "ZONEMAP",
        ]
        if index_type not in valid_index_types:
            raise ValueError(
                f"Index type must be one of {valid_index_types}, not '{index_type}'"
            )

        supported_distributed_types = {"INVERTED", "FTS", "BTREE", "BITMAP"}
        if index_type not in supported_distributed_types:
            raise ValueError(
                "Distributed indexing currently supports "
                f"{sorted(supported_distributed_types)} index types, "
                f"not '{index_type}'"
            )
    elif not isinstance(index_type, IndexConfig):
        raise ValueError(
            "index_type must be a string literal or IndexConfig object, got "
            f"{type(index_type)}"
        )

    # Note: Ray initialization is now handled by the Pool, following the pattern from io.py
    # This removes the need for explicit ray.init() calls

    merged_storage_options: dict[str, Any] = {}
    if storage_options:
        merged_storage_options.update(storage_options)

    # Resolve URI and get storage options from namespace if provided
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is not None and table_id is not None:
        from lance_namespace import DescribeTableRequest

        describe_response = namespace.describe_table(DescribeTableRequest(id=table_id))
        uri = describe_response.location
        if describe_response.storage_options:
            merged_storage_options.update(describe_response.storage_options)

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    # Load dataset
    dataset = LanceDataset(
        uri,
        **_dataset_load_kwargs(merged_storage_options, namespace_kwargs, block_size),
    )

    try:
        field = dataset.schema.field(column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc

    # Check column type according to index type
    value_type = field.type
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_type = field.type.value_type

    if isinstance(index_type, str):
        match index_type:
            case "INVERTED" | "FTS":
                if not pa.types.is_string(value_type):
                    raise TypeError(
                        f"Column {column} must be string type for {index_type} "
                        f"index, got {value_type}"
                    )
            case "BTREE":
                is_supported = (
                    pa.types.is_integer(value_type)
                    or pa.types.is_floating(value_type)
                    or pa.types.is_string(value_type)
                )
                if not is_supported:
                    raise TypeError(
                        f"Column {column} must be numeric or string type for BTREE "
                        f"index, got {value_type}"
                    )
            case _:
                # For other index types, skip strict validation to maintain compatibility
                pass

    if name is None:
        name = f"{column}_idx"

    if replace:
        try:
            existing_indices = dataset.list_indices()
        except Exception:  # pragma: no cover
            existing_indices = []

        if any(idx.get("name") == name for idx in existing_indices):
            # Lance 4.0.0: fragment_ids + replace=True may hit an unimplemented path.
            # Implement replace semantics at the driver by dropping the index first.
            dataset.drop_index(name)
            dataset = LanceDataset(
                uri,
                **_dataset_load_kwargs(
                    merged_storage_options, namespace_kwargs, block_size
                ),
            )

    else:
        index_exists = False
        try:
            existing_indices = dataset.list_indices()
            existing_names = {idx["name"] for idx in existing_indices}
            index_exists = name in existing_names
        except (
            Exception
        ):  # pragma: no cover - list_indices() not available in older lance versions
            pass
        if index_exists:
            raise ValueError(
                f"Index with name '{name}' already exists. Set replace=True "
                "to replace it."
            )

    fragments = dataset.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    if fragment_ids is not None:
        available_fragment_ids = {f.fragment_id for f in fragments}
        invalid_fragments = set(fragment_ids) - available_fragment_ids
        if invalid_fragments:
            raise ValueError(
                f"Fragment IDs {invalid_fragments} do not exist in dataset"
            )
        fragments = [f for f in fragments if f.fragment_id in fragment_ids]
        fragment_ids_to_use = fragment_ids
    else:
        fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    if num_workers > len(fragment_ids_to_use):
        num_workers = len(fragment_ids_to_use)
        logger.info("Adjusted num_workers to %d to match fragment count", num_workers)

    fragment_batches = _distribute_fragments_balanced(fragments, num_workers, logger)

    def create_fragment_handler() -> Any:
        return _handle_fragment_index(
            dataset_uri=uri,
            column=column,
            index_type=index_type,
            name=name,
            index_uuid=index_id,
            replace=False,
            train=train,
            storage_options=merged_storage_options,
            block_size=block_size,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            **kwargs,
        )

    logger.info(
        "Phase 1: Distributing scalar index build across %d workers for %d fragments",
        len(fragment_batches),
        len(fragment_ids_to_use),
    )

    results = _map_async_with_pool(
        create_fragment_handler=create_fragment_handler,
        fragment_batches=fragment_batches,
        num_workers=num_workers,
        ray_remote_args=ray_remote_args,
        error_prefix="Failed to complete distributed index building",
    )

    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(f"Index building failed: {'; '.join(error_messages)}")

    # Reload dataset to get the latest state after fragment index creation
    dataset = LanceDataset(
        uri,
        **_dataset_load_kwargs(merged_storage_options, namespace_kwargs, block_size),
    )

    logger.info("Phase 2: Merging index metadata for index ID: %s", index_id)
    # Convert IndexConfig to string for merge_index_metadata which expects a string
    # (lance's create_scalar_index converts IndexConfig to "scalar" internally)
    index_type_str = "scalar" if isinstance(index_type, IndexConfig) else index_type
    merge_index_metadata_compat(dataset, index_id, index_type=index_type_str, **kwargs)

    logger.info("Phase 3: Creating and committing scalar index '%s'", name)

    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful index creation results found")

    fields = successful_results[0]["fields"]

    index = Index(
        uuid=index_id,
        name=name,
        fields=fields,
        dataset_version=dataset.version,
        fragment_ids=set(fragment_ids_to_use),
        index_version=0,
    )

    create_index_op = lance.LanceOperation.CreateIndex(
        new_indices=[index],
        removed_indices=[],
    )

    updated_dataset = lance.LanceDataset.commit(
        uri,
        create_index_op,
        read_version=dataset.version,
        storage_options=merged_storage_options,
        **namespace_kwargs,
    )

    logger.info(
        "Successfully created distributed scalar index '%s' with three-phase workflow",
        name,
    )
    logger.info(
        "Index ID: %s, Fragments: %d, Workers: %d",
        index_id,
        len(fragment_ids_to_use),
        len(fragment_batches),
    )
    return updated_dataset


# ---------------------------------------------------------------------------
# Distributed vector index support (IVF_* and IVF_HNSW_* families)
# ---------------------------------------------------------------------------

# Vector index types supported by the distributed merge pipeline.
_VECTOR_INDEX_TYPES = {
    "IVF_FLAT",
    "IVF_PQ",
    "IVF_SQ",
    "IVF_HNSW_FLAT",
    "IVF_HNSW_PQ",
    "IVF_HNSW_SQ",
}


def _normalize_index_type(index_type: Any) -> str:
    """Normalize index type to upper-case string and validate support.

    Parameters
    ----------
    index_type : str or enum-like
        Vector index type. Must be one of the precise distributed vector
        types supported by Lance.
    """

    if hasattr(index_type, "value") and isinstance(index_type.value, str):
        index_type_name = index_type.value.upper()
    elif isinstance(index_type, str):
        index_type_name = index_type.upper()
    else:
        raise TypeError(
            "index_type must be a string or an enum-like object with a string 'value' "
            f"attribute, got {type(index_type)}"
        )

    if index_type_name not in _VECTOR_INDEX_TYPES:
        raise ValueError(
            "Distributed vector indexing only supports the following index types: "
            f"{sorted(_VECTOR_INDEX_TYPES)}, not '{index_type_name}'"
        )

    return index_type_name


def _check_pylance_version() -> None:
    """Ensure pylance (lance) provides distributed vector APIs."""

    try:
        lance_version = version.parse(lance.__version__)
        min_required_version = version.parse("7.0.0b7")

        if lance_version < min_required_version:
            raise RuntimeError(
                "Distributed vector indexing requires pylance >= 7.0.0b7, but found "
                f"{lance.__version__}. The distributed vector interfaces are not "
                "available in older versions. Please upgrade pylance by running: "
                "pip install --upgrade pylance"
            )

        logger.info("Pylance version check passed: %s >= 7.0.0b7", lance.__version__)

    except AttributeError as err:  # pragma: no cover - defensive
        raise RuntimeError(
            "Cannot determine pylance version. Distributed vector indexing requires "
            "pylance >= 7.0.0b7. Please upgrade pylance by running: "
            "pip install --upgrade pylance"
        ) from err


def _validate_metric(metric: str) -> str:
    """Normalize and validate the distance metric string."""

    if not isinstance(metric, str):
        raise TypeError(f"Metric must be a string, got {type(metric)}")

    metric_lower = metric.lower()
    valid_metrics = {"l2", "cosine", "euclidean", "dot", "hamming"}
    if metric_lower not in valid_metrics:
        raise ValueError(
            f"Metric {metric} not supported. Valid: {sorted(valid_metrics)}"
        )
    return metric_lower


def _handle_vector_fragment_index(
    dataset_uri: str,
    column: str,
    index_type: str,
    name: str,
    index_uuid: str,
    replace: bool,
    metric: str,
    num_partitions: Optional[int],
    num_sub_vectors: Optional[int],
    ivf_centroids: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
    pq_codebook: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    **kwargs: Any,
):
    """Create a fragment handler closure for vector index builds."""

    def func(fragment_ids: list[int]) -> dict[str, Any]:
        try:
            if not fragment_ids:
                raise ValueError("fragment_ids cannot be empty")

            for fragment_id in fragment_ids:
                if fragment_id < 0 or fragment_id > 0xFFFFFFFF:
                    raise ValueError(f"Invalid fragment_id: {fragment_id}")

            namespace_kwargs = get_namespace_kwargs(
                namespace_impl, namespace_properties, table_id
            )
            dataset = LanceDataset(
                dataset_uri,
                **_dataset_load_kwargs(storage_options, namespace_kwargs, block_size),
            )
            available_fragments = {f.fragment_id for f in dataset.get_fragments()}
            invalid_fragments = set(fragment_ids) - available_fragments
            if invalid_fragments:
                raise ValueError(f"Fragment IDs {invalid_fragments} do not exist")

            logger.info(
                "Building distributed vector index for fragments %s using "
                "LanceDataset.create_index",
                fragment_ids,
            )

            resolved_ivf_centroids = _ray_get_index_artifact(ivf_centroids)
            resolved_pq_codebook = _ray_get_index_artifact(pq_codebook)

            segment_index = dataset.create_index_uncommitted(
                column=column,
                index_type=index_type,
                name=name,
                metric=metric,
                replace=replace,
                num_partitions=num_partitions,
                ivf_centroids=resolved_ivf_centroids,
                pq_codebook=resolved_pq_codebook,
                num_sub_vectors=num_sub_vectors,
                storage_options=storage_options,
                train=True,
                fragment_ids=fragment_ids,
                **kwargs,
            )

            logger.info(
                "Fragment vector index created successfully for fragments %s",
                fragment_ids,
            )

            return {
                "status": "success",
                "fragment_ids": fragment_ids,
                "segment_index": segment_index,
                "uuid": getattr(segment_index, "uuid", index_uuid),
            }

        except Exception as exc:  # pragma: no cover - exercised via integration tests
            logger.error(
                "Fragment vector index task failed for fragments %s: %s",
                fragment_ids,
                exc,
            )
            return {
                "status": "error",
                "fragment_ids": fragment_ids,
                "error": str(exc),
            }

    return func


def create_index(
    uri: Optional[Union[str, "lance.LanceDataset"]] = None,
    column: str = "",
    index_type: str | Any = "",
    name: Optional[str] = None,
    *,
    replace: bool = True,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    fragment_ids: Optional[list[int]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    metric: str = "l2",
    num_partitions: Optional[int] = None,
    num_sub_vectors: Optional[int] = None,
    sample_rate: int = 256,
    segment_native: bool = True,
    training_mode: Literal["driver", "distributed"] = "driver",
    training_max_iters: int = 50,
    training_tolerance: float = 1e-4,
    ivf_centroids: Optional[
        pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray
    ] = None,
    pq_codebook: Optional[
        pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray
    ] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
    """Build distributed vector indices with Ray.

    This function mirrors :func:`create_scalar_index` but targets the precise
    vector index families supported by Lance's distributed segment APIs.

    Args:
        uri: Lance dataset or URI to build index on
        column: Column name to index
        index_type: Type of index to build (e.g., "IVF_PQ", "IVF_HNSW_PQ")
        name: Name of the index (generated if None)
        replace: Whether to replace existing index with the same name (default: True)
        num_workers: Number of Ray workers to use (keyword-only)
        storage_options: Storage options for the dataset (keyword-only)
        block_size: Block size in bytes to use when loading the dataset (keyword-only)
        fragment_ids: Optional list of fragment IDs to build index segments on.
        ray_remote_args: Options for Ray tasks (keyword-only)
        metric: Distance metric to use (default: "l2")
        num_partitions: Number of IVF partitions (optional)
        num_sub_vectors: Number of PQ sub-vectors (optional)
        sample_rate: Number of rows sampled per IVF partition and PQ centroid (default: 256)
        segment_native: Whether to directly commit worker-produced segments.
            Defaults to True. Set to False to use the driver-finalized
            compatibility path that runs build_all() before commit.
        training_mode: Where missing IVF/PQ training artifacts are trained.
            "driver" preserves Lance's existing driver IndicesBuilder path.
            "distributed" runs Ray-distributed KMeans for IVF and PQ training.
        training_max_iters: Maximum KMeans iterations for distributed training.
        training_tolerance: Relative KMeans loss tolerance for distributed training.
        ivf_centroids: Pre-computed IVF centroids (optional)
        pq_codebook: Pre-computed PQ codebook (optional)
        **kwargs: Additional arguments to pass to the fragment index build entrypoint

    Returns:
        Updated Lance dataset with the index created
    """

    _check_pylance_version()

    if not column:
        raise ValueError("Column name cannot be empty")

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")

    if sample_rate <= 0:
        raise ValueError(f"sample_rate must be positive, got {sample_rate}")

    if training_mode not in {"driver", "distributed"}:
        raise ValueError(
            "training_mode must be one of {'driver', 'distributed'}, "
            f"got {training_mode!r}"
        )

    if training_max_iters <= 0:
        raise ValueError(
            f"training_max_iters must be positive, got {training_max_iters}"
        )

    if training_tolerance < 0:
        raise ValueError(
            f"training_tolerance must be non-negative, got {training_tolerance}"
        )

    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    index_type_name = _normalize_index_type(index_type)
    metric_lower = _validate_metric(metric)

    index_id = str(uuid.uuid4())
    logger.info("Starting distributed vector index build with ID: %s", index_id)

    merged_storage_options: dict[str, Any] = {}
    if storage_options:
        merged_storage_options.update(storage_options)

    if isinstance(uri, str | type(None)):
        # URI or namespace mode
        validate_uri_or_namespace(uri, namespace_impl, table_id)

        # Resolve URI and storage options from namespace if provided
        namespace = get_or_create_namespace(namespace_impl, namespace_properties)
        if namespace is not None and table_id is not None:
            from lance_namespace import DescribeTableRequest

            describe_response = namespace.describe_table(
                DescribeTableRequest(id=table_id)
            )
            uri = describe_response.location
            if describe_response.storage_options:
                merged_storage_options.update(describe_response.storage_options)

        dataset_uri = uri
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        dataset_obj = LanceDataset(
            dataset_uri,
            **_dataset_load_kwargs(
                merged_storage_options, namespace_kwargs, block_size
            ),
        )
    else:
        # LanceDataset object passed directly
        dataset_obj = uri
        dataset_uri = dataset_obj.uri
        if not merged_storage_options:
            merged_storage_options = (
                getattr(dataset_obj, "_storage_options", None) or {}
            )
        namespace_kwargs = {}

    try:
        dataset_obj.schema.field(column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset_obj.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc

    if name is None:
        name = f"{column}_idx"

    if not replace:
        index_exists = False
        try:
            existing_indices = dataset_obj.list_indices()
            existing_names = {idx["name"] for idx in existing_indices}
            index_exists = name in existing_names
        except (
            Exception
        ):  # pragma: no cover - list_indices() not available in older lance versions
            pass
        if index_exists:
            raise ValueError(
                f"Index with name '{name}' already exists. Set replace=True "
                "to replace it."
            )

    fragments = dataset_obj.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    if fragment_ids is not None:
        if not fragment_ids:
            raise ValueError("fragment_ids cannot be empty")

        available_fragment_ids = {fragment.fragment_id for fragment in fragments}
        invalid_fragments = set(fragment_ids) - available_fragment_ids
        if invalid_fragments:
            raise ValueError(
                f"Fragment IDs {invalid_fragments} do not exist in dataset"
            )

        requested_fragment_ids = set(fragment_ids)
        fragments = [
            fragment
            for fragment in fragments
            if fragment.fragment_id in requested_fragment_ids
        ]

    fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    if num_workers > len(fragment_ids_to_use):
        num_workers = len(fragment_ids_to_use)
        logger.info("Adjusted num_workers to %d to match fragment count", num_workers)

    fragment_batches = _distribute_fragments_balanced(
        fragments, num_workers=num_workers, logger=logger
    )

    ivf_centroids_artifact = ivf_centroids
    pq_codebook_artifact = pq_codebook

    pq_index_types = {"IVF_PQ", "IVF_HNSW_PQ"}
    needs_pq = index_type_name in pq_index_types
    needs_ivf_training = ivf_centroids_artifact is None
    needs_pq_training = needs_pq and pq_codebook_artifact is None

    if needs_pq and pq_codebook_artifact is not None and ivf_centroids_artifact is None:
        raise ValueError(
            "ivf_centroids must be provided together with pq_codebook for "
            "PQ-based vector indices; PQ codebooks are trained against IVF "
            "residuals and cannot be safely reused without matching IVF centroids"
        )

    if needs_ivf_training or needs_pq_training:
        logger.info(
            "Phase 1: Training vector artifacts (mode=%s, index_type=%s, metric=%s)",
            training_mode,
            index_type_name,
            metric_lower,
        )

    if training_mode == "distributed" and (needs_ivf_training or needs_pq_training):
        (
            ivf_centroids_artifact,
            pq_codebook_artifact,
            num_partitions,
            num_sub_vectors,
        ) = _train_vector_index_artifacts_distributed(
            dataset_uri=dataset_uri,
            column=column,
            index_type=index_type_name,
            metric=metric_lower,
            num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
            sample_rate=sample_rate,
            fragment_batches=fragment_batches,
            num_workers=num_workers,
            storage_options=merged_storage_options,
            block_size=block_size,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            ray_remote_args=ray_remote_args,
            ivf_centroids=ivf_centroids_artifact,
            pq_codebook=pq_codebook_artifact,
            max_iters=training_max_iters,
            tolerance=training_tolerance,
        )
    elif needs_ivf_training or needs_pq_training:
        builder = IndicesBuilder(dataset_obj, column)
        num_rows = dataset_obj.count_rows()
        dimension = builder.dimension

        if needs_ivf_training:
            requested_num_partitions = num_partitions
            logger.info(
                "Training IVF on driver with requested_num_partitions=%s, "
                "num_rows=%d, dimension=%d, sample_rate=%d",
                requested_num_partitions,
                num_rows,
                dimension,
                sample_rate,
            )
            ivf_model = builder.train_ivf(
                num_partitions=requested_num_partitions,
                distance_type=metric_lower,
                sample_rate=sample_rate,
            )
            ivf_centroids_artifact = ivf_model.centroids
            num_partitions = ivf_model.num_partitions
            logger.info(
                "IVF training completed: num_partitions=%d",
                num_partitions,
            )
        else:
            ivf_model = None

        if needs_pq_training:
            if ivf_model is None:
                raise ValueError(
                    "pq_codebook must be provided when ivf_centroids is provided "
                    "in driver training mode; use training_mode='distributed' "
                    "to train both artifacts together"
                )
            requested_num_sub_vectors = num_sub_vectors
            logger.info(
                "Training PQ codebook on driver: requested_num_sub_vectors=%s, "
                "sample_rate=%d",
                requested_num_sub_vectors,
                sample_rate,
            )
            pq_model = builder.train_pq(
                ivf_model,
                num_subvectors=requested_num_sub_vectors,
                sample_rate=sample_rate,
            )
            pq_codebook_artifact = pq_model.codebook
            num_sub_vectors = pq_model.num_subvectors
            logger.info("PQ training completed: num_sub_vectors=%d", num_sub_vectors)

    if num_partitions is None:
        num_partitions = _infer_num_partitions_from_artifact(ivf_centroids_artifact)
    if needs_pq and num_sub_vectors is None:
        num_sub_vectors = _infer_num_sub_vectors_from_artifact(pq_codebook_artifact)

    if not needs_ivf_training and not needs_pq_training:
        logger.info(
            "Phase 1: Reusing provided vector training artifacts "
            "(index_type=%s, metric=%s)",
            index_type_name,
            metric_lower,
        )

    if ivf_centroids_artifact is None:
        raise ValueError(
            "ivf_centroids must be provided or trainable for IVF-based "
            "distributed vector indices"
        )

    if needs_pq and pq_codebook_artifact is None:
        raise ValueError(
            "pq_codebook must be provided or trainable for PQ-based "
            "distributed vector indices"
        )

    if num_partitions is None:
        raise ValueError("num_partitions must be provided when using ivf_centroids")

    if needs_pq and num_sub_vectors is None:
        raise ValueError("num_sub_vectors must be provided when using pq_codebook")

    logger.info(
        "Phase 2: Distributing vector index build across %d workers for %d fragments",
        len(fragment_batches),
        len(fragment_ids_to_use),
    )

    def create_fragment_handler() -> Any:
        shared_ivf_centroids, shared_pq_codebook = (
            _put_vector_index_artifacts_in_object_store(
                ivf_centroids_artifact,
                pq_codebook_artifact,
            )
        )
        return _handle_vector_fragment_index(
            dataset_uri=dataset_uri,
            column=column,
            index_type=index_type_name,
            name=name,
            index_uuid=index_id,
            replace=replace,
            metric=metric_lower,
            num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
            ivf_centroids=shared_ivf_centroids,
            pq_codebook=shared_pq_codebook,
            storage_options=merged_storage_options,
            block_size=block_size,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            **kwargs,
        )

    results = _map_async_with_pool(
        create_fragment_handler=create_fragment_handler,
        fragment_batches=fragment_batches,
        num_workers=num_workers,
        ray_remote_args=ray_remote_args,
        error_prefix="Failed to complete distributed vector index building",
    )

    failed_results = [r for r in results if r.get("status") == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results if "error" in r]
        raise RuntimeError("Vector index building failed: " + "; ".join(error_messages))

    dataset_obj = LanceDataset(
        dataset_uri,
        **_dataset_load_kwargs(merged_storage_options, namespace_kwargs, block_size),
    )

    successful_results = [r for r in results if r.get("status") == "success"]
    if not successful_results:
        raise RuntimeError("No successful vector index creation results found")

    segment_indices = [r["segment_index"] for r in successful_results]
    if segment_native:
        logger.info(
            "Phase 3: Committing segment-native worker segments for vector index '%s'",
            name,
        )
        segments = segment_indices
    else:
        logger.info(
            "Phase 3: Driver-finalizing and committing index segments for vector index '%s'",
            name,
        )
        segment_builder = (
            dataset_obj.create_index_segment_builder()
            .with_index_type(index_type_name)
            .with_segments(segment_indices)
        )
        segments = segment_builder.build_all()

    updated_dataset = dataset_obj.commit_existing_index_segments(
        index_name=name,
        column=column,
        segments=segments,
    )

    logger.info(
        "Successfully created distributed vector index '%s'",
        name,
    )
    logger.info(
        "Index ID: %s, Fragments: %d, Workers: %d",
        index_id,
        len(fragment_ids_to_use),
        len(fragment_batches),
    )

    return updated_dataset


def optimize_indices(
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    indices: Optional[list[str]] = None,
    num_indices_to_merge: int = 1,
    retrain: bool = False,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
    """Optimize indices for newly added data (incremental index update).

    As new data arrives it is not added to existing indexes automatically.
    This function adds the new data to existing indexes, restoring search
    performance. It does not retrain the index by default; it only assigns
    the new data to existing partitions, so the update is quicker than
    retraining but may have less accuracy if the new data has different
    patterns.

    Delegates to ``dataset.optimize.optimize_indices()`` from the lance library.

    Args:
        uri: The URI of the Lance dataset. Either uri OR
            (namespace_impl + table_id) must be provided.
        table_id: The table identifier as a list of strings. Must be provided
            together with namespace_impl.
        indices: Optional list of index names to optimize. If None, all indices
            on the dataset are optimized. Passed to lance as ``index_names``.
            When the dataset has both scalar and vector columns, specifying
            only the index names you need (e.g. ``["label_btree"]``) can avoid
            internal errors on list/vector fields in some lance versions.
            If you still get an error about ``vector.item`` / ``List(Float64)``,
            this is a known lance bug: use a dataset with only scalar columns
            for scalar index optimization until lance fixes it.
        num_indices_to_merge: Number of delta indices to merge (default 1).
            If set to 0, a new delta index will be created instead of merging.
        retrain: If True, retrain the whole index from current data; all indices
            are merged into one and ``num_indices_to_merge`` is ignored.
            Use when data distribution has changed significantly (default False).
        storage_options: Storage options for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with table_id for resolving the dataset location.
        namespace_properties: Properties for connecting to the namespace.
        **kwargs: Additional arguments passed through to the underlying
            ``DatasetOptimizer.optimize_indices`` API.

    Returns:
        The Lance dataset instance (optimization is applied in-place on storage).

    Raises:
        ValueError: If input parameters are invalid.
        RuntimeError: If optimize_indices is not supported by the current
            lance version or if the operation fails.

    Example:
        >>> import lance_ray
        >>> ds = lance_ray.optimize_indices("path/to/dataset")
        >>> ds = lance_ray.optimize_indices(
        ...     "path/to/dataset",
        ...     indices=["vec_idx", "scalar_idx"],
        ...     num_indices_to_merge=2,
        ... )
    """
    logger.info(
        "Starting optimize_indices: uri=%s, indices=%s, num_indices_to_merge=%s, retrain=%s",
        uri if uri else "(from namespace)",
        indices,
        num_indices_to_merge,
        retrain,
    )
    validate_uri_or_namespace(uri, namespace_impl, table_id)

    merged_storage_options: dict[str, Any] = {}
    if storage_options:
        merged_storage_options.update(storage_options)

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is not None and table_id is not None:
        from lance_namespace import DescribeTableRequest

        describe_response = namespace.describe_table(DescribeTableRequest(id=table_id))
        uri = describe_response.location
        if describe_response.storage_options:
            merged_storage_options.update(describe_response.storage_options)
        logger.info(
            "Resolved dataset URI from namespace (table_id=%s): %s",
            table_id,
            uri,
        )

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    dataset = LanceDataset(
        uri,
        storage_options=merged_storage_options,
        **namespace_kwargs,
    )
    logger.info(
        "Loaded dataset: uri=%s, version=%s",
        uri,
        getattr(dataset, "version", "unknown"),
    )

    if not hasattr(dataset, "optimize"):
        raise RuntimeError(
            "LanceDataset has no 'optimize' property. Please ensure "
            "lance is installed with a version that provides DatasetOptimizer."
        )
    optimizer = dataset.optimize
    if not hasattr(optimizer, "optimize_indices"):
        raise RuntimeError(
            "optimize_indices is not available on DatasetOptimizer. Please ensure "
            "lance is installed with a version that provides optimize_indices."
        )

    call_kw: dict[str, Any] = {
        "num_indices_to_merge": num_indices_to_merge,
        "retrain": retrain,
        **kwargs,
    }
    if indices is not None:
        call_kw["index_names"] = indices

    logger.info(
        "Calling DatasetOptimizer.optimize_indices with: %s",
        {k: v for k, v in call_kw.items() if k != "storage_options"},
    )
    optimizer.optimize_indices(**call_kw)
    logger.info(
        "optimize_indices completed successfully for dataset uri=%s",
        uri,
    )
    return dataset
