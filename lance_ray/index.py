# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Callable
from typing import Any, Literal, Optional, TypeAlias, cast, get_args

import lance
import pyarrow as pa
import ray
from lance.dataset import Index, IndexConfig, LanceDataset
from lance.indices import IndicesBuilder
from packaging import version
from ray.util.multiprocessing import Pool

from .field_path import resolve_arrow_field_path, resolve_dataset_field_path
from .utils import (
    get_namespace_kwargs,
    has_namespace_params,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

logger = logging.getLogger(__name__)


# Spelled as strings because pyarrow's classes are not subscriptable at
# runtime, only in the stubs.
_VectorIndexArtifact: TypeAlias = (
    "pa.Array[Any] | pa.FixedSizeListArray[Any] | pa.FixedShapeTensorArray[Any] | None"
)
_VectorIndexArtifactRef: TypeAlias = "_VectorIndexArtifact | ray.ObjectRef[Any]"
_VectorIndexArtifactRefs: TypeAlias = (
    "tuple[_VectorIndexArtifactRef, _VectorIndexArtifactRef]"
)

#: The closure a fragment handler factory hands to the worker pool.
_FragmentHandler: TypeAlias = Callable[[list[int]], dict[str, Any]]


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


def _index_exists(dataset: LanceDataset, name: str) -> bool:
    return any(index.name == name for index in dataset.describe_indices())


def _distribute_fragments_balanced(
    fragments: list[Any], num_segments: int, logger: logging.Logger
) -> list[list[int]]:
    """Distribute fragments across index segments using a balanced algorithm.

    This function implements a greedy algorithm that assigns fragments to the
    segment with the currently smallest total workload, helping to balance the
    processing time across segment batches.

    Parameters
    ----------
    fragments : list
        List of Lance fragment objects.
    num_segments : int
        Number of segment batches to distribute fragments across.
    logger : logging.Logger
        Logger instance for debugging information.

    Returns
    -------
    list[list[int]]
        Each inner list contains fragment IDs for one segment batch.
    """
    if not fragments:
        return [[] for _ in range(num_segments)]

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

    segment_batches: list[list[int]] = [[] for _ in range(num_segments)]
    segment_workloads = [0] * num_segments

    # Greedy assignment: assign each fragment to the segment with minimum workload
    for frag_info in fragment_info:
        min_workload_idx = min(range(num_segments), key=lambda i: segment_workloads[i])
        segment_batches[min_workload_idx].append(frag_info["id"])
        segment_workloads[min_workload_idx] += frag_info["size"]

    total_size = sum(info["size"] for info in fragment_info)
    logger.info("Fragment distribution statistics:")
    logger.info("  Total fragments: %d", len(fragment_info))
    logger.info("  Total size: %d", total_size)
    logger.info("  Segments: %d", num_segments)

    for i, (batch, workload) in enumerate(
        zip(segment_batches, segment_workloads, strict=False)
    ):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info(
            "  Segment %d: %d fragments, workload: %d (%.1f%%)",
            i,
            len(batch),
            workload,
            percentage,
        )

    non_empty_batches = [batch for batch in segment_batches if batch]
    return non_empty_batches


def _map_async_with_pool(
    create_fragment_handler: Callable[[], _FragmentHandler],
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
        results: list[dict[str, Any]] = rst_futures.get()
    except Exception as exc:  # pragma: no cover - exercised via integration tests
        raise RuntimeError(f"{error_prefix}: {exc}") from exc
    finally:
        pool.close()
        pool.join()

    return results


def _resolve_num_segments(num_workers: int, num_segments: Optional[int]) -> int:
    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")
    if num_segments is None:
        return num_workers
    if num_segments <= 0:
        raise ValueError(f"num_segments must be positive, got {num_segments}")
    return num_segments


def _is_ray_object_ref(value: Any) -> bool:
    object_ref_type = getattr(ray, "ObjectRef", None)
    return object_ref_type is not None and isinstance(value, object_ref_type)


def _ray_put_index_artifact(value: Any) -> _VectorIndexArtifactRef:
    if value is None or _is_ray_object_ref(value):
        return cast("_VectorIndexArtifactRef", value)
    return ray.put(value)


def _ray_get_index_artifact(value: Any) -> _VectorIndexArtifact:
    if _is_ray_object_ref(value):
        return cast("_VectorIndexArtifact", ray.get(value))
    return cast("_VectorIndexArtifact", value)


def _put_vector_index_artifacts_in_object_store(
    ivf_centroids: _VectorIndexArtifact,
    pq_codebook: _VectorIndexArtifact,
) -> _VectorIndexArtifactRefs:
    return (
        _ray_put_index_artifact(ivf_centroids),
        _ray_put_index_artifact(pq_codebook),
    )


def _build_rabitq_model(*, dimension: int, num_bits: int = 1) -> str:
    """Build one shared RaBitQ rotation model for distributed IVF_RQ shards.

    ``dimension`` is the vector width and must satisfy Lance's IVF_RQ
    requirement that it is divisible by 8. ``num_bits`` controls the number of
    RaBitQ code bits per vector dimension and defaults to Lance's IVF_RQ default
    of 1; supported values are validated by ``lance.lance.indices.build_rq_model``.
    """
    from lance.lance import indices

    return indices.build_rq_model(dimension=dimension, num_bits=num_bits)


_ScalarIndexType: TypeAlias = Literal[
    "BTREE",
    "BITMAP",
    "LABEL_LIST",
    "INVERTED",
    "FTS",
    "NGRAM",
    "ZONEMAP",
    "BLOOMFILTER",
    "RTREE",
]
_SCALAR_INDEX_TYPES = get_args(_ScalarIndexType)
_SCALAR_SEGMENT_INDEX_TYPES = frozenset(_SCALAR_INDEX_TYPES)


def _scalar_index_type_name(index_type: str | IndexConfig) -> str | None:
    if isinstance(index_type, str):
        return index_type.upper()
    if isinstance(index_type, IndexConfig):
        return index_type.index_type.upper()
    return None


def _handle_scalar_segment_index(
    dataset_uri: str,
    column: str,
    index_type: _ScalarIndexType | IndexConfig,
    name: str,
    replace: bool,
    train: bool,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    **kwargs: Any,
) -> _FragmentHandler:
    """Create a fragment handler closure for scalar segment index builds."""

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
                "Building distributed scalar segment index for fragments %s using "
                "create_index_uncommitted",
                fragment_ids,
            )

            segment_index = dataset.create_index_uncommitted(
                column=column,
                # pylance annotates this as ``str`` but accepts an
                # ``IndexConfig`` too (see ``_prepare_scalar_index_request``).
                index_type=index_type,  # type: ignore[arg-type]
                name=name,
                replace=replace,
                train=train,
                storage_options=storage_options,
                fragment_ids=fragment_ids,
                **kwargs,
            )

            logger.info(
                "Fragment scalar segment index created successfully for fragments %s",
                fragment_ids,
            )

            return {
                "status": "success",
                "fragment_ids": fragment_ids,
                "segment_index": segment_index,
            }

        except Exception as exc:  # pragma: no cover - exercised via integration tests
            logger.error(
                "Fragment scalar segment index task failed for fragments %s: %s",
                fragment_ids,
                exc,
            )
            return {
                "status": "error",
                "fragment_ids": fragment_ids,
                "error": str(exc),
            }

    return func


def _handle_fragment_index(
    dataset_uri: str,
    column: str,
    index_type: _ScalarIndexType | IndexConfig,
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
) -> _FragmentHandler:
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

            field_id = resolve_dataset_field_path(dataset, column).field_id

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


def merge_index_metadata_compat(
    dataset: LanceDataset,
    index_id: str,
    index_type: str,
    **kwargs: Any,
) -> Any:
    """Call ``merge_index_metadata`` with backwards compatible signature."""
    try:
        return dataset.merge_index_metadata(
            index_id, index_type, batch_readhead=kwargs.get("batch_readhead")
        )
    except TypeError:
        return dataset.merge_index_metadata(index_id)  # type: ignore[call-arg]


def create_scalar_index(
    uri: Optional[str | lance.LanceDataset] = None,
    *,
    column: str,
    index_type: _ScalarIndexType | IndexConfig,
    table_id: Optional[list[str]] = None,
    name: Optional[str] = None,
    replace: bool = True,
    train: bool = True,
    fragment_ids: Optional[list[int]] = None,
    index_uuid: Optional[str] = None,
    num_workers: int = 4,
    num_segments: Optional[int] = None,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    **kwargs: Any,
) -> lance.LanceDataset:
    """Build scalar indices with Ray in a distributed workflow.

    Args:
        uri: Lance dataset or URI to build index on. Either uri OR
            (namespace_impl + table_id) must be provided. When passed a
            ``LanceDataset``, its dataset URI is retained so distributed
            workers and the final commit stay on the same Lance branch.
        column: Column name to index.
        index_type: Type of index to build ("BTREE", "BITMAP", "LABEL_LIST",
            "INVERTED", "FTS", "NGRAM", "ZONEMAP", "BLOOMFILTER", "RTREE")
            or IndexConfig object.
        table_id: The table identifier as a list of strings. Must be provided
            together with namespace_impl.
        name: Name of the index (generated if None).
        replace: Whether to replace existing index with the same name (default: True).
        train: Whether to train the index (default: True).
        fragment_ids: Optional list of fragment IDs to build index on.
        index_uuid: Optional fragment UUID for distributed indexing.
        num_workers: Maximum number of Ray Pool workers to use (keyword-only).
        num_segments: Number of fragment batches / index segments to create
            (keyword-only). Defaults to num_workers for backwards compatibility.
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
        TypeError: If the column type is incompatible with the index type.
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

    # Match the vector index entrypoint: URI / namespace mode is selected by
    # the input value; every other value is treated as a dataset object.
    if isinstance(uri, str | type(None)):
        validate_uri_or_namespace(uri, namespace_impl, table_id)

    if not column:
        raise ValueError("Column name cannot be empty")

    requested_num_segments = _resolve_num_segments(num_workers, num_segments)

    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    if isinstance(index_type, str):
        if index_type not in _SCALAR_INDEX_TYPES:
            raise ValueError(
                "Index type must be one of "
                f"{list(_SCALAR_INDEX_TYPES)}, not '{index_type}'"
            )

        if index_type not in _SCALAR_SEGMENT_INDEX_TYPES:
            raise ValueError(
                "Distributed indexing currently supports "
                f"{sorted(_SCALAR_SEGMENT_INDEX_TYPES)} index types, "
                f"not '{index_type}'"
            )
    elif not isinstance(index_type, IndexConfig):
        raise ValueError(
            "index_type must be a string literal or IndexConfig object, got "
            f"{type(index_type)}"
        )

    # Note: Ray initialization is now handled by the Pool, following the pattern from io.py
    # This removes the need for explicit ray.init() calls

    if isinstance(uri, str | type(None)):
        # Resolve URI and get storage options from namespace if provided.
        dataset_uri, merged_storage_options = resolve_namespace_table(
            uri, storage_options, namespace_impl, namespace_properties, table_id
        )
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        dataset = LanceDataset(
            dataset_uri,
            **_dataset_load_kwargs(
                merged_storage_options, namespace_kwargs, block_size
            ),
        )
    else:
        dataset = uri
        dataset_uri = dataset.uri
        merged_storage_options = (
            storage_options or getattr(dataset, "_storage_options", None) or {}
        )
        namespace_kwargs = {}

    try:
        resolved_column = resolve_dataset_field_path(dataset, column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc
    column = resolved_column.path
    field = resolved_column.field

    # Check column type according to index type
    value_type = field.type
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_type = field.type.value_type

    if isinstance(index_type, str):
        match index_type:
            case "INVERTED" | "FTS" | "NGRAM":
                if not (
                    pa.types.is_string(value_type)
                    or pa.types.is_large_string(value_type)
                ):
                    raise TypeError(
                        f"Column {column} must be string type for {index_type} "
                        f"index, got {value_type}"
                    )
            case "BTREE" | "ZONEMAP":
                is_supported = (
                    pa.types.is_integer(value_type)
                    or pa.types.is_floating(value_type)
                    or pa.types.is_date32(value_type)
                    or pa.types.is_string(value_type)
                    or pa.types.is_large_string(value_type)
                )
                if not is_supported:
                    raise TypeError(
                        f"Column {column} must be numeric, date32, or string type for "
                        f"{index_type} index, got {value_type}"
                    )
            case "LABEL_LIST":
                if not (
                    pa.types.is_list(field.type) or pa.types.is_large_list(field.type)
                ):
                    raise TypeError(
                        f"Column {column} must be list or large list type for "
                        f"LABEL_LIST index, got {field.type}"
                    )
            case _:
                # For other index types, skip strict validation to maintain compatibility
                pass

    use_segment_workflow = (
        _scalar_index_type_name(index_type) in _SCALAR_SEGMENT_INDEX_TYPES
    )

    if name is None:
        name = f"{column}_idx"

    if _index_exists(dataset, name):
        if not replace:
            raise ValueError(
                f"Index with name '{name}' already exists. Set replace=True "
                "to replace it."
            )
        if not use_segment_workflow:
            # Lance 4.0.0: fragment_ids + replace=True may hit an unimplemented path.
            # Keep the existing driver-side replacement behavior for the legacy workflow.
            dataset.drop_index(name)
            dataset = LanceDataset(
                dataset_uri,
                **_dataset_load_kwargs(
                    merged_storage_options, namespace_kwargs, block_size
                ),
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

    if requested_num_segments > len(fragment_ids_to_use):
        requested_num_segments = len(fragment_ids_to_use)
        logger.info(
            "Adjusted num_segments to %d to match fragment count",
            requested_num_segments,
        )

    fragment_batches = _distribute_fragments_balanced(
        fragments, num_segments=requested_num_segments, logger=logger
    )
    pool_workers = min(num_workers, len(fragment_batches))
    if pool_workers < num_workers:
        logger.info(
            "Limiting Ray Pool workers to %d (requested %d) because there are "
            "only %d non-empty segment batches",
            pool_workers,
            num_workers,
            len(fragment_batches),
        )

    def create_fragment_handler() -> Any:
        if use_segment_workflow:
            return _handle_scalar_segment_index(
                dataset_uri=dataset_uri,
                column=column,
                index_type=index_type,
                name=name,
                replace=replace,
                train=train,
                storage_options=merged_storage_options,
                block_size=block_size,
                namespace_impl=namespace_impl,
                namespace_properties=namespace_properties,
                table_id=table_id,
                **kwargs,
            )

        return _handle_fragment_index(
            dataset_uri=dataset_uri,
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
        "Phase 1: Distributing scalar index build across %d segment batches "
        "using up to %d workers for %d fragments",
        len(fragment_batches),
        pool_workers,
        len(fragment_ids_to_use),
    )

    results = _map_async_with_pool(
        create_fragment_handler=create_fragment_handler,
        fragment_batches=fragment_batches,
        num_workers=pool_workers,
        ray_remote_args=ray_remote_args,
        error_prefix="Failed to complete distributed index building",
    )

    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(f"Index building failed: {'; '.join(error_messages)}")

    # Reload dataset to get the latest state after fragment index creation
    dataset = LanceDataset(
        dataset_uri,
        **_dataset_load_kwargs(merged_storage_options, namespace_kwargs, block_size),
    )

    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful index creation results found")

    if use_segment_workflow:
        logger.info(
            "Phase 2: Committing scalar distributed index segments for index '%s'",
            name,
        )

        updated_dataset = dataset.commit_existing_index_segments(
            index_name=name,
            column=column,
            segments=[r["segment_index"] for r in successful_results],
        )

        logger.info(
            "Successfully created distributed scalar segment index '%s'",
            name,
        )
        logger.info(
            "Fragments: %d, Segments: %d, Workers: %d",
            len(fragment_ids_to_use),
            len(fragment_batches),
            pool_workers,
        )
        return updated_dataset

    logger.info("Phase 2: Merging index metadata for index ID: %s", index_id)
    # Convert IndexConfig to string for merge_index_metadata which expects a string
    # (lance's create_scalar_index converts IndexConfig to "scalar" internally)
    index_type_str = "scalar" if isinstance(index_type, IndexConfig) else index_type
    merge_index_metadata_compat(dataset, index_id, index_type=index_type_str, **kwargs)

    logger.info("Phase 3: Creating and committing scalar index '%s'", name)

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
        dataset_uri,
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
        "Index ID: %s, Fragments: %d, Segments: %d, Workers: %d",
        index_id,
        len(fragment_ids_to_use),
        len(fragment_batches),
        pool_workers,
    )
    return updated_dataset


# ---------------------------------------------------------------------------
# Distributed vector index support (IVF_* and IVF_HNSW_* families)
# ---------------------------------------------------------------------------

# Vector index types supported by the distributed merge pipeline.
_VECTOR_INDEX_TYPES = {
    "IVF_FLAT",
    "IVF_PQ",
    "IVF_RQ",
    "IVF_SQ",
    "IVF_HNSW_FLAT",
    "IVF_HNSW_PQ",
    "IVF_HNSW_SQ",
}


def _vector_dimension(field: pa.Field[Any]) -> int:
    if pa.types.is_fixed_size_list(field.type):
        return int(field.type.list_size)
    if isinstance(field.type, pa.FixedShapeTensorType) and len(field.type.shape) == 1:
        return field.type.shape[0]
    raise TypeError(
        f"Vector column must be FixedSizeListArray or 1-dimensional "
        f"FixedShapeTensorArray, got {field.type}"
    )


def _validate_vector_value_type(field: pa.Field[Any]) -> None:
    value_type = field.type.value_type
    if not (
        pa.types.is_floating(value_type) or pa.types.is_unsigned_integer(value_type)
    ):
        raise TypeError(
            "Vector column must have floating or unsigned integer value type, "
            f"got {value_type}"
        )


def _schema_names(schema: pa.Schema) -> list[str]:
    names = getattr(schema, "names", None)
    if names is not None:
        return list(names)
    return [field.name for field in schema]


class _NestedVectorIndicesBuilder:
    def __init__(self, dataset: LanceDataset, column: str, field: pa.Field[Any]):
        self.dataset = dataset
        self.column = column
        self.dimension = _vector_dimension(field)
        _validate_vector_value_type(field)

    def train_ivf(
        self,
        num_partitions: Optional[int] = None,
        *,
        distance_type: str = "l2",
        sample_rate: int = 256,
        max_iters: int = 50,
        fragment_ids: Optional[list[int]] = None,
        **kwargs: Any,
    ) -> Any:
        if kwargs.get("accelerator") is not None:
            raise NotImplementedError(
                "Nested vector IVF training does not support accelerator training"
            )

        from lance.indices.ivf import IvfModel
        from lance.lance import indices

        num_rows = _count_rows_for_fragments(self.dataset, fragment_ids)
        partition_count = _determine_num_partitions(num_partitions, num_rows)
        _verify_ivf_sample_rate(sample_rate, partition_count, num_rows)
        distance_type = _normalize_distance_type(distance_type)
        _verify_num_partitions(partition_count)

        centroids = indices.train_ivf_model(
            self.dataset._ds,
            self.column,
            self.dimension,
            partition_count,
            distance_type,
            sample_rate,
            max_iters,
            fragment_ids,
        )
        return IvfModel(centroids, distance_type)

    def train_pq(
        self,
        ivf_model: Any,
        num_subvectors: Optional[int] = None,
        *,
        sample_rate: int = 256,
        max_iters: int = 50,
        num_bits: int = 8,
        fragment_ids: Optional[list[int]] = None,
    ) -> Any:
        from lance.indices.pq import PqModel
        from lance.lance import indices

        num_rows = _count_rows_for_fragments(self.dataset, fragment_ids)
        num_subvectors = _normalize_pq_params(num_subvectors, self.dimension)
        _verify_pq_sample_rate(num_rows, sample_rate, num_bits)
        codebook = indices.train_pq_model(
            self.dataset._ds,
            self.column,
            self.dimension,
            num_subvectors,
            ivf_model.distance_type,
            sample_rate,
            max_iters,
            ivf_model.centroids,
            fragment_ids,
            num_bits=num_bits,
        )
        # ``train_pq_model`` is annotated as returning a generic ``pa.Array``
        # upstream, but always produces a fixed-size-list codebook.
        return PqModel(
            num_subvectors,
            cast("pa.FixedSizeListArray[Any]", codebook),
            num_bits=num_bits,
        )


def _count_rows_for_fragments(
    dataset: LanceDataset,
    fragment_ids: Optional[list[int]],
) -> int:
    if fragment_ids is None:
        return dataset.count_rows()

    row_count = 0
    for fragment_id in fragment_ids:
        fragment = dataset.get_fragment(fragment_id)
        if fragment is None:
            raise ValueError(f"Fragment id does not exist: {fragment_id}")
        row_count += fragment.count_rows()
    return row_count


def _determine_num_partitions(num_partitions: Optional[int], num_rows: int) -> int:
    if num_partitions is None:
        return round(math.sqrt(num_rows))
    return num_partitions


def _verify_base_sample_rate(sample_rate: int) -> None:
    if not isinstance(sample_rate, int) or sample_rate < 2:
        raise ValueError(
            f"The sample_rate must be an int greater than 1, got {sample_rate}"
        )


def _verify_ivf_sample_rate(
    sample_rate: int,
    num_partitions: int,
    num_rows: int,
) -> None:
    _verify_base_sample_rate(sample_rate)
    if num_partitions * sample_rate > num_rows:
        raise ValueError(
            "There are not enough rows in the dataset to create IVF centroids with"
            f" {num_partitions} partitions and a sample rate of {sample_rate}."
            f" {sample_rate * num_partitions} rows needed and there are {num_rows}"
        )


def _verify_num_partitions(num_partitions: int) -> None:
    if not isinstance(num_partitions, int):
        raise TypeError(f"num_partitions must be int, got {type(num_partitions)}")


def _normalize_distance_type(distance_type: str) -> str:
    if not isinstance(distance_type, str) or distance_type.lower() not in [
        "l2",
        "cosine",
        "euclidean",
        "dot",
        "hamming",
    ]:
        raise ValueError(f"Distance type {distance_type} not supported.")
    return distance_type.lower()


def _normalize_pq_params(num_subvectors: Optional[int], dimension: int) -> int:
    if num_subvectors is None:
        if dimension % 16 == 0:
            return dimension // 16
        if dimension % 8 == 0:
            return dimension // 8
        raise ValueError(
            f"vector dimension {dimension} is not divisible by 16 or 8."
            " PQ performance will be poor.  Please specify num_subvectors manually."
        )
    if not isinstance(num_subvectors, int):
        raise ValueError("num_subvectors must be an int")
    if num_subvectors < 1:
        raise ValueError("num_subvectors must be greater than 0")
    if num_subvectors > dimension:
        raise ValueError(
            "num_subvectors must be less than or equal to the dimension of the vectors"
        )
    if dimension % num_subvectors != 0:
        raise ValueError(
            f"dimension ({dimension}) must be divisible by num_subvectors "
            f"({num_subvectors}) without remainder"
        )
    return num_subvectors


def _verify_pq_sample_rate(num_rows: int, sample_rate: int, num_bits: int = 8) -> None:
    _verify_base_sample_rate(sample_rate)
    required_rows = (2**num_bits) * sample_rate
    if required_rows > num_rows:
        raise ValueError(
            "There are not enough rows in the dataset to create PQ codebook with "
            f"a sample rate of {sample_rate} and num_bits of {num_bits}. "
            f"{required_rows} rows needed and there are {num_rows}"
        )


def _indices_builder_for_field_path(
    dataset: LanceDataset,
    column: str,
    field: pa.Field[Any],
) -> IndicesBuilder | _NestedVectorIndicesBuilder:
    if column in _schema_names(dataset.schema):
        return IndicesBuilder(dataset, column)

    return _NestedVectorIndicesBuilder(dataset, column, field)


def _train_pq_for_field_path(
    builder: IndicesBuilder | _NestedVectorIndicesBuilder,
    ivf_model: Any,
    *,
    num_subvectors: Optional[int],
    sample_rate: int,
    num_bits: int,
    max_iters: Optional[int],
) -> Any:
    train_kwargs: dict[str, Any] = {
        "num_subvectors": num_subvectors,
        "sample_rate": sample_rate,
        "num_bits": num_bits,
    }
    if max_iters is not None:
        train_kwargs["max_iters"] = max_iters
    return builder.train_pq(ivf_model, **train_kwargs)


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
        min_required_version = version.parse("0.36.0")

        if lance_version < min_required_version:
            raise RuntimeError(
                "Distributed vector indexing requires pylance >= 0.36.0, but found "
                f"{lance.__version__}. The distributed vector interfaces are not "
                "available in older versions. Please upgrade pylance by running: "
                "pip install --upgrade pylance"
            )

        logger.info("Pylance version check passed: %s >= 0.36.0", lance.__version__)

    except AttributeError as err:  # pragma: no cover - defensive
        raise RuntimeError(
            "Cannot determine pylance version. Distributed vector indexing requires "
            "pylance >= 0.36.0. Please upgrade pylance by running: "
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
    ivf_centroids: _VectorIndexArtifactRef,
    pq_codebook: _VectorIndexArtifactRef,
    sample_rate: int = 256,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    **kwargs: Any,
) -> _FragmentHandler:
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
                # pylance accepts a numpy array or a fixed-size-list/tensor
                # array here; lance-ray also allows the ``pa.Array`` base type,
                # which centroid/codebook arrays always satisfy at runtime.
                ivf_centroids=resolved_ivf_centroids,  # type: ignore[arg-type]
                pq_codebook=resolved_pq_codebook,  # type: ignore[arg-type]
                num_sub_vectors=num_sub_vectors,
                sample_rate=sample_rate,
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
    uri: Optional[str | lance.LanceDataset] = None,
    column: str = "",
    index_type: str | Any = "",
    name: Optional[str] = None,
    *,
    replace: bool = True,
    num_workers: int = 4,
    num_segments: Optional[int] = None,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    metric: str = "l2",
    num_partitions: Optional[int] = None,
    num_sub_vectors: Optional[int] = None,
    sample_rate: int = 256,
    ivf_centroids: _VectorIndexArtifact = None,
    pq_codebook: _VectorIndexArtifact = None,
    rabitq_model: Optional[str] = None,
    **kwargs: Any,
) -> lance.LanceDataset:
    """Build distributed vector indices with Ray.

    This function mirrors :func:`create_scalar_index` but targets the precise
    vector index families supported by Lance's distributed merge pipeline.

    Args:
        uri: Lance dataset or URI to build index on
        column: Column name to index
        index_type: Type of index to build (e.g., "IVF_PQ", "IVF_RQ",
            "IVF_HNSW_PQ")
        name: Name of the index (generated if None)
        replace: Whether to replace existing index with the same name (default: True)
        num_workers: Maximum number of Ray Pool workers to use (keyword-only)
        num_segments: Number of fragment batches / index segments to create
            (keyword-only). Defaults to num_workers for backwards compatibility.
        storage_options: Storage options for the dataset (keyword-only)
        block_size: Block size in bytes to use when loading the dataset (keyword-only)
        ray_remote_args: Options for Ray tasks (keyword-only)
        metric: Distance metric to use (default: "l2")
        num_partitions: Number of IVF partitions (optional)
        num_sub_vectors: Number of PQ sub-vectors (optional)
        num_bits: Number of bits used to encode each PQ centroid (default: 8)
        sample_rate: Number of rows sampled per IVF partition and PQ centroid (default: 256)
        ivf_centroids: Pre-computed IVF centroids (optional)
        pq_codebook: Pre-computed PQ codebook (optional)
        rabitq_model: Pre-built RaBitQ model for IVF_RQ. If omitted, Lance-Ray
            builds one shared model on the driver and sends it to every worker.
            The model dimension is the vector column width and must be divisible
            by 8. The ``num_bits`` keyword controls RaBitQ bits per vector
            dimension and defaults to 1.
        **kwargs: Additional arguments to pass to the fragment index build entrypoint.

    Returns:
        Updated Lance dataset with the index created
    """

    _check_pylance_version()

    if not column:
        raise ValueError("Column name cannot be empty")

    requested_num_segments = _resolve_num_segments(num_workers, num_segments)

    if sample_rate <= 0:
        raise ValueError(f"sample_rate must be positive, got {sample_rate}")

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
        uri, merged_storage_options = resolve_namespace_table(
            uri, storage_options, namespace_impl, namespace_properties, table_id
        )
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
        resolved_column = resolve_arrow_field_path(dataset_obj.schema, column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset_obj.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc
    column = resolved_column.path
    field = resolved_column.field

    if name is None:
        name = f"{column}_idx"

    if not replace and _index_exists(dataset_obj, name):
        raise ValueError(
            f"Index with name '{name}' already exists. Set replace=True to replace it."
        )

    fragments = dataset_obj.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    if requested_num_segments > len(fragment_ids_to_use):
        requested_num_segments = len(fragment_ids_to_use)
        logger.info(
            "Adjusted num_segments to %d to match fragment count",
            requested_num_segments,
        )

    ivf_centroids_artifact = ivf_centroids
    pq_codebook_artifact = pq_codebook
    index_build_kwargs = dict(kwargs)
    max_iters = index_build_kwargs.get("max_iters")
    if rabitq_model is not None:
        index_build_kwargs["rabitq_model"] = rabitq_model

    pq_index_types = {"IVF_PQ", "IVF_HNSW_PQ"}
    needs_pq = index_type_name in pq_index_types
    needs_rq = index_type_name == "IVF_RQ"

    # Always perform global IVF training up front so that all shards share the
    # same centroids and number of partitions. The Ray entrypoint owns the
    # lifecycle of these artifacts and distributes them to workers.
    logger.info(
        "Phase 1: Training IVF centroids (index_type=%s, metric=%s)",
        index_type_name,
        metric_lower,
    )
    builder = _indices_builder_for_field_path(dataset_obj, column, field)
    num_rows = dataset_obj.count_rows()
    dimension = builder.dimension

    requested_num_partitions = num_partitions
    logger.info(
        "Training IVF with requested_num_partitions=%s, num_rows=%d, "
        "dimension=%d, sample_rate=%d",
        requested_num_partitions,
        num_rows,
        dimension,
        sample_rate,
    )
    ivf_train_kwargs: dict[str, Any] = {
        "num_partitions": requested_num_partitions,
        "distance_type": metric_lower,
        "sample_rate": sample_rate,
    }
    if max_iters is not None:
        ivf_train_kwargs["max_iters"] = max_iters
    ivf_model = builder.train_ivf(**ivf_train_kwargs)
    ivf_centroids_artifact = ivf_model.centroids
    num_partitions = ivf_model.num_partitions
    logger.info(
        "IVF training completed: num_partitions=%d",
        num_partitions,
    )

    if needs_pq:
        requested_num_sub_vectors = num_sub_vectors
        pq_num_bits = index_build_kwargs.get("num_bits", 8)
        logger.info(
            "Training PQ codebook: requested_num_sub_vectors=%s, "
            "num_bits=%d, sample_rate=%d",
            requested_num_sub_vectors,
            pq_num_bits,
            sample_rate,
        )
        pq_model = _train_pq_for_field_path(
            builder,
            ivf_model,
            num_subvectors=requested_num_sub_vectors,
            sample_rate=sample_rate,
            num_bits=pq_num_bits,
            max_iters=max_iters,
        )
        pq_codebook_artifact = pq_model.codebook
        num_sub_vectors = pq_model.num_subvectors
        logger.info("PQ training completed: num_sub_vectors=%d", num_sub_vectors)

    if needs_rq and index_build_kwargs.get("rabitq_model") is None:
        num_bits = index_build_kwargs.get("num_bits", 1)
        logger.info(
            "Building shared RaBitQ model: dimension=%d, num_bits=%s",
            dimension,
            num_bits,
        )
        index_build_kwargs["rabitq_model"] = _build_rabitq_model(
            dimension=dimension,
            num_bits=num_bits,
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

    fragment_batches = _distribute_fragments_balanced(
        fragments, num_segments=requested_num_segments, logger=logger
    )
    pool_workers = min(num_workers, len(fragment_batches))
    if pool_workers < num_workers:
        logger.info(
            "Limiting Ray Pool workers to %d (requested %d) because there are "
            "only %d non-empty segment batches",
            pool_workers,
            num_workers,
            len(fragment_batches),
        )

    logger.info(
        "Phase 2: Distributing vector index build across %d segment batches "
        "using up to %d workers for %d fragments",
        len(fragment_batches),
        pool_workers,
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
            sample_rate=sample_rate,
            ivf_centroids=shared_ivf_centroids,
            pq_codebook=shared_pq_codebook,
            storage_options=merged_storage_options,
            block_size=block_size,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            **index_build_kwargs,
        )

    results = _map_async_with_pool(
        create_fragment_handler=create_fragment_handler,
        fragment_batches=fragment_batches,
        num_workers=pool_workers,
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

    logger.info(
        "Phase 3: Building and committing index segments for vector index '%s'",
        name,
    )

    successful_results = [r for r in results if r.get("status") == "success"]
    if not successful_results:
        raise RuntimeError("No successful vector index creation results found")

    updated_dataset = dataset_obj.commit_existing_index_segments(
        index_name=name,
        column=column,
        segments=[r["segment_index"] for r in successful_results],
    )

    logger.info(
        "Successfully created distributed vector index '%s'",
        name,
    )
    logger.info(
        "Index ID: %s, Fragments: %d, Segments: %d, Workers: %d",
        index_id,
        len(fragment_ids_to_use),
        len(fragment_batches),
        pool_workers,
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
) -> lance.LanceDataset:
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

    resolve_from_namespace = uri is None and has_namespace_params(
        namespace_impl, table_id
    )
    uri, merged_storage_options = resolve_namespace_table(
        uri, storage_options, namespace_impl, namespace_properties, table_id
    )
    if resolve_from_namespace:
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
