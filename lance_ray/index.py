# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import uuid
from typing import Any, Literal, Optional, Union

import lance
import pyarrow as pa
from lance.dataset import Index, IndexConfig, LanceDataset
from lance.indices import IndicesBuilder
from packaging import version
from ray.util.multiprocessing import Pool

from .utils import (
    create_storage_options_provider,
    get_or_create_namespace,
    validate_uri_or_namespace,
)

logger = logging.getLogger(__name__)


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
    fragment_handler: Any,
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
    rst_futures = pool.map_async(
        fragment_handler,
        fragment_batches,
        chunksize=1,
    )

    try:
        results = rst_futures.get()
    except Exception as exc:  # pragma: no cover - exercised via integration tests
        raise RuntimeError(f"{error_prefix}: {exc}") from exc
    finally:
        pool.close()

    return results


def _handle_fragment_index(
    dataset_uri: str,
    column: str,
    index_type: str,
    name: str,
    index_uuid: str,
    replace: bool,
    train: bool,
    storage_options: Optional[dict[str, str]] = None,
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

            # Create storage options provider in worker for credentials refresh
            storage_options_provider = create_storage_options_provider(
                namespace_impl, namespace_properties, table_id
            )

            # Load dataset
            dataset = LanceDataset(
                dataset_uri,
                storage_options=storage_options,
                storage_options_provider=storage_options_provider,
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

        supported_distributed_types = {"INVERTED", "FTS", "BTREE"}
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

    # Create storage options provider for local operations
    storage_options_provider = create_storage_options_provider(
        namespace_impl, namespace_properties, table_id
    )

    # Load dataset
    dataset = LanceDataset(
        uri,
        storage_options=merged_storage_options,
        storage_options_provider=storage_options_provider,
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

    if not replace:
        index_exists = False
        try:
            existing_indices = dataset.list_indices()
            existing_names = {idx["name"] for idx in existing_indices}
            index_exists = name in existing_names
        except Exception:  # pragma: no cover - list_indices() not available in older lance versions
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

    fragment_handler = _handle_fragment_index(
        dataset_uri=uri,
        column=column,
        index_type=index_type,
        name=name,
        index_uuid=index_id,
        replace=replace,
        train=train,
        storage_options=merged_storage_options,
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
        fragment_handler=fragment_handler,
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
        storage_options=merged_storage_options,
        storage_options_provider=storage_options_provider,
    )

    logger.info("Phase 2: Merging index metadata for index ID: %s", index_id)
    merge_index_metadata_compat(dataset, index_id, index_type=index_type, **kwargs)

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
        storage_options_provider=storage_options_provider,
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
    ivf_centroids: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
    pq_codebook: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
    storage_options: Optional[dict[str, str]] = None,
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

            dataset = LanceDataset(dataset_uri, storage_options=storage_options)
            available_fragments = {f.fragment_id for f in dataset.get_fragments()}
            invalid_fragments = set(fragment_ids) - available_fragments
            if invalid_fragments:
                raise ValueError(f"Fragment IDs {invalid_fragments} do not exist")

            logger.info(
                "Building distributed vector index for fragments %s using "
                "LanceDataset.create_index",
                fragment_ids,
            )

            dataset.create_index(
                column=column,
                index_type=index_type,
                name=name,
                metric=metric,
                replace=replace,
                num_partitions=num_partitions,
                ivf_centroids=ivf_centroids,
                pq_codebook=pq_codebook,
                num_sub_vectors=num_sub_vectors,
                storage_options=storage_options,
                train=True,
                fragment_ids=fragment_ids,
                index_uuid=index_uuid,
                **kwargs,
            )

            lance_field = dataset.lance_schema.field(column)
            if lance_field is None:
                raise KeyError(f"{column} not found in schema")
            field_id = lance_field.id()

            logger.info(
                "Fragment vector index created successfully for fragments %s",
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
    uri: Union[str, "lance.LanceDataset"],
    column: str,
    index_type: str | Any,
    name: Optional[str] = None,
    *,
    replace: bool = True,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    metric: str = "l2",
    num_partitions: Optional[int] = None,
    num_sub_vectors: Optional[int] = None,
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
    vector index families supported by Lance's distributed merge pipeline.

    Args:
        uri: Lance dataset or URI to build index on
        column: Column name to index
        index_type: Type of index to build (e.g., "IVF_PQ", "IVF_HNSW_PQ")
        name: Name of the index (generated if None)
        replace: Whether to replace existing index with the same name (default: True)
        num_workers: Number of Ray workers to use (keyword-only)
        storage_options: Storage options for the dataset (keyword-only)
        ray_remote_args: Options for Ray tasks (keyword-only)
        metric: Distance metric to use (default: "l2")
        num_partitions: Number of IVF partitions (optional)
        num_sub_vectors: Number of PQ sub-vectors (optional)
        ivf_centroids: Pre-computed IVF centroids (optional)
        pq_codebook: Pre-computed PQ codebook (optional)
        **kwargs: Additional arguments to pass to create_index and train_pq (e.g., sample_rate)

    Returns:
        Updated Lance dataset with the index created
    """

    _check_pylance_version()

    if not column:
        raise ValueError("Column name cannot be empty")

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")

    index_type_name = _normalize_index_type(index_type)
    metric_lower = _validate_metric(metric)

    index_id = str(uuid.uuid4())
    logger.info("Starting distributed vector index build with ID: %s", index_id)

    if isinstance(uri, str):
        dataset_uri = uri
        dataset_obj = LanceDataset(dataset_uri, storage_options=storage_options)
    else:
        dataset_obj = uri
        dataset_uri = dataset_obj.uri
        if storage_options is None:
            storage_options = getattr(dataset_obj, "_storage_options", None)

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
        except Exception:  # pragma: no cover - list_indices() not available in older lance versions
            pass
        if index_exists:
            raise ValueError(
                f"Index with name '{name}' already exists. Set replace=True "
                "to replace it."
            )

    fragments = dataset_obj.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    if num_workers > len(fragment_ids_to_use):
        num_workers = len(fragment_ids_to_use)
        logger.info("Adjusted num_workers to %d to match fragment count", num_workers)

    ivf_centroids_artifact = ivf_centroids
    pq_codebook_artifact = pq_codebook

    pq_index_types = {"IVF_PQ", "IVF_HNSW_PQ"}
    needs_pq = index_type_name in pq_index_types

    # Always perform global IVF training up front so that all shards share the
    # same centroids and number of partitions. The Ray entrypoint owns the
    # lifecycle of these artifacts and distributes them to workers.
    logger.info(
        "Phase 1: Training IVF centroids (index_type=%s, metric=%s)",
        index_type_name,
        metric_lower,
    )
    builder = IndicesBuilder(dataset_obj, column)
    num_rows = dataset_obj.count_rows()
    dimension = builder.dimension

    computed_num_partitions = builder._determine_num_partitions(
        num_partitions, num_rows
    )
    logger.info(
        "Training IVF with num_partitions=%d, num_rows=%d, dimension=%d",
        computed_num_partitions,
        num_rows,
        dimension,
    )
    ivf_model = builder.train_ivf(
        num_partitions=computed_num_partitions,
        distance_type=metric_lower,
    )
    ivf_centroids_artifact = ivf_model.centroids
    num_partitions = ivf_model.num_partitions
    logger.info(
        "IVF training completed: num_partitions=%d",
        num_partitions,
    )

    if needs_pq:
        computed_num_sub_vectors = builder._normalize_pq_params(
            num_sub_vectors, dimension
        )
        logger.info(
            "Training PQ codebook: num_sub_vectors=%d, sample_rate=%d",
            computed_num_sub_vectors,
            kwargs.get("sample_rate", 256),
        )
        pq_model = builder.train_pq(
            ivf_model,
            computed_num_sub_vectors,
            sample_rate=kwargs.get("sample_rate", 256),
        )
        pq_codebook_artifact = pq_model.codebook
        num_sub_vectors = computed_num_sub_vectors
        logger.info("PQ training completed: num_sub_vectors=%d", num_sub_vectors)

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
        fragments, num_workers=num_workers, logger=logger
    )

    logger.info(
        "Phase 2: Distributing vector index build across %d workers for %d fragments",
        len(fragment_batches),
        len(fragment_ids_to_use),
    )

    fragment_handler = _handle_vector_fragment_index(
        dataset_uri=dataset_uri,
        column=column,
        index_type=index_type_name,
        name=name,
        index_uuid=index_id,
        replace=replace,
        metric=metric_lower,
        num_partitions=num_partitions,
        num_sub_vectors=num_sub_vectors,
        ivf_centroids=ivf_centroids_artifact,
        pq_codebook=pq_codebook_artifact,
        storage_options=storage_options,
        **kwargs,
    )

    results = _map_async_with_pool(
        fragment_handler=fragment_handler,
        fragment_batches=fragment_batches,
        num_workers=num_workers,
        ray_remote_args=ray_remote_args,
        error_prefix="Failed to complete distributed vector index building",
    )

    failed_results = [r for r in results if r.get("status") == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results if "error" in r]
        raise RuntimeError("Vector index building failed: " + "; ".join(error_messages))

    dataset_obj = LanceDataset(dataset_uri, storage_options=storage_options)

    logger.info("Phase 3: Merging index metadata for index ID: %s", index_id)
    merge_index_metadata_compat(
        dataset_obj,
        index_id,
        index_type=index_type_name,
        **kwargs,
    )

    logger.info("Phase 4: Creating and committing vector index '%s'", name)

    successful_results = [r for r in results if r.get("status") == "success"]
    if not successful_results:
        raise RuntimeError("No successful vector index creation results found")

    fields = successful_results[0]["fields"]

    index = Index(
        uuid=index_id,
        name=name,
        fields=fields,
        dataset_version=dataset_obj.version,
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
        read_version=dataset_obj.version,
        storage_options=storage_options,
    )

    logger.info(
        "Successfully created distributed vector index '%s' with multi-phase workflow",
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

    storage_options_provider = create_storage_options_provider(
        namespace_impl, namespace_properties, table_id
    )

    dataset = LanceDataset(
        uri,
        storage_options=merged_storage_options,
        storage_options_provider=storage_options_provider,
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
