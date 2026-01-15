# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import uuid
from typing import Any, Literal, Optional

import lance
import pyarrow as pa
from lance.dataset import Index, IndexConfig, LanceDataset
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
    """
    Distribute fragments across workers using a balanced algorithm that considers fragment sizes.

    This function implements a greedy algorithm that assigns fragments to the worker
    with the currently smallest total workload, helping to balance the processing
    time across workers.

    Args:
        fragments: List of Lance fragment objects
        num_workers: Number of workers to distribute fragments across
        logger: Logger instance for debugging information

    Returns:
        List of lists, where each inner list contains fragment IDs for one worker
    """
    if not fragments:
        return [[] for _ in range(num_workers)]

    # Get fragment information (ID and size)
    fragment_info = []
    for fragment in fragments:
        try:
            # Try to get fragment size information
            # fragment.count_rows() gives us the number of rows in the fragment
            row_count = fragment.count_rows()
            fragment_info.append(
                {
                    "id": fragment.fragment_id,
                    "size": row_count,
                }
            )
        except Exception as e:
            # If we can't get size info, use fragment_id as a fallback
            logger.warning(
                f"Could not get size for fragment {fragment.fragment_id}: {e}. "
                "Using fragment_id as size estimate."
            )
            fragment_info.append(
                {
                    "id": fragment.fragment_id,
                    "size": fragment.fragment_id,  # Fallback to fragment_id
                }
            )

    # Sort fragments by size in descending order (largest first)
    # This helps with better load balancing using the greedy algorithm
    fragment_info.sort(key=lambda x: x["size"], reverse=True)

    # Initialize worker batches and their current workloads
    worker_batches = [[] for _ in range(num_workers)]
    worker_workloads = [0] * num_workers

    # Greedy assignment: assign each fragment to the worker with minimum workload
    for frag_info in fragment_info:
        # Find the worker with the minimum current workload
        min_workload_idx = min(range(num_workers), key=lambda i: worker_workloads[i])

        # Assign fragment to this worker
        worker_batches[min_workload_idx].append(frag_info["id"])
        worker_workloads[min_workload_idx] += frag_info["size"]

    # Log distribution statistics for debugging
    total_size = sum(frag_info["size"] for frag_info in fragment_info)
    logger.info("Fragment distribution statistics:")
    logger.info(f"  Total fragments: {len(fragment_info)}")
    logger.info(f"  Total size: {total_size}")
    logger.info(f"  Workers: {num_workers}")

    for i, (batch, workload) in enumerate(
        zip(worker_batches, worker_workloads, strict=False)
    ):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info(
            f"  Worker {i}: {len(batch)} fragments, "
            f"workload: {workload} ({percentage:.1f}%)"
        )

    # Filter out empty batches (shouldn't happen with proper input validation)
    non_empty_batches = [batch for batch in worker_batches if batch]

    return non_empty_batches


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
    """
    Create a function to handle fragment index building for use with Pool.
    This function returns a callable that can be used with Pool.map_async
    to build indices for specific fragments.
    """

    def func(fragment_ids: list[int]) -> dict[str, Any]:
        """
        Handle fragment index building using the distributed API.

        This function calls create_scalar_index directly for specific fragments.
        After execution, fragment-level indices are automatically built.

        Args:
            fragment_ids: List of fragment IDs to build index for

        Returns:
            Dictionary with status and result information
        """
        try:
            # Basic input validation
            if not fragment_ids:
                raise ValueError("fragment_ids cannot be empty")

            # Validate fragment_id ranges
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

            # Validate fragments exist
            available_fragments = {f.fragment_id for f in dataset.get_fragments()}
            invalid_fragments = set(fragment_ids) - available_fragments
            if invalid_fragments:
                raise ValueError(f"Fragment IDs {invalid_fragments} do not exist")

            # Use the distributed index building API - Phase 1: Fragment index creation
            logger.info(
                f"Building distributed index for fragments {fragment_ids} using create_scalar_index"
            )

            # Call create_scalar_index directly - no return value expected
            # After execution, fragment-level indices are automatically built
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

            # Get field ID for the indexed column
            field_id = dataset.schema.get_field_index(column)

            logger.info(
                f"Fragment index created successfully for fragments {fragment_ids}"
            )

            return {
                "status": "success",
                "fragment_ids": fragment_ids,
                "fields": [field_id],
                "uuid": index_uuid,
            }

        except Exception as e:
            logger.error(
                f"Fragment index task failed for fragments {fragment_ids}: {e}"
            )
            return {
                "status": "error",
                "fragment_ids": fragment_ids,
                "error": str(e),
            }

    return func


def merge_index_metadata_compat(dataset, index_id, index_type, **kwargs):
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
    """
    Build scalar indices with Ray in a distributed workflow (supports FTS/INVERTED and BTREE).

    This function distributes the index building process across multiple Ray workers,
    with each worker building indices for a subset of fragments. The indices are then
    merged and committed as a single index.

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
                f"Distributed indexing requires pylance >= 0.36.0, but found {lance.__version__}. "
                "The distribute-related interfaces are not available in older versions. "
                "Please upgrade pylance by running: pip install --upgrade pylance"
            )

        logger.info(f"Pylance version check passed: {lance.__version__} >= 0.36.0")

    except AttributeError as err:
        # If lance.__version__ doesn't exist, assume it's too old
        raise RuntimeError(
            "Cannot determine pylance version. Distributed indexing requires pylance >= 0.36.0. "
            "Please upgrade pylance by running: pip install --upgrade pylance"
        ) from err

    index_id = str(uuid.uuid4())
    logger.info(f"Starting distributed index build with ID: {index_id}")

    # Validate uri or namespace params
    validate_uri_or_namespace(uri, namespace_impl, table_id)

    # Basic input validation
    if not column:
        raise ValueError("Column name cannot be empty")

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")

    # Handle index_type validation
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

        # Validate distributed indexing support
        supported_distributed_types = {"INVERTED", "FTS", "BTREE"}
        if index_type not in supported_distributed_types:
            raise ValueError(
                f"Distributed indexing currently supports {sorted(supported_distributed_types)} index types, "
                f"not '{index_type}'"
            )
    elif not isinstance(index_type, IndexConfig):
        raise ValueError(
            f"index_type must be a string literal or IndexConfig object, got {type(index_type)}"
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

    # Validate column exists and has correct type
    try:
        field = dataset.schema.field(column)
    except KeyError as e:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from e

    # Check column type according to index type
    value_type = field.type
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_type = field.type.value_type

    # Validate column type based on index type requirements
    if isinstance(index_type, str):
        match index_type:
            case "INVERTED" | "FTS":
                # Text-based indexes require string types
                if not pa.types.is_string(value_type):
                    raise TypeError(
                        f"Column {column} must be string type for {index_type} index, got {value_type}"
                    )
            case "BTREE":
                # B-Tree indexes support both numeric and string types
                is_supported = (
                    pa.types.is_integer(value_type)
                    or pa.types.is_floating(value_type)
                    or pa.types.is_string(value_type)
                )
                if not is_supported:
                    raise TypeError(
                        f"Column {column} must be numeric or string type for BTREE index, got {value_type}"
                    )
            case _:
                # For other index types, skip strict validation to maintain compatibility
                pass

    if name is None:
        name = f"{column}_idx"

    # Handle replace parameter - check for existing index with same name
    if not replace:
        try:
            existing_indices = dataset.list_indices()
            existing_names = {idx["name"] for idx in existing_indices}
            if name in existing_names:
                raise ValueError(
                    f"Index with name '{name}' already exists. Set replace=True to replace it."
                )
        except Exception:
            # If we can't check existing indices, continue
            pass

    # Get fragments
    fragments = dataset.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    # Handle fragment_ids parameter - if provided, filter fragments
    if fragment_ids is not None:
        available_fragment_ids = {f.fragment_id for f in fragments}
        invalid_fragments = set(fragment_ids) - available_fragment_ids
        if invalid_fragments:
            raise ValueError(
                f"Fragment IDs {invalid_fragments} do not exist in dataset"
            )
        # Filter fragments to only include requested ones
        fragments = [f for f in fragments if f.fragment_id in fragment_ids]
        fragment_ids_to_use = fragment_ids
    else:
        fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    # Adjust num_workers if needed
    if num_workers > len(fragment_ids_to_use):
        num_workers = len(fragment_ids_to_use)
        logger.info(f"Adjusted num_workers to {num_workers} to match fragment count")

    # Distribute fragments to workers using balanced distribution algorithm
    fragment_batches = _distribute_fragments_balanced(fragments, num_workers, logger)

    # Phase 1: Fragment index creation using Pool pattern (similar to io.py)
    # Use Pool to distribute work instead of direct Ray task submission
    pool = Pool(processes=num_workers, ray_remote_args=ray_remote_args)

    # Create the fragment handler function
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

    # Submit tasks using Pool.map_async
    rst_futures = pool.map_async(
        fragment_handler,
        fragment_batches,
        chunksize=1,
    )

    # Wait for results
    try:
        results = rst_futures.get()
    except Exception as e:
        pool.close()
        raise RuntimeError(f"Failed to complete distributed index building: {e}") from e
    finally:
        pool.close()

    # Check for failures
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

    # Phase 2: Merge index metadata using the distributed API
    logger.info(f"Phase 2: Merging index metadata for index ID: {index_id}")
    merge_index_metadata_compat(dataset, index_id, index_type=index_type, **kwargs)

    # Phase 3: Create Index object and commit the operation
    logger.info(f"Phase 3: Creating and committing index '{name}'")

    # Get field information from successful results
    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful index creation results found")

    fields = successful_results[0]["fields"]

    # Create Index object
    index = Index(
        uuid=index_id,
        name=name,
        fields=fields,
        dataset_version=dataset.version,
        fragment_ids=set(fragment_ids_to_use),
        index_version=0,
    )

    # Create and commit the index operation
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
        f"Successfully created distributed index '{name}' with three-phase workflow"
    )
    logger.info(
        f"Index ID: {index_id}, Fragments: {len(fragment_ids_to_use)}, Workers: {len(fragment_batches)}"
    )
    return updated_dataset
