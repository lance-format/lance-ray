import logging
from typing import Any, Optional

import lance
from lance.lance import CompactionMetrics
from lance.optimize import Compaction, CompactionOptions, CompactionTask
from ray.util.multiprocessing import Pool

from .utils import create_storage_options_provider

logger = logging.getLogger(__name__)


def _handle_compaction_task(
    dataset_uri: str,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
):
    """
    Create a function to handle compaction task execution for use with Pool.
    This function returns a callable that can be used with Pool.map_async
    to execute compaction tasks.
    """

    def func(task: CompactionTask) -> dict[str, Any]:
        """
        Execute a compaction task.

        Args:
            task: CompactionTask to execute

        Returns:
            Dictionary with status and result information
        """
        try:
            # Create storage options provider in worker for credentials refresh
            storage_options_provider = create_storage_options_provider(
                namespace_impl, namespace_properties, table_id
            )

            # Load dataset
            dataset = lance.LanceDataset(
                dataset_uri,
                storage_options=storage_options,
                storage_options_provider=storage_options_provider,
            )

            logger.info(f"Executing compaction task for fragments {task.fragments}")

            # Execute the compaction task
            result = task.execute(dataset)

            logger.info(
                f"Compaction task completed successfully for fragments {task.fragments}"
            )

            return {
                "status": "success",
                "fragments": task.fragments,
                "result": result,
            }

        except Exception as e:
            logger.error(f"Compaction task failed for fragments {task.fragments}: {e}")
            return {
                "status": "error",
                "fragments": task.fragments,
                "error": str(e),
            }

    return func


def compact_files(
    uri: str,
    *,
    compaction_options: Optional[CompactionOptions] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> Optional[CompactionMetrics]:
    """
    Compact files in a Lance dataset using distributed Ray workers.

    This function distributes the compaction process across multiple Ray workers,
    with each worker executing a subset of compaction tasks. The results are then
    committed as a single compaction operation.

    Args:
        uri: The URI of the Lance dataset to compact.
        compaction_options: Options for the compaction operation.
        num_workers: Number of Ray workers to use (default: 4).
        storage_options: Storage options for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id for credentials
            vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id for credentials vending.
        table_id: The table identifier as a list of strings.
            Used together with namespace_impl and namespace_properties for
            credentials vending.
        ray_remote_args: Options for Ray tasks (e.g., num_cpus, resources).

    Returns:
        CompactionMetrics with statistics from the compaction operation.

    Raises:
        ValueError: If input parameters are invalid.
        RuntimeError: If compaction fails.
    """
    storage_options = storage_options or {}

    # Create storage options provider for local operations
    storage_options_provider = create_storage_options_provider(
        namespace_impl, namespace_properties, table_id
    )

    # Load dataset
    dataset = lance.LanceDataset(
        uri,
        storage_options=storage_options,
        storage_options_provider=storage_options_provider,
    )

    logger.info("Starting distributed compaction")

    # Step 1: Create the compaction plan
    compaction_plan = Compaction.plan(dataset, compaction_options)

    logger.info(f"Compaction plan created with {compaction_plan.num_tasks()} tasks")

    if compaction_plan.num_tasks() == 0:
        logger.info("No compaction tasks needed")
        return None

    # Adjust num_workers if needed
    if num_workers > compaction_plan.num_tasks():
        num_workers = compaction_plan.num_tasks()
        logger.info(f"Adjusted num_workers to {num_workers} to match task count")

    # Step 2: Execute tasks in parallel using Ray Pool
    pool = Pool(processes=num_workers, ray_remote_args=ray_remote_args)

    # Create the task handler function
    task_handler = _handle_compaction_task(
        dataset_uri=uri,
        storage_options=storage_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
    )

    # Submit tasks using Pool.map_async
    rst_futures = pool.map_async(
        task_handler,
        compaction_plan.tasks,
        chunksize=1,
    )

    # Wait for results
    try:
        results = rst_futures.get()
    except Exception as e:
        pool.close()
        raise RuntimeError(f"Failed to complete distributed compaction: {e}") from e
    finally:
        pool.close()

    # Check for failures
    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(f"Compaction failed: {'; '.join(error_messages)}")

    # Step 3: Collect successful RewriteResult objects
    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful compaction results found")

    rewrites = [r["result"] for r in successful_results]

    logger.info(
        f"Collected {len(rewrites)} successful compaction results, committing..."
    )

    # Step 4: Commit the compaction
    metrics = Compaction.commit(dataset, rewrites)

    logger.info(f"Compaction completed successfully. Metrics: {metrics}")

    return metrics
