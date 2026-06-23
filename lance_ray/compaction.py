import logging
from typing import Any, Optional

import lance
from lance.lance import CompactionMetrics
from lance.optimize import Compaction, CompactionOptions, CompactionTask
from ray.util.multiprocessing import Pool

from .utils import (
    get_namespace_kwargs,
    get_or_create_namespace,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

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
            namespace_kwargs = get_namespace_kwargs(
                namespace_impl, namespace_properties, table_id
            )

            # Load dataset
            dataset = lance.LanceDataset(
                dataset_uri,
                storage_options=storage_options,
                **namespace_kwargs,
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
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    compaction_options: Optional[CompactionOptions] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> Optional[CompactionMetrics]:
    """
    Compact files in a Lance dataset using distributed Ray workers.

    This function distributes the compaction process across multiple Ray workers,
    with each worker executing a subset of compaction tasks. The results are then
    committed as a single compaction operation.

    Args:
        uri: The URI of the Lance dataset to compact. Either uri OR
            (namespace_impl + table_id) must be provided.
        table_id: The table identifier as a list of strings. Must be provided
            together with namespace_impl.
        compaction_options: Options for the compaction operation.
        num_workers: Number of Ray workers to use (default: 4).
        storage_options: Storage options for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with table_id for resolving the dataset location and
            credentials vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id.
        ray_remote_args: Options for Ray tasks (e.g., num_cpus, resources).

    Returns:
        CompactionMetrics with statistics from the compaction operation.

    Raises:
        ValueError: If input parameters are invalid.
        RuntimeError: If compaction fails.
    """
    validate_uri_or_namespace(uri, namespace_impl, table_id)

    # Resolve URI and get storage options from namespace if provided
    uri, merged_storage_options = resolve_namespace_table(
        uri, storage_options, namespace_impl, namespace_properties, table_id
    )

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    # Load dataset
    dataset = lance.LanceDataset(
        uri,
        storage_options=merged_storage_options,
        **namespace_kwargs,
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
        storage_options=merged_storage_options,
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
        raise RuntimeError(f"Failed to complete distributed compaction: {e}") from e
    finally:
        pool.close()
        pool.join()

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


def compact_database(
    *,
    database: list[str],
    namespace_impl: str,
    namespace_properties: Optional[dict[str, str]] = None,
    compaction_options: Optional[CompactionOptions] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    continue_on_error: bool = False,
) -> list[dict[str, Any]]:
    """
    Compact all tables under a given database (namespace) using distributed Ray workers.

    This function lists all tables under the specified database via the namespace API,
    then runs :func:`compact_files` on each table. Use this when you want to compact
    an entire database instead of a single table.

    Args:
        database: The database (namespace) identifier as a list of path segments,
            e.g. ``["my_database"]``. All tables under this namespace will be compacted.
        namespace_impl: The namespace implementation type (e.g. ``"rest"``, ``"dir"``).
            Required for resolving table locations and credentials.
        namespace_properties: Properties for connecting to the namespace.
        compaction_options: Options for the compaction operation (used for every table).
        num_workers: Number of Ray workers per table (default: 4).
        storage_options: Storage options for the datasets.
        ray_remote_args: Options for Ray tasks (e.g. num_cpus, resources).
        continue_on_error: If False (default), the first table that fails to
            compact aborts the whole run with ``RuntimeError``. If True, failures
            are recorded per table and the remaining tables are still compacted.

    Returns:
        A list of dicts, one per table, with keys:
        - ``"table_id"``: ``list[str]`` – full table identifier (database + table name).
        - ``"metrics"``: :class:`~lance.lance.CompactionMetrics` or ``None`` –
          compaction result for that table, or ``None`` if no compaction was needed
          or the table failed.
        - ``"error"``: ``str`` – only present (and only when ``continue_on_error``
          is True) for tables whose compaction failed.

    Raises:
        ValueError: If database is empty or namespace_impl is not provided.
        RuntimeError: If listing tables fails, or if a table compaction fails
            while ``continue_on_error`` is False.

    Example:
        >>> results = compact_database(
        ...     database=["my_db"],
        ...     namespace_impl="dir",
        ...     namespace_properties={"root": "/path/to/tables"},
        ...     compaction_options=CompactionOptions(target_rows_per_fragment=10000),
        ...     num_workers=2,
        ... )
        >>> for item in results:
        ...     print(item["table_id"], item["metrics"])
    """
    if not database:
        raise ValueError("'database' must be a non-empty list of path segments.")
    if not namespace_impl:
        raise ValueError("'namespace_impl' is required when using compact_database.")

    from lance_namespace import ListTablesRequest

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        raise RuntimeError(
            "Failed to create namespace from namespace_impl and namespace_properties."
        )

    # List all tables under the database (namespace) with pagination
    all_tables: list[str] = []
    page_token: Optional[str] = None
    limit = 500

    while True:
        request = ListTablesRequest(
            id=database,
            page_token=page_token,
            limit=limit,
        )
        response = namespace.list_tables(request)
        all_tables.extend(response.tables)
        page_token = getattr(response, "page_token", None)
        if not page_token:
            break

    if not all_tables:
        logger.info("No tables found under database %s, nothing to compact.", database)
        return []

    # table_id = database + [table_name] for each table under this namespace
    table_ids = [database + [t] for t in all_tables]
    results: list[dict[str, Any]] = []

    for table_id in table_ids:
        logger.info("Compacting table %s", table_id)
        try:
            metrics = compact_files(
                uri=None,
                table_id=table_id,
                compaction_options=compaction_options,
                num_workers=num_workers,
                storage_options=storage_options,
                namespace_impl=namespace_impl,
                namespace_properties=namespace_properties,
                ray_remote_args=ray_remote_args,
            )
            results.append({"table_id": table_id, "metrics": metrics})
        except Exception as e:
            logger.exception("Compaction failed for table %s: %s", table_id, e)
            if not continue_on_error:
                raise RuntimeError(
                    f"Compaction failed for table {table_id}: {e}"
                ) from e
            results.append({"table_id": table_id, "metrics": None, "error": str(e)})

    return results
