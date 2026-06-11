import logging
from collections.abc import Callable
from datetime import timedelta
from typing import Any, Optional

import lance
from lance.lance import CleanupStats
from ray.util.multiprocessing import Pool

from .utils import (
    get_namespace_kwargs,
    get_or_create_namespace,
    validate_uri_or_namespace,
)

logger = logging.getLogger(__name__)

CLEANUP_STATS_FIELDS = (
    "bytes_removed",
    "old_versions",
    "data_files_removed",
    "transaction_files_removed",
    "index_files_removed",
    "deletion_files_removed",
)

# Page size used when listing tables under a database via the namespace API.
_LIST_TABLES_PAGE_SIZE = 500


def _cleanup_stats_to_dict(stats: CleanupStats) -> dict[str, int]:
    """Convert ``CleanupStats`` to a plain, Ray-serializable dict of counters."""
    return {field: getattr(stats, field) for field in CLEANUP_STATS_FIELDS}


def _resolve_dataset(
    uri: Optional[str],
    storage_options: Optional[dict[str, str]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
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

    if uri is None:
        raise ValueError(
            "Namespace table resolution did not return a dataset location."
        )

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    return uri, merged_storage_options, namespace_kwargs


def cleanup_old_versions(
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    older_than: Optional[timedelta] = None,
    retain_versions: Optional[int] = None,
    delete_unverified: bool = False,
    error_if_tagged_old_versions: bool = True,
    delete_rate_limit: Optional[int] = None,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
) -> CleanupStats:
    """Clean up old versions in one Lance dataset.

    This delegates file safety and deletion policy to Lance core. Use
    :func:`cleanup_database_old_versions` to run cleanup across many namespace
    tables with Ray workers.

    Args:
        uri: The URI of the Lance dataset to clean. Either ``uri`` OR
            (``namespace_impl`` + ``table_id``) must be provided.
        table_id: The table identifier as a list of strings. Must be provided
            together with ``namespace_impl``.
        older_than: Optional ``datetime.timedelta``; only versions older than
            this are removed. Delegated to Lance core, which defaults to two
            weeks only when both ``older_than`` and ``retain_versions`` are unset.
        retain_versions: Optional number of latest versions to retain.
        delete_unverified: Delete unverified files without Lance's default age
            guard (which otherwise retains files leftover from a failed
            transaction until they are at least 7 days old). Only set this when
            no other process is writing to the dataset; otherwise in-flight files
            of a concurrent writer may be deleted, corrupting the dataset.
        error_if_tagged_old_versions: Raise if tagged versions match the cleanup
            policy (default: ``True``).
        delete_rate_limit: Optional maximum delete operations per second.
        storage_options: Storage options for the dataset. Merged with any
            options vended by the namespace (namespace values take precedence).
            Pass credentials here rather than inline in ``uri``: ``uri`` is
            logged and may appear in error messages.
        namespace_impl: The namespace implementation type (e.g. ``"rest"``,
            ``"dir"``). Used together with ``table_id`` to resolve the dataset
            location and vend credentials.
        namespace_properties: Properties for connecting to the namespace.

    Returns:
        :class:`~lance.lance.CleanupStats` with statistics from the cleanup.

    Raises:
        ValueError: If neither ``uri`` nor (``namespace_impl`` + ``table_id``)
            is provided, if both are provided, if ``retain_versions`` is not
            positive, or if namespace resolution does not return a dataset
            location.
    """
    validate_uri_or_namespace(uri, namespace_impl, table_id)
    if retain_versions is not None and retain_versions <= 0:
        raise ValueError("'retain_versions' must be positive when provided.")

    dataset_uri, merged_storage_options, namespace_kwargs = _resolve_dataset(
        uri,
        storage_options,
        namespace_impl,
        namespace_properties,
        table_id,
    )

    logger.info("Cleaning up old versions for dataset %s", dataset_uri)
    dataset = lance.LanceDataset(
        dataset_uri,
        storage_options=merged_storage_options,
        **namespace_kwargs,
    )
    stats = dataset.cleanup_old_versions(
        older_than=older_than,
        retain_versions=retain_versions,
        delete_unverified=delete_unverified,
        error_if_tagged_old_versions=error_if_tagged_old_versions,
        delete_rate_limit=delete_rate_limit,
    )
    logger.info(
        "Cleanup completed for dataset %s: bytes_removed=%s, old_versions=%s",
        dataset_uri,
        stats.bytes_removed,
        stats.old_versions,
    )
    return stats


def _handle_cleanup_table(
    *,
    older_than: Optional[timedelta],
    retain_versions: Optional[int],
    delete_unverified: bool,
    error_if_tagged_old_versions: bool,
    delete_rate_limit: Optional[int],
    storage_options: Optional[dict[str, str]],
    namespace_impl: str,
    namespace_properties: Optional[dict[str, str]],
) -> Callable[[list[str]], dict[str, Any]]:
    """Build the per-table cleanup task executed by Ray workers.

    Returns a callable that runs :func:`cleanup_old_versions` for a single
    ``table_id`` and never raises: every outcome is captured as a Ray-
    serializable dict so failures can be aggregated across tables. The dict is
    either::

        {"status": "success", "table_id": list[str], "stats": dict[str, int]}

    or::

        {"status": "error", "table_id": list[str], "error": str}
    """

    def func(table_id: list[str]) -> dict[str, Any]:
        try:
            logger.info("Cleaning up old versions for table %s", table_id)
            stats = cleanup_old_versions(
                uri=None,
                table_id=table_id,
                older_than=older_than,
                retain_versions=retain_versions,
                delete_unverified=delete_unverified,
                error_if_tagged_old_versions=error_if_tagged_old_versions,
                delete_rate_limit=delete_rate_limit,
                storage_options=storage_options,
                namespace_impl=namespace_impl,
                namespace_properties=namespace_properties,
            )
            logger.info("Cleanup completed for table %s: %s", table_id, stats)
            return {
                "status": "success",
                "table_id": table_id,
                "stats": _cleanup_stats_to_dict(stats),
            }
        except Exception as e:
            logger.exception("Cleanup failed for table %s: %s", table_id, e)
            return {
                "status": "error",
                "table_id": table_id,
                "error": str(e),
            }

    return func


def cleanup_database_old_versions(
    *,
    database: list[str],
    namespace_impl: str,
    namespace_properties: Optional[dict[str, str]] = None,
    older_than: Optional[timedelta] = None,
    retain_versions: Optional[int] = None,
    delete_unverified: bool = False,
    error_if_tagged_old_versions: bool = True,
    delete_rate_limit: Optional[int] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Clean up old versions for every table under a namespace database.

    Lists all tables under ``database`` via the namespace API, then runs
    :func:`cleanup_old_versions` on each table. Unlike :func:`compact_database`
    (which compacts tables serially and fails fast on the first error), cleanup
    runs tables **in parallel** across a Ray ``Pool`` and **aggregates** per-table
    errors: every table is attempted, and a single ``RuntimeError`` summarizing
    all failures is raised only after the pool finishes. Accordingly,
    ``num_workers`` here bounds concurrency *across tables*, not within a single
    table.

    Args:
        database: The database (namespace) identifier as a list of path segments,
            e.g. ``["my_database"]``. All tables under this namespace are cleaned.
        namespace_impl: The namespace implementation type (e.g. ``"rest"``,
            ``"dir"``). Required for resolving table locations and credentials.
        namespace_properties: Properties for connecting to the namespace.
        older_than: Optional ``datetime.timedelta``; only versions older than
            this are removed (applied to every table). Lance defaults to two
            weeks only when both ``older_than`` and ``retain_versions`` are unset.
        retain_versions: Optional number of latest versions to retain per table.
        delete_unverified: Delete unverified files without Lance's default
            (7-day) age guard. Only set this when no other process is writing to
            the datasets; otherwise a concurrent writer's files may be deleted.
        error_if_tagged_old_versions: Raise if tagged versions match the cleanup
            policy (default: ``True``).
        delete_rate_limit: Optional maximum delete operations per second *per
            table* (each table runs in its own worker, so the aggregate rate may
            be up to ``num_workers`` times this value).
        num_workers: Maximum number of Ray workers across tables (default: 4).
            Capped at the number of tables found.
        storage_options: Storage options for the datasets.
        ray_remote_args: Options for Ray tasks (e.g. ``num_cpus``, ``resources``).

    Returns:
        A list of dicts, one per table, with keys:
        - ``"table_id"``: ``list[str]`` – full table identifier
          (``database`` + table name).
        - ``"stats"``: ``dict[str, int]`` – the cleanup counters returned by
          Lance (see :data:`CLEANUP_STATS_FIELDS`).

    Raises:
        ValueError: If ``database`` is empty, ``namespace_impl`` is missing,
            ``num_workers`` is not positive, or ``retain_versions`` is not
            positive.
        RuntimeError: If the namespace cannot be created, if the distributed
            cleanup cannot complete, or if any table's cleanup fails (the message
            aggregates every failed table).

    Warning:
        This operation is destructive and **not atomic**. Tables are cleaned
        eagerly by workers, so when a ``RuntimeError`` is raised for a failed
        table, other tables may already have had old versions deleted.

    Example:
        >>> results = cleanup_database_old_versions(
        ...     database=["my_db"],
        ...     namespace_impl="dir",
        ...     namespace_properties={"root": "/path/to/tables"},
        ...     older_than=timedelta(days=7),
        ...     retain_versions=3,
        ...     num_workers=4,
        ... )
        >>> for item in results:
        ...     print(item["table_id"], item["stats"])
    """
    if not database:
        raise ValueError("'database' must be a non-empty list of path segments.")
    if not namespace_impl:
        raise ValueError(
            "'namespace_impl' is required when using cleanup_database_old_versions."
        )
    if num_workers <= 0:
        raise ValueError("'num_workers' must be positive.")
    if retain_versions is not None and retain_versions <= 0:
        raise ValueError("'retain_versions' must be positive when provided.")

    from lance_namespace import ListTablesRequest

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        raise RuntimeError(
            "Failed to create namespace from namespace_impl and namespace_properties."
        )

    all_tables: list[str] = []
    page_token: Optional[str] = None

    while True:
        request = ListTablesRequest(
            id=database,
            page_token=page_token,
            limit=_LIST_TABLES_PAGE_SIZE,
            include_declared=False,
        )
        response = namespace.list_tables(request)
        all_tables.extend(response.tables)
        page_token = getattr(response, "page_token", None)
        if not page_token:
            break

    if not all_tables:
        logger.info("No tables found under database %s, nothing to clean up.", database)
        return []

    table_ids = [database + [table_name] for table_name in all_tables]
    processes = min(num_workers, len(table_ids))
    pool = Pool(processes=processes, ray_remote_args=ray_remote_args)
    task_handler = _handle_cleanup_table(
        older_than=older_than,
        retain_versions=retain_versions,
        delete_unverified=delete_unverified,
        error_if_tagged_old_versions=error_if_tagged_old_versions,
        delete_rate_limit=delete_rate_limit,
        storage_options=storage_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
    )

    try:
        results = pool.map_async(task_handler, table_ids, chunksize=1).get()
    except Exception as e:
        raise RuntimeError(f"Failed to complete distributed cleanup: {e}") from e
    finally:
        pool.close()
        pool.join()

    failed_results = [result for result in results if result["status"] == "error"]
    if failed_results:
        error_messages = [
            f"{result['table_id']}: {result['error']}" for result in failed_results
        ]
        raise RuntimeError(f"Cleanup failed: {'; '.join(error_messages)}")

    return [
        {"table_id": result["table_id"], "stats": result["stats"]} for result in results
    ]
