import logging
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


def _cleanup_stats_to_dict(stats: CleanupStats) -> dict[str, Any]:
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

    This delegates file safety and deletion policy to Lance core.  Use
    ``cleanup_database_old_versions`` to run cleanup across many namespace tables
    with Ray workers.
    """
    validate_uri_or_namespace(uri, namespace_impl, table_id)

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
):
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
    """Clean up old versions for every table under a namespace database."""
    if not database:
        raise ValueError("'database' must be a non-empty list of path segments.")
    if not namespace_impl:
        raise ValueError(
            "'namespace_impl' is required when using cleanup_database_old_versions."
        )
    if num_workers <= 0:
        raise ValueError("'num_workers' must be positive.")

    from lance_namespace import ListTablesRequest

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        raise RuntimeError(
            "Failed to create namespace from namespace_impl and namespace_properties."
        )

    all_tables: list[str] = []
    page_token: Optional[str] = None
    limit = 500

    while True:
        request = ListTablesRequest(
            id=database,
            page_token=page_token,
            limit=limit,
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
