# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Distributed ``cleanup_old_versions`` on Ray."""

from __future__ import annotations

import logging
from datetime import timedelta
from functools import lru_cache
from typing import Any, Optional, cast

import lance
from lance.dataset import LanceDataset

from ._dispatch import _map_async_with_pool
from .utils import (
    get_namespace_kwargs,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

logger = logging.getLogger(__name__)

# Tasks each worker gets by default, so a slow range does not hold up the rest.
_TASKS_PER_WORKER = 4


def _freeze(value: Optional[dict[str, Any]]) -> Optional[tuple[tuple[str, Any], ...]]:
    return tuple(sorted(value.items())) if value else None


def _open_dataset(
    uri: str,
    storage_options: Optional[tuple[tuple[str, Any], ...]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[tuple[tuple[str, str], ...]],
    table_id: Optional[tuple[str, ...]],
) -> LanceDataset:
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl,
        dict(namespace_properties) if namespace_properties else None,
        list(table_id) if table_id else None,
    )
    return lance.LanceDataset(
        uri,
        storage_options=dict(storage_options) if storage_options else None,
        **namespace_kwargs,
    )


@lru_cache(maxsize=8)
def _open_worker_dataset(*dataset_args: Any) -> LanceDataset:
    """Opens the dataset once for all of a worker's tasks: a task runs on any
    version of it. The driver plans on the latest version, so it does not cache."""
    return _open_dataset(*dataset_args)


def _supports_distributed_cleanup() -> bool:
    return hasattr(LanceDataset, "plan_cleanup_old_versions")


def _sweep_handler(
    dataset_args: tuple[Any, ...],
    delete_concurrency: Optional[int],
    delete_rate_limit: Optional[int],
) -> Any:
    def run(task: Any) -> dict[str, Any]:
        try:
            dataset = _open_worker_dataset(*dataset_args)
            result = task.execute(
                dataset,
                delete_concurrency=delete_concurrency,
                delete_rate_limit=delete_rate_limit,
            )
            return {"status": "success", "result": result}
        except Exception as e:
            return {"status": "error", "error": f"{type(e).__name__}: {e}"}

    return run


def _cleanup_kwargs(
    older_than: Optional[timedelta],
    retain_versions: Optional[int],
    versions: Optional[list[int]],
    delete_unverified: bool,
    error_if_tagged_old_versions: bool,
) -> dict[str, Any]:
    return {
        "older_than": older_than,
        "retain_versions": retain_versions,
        "versions": versions,
        "delete_unverified": delete_unverified,
        "error_if_tagged_old_versions": error_if_tagged_old_versions,
    }


def _run_cleanup(
    *,
    uri: Optional[str],
    table_id: Optional[list[str]],
    cleanup_kwargs: dict[str, Any],
    explain: bool,
    max_files: int,
    delete_rate_limit: Optional[int],
    delete_concurrency: Optional[int],
    num_workers: int,
    max_tasks: Optional[int],
    max_task_retries: int,
    storage_options: Optional[dict[str, str]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    ray_remote_args: Optional[dict[str, Any]],
) -> Any:
    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")
    if max_tasks is not None and max_tasks <= 0:
        raise ValueError(f"max_tasks must be positive, got {max_tasks}")
    if max_task_retries < 0:
        raise ValueError(
            f"max_task_retries must be non-negative, got {max_task_retries}"
        )
    validate_uri_or_namespace(uri, namespace_impl, table_id)
    uri, merged_storage_options = resolve_namespace_table(
        uri, storage_options, namespace_impl, namespace_properties, table_id
    )
    dataset_args = (
        uri,
        _freeze(merged_storage_options),
        namespace_impl,
        _freeze(namespace_properties),
        tuple(table_id) if table_id else None,
    )
    dataset = _open_dataset(*dataset_args)

    if not _supports_distributed_cleanup():
        logger.warning(
            "pylance %s cannot distribute cleanup; running it on the driver",
            lance.__version__,
        )
        if explain:
            return dataset.explain_cleanup_old_versions(
                **cleanup_kwargs, include_files=True, max_files=max_files
            )
        # Older releases do not take every limit, so only pass the ones set.
        limits: dict[str, Any] = {
            name: value
            for name, value in [
                ("delete_rate_limit", delete_rate_limit),
                ("delete_concurrency", delete_concurrency),
            ]
            if value is not None
        }
        return dataset.cleanup_old_versions(**cleanup_kwargs, **limits)

    # Typed as Any: the pylance this is type-checked against may predate the API.
    plan = cast(Any, dataset).plan_cleanup_old_versions(
        **cleanup_kwargs,
        delete_rate_limit=delete_rate_limit,
        delete_concurrency=delete_concurrency,
        explain=explain,
        max_files=max_files,
    )
    tasks = plan.tasks(max_tasks or num_workers * _TASKS_PER_WORKER)
    workers = min(num_workers, len(tasks))
    # Each worker deletes at its own rate, so split the budget between them.
    worker_rate = max(1, delete_rate_limit // workers) if delete_rate_limit else None
    tasks_per_subtree: dict[str, int] = {}
    for task in tasks:
        tasks_per_subtree[task.subtree] = tasks_per_subtree.get(task.subtree, 0) + 1
    logger.info(
        "Planned cleanup of %s at version %d: %d old versions, %d tasks (%s) on %d workers",
        uri,
        plan.read_version,
        plan.num_old_versions,
        len(tasks),
        ", ".join(
            f"{name}: {count}" for name, count in sorted(tasks_per_subtree.items())
        ),
        workers,
    )

    progress = {"done": 0, "files": 0, "bytes": 0}

    def on_result(index: int, result: dict[str, Any]) -> None:
        task = tasks[index]
        if result["status"] != "success":
            logger.warning(
                "Cleanup task %d (%s) failed: %s", index, task.subtree, result["error"]
            )
            return
        stats = result["result"].stats
        progress["done"] += 1
        progress["files"] += (
            stats.data_files_removed
            + stats.transaction_files_removed
            + stats.index_files_removed
            + stats.deletion_files_removed
        )
        progress["bytes"] += stats.bytes_removed
        logger.info(
            "Cleanup task %d/%d done: %s (%s, %s]; %d files, %d bytes %s so far",
            progress["done"],
            len(tasks),
            task.subtree,
            task.start_after or "",
            task.end_inclusive or "",
            progress["files"],
            progress["bytes"],
            "found" if explain else "removed",
        )

    results = _map_async_with_pool(
        create_handler=lambda: _sweep_handler(
            dataset_args, delete_concurrency, worker_rate
        ),
        items=tasks,
        num_workers=workers,
        ray_remote_args=ray_remote_args,
        error_prefix="Failed to complete distributed cleanup",
        max_task_retries=max_task_retries,
        on_result=on_result,
    )
    failed = [r for r in results if r["status"] != "success"]
    if failed:
        errors = "; ".join(r["error"] for r in failed)
        if explain:
            raise RuntimeError(
                f"Explaining cleanup failed in {len(failed)} of {len(tasks)} tasks: "
                f"{errors}"
            )
        raise RuntimeError(
            f"Cleanup failed in {len(failed)} of {len(tasks)} tasks; the old versions "
            "are removed, and later cleanups only remove the files these tasks left "
            f"as unverified files: {errors}"
        )
    sweep_results = [r["result"] for r in results]
    if explain:
        return plan.finish_explain(dataset, sweep_results)
    stats = plan.finish(dataset, sweep_results)
    logger.info("Finished cleanup of %s: %s", uri, stats)
    return stats


def cleanup_old_versions(
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    older_than: Optional[timedelta] = None,
    retain_versions: Optional[int] = None,
    versions: Optional[list[int]] = None,
    delete_unverified: bool = False,
    error_if_tagged_old_versions: bool = True,
    delete_rate_limit: Optional[int] = None,
    delete_concurrency: Optional[int] = None,
    num_workers: int = 4,
    max_tasks: Optional[int] = None,
    max_task_retries: int = 2,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> Any:
    """Remove old versions of a Lance dataset, listing and deleting on Ray workers.

    Lance plans the cleanup on the driver: it removes the old manifests and splits
    the listing of the dataset's directories into key ranges. Each range is swept
    by a Ray worker, which removes the files there that no retained version
    references, using the same rules as
    :meth:`lance.LanceDataset.cleanup_old_versions`. Lance-Ray only schedules the
    ranges; it never decides which files are removed.

    A failed range is retried up to ``max_task_retries`` times. If a range still
    fails, a ``RuntimeError`` is raised. The old versions are already gone by
    then, and later cleanups only remove the files the range left as unverified
    files. With pylance releases that cannot distribute a cleanup, it runs on the
    driver instead.

    Args:
        uri: The URI of the dataset. Either uri OR (namespace_impl + table_id)
            must be provided.
        table_id: The table identifier, used together with namespace_impl.
        older_than: Only versions older than this are removed. Defaults to two
            weeks when ``retain_versions`` and ``versions`` are not set either.
        retain_versions: Retain the last N versions.
        versions: Remove only these versions.
        delete_unverified: Also remove recent files no version references. Only
            safe when nothing else writes to the dataset until the cleanup ends.
        error_if_tagged_old_versions: Raise if a tagged version would otherwise
            be removed.
        delete_rate_limit: Approximate maximum delete requests per second for
            the whole cleanup. Each worker gets an equal share, at least one.
        delete_concurrency: Maximum in-flight deletes per worker, and on the
            driver when removing the old manifests.
        num_workers: Number of Ray workers (default: 4).
        max_tasks: Maximum number of ranges to split the listing into. Defaults
            to four per worker. Every listed directory gets at least one.
        max_task_retries: Retries per failed range (default: 2).
        storage_options: Storage options for the dataset.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace.
        ray_remote_args: Options for the Ray workers (e.g., num_cpus, resources).

    Returns:
        The ``CleanupStats`` of the whole cleanup.
    """
    return _run_cleanup(
        uri=uri,
        table_id=table_id,
        cleanup_kwargs=_cleanup_kwargs(
            older_than,
            retain_versions,
            versions,
            delete_unverified,
            error_if_tagged_old_versions,
        ),
        explain=False,
        max_files=1,
        delete_rate_limit=delete_rate_limit,
        delete_concurrency=delete_concurrency,
        num_workers=num_workers,
        max_tasks=max_tasks,
        max_task_retries=max_task_retries,
        storage_options=storage_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        ray_remote_args=ray_remote_args,
    )


def explain_cleanup_old_versions(
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    older_than: Optional[timedelta] = None,
    retain_versions: Optional[int] = None,
    versions: Optional[list[int]] = None,
    delete_unverified: bool = False,
    error_if_tagged_old_versions: bool = True,
    max_files: int = 1000,
    num_workers: int = 4,
    max_tasks: Optional[int] = None,
    max_task_retries: int = 2,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> Any:
    """Explain what :func:`cleanup_old_versions` would remove, without deleting.

    Takes the same arguments as :func:`cleanup_old_versions`, plus ``max_files``,
    the maximum number of candidate files to include. The listing is split
    across Ray workers in the same way.

    Returns:
        A ``CleanupExplanation``, as from
        :meth:`lance.LanceDataset.explain_cleanup_old_versions` with
        ``include_files=True``.
    """
    if max_files <= 0:
        raise ValueError("max_files must be positive")
    return _run_cleanup(
        uri=uri,
        table_id=table_id,
        cleanup_kwargs=_cleanup_kwargs(
            older_than,
            retain_versions,
            versions,
            delete_unverified,
            error_if_tagged_old_versions,
        ),
        explain=True,
        max_files=max_files,
        delete_rate_limit=None,
        delete_concurrency=None,
        num_workers=num_workers,
        max_tasks=max_tasks,
        max_task_retries=max_task_retries,
        storage_options=storage_options,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        ray_remote_args=ray_remote_args,
    )
