import json
import logging
from collections.abc import Callable
from typing import Any, Optional, cast

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

_MEMORY_MULTIPLIER = 2.0


def _handle_compaction_task(
    dataset_uri: str,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
) -> Callable[[CompactionTask], dict[str, Any]]:
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


def _fragment_id(fragment: Any) -> int:
    """Return the integer id of a fragment id, FragmentMetadata, or LanceFragment."""
    if isinstance(fragment, int) and not isinstance(fragment, bool):
        return fragment

    for name in ("id", "fragment_id"):
        value = getattr(fragment, name, None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if callable(value):
            called = value()
            if isinstance(called, int) and not isinstance(called, bool):
                return called

    metadata = getattr(fragment, "metadata", None)
    if metadata is not None:
        value = getattr(metadata, "id", None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value

    raise TypeError(
        "CompactionTask.fragments entries must be fragment ids or "
        f"FragmentMetadata, got {type(fragment).__name__}"
    )


def _estimate_fragment_memory(
    dataset: "lance.LanceDataset",
    fragment_ids: list[int],
    avg_row_size_bytes: Optional[float] = None,
) -> dict[str, Any]:
    """
    Estimate memory requirements for compacting fragments.

    ``fragment_ids`` must already be integers. ``Dataset.get_fragment`` rejects
    ``FragmentMetadata`` objects.

    Returns a dict with total rows/file size, an estimated memory footprint, and
    per-fragment details. Each detail stores an integer ``fragment_id`` and a
    ``size_known`` flag. Base files and overlay files both count. A missing
    ``file_size_bytes`` on either is never treated as a safe zero-cost
    contribution to packing decisions.
    """
    if any(
        not isinstance(frag_id, int) or isinstance(frag_id, bool)
        for frag_id in fragment_ids
    ):
        raise TypeError(
            "fragment_ids must be integers; extract ids with _fragment_id before "
            "calling _estimate_fragment_memory"
        )

    fragment_details: list[dict[str, Any]] = []
    total_rows = 0
    total_file_size_bytes = 0
    known_file_size_bytes = 0
    known_rows = 0

    for frag_id in fragment_ids:
        fragment = dataset.get_fragment(frag_id)
        if fragment is None:
            raise ValueError(f"Fragment {frag_id} not found in dataset")

        frag_rows = int(fragment.count_rows())
        total_rows += frag_rows

        frag_file_size = 0
        size_known = True
        for data_file in _compaction_data_files(fragment):
            if data_file is None:
                size_known = False
                continue
            file_size = _read_file_size_bytes(frag_id, data_file)
            if file_size is None:
                size_known = False
                continue
            frag_file_size += file_size

        if size_known:
            known_file_size_bytes += frag_file_size
            known_rows += frag_rows
            total_file_size_bytes += frag_file_size
        else:
            logger.warning(
                "Fragment %s is missing file_size_bytes on a base or overlay "
                "file; memory cost is unknown and will not be treated as zero",
                frag_id,
            )

        fragment_details.append(
            {
                "fragment_id": int(frag_id),
                "num_rows": frag_rows,
                "file_size_bytes": frag_file_size,
                "size_known": size_known,
            }
        )

    if avg_row_size_bytes is None and known_rows > 0:
        avg_row_size_bytes = known_file_size_bytes / known_rows

    return {
        "total_rows": total_rows,
        "total_file_size_bytes": total_file_size_bytes,
        "estimated_memory_bytes": int(known_file_size_bytes * _MEMORY_MULTIPLIER),
        "avg_row_size_bytes": avg_row_size_bytes,
        "fragment_details": fragment_details,
        "all_sizes_known": all(detail["size_known"] for detail in fragment_details),
    }


def _read_file_size_bytes(frag_id: int, data_file: Any) -> int | None:
    """Return a file size, or None when the size is absent."""
    file_size = getattr(data_file, "file_size_bytes", None)
    if file_size is None:
        return None
    if isinstance(file_size, bool) or not isinstance(file_size, int):
        raise TypeError(
            f"Fragment {frag_id} file_size_bytes must be int or None, "
            f"got {type(file_size).__name__}"
        )
    return file_size


def _compaction_data_files(fragment: Any) -> list[Any]:
    """Return base data files plus overlay data files compaction will read.

    ``LanceFragment.data_files`` exposes only ``metadata.files``. Overlay bytes
    live on ``metadata.overlays`` and are read during compaction too. A missing
    overlay data file is returned as None so the caller treats its size as
    unknown instead of zero.
    """
    files: list[Any] = list(fragment.data_files())
    metadata = getattr(fragment, "metadata", None)
    if callable(metadata):
        metadata = metadata()
    if metadata is None:
        return files

    overlays = getattr(metadata, "overlays", None)
    if not overlays:
        return files

    for overlay in overlays:
        files.append(getattr(overlay, "data_file", None))
    return files


def _compaction_task_json(task: CompactionTask) -> str:
    """Serialize a task.

    PyLance implements ``CompactionTask.json``, but published stubs may omit it.
    """
    payload = cast(Any, task).json()
    if not isinstance(payload, str):
        raise TypeError("CompactionTask.json() must return str")
    return payload


def _compaction_task_from_json(payload: str) -> CompactionTask:
    """Load a task serialized by :func:`_compaction_task_json`."""
    task = cast(Any, CompactionTask).from_json(payload)
    return cast(CompactionTask, task)


def _index_compaction_task(
    task: CompactionTask,
) -> tuple[dict[str, Any], dict[int, Any]]:
    """Parse ``task`` once and index its fragment records by id."""
    payload = json.loads(_compaction_task_json(task))
    if not isinstance(payload, dict):
        raise TypeError("CompactionTask JSON must be an object")
    task_payload = payload.get("task")
    if not isinstance(task_payload, dict):
        raise TypeError("CompactionTask JSON is missing task")
    records = task_payload.get("fragments")
    if not isinstance(records, list):
        raise TypeError("CompactionTask JSON is missing task.fragments")

    by_id: dict[int, Any] = {}
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("id"), int):
            raise TypeError(
                "CompactionTask JSON fragment records must include an integer id"
            )
        by_id[int(record["id"])] = record
    return payload, by_id


def _subset_indexed_compaction_task(
    payload: dict[str, Any],
    by_id: dict[int, Any],
    fragments: list[Any],
) -> CompactionTask:
    """Rebuild one sub-task from an index produced by :func:`_index_compaction_task`."""
    if not fragments:
        raise ValueError("Cannot create an empty compaction sub-task")

    task_payload = payload.get("task")
    if not isinstance(task_payload, dict):
        raise TypeError("CompactionTask JSON is missing task")

    selected: list[Any] = []
    selected_ids: set[int] = set()
    for fragment in fragments:
        frag_id = _fragment_id(fragment)
        record = by_id.get(frag_id)
        if record is None:
            raise ValueError(f"fragment {frag_id} is not part of the compaction task")
        if frag_id in selected_ids:
            raise ValueError(f"duplicate fragment {frag_id} in compaction sub-task")
        selected.append(record)
        selected_ids.add(frag_id)

    subset_task = dict(task_payload)
    subset_task["fragments"] = selected
    subset_payload = dict(payload)
    subset_payload["task"] = subset_task
    return _compaction_task_from_json(json.dumps(subset_payload))


def _subset_compaction_task(
    task: CompactionTask, fragments: list[Any]
) -> CompactionTask:
    """Return ``task`` limited to ``fragments``, keeping read_version and options.

    ``CompactionTask`` cannot be constructed from a bare fragment-id list. The
    original task JSON already contains full fragment records; split tasks are
    rebuilt by filtering those records and calling ``CompactionTask.from_json``.
    """
    payload, by_id = _index_compaction_task(task)
    return _subset_indexed_compaction_task(payload, by_id, fragments)


def _split_compaction_task(
    task: CompactionTask,
    fragment_details: list[dict[str, Any]],
    max_memory_per_task_bytes: int,
) -> list[CompactionTask]:
    """Split one over-budget task into executable sub-tasks."""
    details_by_id = {int(detail["fragment_id"]): detail for detail in fragment_details}
    original_ids = [_fragment_id(fragment) for fragment in task.fragments]
    if len(original_ids) != len(set(original_ids)):
        raise ValueError("compaction task contains duplicate fragment ids")

    limit_mb = max_memory_per_task_bytes / (1024 * 1024)
    known_sizes = [
        detail["file_size_bytes"] * _MEMORY_MULTIPLIER / (1024 * 1024)
        for detail in fragment_details
        if detail["size_known"]
    ]
    if known_sizes and max(known_sizes) > limit_mb:
        logger.warning(
            "Memory limit (%.2f MB) is smaller than the largest fragment "
            "(%.2f MB). Splitting cannot guarantee every sub-task stays within "
            "budget; individual fragments may still exceed the limit.",
            limit_mb,
            max(known_sizes),
        )

    payload, fragments_by_id = _index_compaction_task(task)
    sub_tasks: list[CompactionTask] = []
    split_ids: list[int] = []
    current_batch: list[Any] = []
    current_batch_size = 0

    def flush_batch() -> None:
        nonlocal current_batch, current_batch_size
        if not current_batch:
            return
        batch_ids = [_fragment_id(fragment) for fragment in current_batch]
        if not batch_ids:
            raise RuntimeError("split produced an empty compaction sub-task")
        sub_tasks.append(
            _subset_indexed_compaction_task(payload, fragments_by_id, current_batch)
        )
        split_ids.extend(batch_ids)
        current_batch = []
        current_batch_size = 0

    for fragment in task.fragments:
        frag_id = _fragment_id(fragment)
        frag_detail = details_by_id.get(frag_id)
        if frag_detail is None:
            raise ValueError(
                f"missing memory estimate for fragment {frag_id} while splitting "
                "compaction task"
            )

        if not frag_detail["size_known"]:
            logger.warning(
                "Fragment %s has unknown file size; placing it in its own sub-task "
                "because unknown size cannot be treated as zero memory cost",
                frag_id,
            )
            flush_batch()
            sub_tasks.append(
                _subset_indexed_compaction_task(payload, fragments_by_id, [fragment])
            )
            split_ids.append(frag_id)
            continue

        frag_memory = int(frag_detail["file_size_bytes"] * _MEMORY_MULTIPLIER)
        if (
            current_batch
            and current_batch_size + frag_memory > max_memory_per_task_bytes
        ):
            flush_batch()
        current_batch.append(fragment)
        current_batch_size += frag_memory

    flush_batch()

    if sorted(split_ids) != sorted(original_ids):
        raise RuntimeError(
            "compaction task split changed fragment membership: "
            f"original={sorted(original_ids)} split={sorted(split_ids)}"
        )
    if len(split_ids) != len(set(split_ids)):
        raise RuntimeError("compaction task split produced duplicate fragments")

    return sub_tasks


def _create_memory_aware_compaction_plan(
    dataset: "lance.LanceDataset",
    compaction_options: Optional[CompactionOptions] = None,
    max_memory_per_task_bytes: Optional[int] = None,
) -> tuple[list[CompactionTask], dict[str, Any]]:
    """
    Create a compaction plan that respects memory constraints.

    ``CompactionTask.fragments`` contains ``FragmentMetadata`` objects, while
    ``Dataset.get_fragment`` requires integer ids. Ids are extracted before any
    fragment lookup or estimate comparison. Over-budget tasks are rebuilt through
    ``CompactionTask.json`` / ``from_json`` so read_version, options, and full
    fragment metadata are preserved.
    """
    if max_memory_per_task_bytes is None:
        max_memory_per_task_bytes = 2 * 1024 * 1024 * 1024  # 2GB

    fragments = dataset.get_fragments()
    fragment_ids = [_fragment_id(fragment) for fragment in fragments]

    if not fragment_ids:
        return [], {"message": "No fragments to compact"}

    memory_estimate = _estimate_fragment_memory(dataset, fragment_ids)
    avg_row_size_bytes = memory_estimate["avg_row_size_bytes"]

    logger.info(
        f"Analyzed {len(fragment_ids)} fragments: "
        f"{memory_estimate['total_rows']} rows, "
        f"{memory_estimate['total_file_size_bytes'] / (1024 * 1024):.2f} MB total file size"
    )

    initial_plan = Compaction.plan(
        dataset, compaction_options or cast(CompactionOptions, {})
    )

    if initial_plan.num_tasks() == 0:
        return [], {
            "message": "No compaction needed based on options",
            "initial_plan": initial_plan,
        }

    final_tasks: list[CompactionTask] = []
    tasks_split = 0

    for task in initial_plan.tasks:
        task_fragment_ids = [_fragment_id(fragment) for fragment in task.fragments]
        task_memory_estimate = _estimate_fragment_memory(
            dataset, task_fragment_ids, avg_row_size_bytes
        )
        fragment_details = task_memory_estimate["fragment_details"]
        fits_budget = (
            task_memory_estimate["all_sizes_known"]
            and task_memory_estimate["estimated_memory_bytes"]
            <= max_memory_per_task_bytes
        )
        if fits_budget:
            final_tasks.append(task)
            continue

        tasks_split += 1
        estimated_mb = (
            task_memory_estimate["estimated_memory_bytes"] / (1024 * 1024)
            if task_memory_estimate["all_sizes_known"]
            else float("nan")
        )
        if task_memory_estimate["all_sizes_known"]:
            logger.info(
                "Splitting compaction task for %s fragments (%.2f MB estimated) "
                "into smaller chunks",
                len(task.fragments),
                estimated_mb,
            )
        else:
            logger.info(
                "Splitting compaction task for %s fragments because at least one "
                "fragment has unknown file size",
                len(task.fragments),
            )

        final_tasks.extend(
            _split_compaction_task(task, fragment_details, max_memory_per_task_bytes)
        )

    planning_metrics = {
        "original_tasks": initial_plan.num_tasks(),
        "final_tasks": len(final_tasks),
        "tasks_split": tasks_split,
        "max_memory_per_task_bytes": max_memory_per_task_bytes,
        "total_estimated_memory_bytes": memory_estimate["estimated_memory_bytes"],
        "all_sizes_known": memory_estimate["all_sizes_known"],
    }

    logger.info(
        f"Created memory-aware compaction plan: {len(final_tasks)} tasks "
        f"(originally {initial_plan.num_tasks()}, split {tasks_split} tasks)"
    )

    return final_tasks, planning_metrics


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
    max_memory_per_task_bytes: Optional[int] = None,
) -> Optional[CompactionMetrics]:
    """
    Compact files in a Lance dataset using distributed Ray workers.

    This function distributes the compaction process across multiple Ray workers,
    with each worker executing a subset of compaction tasks. The results are then
    committed as a single compaction operation.

    Memory Management:
        Pass ``max_memory_per_task_bytes`` to estimate each task from fragment
        file sizes and split tasks that would exceed that budget before they
        are sent to Ray. Omitting it keeps Lance's normal compaction plan.

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
        max_memory_per_task_bytes: Maximum estimated memory (in bytes) for one
            compaction task. When omitted, planning stays on ``Compaction.plan``.
            When set, oversized tasks are split before execution. Estimates use
            known fragment file sizes with a 2x multiplier for Arrow
            materialization. Unknown file sizes are not treated as zero.

            **Important**: If this value is smaller than the memory requirement of
            individual fragments, the system will still split tasks to the finest
            granularity (one fragment per task), but OOM may still occur. In such
            cases, consider either:
            1. Increasing `max_memory_per_task_bytes`
            2. Reducing fragment size during data writing (use smaller
               `max_rows_per_file` when creating the dataset)
            3. Ensuring workers have sufficient physical memory

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

    # Step 1: Create the compaction plan with memory awareness
    compaction_tasks: list[CompactionTask] = []
    planning_metrics: dict[str, Any] = {}

    if max_memory_per_task_bytes is not None:
        logger.info(
            f"Using memory-aware compaction planning with max {max_memory_per_task_bytes / (1024 * 1024):.2f} MB per task"
        )
        compaction_tasks, planning_metrics = _create_memory_aware_compaction_plan(
            dataset, compaction_options, max_memory_per_task_bytes
        )

        if planning_metrics.get("tasks_split", 0) > 0:
            logger.info(
                f"Split {planning_metrics['tasks_split']} large tasks into "
                f"{planning_metrics['final_tasks']} smaller tasks "
                "(individual fragments may still exceed the memory budget)"
            )
    else:
        # Use standard compaction planning without memory constraints
        compaction_plan = Compaction.plan(
            dataset, compaction_options or cast(CompactionOptions, {})
        )
        compaction_tasks = (
            compaction_plan.tasks if hasattr(compaction_plan, "tasks") else []
        )
        planning_metrics = {
            "original_tasks": (
                compaction_plan.num_tasks()
                if hasattr(compaction_plan, "num_tasks")
                else 0
            ),
            "final_tasks": len(compaction_tasks),
        }

    logger.info(f"Compaction plan created with {len(compaction_tasks)} tasks")

    if len(compaction_tasks) == 0:
        logger.info("No compaction tasks needed")
        return None

    # Adjust num_workers if needed
    if num_workers > len(compaction_tasks):
        num_workers = len(compaction_tasks)
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
        compaction_tasks,
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
    max_memory_per_task_bytes: Optional[int] = None,
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
        max_memory_per_task_bytes: Maximum estimated memory (in bytes) for one
            compaction task on every table. Passed through to
            :func:`compact_files`. When omitted, each table keeps Lance's
            normal compaction plan.

    Returns:
        A list of dicts, one per table, with keys:
        - ``"table_id"``: ``list[str]`` – full table identifier (database + table name).
        - ``"metrics"``: :class:`~lance.lance.CompactionMetrics` or ``None`` –
          compaction result for that table, or ``None`` if no compaction was needed.

    Raises:
        ValueError: If database is empty or namespace_impl is not provided.
        RuntimeError: If listing tables fails or any table compaction fails.

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
                max_memory_per_task_bytes=max_memory_per_task_bytes,
            )
            results.append({"table_id": table_id, "metrics": metrics})
        except Exception as e:
            logger.exception("Compaction failed for table %s: %s", table_id, e)
            raise RuntimeError(f"Compaction failed for table {table_id}: {e}") from e

    return results
