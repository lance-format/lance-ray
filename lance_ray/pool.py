# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Optional

from ray.util.multiprocessing import Pool

logger = logging.getLogger(__name__)

_GLOBAL_POOL: Any | None = None
_GLOBAL_POOL_PROCESSES: int | None = None
_GLOBAL_POOL_LOCK = threading.RLock()


def _read_pool_processes(pool: Any) -> int | None:
    for attr in ("processes", "_processes", "_num_processes"):
        value = getattr(pool, attr, None)
        if isinstance(value, int):
            return value
    return None


def _warn_if_process_count_differs(requested_processes: int) -> None:
    if _GLOBAL_POOL_PROCESSES is None:
        return
    if requested_processes == _GLOBAL_POOL_PROCESSES:
        return

    logger.warning(
        "Reusing global Ray Pool with %d processes; requested %d workers will be "
        "ignored while the global Pool is active.",
        _GLOBAL_POOL_PROCESSES,
        requested_processes,
    )


def set_global_pool(pool: Any | None) -> None:
    """Set a process-wide Ray Pool for Lance-Ray operations to reuse.

    Passing ``None`` clears the global Pool reference without closing it.
    """
    global _GLOBAL_POOL, _GLOBAL_POOL_PROCESSES
    with _GLOBAL_POOL_LOCK:
        _GLOBAL_POOL = pool
        _GLOBAL_POOL_PROCESSES = _read_pool_processes(pool) if pool is not None else None


def get_global_pool() -> Any | None:
    """Return the currently configured global Ray Pool, if any."""
    with _GLOBAL_POOL_LOCK:
        return _GLOBAL_POOL


def init_global_pool(
    processes: int,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> Any:
    """Create and register a global Ray Pool if one does not already exist."""
    if processes <= 0:
        raise ValueError(f"processes must be positive, got {processes}")

    global _GLOBAL_POOL, _GLOBAL_POOL_PROCESSES
    with _GLOBAL_POOL_LOCK:
        if _GLOBAL_POOL is not None:
            _warn_if_process_count_differs(processes)
            return _GLOBAL_POOL

        _GLOBAL_POOL = Pool(processes=processes, ray_remote_args=ray_remote_args)
        _GLOBAL_POOL_PROCESSES = processes
        return _GLOBAL_POOL


def clear_global_pool(*, close: bool = False, join: bool = True) -> None:
    """Clear the global Pool reference, optionally closing and joining it."""
    global _GLOBAL_POOL, _GLOBAL_POOL_PROCESSES
    with _GLOBAL_POOL_LOCK:
        pool = _GLOBAL_POOL
        _GLOBAL_POOL = None
        _GLOBAL_POOL_PROCESSES = None

    if close and pool is not None:
        pool.close()
        if join:
            pool.join()


@contextmanager
def get_or_create_pool(
    *,
    processes: int,
    ray_remote_args: Optional[dict[str, Any]],
) -> Iterator[Any]:
    """Yield the global Pool if present, otherwise a local close-and-join Pool."""
    with _GLOBAL_POOL_LOCK:
        pool = _GLOBAL_POOL
        if pool is not None:
            _warn_if_process_count_differs(processes)

    if pool is not None:
        yield pool
        return

    local_pool = Pool(processes=processes, ray_remote_args=ray_remote_args)
    try:
        yield local_pool
    finally:
        local_pool.close()
        local_pool.join()
