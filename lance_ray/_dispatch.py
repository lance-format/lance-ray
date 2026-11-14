# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Run independent, idempotent work items on a set of Ray actors."""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from typing import Any, Optional

import ray
from ray.exceptions import ActorUnavailableError

logger = logging.getLogger(__name__)

Handler = Callable[[Any], dict[str, Any]]


class _BatchWorker:
    """Ray actor that runs one handler call at a time."""

    def run(self, handler: Handler, item: Any) -> dict[str, Any]:
        return handler(item)


def _map_async_with_pool(
    create_handler: Callable[[], Handler],
    items: list[Any],
    *,
    num_workers: int,
    ray_remote_args: Optional[dict[str, Any]],
    error_prefix: str,
    max_task_retries: int = 0,
    on_result: Optional[Callable[[int, dict[str, Any]], None]] = None,
) -> list[dict[str, Any]]:
    """Run items on a pool of Ray actors, one item per idle worker.

    Each item is handed to whichever worker is idle at that moment, so a slow
    node only delays the items it is actually running. (Ray's
    ``multiprocessing.Pool`` assigns items round-robin at submission time,
    which makes a map of N items wait for the slowest worker's fixed share
    of N.) Items whose handler returned ``{"status": "error"}`` or whose
    Ray task/actor failed are resubmitted immediately, up to
    ``max_task_retries`` times each, rather than after every other item has
    finished; handlers must therefore be idempotent.

    ``on_result(index, result)`` is called once per item, with its final result.

    Workers are actors created with ``ray_remote_args`` (``num_cpus=0`` unless
    given, like Ray's Pool). A worker whose process dies is dropped from the
    pool unless ``{"max_restarts": ...}`` is set, in which case Ray restarts
    it and it keeps receiving items.
    """
    remote_args = {"num_cpus": 0, **(ray_remote_args or {})}
    worker_cls: Any = ray.remote(**remote_args)(_BatchWorker)
    workers: list[Any] = [worker_cls.remote() for _ in range(num_workers)]
    try:
        handler = create_handler()
        results: dict[int, dict[str, Any]] = {}
        pending: deque[tuple[int, int]] = deque(
            (index, 0) for index in range(len(items))
        )
        idle: list[Any] = []
        # A worker joins the idle list once Ray has actually started it, so an
        # item never queues on an actor still waiting for cluster resources.
        starting: dict[Any, Any] = {w.__ray_ready__.remote(): w for w in workers}
        in_flight: dict[Any, tuple[Any, int, int]] = {}
        while pending or in_flight:
            while pending and idle:
                index, attempt = pending.popleft()
                worker = idle.pop()
                ref = worker.run.remote(handler, items[index])
                in_flight[ref] = (worker, index, attempt)
            if not in_flight and not starting:
                raise RuntimeError("every worker has died")
            [ref], _ = ray.wait(list(in_flight) + list(starting), num_returns=1)
            if ref in starting:
                worker = starting.pop(ref)
                try:
                    ray.get(ref)
                    idle.append(worker)
                except ActorUnavailableError:
                    # Restarting under max_restarts: probe again shortly.
                    time.sleep(0.5)
                    starting[worker.__ray_ready__.remote()] = worker
                except Exception as exc:
                    logger.warning(
                        "Worker unavailable (%s); dropping it. Pass max_restarts "
                        "in ray_remote_args to restart dead workers",
                        exc,
                    )
                continue
            worker, index, attempt = in_flight.pop(ref)
            try:
                result = ray.get(ref)
                idle.append(worker)
            except Exception as exc:
                result = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
                # A live worker is ready again at once; a dead one is dropped
                # (or, with max_restarts, restarted) by the start path above.
                starting[worker.__ray_ready__.remote()] = worker
            results[index] = result
            if result.get("status") != "success" and attempt < max_task_retries:
                logger.warning(
                    "Batch %d failed (attempt %d of %d): %s; resubmitting",
                    index,
                    attempt + 1,
                    max_task_retries + 1,
                    result.get("error"),
                )
                pending.append((index, attempt + 1))
            elif on_result is not None:
                on_result(index, result)
        return [results[i] for i in range(len(items))]
    except Exception as exc:
        raise RuntimeError(f"{error_prefix}: {exc}") from exc
    finally:
        for worker in workers:
            ray.kill(worker)
