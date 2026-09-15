"""Helpers loaded by Ray workers in tests.

Kept as a top-level module (not inside ``conftest.py``) so it is importable
inside Ray worker processes via ``runtime_env.worker_process_setup_hook``.
"""

from __future__ import annotations

import importlib
import threading
from typing import Any, Optional


def patch_memory_profiler() -> None:
    """Make Ray Data's ``MemoryProfiler`` resilient to sandboxed PID namespaces.

    In some containerized environments ``psutil.Process(os.getpid())`` raises
    ``NoSuchProcess`` even though the worker is alive (different PID namespace
    visibility). Ray Data instantiates ``MemoryProfiler`` inside every map task
    unconditionally, so we make construction non-fatal here.
    """
    try:
        from ray.data._internal import util as ray_data_util
    except ImportError:
        return

    memory_profiler = getattr(ray_data_util, "MemoryProfiler", None)
    if memory_profiler is None:
        # Ray 2.41-2.44 do not create this profiler, so no patch is needed.
        return

    original_init = memory_profiler.__init__

    def safe_init(self: Any, poll_interval_s: Optional[float]) -> None:
        self._poll_interval_s = poll_interval_s
        try:
            original_init(self, poll_interval_s)
        except Exception:
            self._process = None
            self._max_uss = None
            self._max_uss_lock = threading.Lock()
            self._uss_poll_thread = None
            self._stop_uss_poll_event = None

    # Monkey-patching methods is the point of this module, so the
    # ``method-assign`` guard does not apply here.
    memory_profiler.__init__ = safe_init
    if hasattr(memory_profiler, "_can_estimate_uss"):

        def _cannot_estimate_uss(self: Any) -> bool:
            return False

        memory_profiler._can_estimate_uss = _cannot_estimate_uss


def patch_psutil_for_containers() -> None:
    """Patch Ray's psutil usage so it does not crash in containerised
    environments where PID namespace isolation makes
    ``psutil.Process().parents()`` raise ``NoSuchProcess``.

    Affected call-sites in Ray 2.48+:

    * ``ray._private.runtime_env.uv_runtime_env_hook._get_uv_run_cmdline``
      — called during ``ray.init()`` when ``RAY_ENABLE_UV_RUN_RUNTIME_ENV``
      is truthy.

    * ``ray._private.worker._maybe_modify_runtime_env``
      — same call-chain as above.
    """
    try:
        uv_runtime_env_hook = importlib.import_module(
            "ray._private.runtime_env.uv_runtime_env_hook"
        )
    except ImportError:
        return

    original_fn = getattr(uv_runtime_env_hook, "_get_uv_run_cmdline", None)
    if original_fn is None:
        # Older Ray releases have the module but not the uv hook being patched.
        return

    def _safe_get_uv_run_cmdline() -> Any:
        try:
            return original_fn()
        except Exception:
            return None

    uv_runtime_env_hook._get_uv_run_cmdline = _safe_get_uv_run_cmdline  # type: ignore[attr-defined]

    _patch_worker_log_offset()


def _patch_worker_log_offset() -> None:
    """Make ``Worker.get_current_out_offset`` / ``get_current_err_offset``
    resilient to log-file rotation in long-running test sessions.

    In Ray 2.48+ these helpers call ``os.path.getsize()`` on the worker
    log file.  When Ray's log monitor rotates or removes the file between
    tasks, the call raises ``FileNotFoundError`` which then propagates as
    a ``RayTaskError`` and fails the test.  We wrap both methods to return
    a safe fallback (0) when the file is gone.
    """
    try:
        from ray._private.worker import Worker
    except ImportError:
        return

    _orig_out = Worker.get_current_out_offset
    _orig_err = Worker.get_current_err_offset

    def _safe_out_offset(self: Any) -> int:
        try:
            return _orig_out(self)
        except FileNotFoundError:
            return 0

    def _safe_err_offset(self: Any) -> int:
        try:
            return _orig_err(self)
        except FileNotFoundError:
            return 0

    Worker.get_current_out_offset = _safe_out_offset  # type: ignore[method-assign]
    Worker.get_current_err_offset = _safe_err_offset  # type: ignore[method-assign]


def setup_worker() -> None:
    """Entry-point for ``runtime_env.worker_process_setup_hook``.

    Applies all container-compatibility patches on each Ray worker process.
    """
    patch_memory_profiler()
    patch_psutil_for_containers()
