"""Shared pytest fixtures for lance-ray tests."""

import os
import shutil
import sys
import tempfile

import pytest
import ray

# Ensure the ``tests`` package directory is importable from Ray workers, which
# don't inherit pytest's sys.path.
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from _ray_test_support import (  # noqa: E402
    patch_memory_profiler,
    patch_psutil_for_containers,
)


@pytest.fixture(scope="session", autouse=True)
def ray_context():
    """Initialize Ray once per pytest session.

    Defined in conftest.py so that running multiple test files concurrently
    does not cause one file's fixture to shutdown Ray for another.

    By default a fresh local Ray instance is started in an isolated temp
    directory. Set ``RAY_TEST_ADDRESS`` (e.g. ``auto`` or ``host:port``) to
    connect to an existing cluster instead.
    """
    # In containerized environments with PID namespace isolation,
    # ``psutil.Process().parents()`` raises ``NoSuchProcess``.  Ray 2.48+
    # calls this inside ``_get_uv_run_cmdline`` during ``ray.init()``.
    # Disable the uv runtime-env hook entirely — it is not needed in tests.
    os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

    # Also monkey-patch psutil calls that Ray makes before / after init
    # so that containerised PID-namespace issues do not crash the driver
    # or workers.
    patch_psutil_for_containers()

    if ray.is_initialized():
        ray.shutdown()

    # Patch on the driver side first so any in-process Ray Data calls work.
    patch_memory_profiler()

    runtime_env = {
        "worker_process_setup_hook": "_ray_test_support.setup_worker",
        "env_vars": {
            "PYTHONPATH": _TESTS_DIR + os.pathsep + os.environ.get("PYTHONPATH", ""),
            "RAY_ENABLE_UV_RUN_RUNTIME_ENV": "0",
        },
    }

    address = os.environ.get("RAY_TEST_ADDRESS")
    if address:
        ray.init(address=address, ignore_reinit_error=True, runtime_env=runtime_env)
        yield
    else:
        # Use an isolated temp dir so we don't pick up a stale
        # /tmp/ray/ray_current_cluster pointer from previous runs.
        temp_dir = tempfile.mkdtemp(prefix="rl_", dir="/tmp")
        ray.init(
            num_cpus=4,
            ignore_reinit_error=True,
            include_dashboard=False,
            _temp_dir=temp_dir,
            runtime_env=runtime_env,
        )
        try:
            yield
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    if ray.is_initialized():
        ray.shutdown()
