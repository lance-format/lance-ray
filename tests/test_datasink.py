# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Unit tests for LanceDatasink behaviour that does not require a Ray cluster."""

import warnings
from pathlib import Path
from typing import Any, cast

import pytest
from lance_ray.datasink import LanceDatasink


def test_on_write_complete_empty_results_warns_runtimewarning(tmp_path: Path) -> None:
    sink = LanceDatasink(str(tmp_path / "ds.lance"))

    with pytest.warns(RuntimeWarning, match="empty"):
        sink.on_write_complete([])

    # It must no longer raise a DeprecationWarning for this operational case.
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        sink.on_write_complete([])


def test_on_write_complete_empty_write_returns_warns_runtimewarning(
    tmp_path: Path,
) -> None:
    """The second branch (write_returns unwraps to empty) must also warn RuntimeWarning."""
    sink = LanceDatasink(str(tmp_path / "ds.lance"))

    class _Wrapped:
        # Truthy object whose write_returns is empty -> reaches the len()==0 branch.
        write_returns: list[Any] = []

    with pytest.warns(RuntimeWarning, match="please check"):
        sink.on_write_complete(cast("Any", _Wrapped()))

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        sink.on_write_complete(cast("Any", _Wrapped()))
