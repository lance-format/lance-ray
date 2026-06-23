# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Unit tests for normalize_initial_bases ID assignment."""

import pytest
from lance_ray.utils import normalize_initial_bases


def _ids_by_path(bases):
    return {spec["path"]: spec["id"] for spec in normalize_initial_bases(bases)}


def test_none_and_empty_return_none():
    assert normalize_initial_bases(None) is None
    assert normalize_initial_bases([]) is None


def test_root_gets_zero_and_auto_ids_are_sequential():
    by_path = _ids_by_path(
        [
            {"path": "/root", "is_dataset_root": True},
            {"path": "/x"},
            {"path": "/y"},
        ]
    )
    assert by_path == {"/root": 0, "/x": 1, "/y": 2}


def test_auto_id_skips_explicit_id_declared_later():
    # On the old code /c auto-takes 1, then /b's explicit 1 collides -> raise.
    by_path = _ids_by_path(
        [
            {"path": "/c"},
            {"path": "/b", "id": 1},
        ]
    )
    assert by_path == {"/c": 2, "/b": 1}


def test_duplicate_explicit_ids_still_raise():
    with pytest.raises(ValueError, match="Duplicate base path ID"):
        normalize_initial_bases(
            [
                {"path": "/x", "id": 5},
                {"path": "/y", "id": 5},
            ]
        )
