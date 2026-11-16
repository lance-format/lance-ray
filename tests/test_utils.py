from collections.abc import Sequence

import pytest
from lance_ray.utils import array_split


def test_array_split_returns_exactly_n_chunks() -> None:
    # A Ray read with parallelism=10 over 11 fragments must produce 10
    # read tasks, mirroring ``numpy.array_split``.
    chunks = array_split(list(range(11)), 10)
    assert len(chunks) == 10
    assert [len(chunk) for chunk in chunks] == [2, 1, 1, 1, 1, 1, 1, 1, 1, 1]


def test_array_split_chunks_are_contiguous() -> None:
    items = list(range(11))
    chunks = array_split(items, 4)
    flat = [item for chunk in chunks for item in chunk]
    assert flat == items


def test_array_split_empty_input_yields_n_empty_chunks() -> None:
    chunks: list[Sequence[int]] = array_split([], 3)
    assert len(chunks) == 3
    assert all(len(chunk) == 0 for chunk in chunks)


def test_array_split_n_above_len_yields_empty_tail_chunks() -> None:
    chunks = array_split([1, 2, 3], 5)
    assert len(chunks) == 5
    assert [len(chunk) for chunk in chunks] == [1, 1, 1, 0, 0]


def test_array_split_rejects_non_positive_n() -> None:
    with pytest.raises(ValueError, match="n must be at least one"):
        array_split([1, 2, 3], 0)
