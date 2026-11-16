"""Regression tests for contiguous index segment coverage (#5317)."""

import logging
from dataclasses import dataclass
from itertools import combinations

import pytest
from lance_ray.index import _distribute_fragments_balanced


@dataclass
class _Fragment:
    fragment_id: int
    rows: int

    def count_rows(self) -> int:
        return self.rows


def test_equal_fragments_form_contiguous_segments() -> None:
    fragments = [_Fragment(i, 100) for i in range(8)]

    assert _distribute_fragments_balanced(
        fragments, 2, logging.getLogger(__name__)
    ) == [
        [0, 1, 2, 3],
        [4, 5, 6, 7],
    ]


@pytest.mark.parametrize(
    "rows, num_segments, expected_lengths",
    [
        ([5000] + [100] * 9, 5, [1, 3, 2, 2, 2]),
        ([1_000_000] + [1000] * 99, 10, [1, 7, 6, 12, 13, 12, 13, 12, 12, 12]),
        ([0] * 8, 4, [2, 2, 2, 2]),
    ],
)
def test_missing_segments_split_heaviest_run(
    rows: list[int], num_segments: int, expected_lengths: list[int]
) -> None:
    fragments = [_Fragment(i, count) for i, count in enumerate(rows)]

    batches = _distribute_fragments_balanced(
        fragments, num_segments, logging.getLogger(__name__)
    )

    assert [len(batch) for batch in batches] == expected_lengths
    assert [fragment_id for batch in batches for fragment_id in batch] == list(
        range(len(rows))
    )


def test_fragment_ids_determine_order_without_mutating_input() -> None:
    fragments = [_Fragment(i, 100) for i in [20, 3, 10, 0, 30, 7]]

    assert _distribute_fragments_balanced(
        fragments, 3, logging.getLogger(__name__)
    ) == [
        [0, 3],
        [7, 10],
        [20, 30],
    ]
    assert [fragment.fragment_id for fragment in fragments] == [20, 3, 10, 0, 30, 7]


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [0],
        [0] * 8,
        [0, 10, 0, 0, 20, 0],
        [100, 80, 60, 40, 20],
        [10, 1, 1, 1, 1, 10],
        [2, 3, 8, 1, 6],
        [2**60, 2**60 + 1, 0, 1],
    ],
)
@pytest.mark.parametrize("num_segments", [1, 2, 3, 8, 10])
def test_contiguous_distribution_minimizes_maximum_rows(
    rows: list[int], num_segments: int
) -> None:
    # IDs deliberately differ from positions and are descending: IDs determine
    # dataset order, never input order or row-count order.
    fragments = [_Fragment(100 - i * 3, count) for i, count in enumerate(rows)]
    logger = logging.getLogger(__name__)
    batches = _distribute_fragments_balanced(fragments, num_segments, logger)

    assert len(batches) == min(num_segments, len(rows))
    assert all(batches)
    assert [fragment_id for batch in batches for fragment_id in batch] == sorted(
        fragment.fragment_id for fragment in fragments
    )
    assert batches == _distribute_fragments_balanced(fragments, num_segments, logger)
    if not rows:
        return

    ordered_rows = [
        fragment.rows for fragment in sorted(fragments, key=lambda f: f.fragment_id)
    ]
    sizes = {fragment.fragment_id: fragment.rows for fragment in fragments}
    actual = max(sum(sizes[fragment_id] for fragment_id in batch) for batch in batches)
    # Enumerate every legal cut placement as an independent optimality oracle.
    optimal = min(
        max(
            sum(ordered_rows[start:end])
            for start, end in zip((0, *cuts), (*cuts, len(ordered_rows)), strict=True)
        )
        for cuts in combinations(range(1, len(rows)), len(batches) - 1)
    )
    assert actual == optimal


def test_unknown_fragment_sizes_preserve_order(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class UnknownSizeFragment:
        def __init__(self, fragment_id: int) -> None:
            self.fragment_id = fragment_id

        def count_rows(self) -> int:
            raise OSError("row count unavailable")

    fragments = [UnknownSizeFragment(i) for i in [0, 3, 10, 100]]
    assert _distribute_fragments_balanced(
        fragments, 2, logging.getLogger(__name__)
    ) == [
        [0, 3],
        [10, 100],
    ]
    assert "Using 1 as size estimate" in caplog.text


@pytest.mark.parametrize("num_segments", [0, -1])
def test_distribution_requires_positive_segment_count(num_segments: int) -> None:
    with pytest.raises(ValueError, match="num_segments must be positive"):
        _distribute_fragments_balanced(
            [_Fragment(0, 100)], num_segments, logging.getLogger(__name__)
        )
