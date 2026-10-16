# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Plan physical index segments and prewarm a caller-owned worker dataset.

Logical worker IDs are scheduling slots, not Ray placement guarantees. Execute
assignments where subsequent queries reuse the same Dataset and Session.
"""

from __future__ import annotations

import inspect
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from uuid import UUID

if TYPE_CHECKING:
    from lance.dataset import LanceDataset

__all__ = [
    "IndexPrewarmAssignment",
    "IndexPrewarmError",
    "IndexPrewarmPlan",
    "IndexPrewarmReport",
    "plan_index_prewarm",
    "prewarm_index",
]


@dataclass(frozen=True)
class IndexPrewarmAssignment:
    """Indivisible segments assigned to one logical worker slot.

    ``fragment_ids`` is actual whole-segment coverage. Estimated bytes are
    index storage bytes, not a cache memory requirement.
    """

    worker_id: int
    index_segments: tuple[str, ...]
    fragment_ids: tuple[int, ...]
    estimated_size_bytes: int | None


@dataclass(frozen=True)
class IndexPrewarmPlan:
    """Serializable snapshot of selected segments and deterministic assignments."""

    dataset_uri: str
    dataset_version: int
    index_name: str
    with_position: bool
    assignments: tuple[IndexPrewarmAssignment, ...]
    uncovered_fragment_ids: tuple[int, ...]
    scheduling_basis: Literal["bytes", "segments"]


@dataclass(frozen=True)
class IndexPrewarmReport:
    """One local call's outcome; success does not guarantee cache residency."""

    dataset_uri: str
    dataset_version: int
    index_name: str
    with_position: bool
    index_segments: tuple[str, ...]
    fragment_ids: tuple[int, ...]
    uncovered_fragment_ids: tuple[int, ...]
    worker_id: str | None
    process_id: int
    elapsed_seconds: float
    cache_bytes_before: int | None
    cache_bytes_after: int | None
    status: Literal["success", "skipped", "failed"]
    error: str | None


class IndexPrewarmError(RuntimeError):
    """Native prewarm failed; ``report`` contains context and ``__cause__`` the error."""

    def __init__(self, report: IndexPrewarmReport):
        self.report = report
        super().__init__(
            f"Prewarming index {report.index_name!r} failed: {report.error}"
        )

    def __reduce__(self) -> tuple[type[IndexPrewarmError], tuple[IndexPrewarmReport]]:
        return type(self), (self.report,)


@dataclass(frozen=True)
class _Segment:
    uuid: str
    fragments: frozenset[int]
    size_bytes: int | None


def _uuid(value: str | UUID) -> str:
    if not isinstance(value, str | UUID):
        raise ValueError("index_segments must contain UUID strings or UUID objects")
    try:
        return str(UUID(str(value)))
    except ValueError as exc:
        raise ValueError(f"Invalid index segment UUID: {value!r}") from exc


def _fragment_ids(values: Iterable[int]) -> set[int]:
    result = set()
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("fragment_ids must contain nonnegative integers")
        result.add(value)
    return result


def plan_index_prewarm(
    dataset: LanceDataset,
    index_name: str,
    *,
    index_segments: Iterable[str | UUID] | None = None,
    fragment_ids: Iterable[int] | None = None,
    num_workers: int = 1,
    with_position: bool = False,
) -> IndexPrewarmPlan:
    """Plan all segments, explicit UUIDs, or whole segments overlapping fragments.

    Explicit empty selections produce no assignments. Unknown UUIDs or fragment
    IDs are rejected. Existing fragments with no segment coverage are reported
    in ``uncovered_fragment_ids``. If any selected size is unavailable, every
    segment receives equal scheduling weight. No Ray processes are started.
    """
    if not isinstance(index_name, str) or not index_name:
        raise ValueError("index_name must be a nonempty string")
    if (
        isinstance(num_workers, bool)
        or not isinstance(num_workers, int)
        or num_workers < 1
    ):
        raise ValueError("num_workers must be a positive integer")
    if not isinstance(with_position, bool):
        raise ValueError("with_position must be a bool")
    if index_segments is not None and fragment_ids is not None:
        raise ValueError("Specify only one of index_segments and fragment_ids")
    if isinstance(index_segments, str | UUID):
        raise ValueError(
            "index_segments must be an iterable of UUIDs, not a single UUID"
        )
    describe = getattr(dataset, "describe_indices", None)
    if not callable(describe):
        raise NotImplementedError("This PyLance must support describe_indices()")
    index = next((item for item in describe() if item.name == index_name), None)
    if index is None:
        raise ValueError(f"Index {index_name!r} was not found")
    if with_position and str(index.index_type).upper() != "INVERTED":
        raise ValueError("with_position is supported only for INVERTED indices")

    segments = {}
    for item in index.segments:
        segment_uuid = _uuid(item.uuid)
        size = getattr(item, "size_bytes", None)
        valid_size = isinstance(size, int) and not isinstance(size, bool) and size >= 0
        segments[segment_uuid] = _Segment(
            segment_uuid, frozenset(item.fragment_ids), size if valid_size else None
        )
    requested_fragments: set[int] = set()
    if index_segments is not None:
        requested = {_uuid(value) for value in index_segments}
        unknown = requested - segments.keys()
        if unknown:
            raise ValueError(f"Unknown index segment UUIDs: {sorted(unknown)}")
        selected = [segments[value] for value in sorted(requested)]
    elif fragment_ids is not None:
        requested_fragments = _fragment_ids(fragment_ids)
        existing = {fragment.fragment_id for fragment in dataset.get_fragments()}
        unknown_fragments = requested_fragments - existing
        if unknown_fragments:
            raise ValueError(f"Unknown fragment IDs: {sorted(unknown_fragments)}")
        selected = [
            segment
            for segment in segments.values()
            if segment.fragments & requested_fragments
        ]
    else:
        selected = list(segments.values())

    use_bytes = bool(selected) and all(s.size_bytes is not None for s in selected)
    count = min(num_workers, len(selected))
    groups: list[list[_Segment]] = [[] for _ in range(count)]
    loads = [0] * count
    # UUID breaks equal-weight ties. Use segment count before slot index
    # for equal loads so zero-byte segments still spread across worker slots.
    for segment in sorted(
        selected,
        key=lambda s: (-(s.size_bytes or 0) if use_bytes else -1, s.uuid),
    ):
        slot = min(range(count), key=lambda i: (loads[i], len(groups[i]), i))
        groups[slot].append(segment)
        loads[slot] += (segment.size_bytes or 0) if use_bytes else 1
    coverage = {fid for segment in selected for fid in segment.fragments}
    assignments = tuple(
        IndexPrewarmAssignment(
            worker_id=slot,
            index_segments=tuple(sorted(segment.uuid for segment in group)),
            fragment_ids=tuple(sorted({fid for s in group for fid in s.fragments})),
            estimated_size_bytes=(
                sum(s.size_bytes or 0 for s in group)
                if all(s.size_bytes is not None for s in group)
                else None
            ),
        )
        for slot, group in enumerate(groups)
        if group
    )
    return IndexPrewarmPlan(
        dataset_uri=dataset.uri,
        dataset_version=dataset.version,
        index_name=index_name,
        with_position=with_position,
        assignments=assignments,
        uncovered_fragment_ids=tuple(sorted(requested_fragments - coverage)),
        scheduling_basis="bytes" if use_bytes else "segments",
    )


def _cache_bytes(dataset: LanceDataset) -> int | None:
    try:
        value = dataset.session().index_cache_size_bytes()
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    except Exception:
        # Optional diagnostics must never prevent prewarm or mask its failure.
        pass
    return None


def prewarm_index(
    dataset: LanceDataset,
    index_name: str,
    *,
    index_segments: Iterable[str | UUID] | None = None,
    fragment_ids: Iterable[int] | None = None,
    with_position: bool = False,
    expected_version: int | None = None,
    worker_id: str | None = None,
) -> IndexPrewarmReport:
    """Prewarm selected segments in the supplied Dataset's existing Session.

    Execute inside the worker that will query this same Dataset. This function
    neither reopens nor closes the Dataset. ``expected_version`` can guard a
    previously generated plan. Native errors raise ``IndexPrewarmError`` with a
    failed report and preserve the original exception as their cause.
    """
    if expected_version is not None:
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 0
        ):
            raise ValueError("expected_version must be a nonnegative integer")
        if dataset.version != expected_version:
            raise ValueError(
                f"Dataset version {dataset.version} does not match "
                f"expected_version {expected_version}"
            )
    if worker_id is not None and not isinstance(worker_id, str):
        raise ValueError("worker_id must be a string or None")
    plan = plan_index_prewarm(
        dataset,
        index_name,
        index_segments=index_segments,
        fragment_ids=fragment_ids,
        with_position=with_position,
    )
    assignment = plan.assignments[0] if plan.assignments else None
    native = getattr(dataset, "prewarm_index", None)
    if assignment is not None:
        if not callable(native):
            raise NotImplementedError("This PyLance must support prewarm_index()")
        try:
            parameters = inspect.signature(native).parameters
        except (TypeError, ValueError) as exc:
            raise NotImplementedError(
                "Cannot inspect prewarm_index(); use PyLance with explicit "
                "index_segments support"
            ) from exc
        for name in ["index_segments"] + (["with_position"] if with_position else []):
            parameter = parameters.get(name)
            if parameter is None or parameter.kind not in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                raise NotImplementedError(
                    f"This PyLance prewarm_index() must support keyword {name!r}"
                )
    before = _cache_bytes(dataset)
    start = time.perf_counter()
    failure = None
    if assignment is not None:
        assert callable(native)  # Validated before telemetry or native I/O.
        try:
            if with_position:
                native(
                    index_name,
                    index_segments=list(assignment.index_segments),
                    with_position=True,
                )
            else:
                native(index_name, index_segments=list(assignment.index_segments))
        except Exception as exc:
            failure = exc
    elapsed = time.perf_counter() - start
    after = _cache_bytes(dataset)
    report = IndexPrewarmReport(
        dataset_uri=plan.dataset_uri,
        dataset_version=plan.dataset_version,
        index_name=index_name,
        with_position=with_position,
        index_segments=assignment.index_segments if assignment else (),
        fragment_ids=assignment.fragment_ids if assignment else (),
        uncovered_fragment_ids=plan.uncovered_fragment_ids,
        worker_id=worker_id,
        process_id=os.getpid(),
        elapsed_seconds=elapsed,
        cache_bytes_before=before,
        cache_bytes_after=after,
        status="failed"
        if failure is not None
        else "success"
        if assignment
        else "skipped",
        error=f"{type(failure).__name__}: {failure}" if failure is not None else None,
    )
    if failure is not None:
        raise IndexPrewarmError(report) from failure
    return report
