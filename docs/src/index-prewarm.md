# Index Prewarm

`plan_index_prewarm` assigns physical index segments to logical workers without
loading index data. `prewarm_index` warms selected segments on an existing
`lance.LanceDataset` and returns a report. Both functions work with scalar,
full-text and vector index descriptions.

The caller owns the Dataset and its Session. Execute prewarm in the worker that
will query that same Dataset. These helpers do not create a Ray pool, move caches
between processes, or change `vector_search`.

## Plan segment assignments

```python
import lance
import lance_ray as lr

dataset = lance.dataset("/path/to/table.lance")
plan = lr.plan_index_prewarm(dataset, "text_fts", num_workers=4)

for assignment in plan.assignments:
    print(
        assignment.worker_id,
        assignment.index_segments,
        assignment.covered_fragment_ids,
        assignment.estimated_size_bytes,
    )
```

`num_workers` is a positive number of logical assignment slots, not a request to
launch workers. Each selected segment is assigned exactly once. Assignments are
deterministic for the same metadata and options. The planner balances segment
bytes when every selected segment has size metadata; otherwise it balances
segment counts. `plan.scheduling_basis` is `"bytes"` or `"segments"` respectively.
Estimated bytes describe index files, not the memory required by the index cache.

Choose one planning scope:

- Omit both `index_segments` and `fragment_ids` to plan every segment of the
  named index.
- Supply `index_segments` as an iterable of segment UUID strings or `uuid.UUID`
  objects. Obtain UUIDs from `dataset.describe_indices()` and its `segments`.
- Supply `fragment_ids` to select segments overlapping those dataset fragments.
  Segments are indivisible: the plan includes each selected segment's full
  fragment coverage, which can extend beyond the requested fragments.

Supplying both filters is an error. Duplicate requests are deduplicated, unknown
IDs are rejected, and an explicit empty iterable selects nothing. A valid
fragment with no coverage in this index appears in `plan.uncovered_fragment_ids`;
it produces no prewarm work. Planning does not scan fragment data or prewarm an
index. The returned plan and assignment dataclasses are immutable and can be
serialized as worker arguments.

## Execute on an existing worker Dataset

Pass an assignment to the corresponding worker in your application's scheduler.
The following helper runs inside that worker, using its already-open Dataset:

```python
def warm_assignment(worker_dataset, plan, assignment):
    return lr.prewarm_index(
        worker_dataset,
        plan.index_name,
        index_segments=assignment.index_segments,
        with_position=plan.with_position,
        expected_version=plan.dataset_version,
        worker_id=str(assignment.worker_id),
    )
```

`prewarm_index` accepts segment UUIDs only. `covered_fragment_ids` on assignments
and reports is diagnostic coverage. Feeding it back into the planner as
`fragment_ids` can select additional overlapping segments; always execute or
retry with `index_segments` to retain the exact scope.

The worker Dataset must refer to the same table and version used for planning.
`expected_version` rejects a version mismatch before prewarming. Worker labels
are reporting metadata; they do not enforce Ray placement or routing. Retain the
worker Dataset for subsequent queries so its Session and cache can be reused.
An already-open namespace-backed Dataset keeps its existing credentials and
storage configuration because the helper neither reopens nor clones it.

For a single worker, select fragments during planning and execute the resulting
segment UUIDs:

```python
plan = lr.plan_index_prewarm(
    dataset,
    "text_fts",
    fragment_ids=[0, 1],  # use actual fragment IDs from your Dataset
    with_position=True,
)
reports = [warm_assignment(dataset, plan, assignment) for assignment in plan.assignments]
```

`with_position=True` is supported only for an `INVERTED` index. It also warms
positions used by phrase queries. `False` leaves the native default in effect.

The installed PyLance must expose `prewarm_index` with an `index_segments`
parameter, and `with_position` when that option is requested. Unsupported or
uninspectable interfaces raise an actionable error. The helper never broadens a
segment selection to a full-index prewarm as a compatibility fallback. Explicit
empty selections return a `"skipped"` report without calling native prewarm.

## Reports and errors

`IndexPrewarmReport` records:

- Dataset URI and version, index name and position option.
- Selected segment UUIDs and diagnostic `covered_fragment_ids`. Uncovered
  requested fragments are reported by the plan.
- Caller-supplied worker label and the executing process ID.
- Elapsed seconds and status (`"success"`, `"skipped"`, or `"failed"`).
- Index-cache bytes before and after the call, when available, and an error string
  for failures.

The cache readings use `dataset.session().index_cache_size_bytes()`. A reading is
`None` if unavailable; telemetry failures do not prevent prewarm. Cache-size
changes are observations of the Session, not exact per-index allocations or a
guarantee of continued residency. Prewarm can evict other cached data, and loading
more than the cache can hold can waste I/O.

Invalid selections and unsupported capabilities raise before native prewarm.
Native execution failures raise `IndexPrewarmError`, retaining the report and
the native exception as the cause in the executing process. After pickle, the
cause is a `RuntimeError` containing the original exception's type name and
message from `report.error`. This keeps the failure reason available even when
the native exception itself cannot be pickled; native exception objects and
tracebacks are not transported:

```python
try:
    report = lr.prewarm_index(dataset, "text_fts")
except lr.IndexPrewarmError as exc:
    print(exc.report.status, exc.report.index_segments, exc.report.error)
    raise
```

This API does not retry, refresh a Dataset version, or schedule persistent actors.
Applications decide how to retain workers, route queries, handle failed reports,
and choose a cache budget.
