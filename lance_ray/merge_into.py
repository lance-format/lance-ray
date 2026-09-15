# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Distributed merge for Lance datasets on Ray.

This module provides a fragment-parallel, atomic merge: every source row that
matches a target row on the join key replaces that row (all columns), and
every source row with no match is inserted. The plan executes across Ray
workers so that neither the source rows nor the rewritten fragments ever have
to fit on the driver:

0. DEDUPE (distributed): the source is range-partitioned with a
   Ray Data sort on the join key -- all copies of a key land adjacent in
   exactly one block -- and one arbitrary row per key is kept by dropping
   adjacent duplicates per block. The sort also gives the plan phase
   contiguous key slices, so each chunk's index lookups stay within few
   BTREE leaf pages.
1. PLAN (distributed): each plan task takes one source chunk and maps every
   join key to its target fragment id using batched ``key IN (...)`` lookups
   against the target dataset (``_rowaddr >> 32`` = fragment id; served by the
   scalar index on the key column when one exists). Each plan task then
   hash-partitions its rows into per-apply-task buckets with a static
   ownership function -- ``owner(fragment_id) = crc32(fragment_id) % n_apply``
   -- and returns the buckets as separate Ray objects (a map-side shuffle). The
   driver only receives small metadata; bucket bytes move from plan node to
   apply node directly through the Ray object store.
2. APPLY (distributed): each apply task owns a disjoint set of target
   fragments (guaranteed by the ownership function). Updates are
   merge-on-read: for every owned fragment the task writes a *deletion file*
   marking the matched rows dead (addressed by the ``_rowid`` values from
   the plan phase, so no fragment data is rescanned), and the replacement
   values, together with the rows that had
   no match, are appended as brand-new fragments.
3. COMMIT (driver): the per-task results are unioned into a single
   ``lance.LanceOperation.Update`` and committed once, so the whole merge is
   one atomic version change (all-or-nothing). Conflict detection is
   fragment-level: concurrent appends are rebased over with bounded retries,
   while a concurrent commit that touched any fragment this merge modifies
   fails with an explicit error.

Example:
    >>> import lance_ray as lr
    >>> dataset = lr.merge_into(source, "/path/to/table.lance", on="id", num_workers=4)
    >>> dataset.version
    5
"""

import collections
import logging
import pickle
import time
import zlib
from typing import Any, Optional

import lance
import pyarrow as pa
import pyarrow.compute as pc
import ray
import ray.data

from .utils import (
    get_namespace_kwargs,
    get_write_fragments_kwargs,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

__all__ = [
    "merge_into",
]

logger = logging.getLogger(__name__)

_COMMIT_MAX_RETRIES = 3
_COMMIT_RETRY_DELAY_S = 1.0
# Number of join keys per ``IN (...)`` index lookup in the plan phase.
_LOOKUP_BATCH_SIZE = 10_000

# Helper column shipped inside the plan buckets: the ``_rowid`` of the
# matched target row, so the apply phase can delete rows directly without
# rescanning the fragment for the keys. ``_rowid`` (not ``_rowaddr``) is the
# id ``LanceFragment.delete`` resolves on BOTH dataset flavors: it equals the
# physical row address on regular datasets and the stable id on
# stable-row-id datasets (where ``_rowaddr`` predicates do not match).
_ROWID_COLUMN = "__merge_into_rowid"


# ---------------------------------------------------------------------------
# helpers shared by driver and workers
# ---------------------------------------------------------------------------


def _sql_literal(value: Any) -> str:
    """Render a join-key value as a SQL literal for an ``IN (...)`` filter."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    raise TypeError(
        f"Unsupported join key type {type(value).__name__!r}; "
        "merge_into currently supports string, integer, and float keys."
    )


def _chunked(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _align_chunk(chunk: pa.Table, target_schema: pa.Schema, on: str) -> pa.Table:
    """Project/cast a source chunk to the target schema and validate the key."""
    missing = [name for name in target_schema.names if name not in chunk.column_names]
    if missing:
        raise ValueError(f"Source is missing target-table columns: {missing}")
    chunk = chunk.select(target_schema.names)
    if chunk.schema != target_schema:
        chunk = chunk.cast(target_schema)
    key_column = chunk.column(on)
    if key_column.null_count:
        raise ValueError(f"Source contains null values in join key column {on!r}")
    return chunk


def _raise_on_duplicate_keys(keys: list, on: str, context: str) -> None:
    if len(keys) != len(set(keys)):
        raise ValueError(
            f"Duplicate join keys detected ({context}). Each source row must "
            f"match at most one target row; the dedupe pass keeps one row "
            f"per {on!r} key, so this indicates an internal routing error."
        )


def _key_bucket(key: Any, n: int) -> int:
    """Deterministic key -> bucket assignment for the plan shuffle
    (apply-task ownership, bucket by fragment id). One property matters:

    - **Process-stable.** Python's built-in ``hash()`` is salted per process,
      so two tasks running in different Ray workers would disagree on the
      bucket of the same key; crc32 over ``repr`` is stable everywhere.
    """
    return zlib.crc32(repr(key).encode("utf-8")) % n


def _drop_adjacent_duplicate_keys(batch: pa.Table, on: str) -> pa.Table:
    """Keep one row per key within a sorted block (keep-any).

    After ``Dataset.sort(on)`` all copies of a key are adjacent *and* live in
    the same range partition (Ray's sort assigns each key to exactly one
    half-open boundary range), so dropping adjacent duplicates per block is a
    complete, global dedupe.
    """
    keys = batch.column(on)
    if keys.null_count:
        raise ValueError(f"Source contains null values in join key column {on!r}")
    n = batch.num_rows
    if n <= 1:
        return batch
    changed = pc.not_equal(keys.slice(1), keys.slice(0, n - 1))
    chunks = changed.chunks if isinstance(changed, pa.ChunkedArray) else [changed]
    mask = pa.chunked_array([pa.array([True]), *chunks])
    return batch.filter(mask)


# ---------------------------------------------------------------------------
# Ray tasks (module-level so they are picklable)
# ---------------------------------------------------------------------------


@ray.remote
def _plan_task(
    task_id: int,
    uri: str,
    read_version: int,
    on: str,
    storage_options: Optional[dict[str, Any]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    source_chunk: pa.Table,
    target_schema: pa.Schema,
    n_apply: int,
):
    """PLAN + map-side shuffle: map keys to target fragments, bucket by owner.

    Declared with ``num_returns=n_apply + 1`` at the call site: the first
    ``n_apply`` returns are the bucket payloads
    ``{"frags": {fragment_id: rows}, "inserts": rows | None}`` and the last is
    a small metadata dict. The driver only fetches the metadata; bucket bytes
    stay in the object store on this node until the owning apply task pulls
    them. Ownership is a pure function of the fragment id
    (``crc32(fragment_id) % n_apply``), so every plan task routes a given
    fragment to the same apply task without coordination -- that is what makes
    the apply phase write-disjoint by construction. Hashing (rather than a raw
    modulo) keeps the buckets balanced.
    """
    t0 = time.perf_counter()
    # The dedupe sort can leave some range partitions empty (fewer rows than
    # partitions); such blocks materialize as zero-column tables, so bail
    # out before schema alignment.
    if source_chunk.num_rows == 0:
        meta = {
            "label": f"plan-{task_id}",
            "rows": 0,
            "matched": 0,
            "touched_fragments": [],
            "bucket_rows": [0] * n_apply,
            "elapsed_s": time.perf_counter() - t0,
        }
        return (*({"frags": {}, "inserts": None} for _ in range(n_apply)), meta)

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    source_chunk = _align_chunk(source_chunk, target_schema, on)
    keys = source_chunk.column(on).to_pylist()
    _raise_on_duplicate_keys(keys, on, f"plan task {task_id}")

    dataset = lance.LanceDataset(
        uri,
        version=read_version,
        storage_options=storage_options or None,
        **namespace_kwargs,
    )
    # Batched index lookups: only the key column plus _rowaddr and _rowid
    # are materialized. _rowaddr >> 32 is the physical fragment id (the
    # shuffle key); _rowid is what the apply phase deletes by, so matched
    # rows never have to be re-found by rescanning the fragment.
    rowinfo_of: dict[Any, tuple[int, int]] = {}  # key -> (fragment id, rowid)
    for batch in _chunked(keys, _LOOKUP_BATCH_SIZE):
        in_list = ", ".join(_sql_literal(key) for key in batch)
        # Backticks are Lance's identifier quoting (double quotes would be
        # parsed as a string literal by the filter planner).
        hits = dataset.to_table(
            columns=[on],
            filter=f"`{on}` IN ({in_list})",
            with_row_address=True,
            with_row_id=True,
        )
        for key, rowaddr, rowid in zip(
            hits.column(on).to_pylist(),
            hits.column("_rowaddr").to_pylist(),
            hits.column("_rowid").to_pylist(),
            strict=False,
        ):
            rowinfo_of[key] = (rowaddr >> 32, rowid)

    fragment_ids = [rowinfo_of.get(key, (-1, -1))[0] for key in keys]
    row_ids = [rowinfo_of.get(key, (-1, -1))[1] for key in keys]
    num_matched = sum(1 for f in fragment_ids if f != -1)

    # Ship each row's target rowid with it so every bucket slice below
    # carries the ids of the target rows it replaces (-1 on insert rows).
    source_chunk = source_chunk.append_column(
        _ROWID_COLUMN, pa.array(row_ids, type=pa.int64())
    )

    # Map-side shuffle: route each row to its owner's bucket. Matched rows go
    # to owner(fragment_id); inserts are spread round-robin.
    updates_by_owner: list[dict[int, list[int]]] = [
        collections.defaultdict(list) for _ in range(n_apply)
    ]
    inserts_by_owner: list[list[int]] = [[] for _ in range(n_apply)]
    for i, fragment_id in enumerate(fragment_ids):
        if fragment_id == -1:
            inserts_by_owner[i % n_apply].append(i)
        else:
            updates_by_owner[_key_bucket(fragment_id, n_apply)][fragment_id].append(i)

    buckets = []
    bucket_rows: list[int] = []
    for owner in range(n_apply):
        frags = {
            fragment_id: source_chunk.take(indices)
            for fragment_id, indices in updates_by_owner[owner].items()
        }
        inserts = (
            source_chunk.take(inserts_by_owner[owner])
            if inserts_by_owner[owner]
            else None
        )
        buckets.append({"frags": frags, "inserts": inserts})
        bucket_rows.append(
            sum(t.num_rows for t in frags.values())
            + (inserts.num_rows if inserts is not None else 0)
        )

    meta = {
        "label": f"plan-{task_id}",
        "rows": len(keys),
        "matched": num_matched,
        "touched_fragments": sorted({f for f in fragment_ids if f != -1}),
        "bucket_rows": bucket_rows,
        "elapsed_s": time.perf_counter() - t0,
    }
    return (*buckets, meta)


@ray.remote
def _apply_task(
    task_id: int,
    uri: str,
    read_version: int,
    on: str,
    storage_options: Optional[dict[str, Any]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    bucket_refs: list,
) -> dict[str, Any]:
    """APPLY: merge-on-read updates for a disjoint set of target fragments.

    ``bucket_refs`` are this task's bucket ObjectRefs from every plan task.
    They are nested inside a list on purpose so Ray does not resolve them on
    the driver -- this task fetches them here, i.e. the bytes move from the
    plan node to this node directly. A fragment's matched rows can arrive from
    several plan chunks, so per-fragment sub-tables are concatenated first.

    For each owned fragment the task writes
    a new *deletion file* marking the matched rows dead
    (``LanceFragment.delete`` by ``_rowid``, using the ids gathered by the
    plan phase's index lookups, so no fragment data is rescanned and
    the data files are untouched); the
    replacement rows and the inserts are appended together as brand-new
    fragments. A fragment left empty by the deletion is removed instead.

    Returns the parts of the final ``LanceOperation.Update``: removed
    fragment ids (fully-emptied fragments), pickled metadata of the fragments
    that received a new deletion file, and pickled metadata of the new
    fragments it wrote.
    """
    t0 = time.perf_counter()
    payloads = ray.get(bucket_refs)
    tables_by_fragment: dict[int, list[pa.Table]] = collections.defaultdict(list)
    insert_parts: list[pa.Table] = []
    for payload in payloads:
        for fragment_id, table in payload["frags"].items():
            tables_by_fragment[fragment_id].append(table)
        if payload["inserts"] is not None and payload["inserts"].num_rows:
            # Insert rows carry no useful rowid (-1); strip the helper
            # column so the appended rows match the target schema.
            insert_parts.append(payload["inserts"].drop_columns([_ROWID_COLUMN]))

    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    write_kwargs = get_write_fragments_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    dataset = lance.LanceDataset(
        uri,
        version=read_version,
        storage_options=storage_options or None,
        **namespace_kwargs,
    )
    fragment_by_id = {f.fragment_id: f for f in dataset.get_fragments()}

    removed_fragment_ids: list[int] = []
    updated_fragments: list[bytes] = []
    replacement_parts: list[pa.Table] = []
    updated_rows = 0
    for fragment_id in tables_by_fragment:
        source_rows = pa.concat_tables(tables_by_fragment[fragment_id])
        keys = source_rows.column(on).to_pylist()
        # Duplicates that landed on the same target fragment (possibly from
        # different plan chunks) are caught here.
        _raise_on_duplicate_keys(keys, on, f"target fragment {fragment_id}")
        # Mark the matched rows dead with a deletion file, addressed by the
        # rowids gathered in the plan phase. A key predicate would force the
        # delete to rescan and decode the fragment's key column thus avoided.
        row_ids = source_rows.column(_ROWID_COLUMN).to_pylist()
        source_rows = source_rows.drop_columns([_ROWID_COLUMN])
        in_list = ", ".join(str(rowid) for rowid in row_ids)
        new_meta = fragment_by_id[fragment_id].delete(f"_rowid IN ({in_list})")
        if new_meta is None:
            removed_fragment_ids.append(fragment_id)
        else:
            updated_fragments.append(pickle.dumps(new_meta))
        replacement_parts.append(source_rows)
        updated_rows += source_rows.num_rows

    # Replacement rows and inserts are all full rows in target-schema order
    # (aligned in the plan phase), so they are appended together in one write.
    inserted_rows = sum(t.num_rows for t in insert_parts)
    new_fragments: list[bytes] = []
    append_parts = replacement_parts + insert_parts
    if append_parts:
        append_table = pa.concat_tables(append_parts)
        fragments = lance.fragment.write_fragments(
            append_table,
            uri,
            mode="append",
            storage_options=storage_options or None,
            **write_kwargs,
        )
        new_fragments.extend(pickle.dumps(f) for f in fragments)

    return {
        "label": f"apply-{task_id}",
        "removed_fragment_ids": removed_fragment_ids,
        "updated_fragments": updated_fragments,
        "new_fragments": new_fragments,
        "updated_rows": updated_rows,
        "inserted_rows": inserted_rows,
        "rows": updated_rows + inserted_rows,
        "elapsed_s": time.perf_counter() - t0,
    }


# ---------------------------------------------------------------------------
# driver-side scheduling helpers
# ---------------------------------------------------------------------------


def _bounded_map(remote_fn, arg_tuples: list[tuple], max_in_flight: int) -> list[dict]:
    """Run one Ray task per arg tuple with at most ``max_in_flight`` in flight."""
    total = len(arg_tuples)
    results: list[dict] = []
    pending: list = []
    i = 0
    while i < total and len(pending) < max_in_flight:
        pending.append(remote_fn.remote(*arg_tuples[i]))
        i += 1
    while pending:
        done, pending = ray.wait(pending, num_returns=1)
        result = ray.get(done[0])
        results.append(result)
        if i < total:
            pending.append(remote_fn.remote(*arg_tuples[i]))
            i += 1
    return results


def _bounded_map_shuffle(
    remote_fn, arg_tuples: list[tuple], max_in_flight: int
) -> list[dict]:
    """``_bounded_map`` for tasks declared with ``num_returns > 1``.

    Only the last return value (a small metadata dict) is fetched on the
    driver; the data returns stay in the object store on the producing node
    and are attached to the metadata as ``meta["bucket_refs"]``. Because the
    returns are task outputs (not worker-side ``ray.put``), the driver owns
    them and they survive Ray recycling idle worker processes between phases.
    """
    total = len(arg_tuples)
    results: list[dict] = []
    pending: dict = {}  # meta ObjectRef -> bucket ObjectRefs
    i = 0

    def _submit(j: int) -> None:
        refs = remote_fn.remote(*arg_tuples[j])
        pending[refs[-1]] = refs[:-1]  # meta is the last return value

    while i < total and len(pending) < max_in_flight:
        _submit(i)
        i += 1
    while pending:
        ready_meta_refs, _ = ray.wait(list(pending), num_returns=1)
        bucket_refs = pending.pop(ready_meta_refs[0])
        meta = ray.get(ready_meta_refs[0])
        meta["bucket_refs"] = bucket_refs
        results.append(meta)
        if i < total:
            _submit(i)
            i += 1
    return results


def _commit_update_with_retry(
    uri: str,
    operation: "lance.LanceOperation.Update",
    read_version: int,
    storage_options: dict[str, Any],
    namespace_kwargs: dict[str, Any],
    touched_fragment_ids: set[int],
) -> "lance.LanceDataset":
    """Commit an Update operation, retrying on concurrent-commit conflicts.

    A retry is only safe while every fragment this operation touches --
    removed (fully emptied) or updated with a new deletion file -- still
    exists in the latest version. If any of them disappeared (e.g. a
    concurrent compaction or another update rewrote them), rebasing would
    silently drop the concurrent change, so we fail instead.
    """
    last_exc = None
    for attempt in range(_COMMIT_MAX_RETRIES):
        try:
            return lance.LanceDataset.commit(
                uri,
                operation,
                read_version=read_version,
                storage_options=storage_options or None,
                **namespace_kwargs,
            )
        except Exception as exc:
            last_exc = exc
            if attempt < _COMMIT_MAX_RETRIES - 1:
                time.sleep(_COMMIT_RETRY_DELAY_S * (2**attempt))
                try:
                    current = lance.LanceDataset(
                        uri,
                        storage_options=storage_options or None,
                        **namespace_kwargs,
                    )
                    current_ids = {f.fragment_id for f in current.get_fragments()}
                    if not touched_fragment_ids.issubset(current_ids):
                        raise ValueError(
                            "Concurrent write detected: fragments "
                            f"{sorted(touched_fragment_ids - current_ids)} were "
                            "rewritten or removed by another commit. Cannot "
                            "safely retry merge_into; re-run it against the "
                            "latest version."
                        ) from exc
                    read_version = current.version
                except ValueError:
                    raise
                except Exception:  # noqa: BLE001 - probe failure, retry blind
                    pass
    raise last_exc


def _has_scalar_index_on(dataset: "lance.LanceDataset", column: str) -> bool:
    try:
        if hasattr(dataset, "describe_indices"):
            for index in dataset.describe_indices():
                names = getattr(index, "field_names", None) or []
                # field_names may render identifiers quoted (e.g. '"id"').
                if any(name.strip('"') == column for name in names):
                    return True
            return False
        for index in dataset.list_indices():
            fields = (
                index.get("fields")
                if isinstance(index, dict)
                else getattr(index, "fields", None)
            )
            if fields and column in fields:
                return True
    except Exception:  # noqa: BLE001 - best effort, only used for a warning
        return True
    return False


def _source_to_chunk_refs(
    source: ray.data.Dataset | pa.Table, on: str, num_partitions: int
) -> list["ray.ObjectRef"]:
    """Sort-dedupe the source on the join key; return Arrow-table ObjectRefs.

    The source is range-partitioned with a Ray Data sort on the key: all
    copies of a key land adjacent in exactly one block, so dropping adjacent
    duplicates per block is a complete global dedupe (one arbitrary row per
    key survives). The sort also hands the plan phase contiguous key slices,
    which keeps each chunk's index lookups within few BTREE leaf pages.
    """
    if isinstance(source, pa.Table):
        if source.num_rows == 0:
            return []
        source = ray.data.from_arrow(source)
    elif not isinstance(source, ray.data.Dataset):
        raise TypeError(
            "source must be a ray.data.Dataset or a pyarrow.Table, got "
            f"{type(source).__name__}"
        )
    deduped = (
        source.repartition(num_partitions)
        .sort(on)
        .map_batches(
            _drop_adjacent_duplicate_keys,
            fn_kwargs={"on": on},
            batch_size=None,
            batch_format="pyarrow",
        )
        .materialize()
    )
    return list(deduped.to_arrow_refs())  # pyright: ignore[reportReturnType]


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def merge_into(
    ds: ray.data.Dataset | pa.Table,
    uri: Optional[str] = None,
    *,
    on: str,
    table_id: Optional[list[str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    num_workers: int = 4,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> "lance.LanceDataset":
    """Distributed merge of ``ds`` into a Lance dataset.

    Every source row that matches a target row on the ``on`` key replaces
    that row entirely (all columns), and every source row with no match is
    inserted. All changes -- across every touched fragment -- are committed
    as one atomic version; on any failure before the commit, the visible
    table is untouched.

    This is the distributed counterpart of pylance's
    ``LanceDataset.merge_insert(on).when_matched_update_all()
    .when_not_matched_insert_all()``: the source rows are matched to their
    target fragments with distributed index lookups, and Ray workers apply
    the updates (each fragment owned by exactly one
    worker): matched rows are masked out with per-fragment deletion files,
    and replacement plus insert rows are
    appended as new fragments. All changes are committed as a single atomic
    version. Scans filter through the deletion vectors until the next
    compaction folds them away.

    Concurrency: conflict detection is fragment-level. Concurrent appends
    that land during the merge_into are tolerated (the commit rebases with
    bounded retries); a concurrent commit that rewrote or removed any
    fragment this merge_into touches fails with an explicit error -- re-run
    against the latest version. Two concurrent merge_into calls inserting
    the same *new* key are physically disjoint and would both succeed,
    duplicating the key; serialize merge_into against the same table to
    avoid this.

    Args:
        ds: The rows to merge into the target table, as a
            ``ray.data.Dataset`` or an in-memory ``pyarrow.Table``. The
            source must contain every column of the target schema (columns
            are reordered/cast as needed) and must not contain null join
            keys. Duplicate join keys are deduplicated, keeping one
            arbitrary occurrence per key (which copy survives is
            unspecified).
        uri: The URI of the target Lance dataset. Either ``uri`` OR
            (``namespace_impl`` + ``table_id``) must be provided.
        on: The join key column name. A scalar index on this column is
            strongly recommended for large targets (the plan phase falls
            back to filtered scans without one).
        table_id: The table identifier as a list of strings. Must be provided
            together with ``namespace_impl``.
        namespace_impl: The namespace implementation type (e.g. ``"rest"``,
            ``"dir"``), used for resolving the dataset location and credential
            vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
        storage_options: Storage options for the dataset.
        num_workers: Maximum number of Ray tasks running concurrently in each
            phase (default: 4). Lower it to reduce peak memory and IO
            pressure without changing how the data is partitioned.
        num_partitions: How the work is partitioned: the number of source
            chunks in the plan phase and of fragment buckets in the apply
            phase (default: ``num_workers``). Unlike ``num_workers``, this is
            baked into the shuffle layout, so raise it to get smaller,
            more granular tasks (e.g. ``num_partitions=32`` with
            ``num_workers=8``).
        ray_remote_args: Options for the Ray tasks (e.g. ``num_cpus``,
            ``resources``).

    Returns:
        The updated :class:`lance.LanceDataset` at the committed version.
        When the source produced no updates and no inserts, returns the
        dataset pinned at ``read_version`` (no empty commit).

    Example:
        >>> import lance_ray as lr
        >>> dataset = lr.merge_into(
        ...     daily_batch,                  # ray.data.Dataset or pyarrow.Table
        ...     "s3://bucket/users.lance",
        ...     on="user_id",
        ...     num_workers=8,
        ... )
        >>> dataset.version
        5
    """
    if not on:
        raise ValueError("merge_into requires a join key column name ('on')")
    if num_workers < 1:
        raise ValueError("num_workers must be >= 1")
    if num_partitions is not None and num_partitions < 1:
        raise ValueError("num_partitions must be >= 1")
    num_partitions = num_partitions or num_workers
    ray_remote_args = ray_remote_args or {}

    validate_uri_or_namespace(uri, namespace_impl, table_id)
    uri, storage_options = resolve_namespace_table(
        uri,
        storage_options,
        namespace_impl,
        namespace_properties,
        table_id,
    )
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    dataset = lance.LanceDataset(
        uri,
        storage_options=storage_options or None,
        **namespace_kwargs,
    )
    read_version = dataset.version
    target_schema = dataset.schema
    field_ids = [field.id() for field in dataset.lance_schema.fields()]
    if on not in target_schema.names:
        raise ValueError(
            f"Join key column {on!r} not found in target schema {target_schema.names}"
        )
    if not _has_scalar_index_on(dataset, on):
        logger.warning(
            "No scalar index found on join key column %r; the merge_into plan "
            "phase will fall back to filtered scans of the target table. "
            "Create a scalar index on the key column for large targets.",
            on,
        )

    # Phase 0: sort-based dedupe. The deduplicated, range-partitioned blocks
    # become the plan chunks; every downstream duplicate check then passes
    # by construction.
    chunk_refs = _source_to_chunk_refs(ds, on, num_partitions)

    # Phase 1: PLAN + map-side shuffle. Chunk refs are passed as top-level
    # args (resolved on the worker); each plan task returns num_partitions
    # bucket objects plus a small metadata dict.
    plan_remote = _plan_task.options(num_returns=num_partitions + 1, **ray_remote_args)
    plan_args = [
        (
            i,
            uri,
            read_version,
            on,
            storage_options,
            namespace_impl,
            namespace_properties,
            table_id,
            chunk_ref,
            target_schema,
            num_partitions,
        )
        for i, chunk_ref in enumerate(chunk_refs)
    ]
    plan_results = (
        _bounded_map_shuffle(plan_remote, plan_args, num_workers) if plan_args else []
    )
    touched_fragment_ids = {
        f for meta in plan_results for f in meta["touched_fragments"]
    }
    logger.info(
        "merge_into plan done: %d source rows, %d matched, %d fragment(s) touched",
        sum(meta["rows"] for meta in plan_results),
        sum(meta["matched"] for meta in plan_results),
        len(touched_fragment_ids),
    )

    # Phase 2: route each bucket's ObjectRef to its owning apply task. The
    # refs are nested in a list on purpose so Ray does not resolve them on
    # the driver; the apply task fetches them node-to-node itself.
    apply_args = []
    for owner in range(num_partitions):
        bucket_refs = [
            meta["bucket_refs"][owner]
            for meta in plan_results
            if meta["bucket_rows"][owner]
        ]
        if bucket_refs:
            apply_args.append(
                (
                    owner,
                    uri,
                    read_version,
                    on,
                    storage_options,
                    namespace_impl,
                    namespace_properties,
                    table_id,
                    bucket_refs,
                )
            )
    apply_remote = _apply_task.options(**ray_remote_args)
    apply_results = (
        _bounded_map(apply_remote, apply_args, num_workers) if apply_args else []
    )

    # Phase 3: union the parts into ONE atomic LanceOperation.Update.
    num_inserted_rows = sum(r["inserted_rows"] for r in apply_results)
    num_updated_rows = sum(r["updated_rows"] for r in apply_results)
    if not apply_results:
        logger.info("merge_into: nothing to update or insert; no commit")
        return lance.LanceDataset(
            uri,
            version=read_version,
            storage_options=storage_options or None,
            **namespace_kwargs,
        )

    removed = [f for r in apply_results for f in r["removed_fragment_ids"]]
    updated = [pickle.loads(f) for r in apply_results for f in r["updated_fragments"]]
    new_fragments = [pickle.loads(f) for r in apply_results for f in r["new_fragments"]]
    touched_ids = removed + [f.id for f in updated]
    if len(touched_ids) != len(set(touched_ids)):
        raise RuntimeError(
            "Internal error: fragment overlap across apply tasks -- the "
            "assignment is not fragment-disjoint"
        )
    operation = lance.LanceOperation.Update(
        removed_fragment_ids=removed,
        updated_fragments=updated,
        new_fragments=new_fragments,
        fields_modified=[],
        fields_for_preserving_frag_bitmap=field_ids,
        update_mode="rewrite_rows",
    )

    committed = _commit_update_with_retry(
        uri,
        operation,
        read_version,
        storage_options,
        namespace_kwargs,
        set(touched_ids),
    )
    logger.info(
        "merge_into committed version %d: %d row(s) updated, %d row(s) "
        "inserted, %d fragment(s) deletion-vector-updated, %d removed",
        committed.version,
        num_updated_rows,
        num_inserted_rows,
        len(updated),
        len(removed),
    )
    return committed
