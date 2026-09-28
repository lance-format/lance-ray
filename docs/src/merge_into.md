# Distributed Merge Into

`merge_into` merges a source dataset into a target Lance table by join key: every target row whose key exists in the source is **updated** (all columns replaced; if the key is duplicated in the target, every matching row is updated), and source rows with a new key are **inserted**. It is the distributed counterpart of pylance's `LanceDataset.merge_insert`, designed for sources and targets that are too large to process on a single machine.

The whole operation commits as a **single atomic version** — readers see either the old table or the fully merged table, never an intermediate state.

## How it works

1. **Plan (distributed):** the source is split into `num_partitions` chunks; each Ray task maps its keys to their target fragments using batched index lookups on the join key column, then routes rows to `num_workers` per-owner buckets keyed by target fragment. Only non-empty buckets are materialized. The driver only handles object references and small metadata — source rows never pass through it.
2. **Apply (distributed):** each Ray task owns a disjoint set of target fragments. Updates are merge-on-read: the task masks the matched rows of every owned fragment with a deletion vector written from local physical offsets (`LanceFragment.delete_rows`; fragment data files are never rewritten), and appends the replacement values together with unmatched rows as new fragments. On datasets with stable row IDs, replacement fragments keep the matched rows' logical `_rowid` values; inserts receive newly assigned IDs. Scans filter through the deletion vectors until the next compaction folds them away.
3. **Commit (driver):** all per-task results are unioned into one `lance.LanceOperation.Update` and committed once. Concurrent appends are rebased inside `LanceDataset.commit`. If that call raises after the write is already in the latest manifest, `merge_into` still returns that dataset.

## `merge_into`

```python
merge_into(
    ds,
    uri=None,
    *,
    on,
    table_id=None,
    namespace_impl=None,
    namespace_properties=None,
    storage_options=None,
    num_workers=4,
    num_partitions=None,
    ray_remote_args=None,
)
```

Returns the updated `lance.LanceDataset` at the committed version. When the source produced no updates and no inserts, returns the dataset pinned at the read version (no empty commit).

**Parameters:**

- `ds`: The source rows, as a `ray.data.Dataset` or a `pyarrow.Table`. The source must contain every column of the target schema (columns are reordered/cast as needed) and must not contain null join keys. Duplicate join keys are deduplicated, keeping one arbitrary occurrence per key (which copy survives is unspecified).
- `uri`: Target dataset URI (either `uri` OR `namespace_impl` + `table_id` required)
- `on`: Join key column name (required, keyword-only). Supported scalar types: boolean, integer, floating, string, date, timestamp, time, decimal, and binary (including dictionary-encoded scalars). Nested types such as list or struct are rejected on the driver before any Ray task starts. A scalar index on this column is strongly recommended for large targets (the plan phase falls back to filtered scans without one). Every matching target row is updated (join-all, same as pylance `merge_insert`).
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `storage_options`: Optional storage configuration dictionary
- `num_workers`: Concurrent Ray tasks per phase **and** the number of apply-side fragment owners (default: 4). Ownership is `crc32(fragment_id) % num_workers`. Lower it to reduce peak memory, IO, and shuffle fan-out.
- `num_partitions`: Number of source chunks for the plan phase only (default: `num_workers`). Raise this to shrink each plan task without creating more apply workers or a quadratic number of Ray objects.
- `ray_remote_args`: Optional kwargs for Ray remote tasks (e.g., `num_cpus`)

## Best practices

- Size **`num_workers`** to the cluster slots you want busy during plan and apply (typically 8–64). This is also apply parallelism: each worker owns a disjoint subset of target fragments.
- Raise **`num_partitions` above `num_workers`** when plan tasks are memory-heavy (large source chunks or expensive index probes). Example: `num_workers=8`, `num_partitions=32` runs 32 smaller plan tasks with at most 8 in flight, and still only 8 apply owners. Plan tasks yield only non-empty owner buckets, so empty plan→apply edges are not stored as Ray objects.
- Do **not** set `num_partitions` in the hundreds or thousands expecting more apply workers. Apply fan-out follows `num_workers`. A large `num_partitions` only increases how many plan tasks run.
- Create a scalar index (e.g. BTREE) on the join key before merging into large tables so planning is index lookups instead of filtered scans.
- Join on a scalar column. Dates, timestamps, times, decimals, and binary keys are encoded as Lance SQL literals in the plan phase. Timestamp and time keys retain their Arrow precision, including nanoseconds; timestamp keys also retain their timezone. List, struct, and other nested types fail immediately from the target schema — they do not wait for a remote plan task.
- Plan/apply may attach temporary helper columns (default `__merge_into_rowid` and `__merge_into_offset`). If those names already exist on the target, the next free `_2` / `_3` / ... suffix is chosen so user columns are never overwritten.

## Examples

### Merge a Ray dataset into a table

```python
import lance_ray as lr
import ray

source = ray.data.read_parquet("s3://bucket/daily_updates/")

dataset = lr.merge_into(
    source,
    "/path/to/table.lance",
    on="id",
    num_workers=8,
    num_partitions=32,  # smaller plan tasks; still 8 apply owners
)
print(dataset.version)
```

### Merge via namespace

```python
dataset = lr.merge_into(
    source,
    on="id",
    namespace_impl="dir",
    namespace_properties={"root": "/path/to/tables"},
    table_id=["my_table"],
)
```

## Notes and limitations

- Each source row must match at most one target row. Duplicate source keys are always resolved automatically by a sort-based dedupe pass (a Ray Data sort of the source by key plus a vectorized adjacent-duplicate drop), keeping one arbitrary occurrence per key — which copy survives is unspecified.
- Concurrent writes: appends that land during the merge_into are rebased inside `LanceDataset.commit`. If a concurrent commit rewrites, removes, or updates-in-place (new deletion file / fragment metadata) one of the fragments this merge_into touches (e.g. compaction or another merge-on-read update), the operation fails rather than silently dropping the concurrent change. If `commit` raises after this merge's fragments are already visible (lost success ack), the call returns the latest dataset instead of failing, so a job-level retry cannot double-insert.
- Concurrent inserts of the same key: Lance's conflict detection is fragment-level, so two concurrent `merge_into` calls inserting the same *new* join key are physically disjoint — both commits succeed and the key is duplicated. Serialize `merge_into` against the same table externally (e.g. one scheduled writer). Key-level conflict detection is discussed as future work in the [design doc](merge-insert-design.md#112-key-level-conflict-detection-isolation-parity-with-native-merge_insert).
- Create a scalar index (e.g. BTREE) on the join key column before calling `merge_into` on large tables — key-to-fragment planning is served by the index instead of scanning the table. See [Best practices](#best-practices).
- Join key types: boolean, integer, floating, string, date, timestamp, time, decimal, and binary. Nested types are rejected on the driver.
- Internal helper column names are allocated from the target schema so they cannot collide with user fields.
