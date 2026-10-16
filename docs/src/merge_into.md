# Distributed Merge Into

`merge_into` merges a source dataset into a target Lance table by join key: every target row whose key exists in the source is **updated** (all columns replaced; if the key is duplicated in the target, every matching row is updated), and source rows with a new key are **inserted**. It is the distributed counterpart of pylance's `LanceDataset.merge_insert`, designed for sources and targets that are too large to process on a single machine.

The whole operation commits as a **single atomic version** — readers see either the old table or the fully merged table, never an intermediate state.

## How it works

1. **Plan (distributed):** the source is split into `num_partitions` chunks; each Ray task maps its keys to their target fragments using batched index lookups on the join key column, then routes rows to `num_workers` per-owner buckets keyed by target fragment. Only non-empty buckets are materialized. The driver only handles object references and small metadata — source rows never pass through it.
2. **Apply (distributed):** each Ray task owns a disjoint set of target fragments and streams its plan buckets one at a time. Updates are merge-on-read: the task masks the matched rows of every owned fragment with a deletion vector written from local physical offsets (`LanceFragment.delete_rows`; fragment data files are never rewritten), and appends the replacement values together with unmatched rows as new fragments. On datasets with stable row IDs, replacement fragments keep the matched rows' logical `_rowid` values; inserts receive newly assigned IDs. Scans filter through the deletion vectors until the next compaction folds them away. Full-row rewrites mark every field, including nested leaves, so an existing index on a nested column is not reused for the rewritten fragment.
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

- `ds`: The source rows, as a `ray.data.Dataset` or a `pyarrow.Table`. The source must contain every column of the target schema (columns are reordered/cast as needed) and must not contain null join keys. Duplicate join keys are deduplicated after that cast, keeping one arbitrary occurrence per target-typed key (which copy survives is unspecified).
- `uri`: Target dataset URI (either `uri` OR `namespace_impl` + `table_id` required)
- `on`: Join key column name (required, keyword-only). Supported scalar types: boolean, integer, floating, string, date, timestamp, time, decimal, and binary (including dictionary-encoded scalars). Floating keys must be finite; NaN and infinity are rejected before dedupe. The Int64 minimum and every UInt64 value at or above 2**63 are looked up with a typed literal, because a bare SQL number that does not fit in a signed 64-bit token is parsed as Float64 and cannot be cast back. Dates outside year 1–9999 are sorted and looked up as integer day or millisecond counts. Decimals with a negative scale are sorted on their unscaled coefficient. The lookup multiplies that coefficient by a precision-1 power of ten at the same negative scale, so a full-precision coefficient is not widened before the final cast. When that power has more than 76 digits, as with `decimal256(76, -76)`, it is a product of smaller powers that each fit in Decimal256. A column name that contains a backtick is rejected: Lance 12 drops that field from its query schema, so the plan filter cannot read it. Nested types such as list or struct are rejected on the driver before any Ray task starts. A scalar index on this column is strongly recommended for large targets (the plan phase falls back to filtered scans without one). Every matching target row is updated (join-all, same as pylance `merge_insert`).
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `storage_options`: Optional storage configuration dictionary
- `num_workers`: Concurrent Ray tasks per phase **and** the number of apply-side fragment owners (default: 4). Ownership is `crc32(fragment_id) % num_workers`. Lower it to reduce concurrent IO and shuffle fan-out.
- `num_partitions`: Number of source chunks for the plan phase only (default: `num_workers`). Raise this to shrink each plan chunk without creating more apply workers. Apply tasks stream those chunks one at a time, so the same setting bounds the deserialized source an apply worker holds to the largest chunk. It does not shrink cluster object-store use. After dedupe, `materialize()` and `to_arrow_refs()` keep the full deduplicated source in the object store. The driver holds every bucket reference until commit, and each apply task holds its own references for the whole task. Peak object-store use is about that deduplicated source plus every bucket. Join-all can place one source row in several buckets, so the total can exceed the source. Dropping a fetched payload frees only that worker's deserialized copy. A hot fragment stays on one owner; its rows are written chunk by chunk.
- `ray_remote_args`: Optional kwargs for Ray remote tasks (e.g., `num_cpus`). The `_generator_backpressure_num_objects` option is rejected whenever present, regardless of its value. The plan scheduler waits for generators to finish before consuming their outputs, so enabling generator backpressure can deadlock it.

## Best practices

- Size **`num_workers`** to the cluster slots you want busy during plan and apply (typically 8–64). This is also apply parallelism: each worker owns a disjoint subset of target fragments.
- Raise **`num_partitions` above `num_workers`** when plan or apply tasks are memory-heavy (large source chunks or expensive index probes). Example: `num_workers=8`, `num_partitions=32` runs 32 smaller plan tasks with at most 8 in flight, and still only 8 apply owners. Each apply owner pulls one bucket at a time, so the deserialized source rows and preserved row ids inside that worker follow the largest chunk. The object store still holds the deduplicated source and the buckets. Match offsets are copied into compact integer buffers, so the source bucket can be released inside the worker, and those offsets are released after that fragment's deletion file is written. Plan tasks yield only non-empty owner buckets, so empty plan→apply edges are not stored as Ray objects.
- Do **not** set `num_partitions` in the hundreds or thousands expecting more apply workers. Apply fan-out follows `num_workers`. A large `num_partitions` only increases how many plan tasks run.
- Create a scalar index (e.g. BTREE) on the join key before merging into large tables so planning is index lookups instead of filtered scans.
- Join on a scalar column. Dates, timestamps, times, decimals, and binary keys are encoded as Lance SQL literals in the plan phase. Timestamp and time keys retain their Arrow precision, including nanoseconds; timestamp keys also retain their timezone. Date keys outside year 1–9999 stay integer ticks through the sort and the lookup. Decimal keys with a negative scale sort on the unscaled coefficient. List, struct, and other nested types fail immediately from the target schema — they do not wait for a remote plan task. Column names containing a backtick are rejected.
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

- Duplicate source keys are collapsed before planning by a sort and an adjacent-duplicate drop, keeping one arbitrary row per key. Which copy survives is unspecified. If that key matches several target rows, every matching target row is updated (join-all).
- Apply tasks read one source bucket at a time and drop that payload before fetching the next one. The first pass still keeps that owner's match row ids and offsets for every matched row, about 16 bytes per row, plus temporary memory while those integers are sorted for the duplicate check. Row ids are released after that check. Offsets are released as each fragment's deletion file is written.
- Concurrent writes are a hard constraint: serialize every writer to the table for the whole `merge_into`, including ordinary appends. Lance's conflict detection is fragment-level, and this call does not retry the plan. An append that lands during the merge is rebased inside `LanceDataset.commit`, so both commits succeed. If that append inserts a key this merge also inserts, the plan ran at `read_version` and never saw the new row, and the table keeps both copies. The same gap applies to two concurrent `merge_into` calls inserting the same new key. If a concurrent commit rewrites, removes, or updates-in-place (new deletion file / fragment metadata) one of the fragments this merge touches (e.g. compaction or another merge-on-read update), the operation fails rather than silently dropping the concurrent change. If `commit` raises after this merge's fragments are already visible (lost success ack), the call returns the latest dataset instead of failing, so a job-level retry cannot double-insert. Key-level conflict detection is not implemented.
- Failed applies and commit conflicts leave orphan files. Data files from `write_fragments` and deletion files already written by `delete_rows` stay in storage when a later apply task fails or the commit conflicts. They are not registered in a committed manifest. `LanceDataset.cleanup_old_versions()` can reclaim them. With the default arguments, files left by an unverified failed transaction are kept until they are 7 days old. Pass `delete_unverified=True` only when no other process is writing the dataset.
- Create a scalar index (e.g. BTREE) on the join key column before calling `merge_into` on large tables — key-to-fragment planning is served by the index instead of scanning the table. See [Best practices](#best-practices).
- Join key types: boolean, integer, floating, string, date, timestamp, time, decimal, and binary. Floating keys must be finite. Int64's minimum value, UInt64 values at or above 2**63, dates outside year 1–9999, and decimals with a negative scale use a typed literal and, where Arrow has no comparison kernel, a helper column. Nested types are rejected on the driver. Join column names containing a backtick are rejected because Lance 12 cannot resolve them in a filter. An in-memory `pyarrow.Table` whose string or binary columns are view types is cast to the target schema before it is handed to Ray.
- Internal helper column names are allocated from the target schema so they cannot collide with user fields.
- New fragments are written with the target dataset's `data_storage_version`. When the dataset already exists, Lance also inherits that format from the manifest if the argument is omitted. File-size limits and base placement stay at the Lance writer defaults.
