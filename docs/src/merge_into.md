# Distributed Merge Into

`merge_into` merges a source dataset into a target Lance table by join key: rows whose key already exists in the target are **updated** (all columns replaced), and rows with a new key are **inserted**. It is the distributed counterpart of pylance's `LanceDataset.merge_insert`, designed for sources and targets that are too large to process on a single machine.

The whole operation commits as a **single atomic version** — readers see either the old table or the fully merged table, never an intermediate state.

## How it works

1. **Plan (distributed):** the source is split into chunks; each Ray task maps its keys to their target fragments using batched index lookups on the join key column, then routes rows to per-worker buckets keyed by target fragment (a map-side shuffle). The driver only handles object references and small metadata — source rows never pass through it.
2. **Apply (distributed):** each Ray task owns a disjoint set of target fragments. Updates are merge-on-read: the task masks the matched rows of every owned fragment with a deletion vector (fragment data files are never rewritten), and appends the replacement values together with unmatched rows as new fragments. Scans filter through the deletion vectors until the next compaction folds them away.
3. **Commit (driver):** all per-task results are unioned into one `lance.LanceOperation.Update` and committed once, with bounded retries on conflicting concurrent commits.

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
- `on`: Join key column name (required, keyword-only). A scalar index on this column is strongly recommended for large targets (the plan phase falls back to filtered scans without one).
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `storage_options`: Optional storage configuration dictionary
- `num_workers`: Maximum number of Ray tasks running concurrently in each phase (default: 4). Lower it to reduce peak memory and IO pressure.
- `num_partitions`: How the work is partitioned — the number of source chunks (plan phase) and fragment buckets (apply phase); default: `num_workers`. Raise it above `num_workers` to get smaller, more granular tasks on a memory-constrained cluster.
- `ray_remote_args`: Optional kwargs for Ray remote tasks (e.g., `num_cpus`)

## Examples

### Merge a Ray dataset into a table

```python
import lance_ray as lr
import ray

source = ray.data.read_parquet("s3://bucket/daily_updates/")

dataset = lr.merge_into(source, "/path/to/table.lance", on="id", num_workers=8)
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
- Concurrent writes: appends that land during the merge_into are tolerated (the commit is retried on the newer version). If a concurrent commit rewrites or removes one of the fragments this merge_into touches (e.g. compaction or another update), the operation fails rather than silently dropping the concurrent change.
- Concurrent inserts of the same key: Lance's conflict detection is fragment-level, so two concurrent `merge_into` calls inserting the same *new* join key are physically disjoint — both commits succeed and the key is duplicated. Serialize `merge_into` against the same table externally (e.g. one scheduled writer). Key-level conflict detection is discussed as future work in the [design doc](merge-insert-design.md#112-key-level-conflict-detection-isolation-parity-with-native-merge_insert).
- Create a scalar index (e.g. BTREE) on the join key column before calling `merge_into` on large tables — key-to-fragment planning is served by the index instead of scanning the table.
