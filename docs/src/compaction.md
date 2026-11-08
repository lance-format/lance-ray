# Distributed Compaction

As Lance datasets evolve over time (e.g., frequent appends / overwrites), they can accumulate many small fragments. Compaction rewrites fragments into fewer, larger fragments to improve scan and query performance.

Lance-Ray provides a distributed compaction workflow backed by Ray workers.

## `compact_files`

```python
compact_files(
    uri=None,
    *,
    table_id=None,
    compaction_options=None,
    num_workers=4,
    storage_options=None,
    namespace_impl=None,
    namespace_properties=None,
    ray_remote_args=None,
    max_task_retries=0,
)
```

Compact files in a single Lance table (dataset) using distributed Ray workers.

**Parameters:**

- `uri`: Dataset URI to compact (either `uri` OR `namespace_impl` + `table_id` required)
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `compaction_options`: Optional `lance.optimize.CompactionOptions` instance
- `num_workers`: Number of Ray workers to use (default: 4)
- `storage_options`: Optional storage configuration dictionary
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `ray_remote_args`: Optional kwargs for the Ray workers (actors); `num_cpus` defaults to 0, as in Ray's `Pool`
- `max_task_retries`: How many times a failed compaction task is resubmitted before the compaction fails (default: 0). A transient object-store error then costs one task rather than the whole run. A failed attempt, or a run stopped by a failure, may leave unreferenced data files behind

Tasks run on whichever worker is idle. If a task still fails after its retries, no further task is started, nothing is committed, and the `RuntimeError` reports that first failure only. This holds for the default `max_task_retries=0` too. `num_workers` must be positive.

**Returns:** `CompactionMetrics` or `None` if no compaction tasks are needed.

## `compact_database`

```python
compact_database(
    *,
    database,
    namespace_impl,
    namespace_properties=None,
    compaction_options=None,
    num_workers=4,
    storage_options=None,
    ray_remote_args=None,
    max_task_retries=0,
)
```

Compact all tables under a given database (namespace).

This function lists tables under `database` via the namespace API and runs `compact_files` on each table.

**Parameters:**

- `database`: Database (namespace) identifier as a list of path segments, e.g. `['my_database']`
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `compaction_options`: Optional `lance.optimize.CompactionOptions` instance (applied to every table)
- `num_workers`: Number of Ray workers per table (default: 4)
- `storage_options`: Optional storage configuration dictionary
- `ray_remote_args`: Optional kwargs for the Ray workers (actors), as for `compact_files`
- `max_task_retries`: Passed to `compact_files` for every table (default: 0)

**Returns:** A list of dictionaries, one per table, with keys `table_id` (the full table identifier) and `metrics` (the compaction result, or `None` if no compaction was needed).

## Examples

### Compact a single table by URI

```python
import lance_ray as lr

metrics = lr.compact_files(
    uri="/path/to/table.lance",
    num_workers=4,
)
print(metrics)
```

### Compact a table via namespace

```python
import lance_ray as lr

metrics = lr.compact_files(
    uri=None,
    namespace_impl="dir",
    namespace_properties={"root": "/path/to/tables"},
    table_id=["my_table"],
    num_workers=2,
)
print(metrics)
```

### Compact an entire database

```python
from lance.optimize import CompactionOptions
import lance_ray as lr

results = lr.compact_database(
    database=["my_db"],
    namespace_impl="dir",
    namespace_properties={"root": "/path/to/tables"},
    compaction_options=CompactionOptions(target_rows_per_fragment=10000),
    num_workers=2,
)

for item in results:
    print(item["table_id"], item["metrics"])
```
