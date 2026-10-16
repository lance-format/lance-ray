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
    max_memory_per_task_bytes=None,
)
```

Compact files in a single Lance table (dataset) using distributed Ray workers.

When `max_memory_per_task_bytes` is omitted, Lance's normal compaction plan is executed as-is. When it is set, each task is estimated from base data files and overlay files (2x the known physical size, to allow for Arrow materialization). `data_files()` does not include overlays, so those sizes are read from fragment metadata. Tasks above that budget are split before they are sent to Ray workers. A missing size on a base file or an overlay keeps the fragment out of any task that only looks under budget.

`CompactionTask.fragments` contains fragment metadata, while fragment lookup needs integer ids. The planner extracts those ids before estimating memory. A split task is rebuilt from the original task JSON, so `read_version`, compaction options, and the full fragment records stay intact. A fragment with unknown `file_size_bytes` is placed in its own task instead of being treated as free. A single fragment larger than the budget is still scheduled alone, and the planner warns that splitting cannot keep it under the limit.

**Parameters:**

- `uri`: Dataset URI to compact (either `uri` OR `namespace_impl` + `table_id` required)
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `compaction_options`: Optional `lance.optimize.CompactionOptions` instance
- `num_workers`: Number of Ray workers to use (default: 4)
- `storage_options`: Optional storage configuration dictionary
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `max_memory_per_task_bytes`: Optional per-task memory budget in bytes. Omit it to keep the normal plan. Set it to split tasks that exceed the budget.

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
    max_memory_per_task_bytes=None,
)
```

Compact all tables under a given database (namespace).

This function lists tables under `database` via the namespace API and runs `compact_files` on each table. `max_memory_per_task_bytes` is applied to every table.

**Parameters:**

- `database`: Database (namespace) identifier as a list of path segments, e.g. `['my_database']`
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `compaction_options`: Optional `lance.optimize.CompactionOptions` instance (applied to every table)
- `num_workers`: Number of Ray workers per table (default: 4)
- `storage_options`: Optional storage configuration dictionary
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `max_memory_per_task_bytes`: Optional per-task memory budget in bytes, forwarded to `compact_files` for every table

**Returns:** A list of dictionaries, one per table, with keys `table_id` (the full table identifier) and `metrics` (the compaction result, or `None` if no compaction was needed).

## Examples

### Compact a single table by URI

```python
import lance_ray as lr

metrics = lr.compact_files(
    uri="/path/to/table.lance",
    num_workers=4,
    max_memory_per_task_bytes=2 * 1024 * 1024 * 1024,
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
