# Distributed Compaction and Cleanup

As Lance datasets evolve over time (e.g., frequent appends / overwrites), they can accumulate many small fragments. Compaction rewrites fragments into fewer, larger fragments to improve scan and query performance.

Lance-Ray provides distributed maintenance workflows backed by Ray workers.
Compaction can split one table into multiple compaction tasks; cleanup runs
old-version cleanup across namespace tables.

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
- `ray_remote_args`: Optional kwargs for Ray remote tasks

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
- `ray_remote_args`: Optional kwargs for Ray remote tasks

**Returns:** A list of dictionaries, one per table, with keys `table_id` (the full table identifier) and `metrics` (the compaction result, or `None` if no compaction was needed).

## `cleanup_old_versions`

```python
cleanup_old_versions(
    uri=None,
    *,
    table_id=None,
    older_than=None,
    retain_versions=None,
    delete_unverified=False,
    error_if_tagged_old_versions=True,
    delete_rate_limit=None,
    storage_options=None,
    namespace_impl=None,
    namespace_properties=None,
)
```

Clean old dataset versions in a single Lance table. This delegates deletion
planning and safety checks to Lance core.

**Parameters:**

- `uri`: Dataset URI to clean (either `uri` OR `namespace_impl` + `table_id` required)
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `older_than`: Optional `datetime.timedelta`; versions older than this may be removed
- `retain_versions`: Optional number of latest versions to retain
- `delete_unverified`: Delete unverified files without Lance's default (7-day) age guard. Only use this when no other process is writing to the dataset.
- `error_if_tagged_old_versions`: Raise if tagged versions match the cleanup policy (default: `True`)
- `delete_rate_limit`: Optional maximum delete operations per second
- `storage_options`: Optional storage configuration dictionary
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace

**Returns:** `CleanupStats`.

## `cleanup_database_old_versions`

```python
cleanup_database_old_versions(
    *,
    database,
    namespace_impl,
    namespace_properties=None,
    older_than=None,
    retain_versions=None,
    delete_unverified=False,
    error_if_tagged_old_versions=True,
    delete_rate_limit=None,
    num_workers=4,
    storage_options=None,
    ray_remote_args=None,
)
```

Clean old versions for all tables under a database (namespace). This function
lists tables under `database` via the namespace API and runs one table cleanup
task per table using a Ray Pool.

Unlike `compact_database` (which processes tables serially and fails fast on the
first error), cleanup runs tables **in parallel** and **aggregates** per-table
errors: every table is attempted, and a single error summarizing all failures is
raised only after the pool finishes. `num_workers` therefore bounds concurrency
*across tables*, not within a single table.

!!! warning

    This operation is destructive and **not atomic**. Tables are cleaned eagerly
    by workers, so when it raises for a failed table, other tables may already
    have had old versions deleted.

**Parameters:**

- `database`: Database (namespace) identifier as a list of path segments, e.g. `['my_database']`
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `older_than`: Optional `datetime.timedelta`; versions older than this may be removed
- `retain_versions`: Optional number of latest versions to retain
- `delete_unverified`: Delete unverified files without Lance's default (7-day) age guard. Only use this when no other process is writing to the datasets.
- `error_if_tagged_old_versions`: Raise if tagged versions match the cleanup policy (default: `True`)
- `delete_rate_limit`: Optional maximum delete operations per second per table
- `num_workers`: Number of Ray workers across tables (default: 4)
- `storage_options`: Optional storage configuration dictionary
- `ray_remote_args`: Optional kwargs for Ray remote tasks

**Returns:** A list of dictionaries, one per table, with keys `table_id` and
`stats`. `stats` is a plain dictionary with the cleanup counters returned by
Lance: `bytes_removed`, `old_versions`, `data_files_removed`,
`transaction_files_removed`, `index_files_removed`, and `deletion_files_removed`.

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

### Clean old versions for one table

```python
from datetime import timedelta
import lance_ray as lr

stats = lr.cleanup_old_versions(
    uri="/path/to/table.lance",
    older_than=timedelta(days=7),
    retain_versions=3,
)
print(stats)
```

### Clean old versions across a database

```python
from datetime import timedelta
import lance_ray as lr

results = lr.cleanup_database_old_versions(
    database=["my_db"],
    namespace_impl="dir",
    namespace_properties={"root": "/path/to/tables"},
    older_than=timedelta(days=7),
    retain_versions=3,
    num_workers=4,
)

for item in results:
    print(item["table_id"], item["stats"])
```
