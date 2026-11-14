# Distributed Cleanup

Overwrites, compactions and index rebuilds leave old versions behind, together with the data, deletion and index files only those versions reference. `cleanup_old_versions` removes them. On a large table most of its time goes into listing the dataset's directories, which a single process does one page at a time, and deleting the files it finds one request at a time.

Lance-Ray splits that work across Ray workers. Lance plans the cleanup on the driver: it reads every manifest, removes the old ones, and splits the listing of each directory into key ranges. Each range is swept by a worker, which lists it and removes the files there that no retained version references.

Which files are removed is decided by Lance with the same rules as `lance.LanceDataset.cleanup_old_versions`. Lance-Ray only schedules the ranges.

Requires a pylance release with `LanceDataset.plan_cleanup_old_versions`. With older releases the cleanup runs on the driver, and a warning is logged.

## `cleanup_old_versions`

```python
cleanup_old_versions(
    uri=None,
    *,
    table_id=None,
    older_than=None,
    retain_versions=None,
    versions=None,
    delete_unverified=False,
    error_if_tagged_old_versions=True,
    delete_rate_limit=None,
    delete_concurrency=None,
    num_workers=4,
    max_tasks=None,
    max_task_retries=2,
    storage_options=None,
    namespace_impl=None,
    namespace_properties=None,
    ray_remote_args=None,
)
```

**Parameters:**

- `uri`: Dataset URI (either `uri` OR `namespace_impl` + `table_id` required)
- `table_id`: Table identifier as a list of strings (requires `namespace_impl`)
- `older_than`, `retain_versions`, `versions`, `delete_unverified`, `error_if_tagged_old_versions`: As for `lance.LanceDataset.cleanup_old_versions`. Without `older_than`, `retain_versions` or `versions`, versions older than two weeks are removed.
- `delete_rate_limit`: Approximate maximum delete requests per second for the whole cleanup
- `delete_concurrency`: Maximum in-flight deletes per worker
- `num_workers`: Number of Ray workers (default: 4)
- `max_tasks`: Maximum number of key ranges (default: four per worker). Every listed directory gets at least one.
- `max_task_retries`: Retries per failed range (default: 2)
- `storage_options`: Optional storage configuration dictionary
- `namespace_impl`: Namespace implementation type (e.g., `"rest"`, `"dir"`)
- `namespace_properties`: Properties for connecting to the namespace
- `ray_remote_args`: Optional kwargs for the Ray workers

**Returns:** `CleanupStats` for the whole cleanup.

The driver logs a line as each range finishes, with the running number of files and bytes removed.

## `explain_cleanup_old_versions`

Takes the same arguments, without the delete limits, plus `max_files` (default: 1000), and returns the `CleanupExplanation` of what `cleanup_old_versions` would remove. Nothing is deleted.

## Safety

- A file is removed only if no retained version references it. Before listing its range, each worker reads the versions committed since the plan and keeps their files.
- A file that no version references at all, for example one left by a failed write, is removed only once it is seven days old, counted from when the plan was created. `delete_unverified=True` removes such files regardless of age. Only set it when nothing else writes to the dataset until the cleanup ends, or the files of an in-progress write can be removed.
- The old versions are removed while planning, before any file is swept, so none of them can be restored, tagged or branched from while its files are being removed. If a tag or branch is created on one of them while the plan is being made, the cleanup fails before removing anything.
- If a range still fails after its retries, a `RuntimeError` is raised. The old versions are already gone, and no version references the files the range left, so later cleanups only remove them as unverified files.
- Every worker must run the same pylance version as the driver.
- Cleaning referenced branches is not supported.

## Limits

- `delete_rate_limit` is divided evenly between the workers, at least one request per second each, so the total is approximate.
- On a store that does not list keys in lexical order (for example S3 Express One Zone), a range would have to list its whole directory, so each directory gets a single range.
- Reading the manifests and removing the old ones still happens on the driver.

## Example

```python
from datetime import timedelta

import lance_ray as lr

stats = lr.cleanup_old_versions(
    "s3://bucket/path/to/table.lance",
    older_than=timedelta(days=7),
    num_workers=16,
    delete_rate_limit=2000,
)
print(stats.old_versions, stats.bytes_removed)
```
