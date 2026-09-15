# Writing to Lance Dataset

## `write_lance`

```python
write_lance(
    ds, 
    uri=None, 
    *, 
    namespace=None, 
    table_id=None, 
    schema=None, 
    mode="create", 
    target_bases=None,
    **kwargs)
```

Write a Ray Dataset to Lance format.

**Parameters:**

- `ds`: Ray Dataset to write
- `uri`: Path to the destination Lance dataset (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `schema`: Optional PyArrow schema
- `mode`: Write mode - "create", "append", or "overwrite"
- `target_bases`: Optional list of registered base names or base path URIs where new data files should be written. In `create` mode, entries must match `initial_bases`; in `append` and `overwrite` modes, entries must match bases already registered in the dataset manifest
- `min_rows_per_file`: Minimum rows per file (default: 1024 * 1024)
- `max_rows_per_file`: Maximum rows per file (default: 64 * 1024 * 1024)
- `data_storage_version`: Optional data storage version
- `storage_options`: Optional storage configuration dictionary
- `base_store_params`: Optional runtime storage options keyed by registered base path URI, used for BlobV2 references outside the dataset root
- `initial_bases`: Optional Lance `DatasetBasePath` objects to register when creating a new dataset
- `external_blob_mode`: Optional BlobV2 external URI handling mode. `"reference"` stores external references; `"ingest"` reads external bytes and writes them into Lance-managed storage
- `allow_external_blob_outside_bases`: Optional boolean to allow BlobV2 external references outside registered non-dataset-root base paths when `external_blob_mode="reference"`
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks

**Returns:** None

## Write retries and data integrity

`LanceDatasink`, used by the default `write_lance` path, allows up to ten fragment
write attempts for matching I/O errors. Direct calls to `write_fragment`, and
`LanceFragmentWriter` without `retry_params`, use one streaming attempt.

When multiple attempts are allowed, each write call first converts its complete
input to an Arrow IPC stream using Python's `SpooledTemporaryFile`. Every
attempt opens a fresh reader at the start of that stream, preventing retries
from silently omitting batches consumed by a failed attempt. The spool holds
one write call's complete input, which may produce multiple Lance fragments,
and is closed on success or failure. Single-attempt writes remain streaming,
create no spool, and do not read the replay configuration.

Small streams stay in memory. When their serialized size exceeds **128 MiB**
by default, the entire stream moves to a temporary disk file, and subsequent
writes go to that file. Set a different threshold in bytes with:

```bash
export LANCE_RAY_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES=134217728
```

An unset variable uses the default. A positive integer sets the threshold;
`0` forces disk storage immediately. Negative values, empty strings, and
non-integers raise `ValueError` naming the variable, before spool creation or
destination writing. The setting is read on every retry-enabled call; empty
input still returns immediately.

For Ray tasks, make the variable available in each **worker's** environment;
changing only the driver's environment after workers start is insufficient.
For example, when initializing Ray:

```python
ray.init(runtime_env={
    "env_vars": {"LANCE_RAY_WRITE_REPLAY_MEMORY_THRESHOLD_BYTES": "134217728"}
})
```

The threshold is **not a process or node peak memory limit**. Concurrent calls
each have their own spool, and a single large IPC write may temporarily exceed
the threshold before automatic rollover. Original Arrow data, serialization
buffers, and memory used during rollover are outside this threshold. Provision
memory for their combined usage. Large inputs require disk space for the
**complete** serialized input, not just the bytes above the threshold. Configure
the worker's temporary directory through Python's standard `tempfile` settings
(for example, `TMPDIR`), and provision space for concurrent calls.
`max_bytes_per_file` limits destination Lance files, not replay space.

Both storage modes fully stage the input before starting the destination write,
even if the first attempt succeeds. In-memory spooling avoids temporary disk
I/O for small inputs but still incurs serialization, memory usage, and startup
delay; rolled streams also incur disk I/O.

Before returning fragments for commit, the writer checks that their total row
count matches the input and raises `RuntimeError` on a mismatch. This is a row
count check, not a comparison of every row's contents. Replay and row counting
do not provide job-wide exactly-once semantics or remove uncommitted destination
files left by failed attempts.
