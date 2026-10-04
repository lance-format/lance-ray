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
    target_all_bases=None,
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
- `target_all_bases`: Select all registered bases, including primary storage when `True` and excluding it when `False`. `False` requires at least one registered base. `None` (default) leaves base selection unchanged. Mutually exclusive with non-empty `target_bases`
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

### Blob storage formats

`data_storage_version=None` uses PyLance's default stable format. Legacy blob
columns declared with `lance-encoding:blob=true` field metadata are supported by
file formats 2.0 and 2.1. When the version is omitted, `write_lance` rejects a
legacy blob schema before dispatching the write. To write these columns,
explicitly select a compatible format for both regular and streaming writes,
for example:

```python
write_lance(ds, uri, data_storage_version="2.1")
```

To write blob columns in file formats 2.2 and later, use the Blob v2 extension
type (`lance.blob.v2`), constructed with `lance.blob_field` and `lance.blob_array`.

## Write retries

`write_lance` uses `LanceDatasink`, which allows up to ten fragment write attempts
for matching I/O errors. `write_fragment` and `LanceFragmentWriter` use one attempt
unless `retry_params["max_attempts"]` is explicitly greater than one.

Retries in `write_fragment` require a replayable iterable, such as a list or tuple,
or a factory that returns fresh, logically equivalent input for each attempt.
For a generator function `read_blocks`, pass the function itself to enable replay:

```python
write_fragment(read_blocks(), "single.lance")  # Single streaming attempt.

write_fragment(
    read_blocks,
    "retry.lance",
    retry_params={
        "description": "write lance fragments",
        "match": ["LanceError(IO)"],
        "max_attempts": 3,
        "max_backoff_s": 8,
    },
)
```

Retries use fresh readers without IPC or temporary-file staging. `LanceDatasink`
retains all block references for a write call without copying Arrow data, which
may extend their memory lifetime. `max_bytes_per_file` does not limit that memory.
`LanceFragmentWriter` reruns its transform on each attempt; the transform must
produce equivalent output and handle repeated side effects. Returning the same
generator from a factory is not sufficient for replay.

Before returning fragments, `write_fragment` checks their total row count against
the input and raises `RuntimeError` on a mismatch. This does not validate row
contents, provide job-wide exactly-once semantics, or clean up failed-write files.
