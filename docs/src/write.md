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
- `mode`: Write mode - "create" (default), "append", or "overwrite". "create" and "overwrite" write data using the incoming schema, so new columns are persisted (schema evolution); "append" adds data under the existing dataset schema without evolving it (in the default non-streaming write, columns not in the existing schema are dropped; with `stream=True` they raise a schema-mismatch error)
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
