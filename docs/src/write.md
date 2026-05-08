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
- `min_rows_per_file`: Minimum rows per file (default: 1024 * 1024)
- `max_rows_per_file`: Maximum rows per file (default: 64 * 1024 * 1024)
- `data_storage_version`: Optional data storage version
- `storage_options`: Optional storage configuration dictionary
- `base_store_params`: Optional runtime storage options keyed by registered base path URI, used for BlobV2 references outside the dataset root
- `initial_bases`: Optional Lance `DatasetBasePath` objects to register when creating a new dataset
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks

**Returns:** None
