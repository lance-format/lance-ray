# Writing to Lance Dataset

## `write_lance`

```python
write_lance(
    ds, 
    uri=None, 
    *, 
    table_id=None, 
    schema=None, 
    mode="create", 
    target_bases=None,
    storage_options=None,
    namespace_impl=None,
    namespace_properties=None,
    **kwargs)
```

Write a Ray Dataset to Lance format.

**Parameters:**

- `ds`: Ray Dataset to write
- `uri`: Path to the destination Lance dataset in URI mode. In namespace mode, append requires namespace lookup only; create and overwrite register or resolve the location through the namespace.
- `table_id`: Table identifier as list of strings (requires `namespace_impl`)
- `schema`: Optional PyArrow schema
- `mode`: Write mode - "create", "append", or "overwrite"
- `target_bases`: Optional list of registered base names or base path URIs where new data files should be written. In `create` mode, entries must match `initial_bases`; in `append` and `overwrite` modes, entries must match bases already registered in the dataset manifest
- `min_rows_per_file`: Minimum rows per file (default: 1024 * 1024)
- `max_rows_per_file`: Maximum rows per file (default: 64 * 1024 * 1024)
- `data_storage_version`: Optional data storage version
- `storage_options`: Optional Lance storage configuration dictionary, such as S3 or MinIO `access_key_id`, `secret_access_key`, `endpoint` for the S3 API endpoint such as `http://localhost:9000`, and `allow_http`
- `base_store_params`: Optional runtime storage options keyed by registered base path URI, used for BlobV2 references outside the dataset root
- `initial_bases`: Optional Lance `DatasetBasePath` objects to register when creating a new dataset
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks
- `namespace_impl`: Optional namespace implementation type, such as `"dir"` or `"rest"`
- `namespace_properties`: Optional namespace connection properties used with `namespace_impl`

**Returns:** None

When using a namespace with S3-compatible object storage, pass object-store
credentials through `storage_options`. Namespace returned storage options are
merged into these values and passed to Lance writers on Ray workers.

For append mode, provide either `uri` or `namespace_impl` + `table_id`, not both.
For create and overwrite modes with namespace parameters, the namespace determines
the registered table location.
