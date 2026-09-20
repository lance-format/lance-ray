# Reading Lance Datasets

## `read_lance`

```python
read_lance(
    uri=None, 
    *, 
    table_id=None, 
    columns=None, 
    filter=None, 
    storage_options=None, 
    namespace_impl=None,
    namespace_properties=None,
    **kwargs)
```

Read a Lance dataset and return a Ray Dataset.

**Parameters:**

- `uri`: The URI of the Lance dataset to read from (either `uri` OR `namespace_impl` + `table_id` required)
- `table_id`: Table identifier as list of strings (requires `namespace_impl`)
- `columns`: Optional list of column names to read
- `filter`: Optional filter expression to apply
- `storage_options`: Optional Lance storage configuration dictionary, such as S3 or MinIO `access_key_id`, `secret_access_key`, `endpoint` for the S3 API endpoint such as `http://localhost:9000`, and `allow_http`
- `namespace_impl`: Optional namespace implementation type, such as `"dir"` or `"rest"`
- `namespace_properties`: Optional namespace connection properties used with `namespace_impl`
- `base_store_params`: Optional runtime storage options keyed by registered base path URI, used for BlobV2 references outside the dataset root
- `scanner_options`: Optional scanner configuration dictionary
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks
- `override_num_blocks`: Optional override for number of output blocks

**Returns:** Ray Dataset

When using a namespace with S3-compatible object storage, pass object-store
credentials through `storage_options`. Namespace returned storage options are
merged into these values and passed to Lance readers on Ray workers.
