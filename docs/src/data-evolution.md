# Data Evolution

## `add_columns`

```python
add_columns(
    uri,
    *, 
    transform,
    storage_options=None,
    namespace_impl=None,
    namespace_properties=None,
    table_id=None,
    **kwargs)
```

Add columns to an existing Lance dataset using Ray's distributed processing.

**Parameters:**

- `uri`: Path to the Lance dataset. `add_columns` requires a concrete URI.
- `transform`: Transform function to apply for adding columns
- `storage_options`: Optional Lance storage configuration dictionary, such as S3 or MinIO `access_key_id`, `secret_access_key`, `endpoint` for the S3 API endpoint such as `http://localhost:9000`, and `allow_http`
- `table_id`: Optional table identifier used with `namespace_impl` for credential vending on distributed workers
- `namespace_impl`: Optional namespace implementation type, such as `"dir"` or `"rest"`
- `namespace_properties`: Optional namespace connection properties used with `namespace_impl`
- `filter`: Optional filter expression to apply
- `read_columns`: Optional list of columns to read from original dataset
- `reader_schema`: Optional schema for the reader
- `read_version`: Optional version to read
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `batch_size`: Batch size for processing (default: 1024)
- `concurrency`: Optional number of concurrent processes

**Returns:** None
