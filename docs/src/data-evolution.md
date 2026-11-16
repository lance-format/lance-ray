# Data Evolution

## `add_columns`

```python
add_columns(
    uri=None,
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

- `uri`: Path to the Lance dataset (either `uri` OR `namespace_impl` + `table_id` required)
- `transform`: Transform function to apply for adding columns
- `storage_options`: Optional Lance storage configuration dictionary, for example S3 or MinIO credentials and `endpoint`
- `table_id`: Table identifier as list of strings (requires `namespace_impl`)
- `namespace_impl`: Optional namespace implementation type, such as `"dir"` or `"rest"`
- `namespace_properties`: Optional namespace connection properties used with `namespace_impl`
- `filter`: Not supported yet; reserved for when fragment filtering is available
- `read_columns`: Optional list of columns to read from original dataset
- `reader_schema`: Optional schema for the reader
- `read_version`: Optional version to read
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `batch_size`: Batch size for processing (default: 1024)
- `concurrency`: Optional number of concurrent processes

**Returns:** None
