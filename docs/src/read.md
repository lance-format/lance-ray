# Reading Lance Datasets

## `read_lance`

```python
read_lance(
    uri=None, 
    *, 
    namespace=None, 
    table_id=None, 
    columns=None, 
    filter=None, 
    storage_options=None, 
    **kwargs)
```

Read a Lance dataset and return a Ray Dataset.

**Parameters:**

- `uri`: The URI of the Lance dataset to read from (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `columns`: Optional list of column names to read. Unless `override_num_blocks` is set, Ray sizes the read tasks from the estimated in-memory size of the columns read, not of the whole table. The estimate ignores `filter`, and for expression projections or reads that include a blob column it falls back to the total size of the data files
- `filter`: Optional filter expression to apply
- `storage_options`: Optional storage configuration dictionary
- `base_store_params`: Optional runtime storage options keyed by registered base path URI, used for BlobV2 references outside the dataset root
- `scanner_options`: Optional scanner configuration dictionary
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks
- `override_num_blocks`: Optional override for number of output blocks

**Returns:** Ray Dataset

