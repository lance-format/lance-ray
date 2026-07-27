# Data Evolution

## `add_columns`

```python
add_columns(
    uri=None, 
    *, 
    namespace=None, 
    table_id=None, 
    transform, 
    **kwargs)
```

Add columns to an existing Lance dataset using Ray's distributed processing.

**Parameters:**

- `uri`: Path to the Lance dataset (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `transform`: Transform function to apply for adding columns
- `filter`: Optional filter expression to apply
- `read_columns`: Optional list of columns to read from original dataset
- `reader_schema`: Optional schema for the reader
- `read_version`: Optional version to read
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `storage_options`: Optional storage configuration dictionary
- `batch_size`: Batch size for processing (default: 1024)
- `concurrency`: Optional number of concurrent processes

**Returns:** None

## `update_columns`

```python
from lance_ray import update_columns
import pyarrow as pa
import pyarrow.compute as pc


def increase_price(batch: pa.RecordBatch) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {"price": pc.multiply(batch["price"], 1.1)}
    )


result = update_columns(
    "products.lance",
    transform=increase_price,
    output_schema=pa.schema([pa.field("price", pa.float64())]),
    filter="status = 'active'",
    read_columns=["price"],
)
print(result.version, result.rows_updated)
```

Overwrite existing columns with a distributed, fragment-local Ray transform.
Lance-Ray pins a dataset snapshot and processes each fragment in one Ray task.
The task scans the fragment, applies `filter` and `transform`, then rewrites
only the requested columns. Untouched columns retain their original data files.

`transform` accepts and returns a `pyarrow.RecordBatch`. A `lance.udf.BatchUDF`
is also accepted. Its result must contain exactly the fields in `output_schema`.
The transform must preserve both row count and row order: do not filter, sort,
join, deduplicate, aggregate, or explode rows inside it.

**Parameters:**

- `uri`: Path to the target Lance dataset. Alternatively, resolve the table
  with `namespace_impl` and `table_id`.
- `transform`: A `RecordBatch` transform or `BatchUDF` that produces replacement
  values.
- `output_schema`: Required schema for the replacement columns. Every field
  must already exist in the dataset, and its type and nullability must match.
- `filter`: Optional Lance filter expression. Only matching rows receive new
  values; the containing fragment is nevertheless rewritten.
- `read_columns`: Columns supplied to `transform`. When omitted, all top-level
  non-Blob columns are read. Request Blob columns explicitly; they are passed
  to the transform as raw `LargeBinary` bytes and cannot be written by this API.
- `batch_size`: Maximum rows in each scanner and transform batch. Lance receives
  the update values as a RecordBatch stream, though its underlying update join
  can still materialize a fragment's matching rows.
- `ray_remote_args`: Ray resource options for each fragment task, such as
  `{"num_gpus": 1}`.
- `concurrency`: Maximum number of fragment tasks running at once. Lower it to
  bound aggregate fragment-update memory.
- `storage_options`, `base_store_params`, `namespace_impl`,
  `namespace_properties`, `table_id`: Dataset storage and namespace options.

**Returns:** `UpdateColumnsResult(version, rows_updated)`. A filter that
matches no rows succeeds without a transaction; `rows_updated` is `0` and
`version` remains unchanged.

### Limitations and operational notes

- Datasets with stable row IDs are rejected. The underlying Python binding does
  not expose the updated row offsets required for correct CDF metadata.
- Updating an indexed column is allowed, but current Lance index maintenance
  may leave the affected index stale. Rebuild indexes before relying on them
  after such an update.
- A sparse filter still causes fragment-wide rewrites. Prefer this API when the
  updated rows are reasonably concentrated in their fragments.
