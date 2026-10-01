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

## `update_columns_from`

```python
update_columns_from(
    uri=None,
    ds=None,
    *,
    columns,
    read_version=None,
    ray_remote_args=None,
    storage_options=None,
    namespace_impl=None,
    namespace_properties=None,
    table_id=None,
    batch_size=1024,
)
```

Update existing columns in a Lance dataset using row metadata. The input Ray
Dataset must contain `_rowaddr` and every column listed in `columns`. If
`_fragid` is absent, it is derived from `_rowaddr`; if supplied, it must match
the fragment ID encoded in `_rowaddr`.

```python
import lance_ray as lr

source = lr.read_lance("my_dataset.lance", with_metadata=True)
source = source.map_batches(modify_status, batch_format="pandas")

lr.update_columns_from(
    "my_dataset.lance",
    source,
    columns=["status"],
)
```

Unmatched source rows are ignored. This API updates existing columns only; it
does not insert, delete, or upsert rows. Transform functions must preserve
`_rowaddr`. An empty source is treated as a no-op and emits a warning.

Each `_rowaddr` must be a non-null integer value and may appear only once; it
is normalized to `uint64` before routing. Names in `columns` must also be
unique, and every source update column must have the same Arrow type as the
corresponding target column.

The final update operation is committed once against the dataset version that
was originally read. Commit conflicts are returned to the caller; safely
retrying requires rerunning the update pipeline from the latest dataset version.
`read_lance()` retains that version and an irreversible SHA-256 digest of the
stable dataset identity in Ray's logical source lineage, so renaming or
combining lazy Dataset branches does not change the update base. Combining rows
from different Lance datasets, or updating a different dataset than the one
that was read, is rejected even when the version numbers match, but only while
that source lineage remains present. If an operation replaces the lineage, such
as materializing the Dataset, provide `read_version` explicitly; the update
will not silently use the latest Lance version. An explicit `read_version`
proves which target version is used, not which dataset produced the source
rows, so after lineage is lost the caller must ensure that the source rows
still correspond to that target dataset. If a reliable identity cannot be
derived for an object-store dataset because no persistent UUID or non-sensitive
backend discriminator is available, `update_columns_from` also requires an
explicit `read_version` instead of performing unsafe automatic validation.

Before fragment partitioning, the source is projected to `_rowaddr`, `_fragid`
when present, and the columns listed in `columns`. Unused source columns are
not routed into fragment update workers, and each fragment is streamed as
bounded `RecordBatch` values rather than materialized as a full table.

**Parameters:**

- `uri`: Path to the Lance dataset. Either `uri` or namespace parameters are required.
- `ds`: Ray Dataset containing `_rowaddr` and the columns to update. `_fragid`
  is derived when absent and validated against `_rowaddr` when supplied.
- `columns`: Existing columns to update.
- `read_version`: Dataset version to update. Defaults to the unique Lance source
  version retained in the Ray Dataset's logical lineage. Required when the
  lineage is unavailable, such as after materializing the source. When lineage
  is present, the source dataset identity digest must match the update target.
  An explicit value is also required when a reliable object-store identity
  cannot be determined.
- `ray_remote_args`: Optional Ray Data task options.
- `storage_options`: Optional storage configuration dictionary.
- `namespace_impl`, `namespace_properties`, `table_id`: Namespace resolution arguments.
- `batch_size`: Batch size for the update reader. Must be positive.

**Returns:** None
