
# Distributed Index Building

Lance-Ray provides distributed index building functionality that leverages Ray's distributed computing capabilities to efficiently create indices for Lance datasets. This is particularly useful for large-scale datasets as it can distribute index building work across multiple Ray worker nodes.

## Distributed APIs

### Scalar Indexing

`create_scalar_index()` - Distributedly create scalar index using ray. Currently only Inverted/FTS/BTREE/BITMAP are supported. Will add more index type support in the future.

#### How It Works
The `create_scalar_index` function allows you to create scalar indices for Lance datasets using the Ray distributed computing framework. This function distributes the index building process across multiple Ray worker nodes, with each node responsible for creating uncommitted index segments for a subset of dataset fragments. These segments are then committed as a single index.

**`create_scalar_index`**

```python
def create_scalar_index(
    uri: Optional[str] = None,
    *,
    column: str,
    index_type: Union[
        Literal["BTREE"],
        Literal["BITMAP"],
        Literal["LABEL_LIST"],
        Literal["INVERTED"],
        Literal["FTS"],
        Literal["NGRAM"],
        Literal["ZONEMAP"],
        IndexConfig,
    ],
    table_id: Optional[list[str]] = None,
    name: Optional[str] = None,
    replace: bool = True,
    train: bool = True,
    fragment_ids: Optional[list[int]] = None,
    index_uuid: Optional[str] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":

```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uri` | `str`, optional | The URI of the Lance dataset. Either `uri` OR (`namespace_impl` + `table_id`) must be provided. |
| `column` | `str` | Column name to index |
| `index_type` | `str` or `IndexConfig` | Index type, can be `"INVERTED"`, `"FTS"`, `"BTREE"`, `"BITMAP"`, `"LABEL_LIST"`, `"NGRAM"`, `"ZONEMAP"`, or `IndexConfig` object |
| `table_id` | `list[str]`, optional | The table identifier as a list of strings. |
| `name` | `str`, optional | Index name, auto-generated if not provided |
| `replace` | `bool`, optional | Whether to replace existing index with the same name, default is `True` |
| `train` | `bool`, optional | Whether to train the index, default is `True` |
| `fragment_ids` | `list[int]`, optional | Optional list of fragment IDs to build index on |
| `index_uuid` | `str`, optional | Optional fragment UUID for distributed indexing |
| `num_workers` | `int`, optional | Number of Ray worker nodes to use, default is 4 |
| `storage_options` | `Dict[str, str]`, optional | Storage options for the dataset |
| `block_size` | `int`, optional | Block size in bytes to use when loading the dataset |
| `namespace_impl` | `str`, optional | The namespace implementation type (e.g., `"rest"`, `"dir"`) |
| `namespace_properties` | `Dict[str, str]`, optional | Properties for connecting to the namespace |
| `ray_remote_args` | `Dict[str, Any]`, optional | Ray task options (e.g., `num_cpus`, `resources`) |
| `**kwargs` | `Any` | Additional arguments passed to `create_scalar_index` |

**Note:** For distributed scalar indexing, currently only `"INVERTED"`, `"FTS"`, `"BTREE"` and `"BITMAP"` index types are supported.

#### Return Value

The function returns an updated Lance dataset with the newly created index.

### Vector Indexing

`create_index()` - Distributedly create vector indices using Ray. It leverages Ray to parallelize the index building process across multiple workers.

#### Supported Index Types
The following vector index types are supported for distributed building:
- `IVF_FLAT`
- `IVF_SQ`
- `IVF_PQ`

#### `create_index`

```python
def create_index(
    uri: Optional[Union[str, "lance.LanceDataset"]] = None,
    column: str = "",
    index_type: str = "",
    name: Optional[str] = None,
    *,
    replace: bool = True,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    metric: str = "l2",
    num_partitions: Optional[int] = None,
    num_sub_vectors: Optional[int] = None,
    sample_rate: int = 256,
    ivf_centroids: Optional["pyarrow.Array"] = None,
    pq_codebook: Optional["pyarrow.Array"] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uri` | `str` or `lance.LanceDataset`, optional | Lance dataset object, or its URI. Either `uri` OR (`namespace_impl` + `table_id`) must be provided when using URI mode. If you pass a `lance.LanceDataset` object, namespace parameters are ignored. |
| `column` | `str` | Vector column name to index |
| `index_type` | `str` | Vector index type (e.g., `"IVF_PQ"`, `"IVF_SQ"`, `"IVF_FLAT"`) |
| `name` | `str`, optional | Index name, auto-generated if not provided |
| `replace` | `bool`, optional | Whether to replace existing index, default is `True` |
| `num_workers` | `int`, optional | Number of Ray workers to use, default is 4 |
| `storage_options` | `Dict[str, str]`, optional | Storage options for the dataset. These are merged with the storage options returned by the namespace (if any). |
| `block_size` | `int`, optional | Block size in bytes to use when loading the dataset |
| `namespace_impl` | `str`, optional | The namespace implementation type (e.g., `"rest"`, `"dir"`) |
| `namespace_properties` | `Dict[str, str]`, optional | Properties for connecting to the namespace |
| `table_id` | `list[str]`, optional | The table identifier as a list of strings. Must be provided together with `namespace_impl`. |
| `ray_remote_args` | `Dict[str, Any]`, optional | Ray task options (e.g., `num_cpus`, `resources`) |
| `metric` | `str`, optional | Distance metric to use (e.g., `"l2"`, `"cosine"`, `"dot"`, `"hamming"`), default is `"l2"` |
| `num_partitions` | `int`, optional | Number of IVF partitions |
| `num_sub_vectors` | `int`, optional | Number of PQ sub-vectors |
| `sample_rate` | `int`, optional | Number of rows sampled per IVF partition and PQ centroid, default is 256 |
| `ivf_centroids` | `pyarrow.Array`, optional | Pre-computed IVF centroids (advanced) |
| `pq_codebook` | `pyarrow.Array`, optional | Pre-computed PQ codebook for PQ-based indices (advanced) |
| `**kwargs` | `Any` | Additional arguments to pass through to Lance index creation |

#### Return Value

The function returns an updated Lance dataset with the newly created vector index.

### Index Optimization (Incremental Updates)

`optimize_indices()` - Incrementally update existing indices for newly appended data.

This is useful when you frequently append/overwrite data and want to restore search performance without rebuilding indices from scratch.

#### `optimize_indices`

```python
def optimize_indices(
    uri: Optional[str] = None,
    *,
    table_id: Optional[list[str]] = None,
    indices: Optional[list[str]] = None,
    num_indices_to_merge: int = 1,
    retrain: bool = False,
    storage_options: Optional[dict[str, str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uri` | `str`, optional | Dataset URI. Either `uri` OR (`namespace_impl` + `table_id`) must be provided. |
| `table_id` | `list[str]`, optional | The table identifier as a list of strings. Must be provided together with `namespace_impl`. |
| `indices` | `list[str]`, optional | Index names to optimize. If not provided, all indices are optimized. |
| `num_indices_to_merge` | `int`, optional | Number of delta indices to merge (default 1). Set to 0 to create a new delta index without merging. |
| `retrain` | `bool`, optional | If `True`, retrain the whole index from current data (default `False`). |
| `storage_options` | `Dict[str, str]`, optional | Storage options for the dataset |
| `namespace_impl` | `str`, optional | The namespace implementation type (e.g., `"rest"`, `"dir"`) |
| `namespace_properties` | `Dict[str, str]`, optional | Properties for connecting to the namespace |
| `**kwargs` | `Any` | Passed through to Lance `DatasetOptimizer.optimize_indices` |

#### Return Value

The function returns the Lance dataset instance (optimization is applied on storage).

### Distributed Vector Search

`vector_search()` - Run vector search with Ray workers and merge the global top-k on the driver.

The driver opens one fixed dataset version, reads vector index segment metadata once, and plans work by index segment ownership.  Indexed worker tasks receive only their assigned `index_segments`, so a segment covering multiple fragments is never split across workers.  Fragments not covered by an index can be included as separate flat-search fallback work unless `fast_search=True`; fallback tasks use regular fragment scans and compute vector distances in Lance-Ray.

#### `vector_search`

```python
def vector_search(
    uri: Optional[Union[str, "lance.LanceDataset"]] = None,
    *,
    nearest: dict[str, Any],
    index_name: Optional[str] = None,
    columns: Optional[Union[list[str], dict[str, str]]] = None,
    filter: Optional[Any] = None,
    storage_options: Optional[dict[str, Any]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    num_workers: int = 4,
    ray_remote_args: Optional[dict[str, Any]] = None,
    oversample_factor: float = 1.0,
    include_unindexed: bool = True,
    fast_search: bool = False,
    analyze_plan: bool = False,
    scanner_options: Optional[dict[str, Any]] = None,
) -> Union[pyarrow.Table, str]:
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uri` | `str` or `lance.LanceDataset`, optional | Lance dataset object, or its URI. Either `uri` OR (`namespace_impl` + `table_id`) must be provided when using URI mode. If a `LanceDataset` object is provided, namespace parameters are ignored and workers reopen the same dataset URI/version. |
| `nearest` | `dict[str, Any]` | Lance vector search options. Must include `column`, `q`, and `k`. Other Lance nearest options such as `minimum_nprobes`, `maximum_nprobes`, `refine_factor`, and distance range are forwarded to every worker. Lance-Ray raises worker-side `k` to at least `k * oversample_factor` before global merge. |
| `index_name` | `str`, optional | Vector index name to use. If provided and not found, `vector_search()` raises `ValueError` instead of silently falling back. If omitted, Lance-Ray uses the first vector index covering `nearest["column"]`; if none exists, the search uses flat fallback plans unless `fast_search=True`. |
| `columns` | `list[str]` or `dict[str, str]`, optional | Projection passed to the Lance scanner. When a list is provided and `_distance` is missing, Lance-Ray appends `_distance` automatically because the driver needs it for global top-k merge. |
| `filter` | `Any`, optional | Filter passed unchanged to every worker scanner. |
| `storage_options` | `Dict[str, Any]`, optional | Storage options for the dataset. These are merged with namespace storage options when available. |
| `block_size` | `int`, optional | Block size in bytes to use when loading the dataset on the driver and workers. |
| `namespace_impl` | `str`, optional | Namespace implementation type, such as `"dir"` or `"rest"`. |
| `namespace_properties` | `Dict[str, str]`, optional | Namespace connection properties used with `namespace_impl`. |
| `table_id` | `list[str]`, optional | Table identifier used with namespace parameters. Must be provided together with `namespace_impl` in namespace mode. |
| `num_workers` | `int`, optional | Maximum number of Ray Pool workers to use. Lance-Ray may create fewer worker tasks when there are fewer search plans. |
| `ray_remote_args` | `Dict[str, Any]`, optional | Ray task options for Pool workers, such as `num_cpus` or custom resources. |
| `oversample_factor` | `float`, optional | Multiplier for local worker candidates. Each worker returns at least `nearest["k"] * oversample_factor` rows before driver-side merge. Must be greater than or equal to 1. |
| `include_unindexed` | `bool`, optional | Include fragments not covered by vector index segments using separate flat-search fallback plans. Fallback plans use regular fragment scans and compute vector distance in Lance-Ray. Ignored when `fast_search=True`. |
| `fast_search` | `bool`, optional | Search only indexed data. When enabled, Lance-Ray does not schedule flat-search fallback plans for fragments not covered by vector index segments. |
| `analyze_plan` | `bool`, optional | If `True`, call `LanceScanner.analyze_plan()` for each planned shard and return a string containing the per-shard analysis instead of executing search and returning a table. |
| `scanner_options` | `Dict[str, Any]`, optional | Extra Lance scanner options, such as `batch_size`, `prefilter`, `with_row_id`, or `late_materialization`. Lance-Ray manages `nearest`, `fragments`, `index_segments`, `fast_search`, `limit`, and `offset` internally, so those options cannot be supplied here. |

#### Return Value

The function returns a `pyarrow.Table` containing the global top-k rows sorted by `_distance`. If `analyze_plan=True`, it returns a `str` containing one Lance scanner analysis section per planned shard.

## Examples

### FTS Index (Scalar)
```python
import lance
import lance_ray as lr

# Create or load Lance dataset
dataset = lance.dataset("path/to/dataset")

# Build distributed index
updated_dataset = lr.create_scalar_index(
   uri=dataset.uri,
   column="text",
   index_type="INVERTED",
   num_workers=4
)

# Verify index creation
indices = updated_dataset.list_indices()
print(f"Index list: {indices}")

# Use index for search
results = updated_dataset.scanner(
   full_text_query="search term",
   columns=["id", "text"]
).to_table()
print(f"Search results: {results}")
```

### BTREE Index (Scalar)
```python
# Assume a LanceDataset with a numeric column "id" exists at this path
import lance_ray as lr

updated_dataset = lr.create_scalar_index(
    uri="path/to/dataset",
    column="id",
    index_type="BTREE",
    name="btree_multiple_fragment_idx",
    replace=False,
    num_workers=4,
)

# Example queries
updated_dataset.scanner(filter="id = 100", columns=["id", "text"]).to_table()
updated_dataset.scanner(filter="id >= 200 AND id < 800", columns=["id", "text"]).to_table()
```

### Vector Index (IVF_PQ / IVF_SQ / IVF_FLAT)
```python
import lance_ray as lr

# Build a distributed IVF_PQ index
updated_dataset = lr.create_index(
    uri="path/to/dataset.lance",
    column="vector",
    index_type="IVF_PQ",
    name="idx_ivf_pq",
    num_workers=4,
    num_partitions=256,
    num_sub_vectors=16,
    sample_rate=64,
    metric="l2"
)

# Build a distributed IVF_SQ index
updated_dataset = lr.create_index(
    uri="path/to/dataset.lance",
    column="vector",
    index_type="IVF_SQ",
    name="idx_ivf_sq",
    num_workers=4,
    num_partitions=256,
)

# Build a distributed IVF_FLAT index
updated_dataset = lr.create_index(
    uri="path/to/dataset.lance",
    column="vector",
    index_type="IVF_FLAT",
    name="idx_ivf_flat",
    num_workers=4,
    num_partitions=256,
)

# Run distributed vector search against index-owned shards.
results = lr.vector_search(
    uri="path/to/dataset.lance",
    nearest={
        "column": "vector",
        "q": query_vector,
        "k": 10,
        "minimum_nprobes": 20,
    },
    index_name="idx_ivf_flat",
    columns=["id", "vector"],
    num_workers=8,
    oversample_factor=2,
    fast_search=False,
)

# Inspect the per-shard Lance scanner plans instead of executing the search.
plan = lr.vector_search(
    uri="path/to/dataset.lance",
    nearest={"column": "vector", "q": query_vector, "k": 10},
    index_name="idx_ivf_flat",
    analyze_plan=True,
)
print(plan)
```

### Custom Ray Options

```python
updated_dataset = lr.create_scalar_index(
   uri="path/to/dataset",
   column="text",
   index_type="INVERTED",
   num_workers=4,
   ray_remote_args={"num_cpus": 2, "resources": {"custom_resource": 1}}
)
```

### Reusing a Ray Pool

Creating a Ray Pool can be expensive if you repeatedly run distributed vector searches in the same process.  You can explicitly initialize a process-wide Pool with `init_global_pool()`.  After that, Lance-Ray will reuse this global Pool for `vector_search()` calls instead of creating a new local Pool each time.

Use this when the same driver process will call `vector_search()` multiple times in a serial workflow:

```python
import lance_ray as lr

lr.init_global_pool(
    processes=16,
    ray_remote_args={"num_cpus": 2},
)

try:
    results = lr.vector_search(
        uri="path/to/dataset.lance",
        nearest={"column": "vector", "q": query_vector, "k": 10},
        num_workers=16,
    )
finally:
    lr.clear_global_pool(close=True)
```

`init_global_pool()` is idempotent while a global Pool exists: later calls return the existing Pool instead of replacing it.  If a global Pool exists, `vector_search()` reuses it and does not close it after the operation.  In that case, the Pool's original `processes` and `ray_remote_args` control the workers; per-call `num_workers` and `ray_remote_args` are only used when Lance-Ray has to create a local Pool for that call.  Lance-Ray logs a warning when it can determine that the requested worker count differs from the configured global Pool size.

Call `clear_global_pool(close=True)` when the driver is done with the shared Pool.  If you manage the Pool lifecycle yourself, use `set_global_pool(pool)` to register it and `clear_global_pool(close=False)` to clear Lance-Ray's reference without closing the Pool.

The global Pool registry is protected for basic set/get/clear operations, but the intended usage is still a single driver process that reuses the Pool serially across operations.  Avoid concurrently mutating the global Pool while other threads are running Lance-Ray operations.

The current global Pool integration is limited to `vector_search()`.  The same pattern can be applied to I/O, index building, and compaction in follow-up changes.

### Index Replacement Control

```python
# Create index with custom name
updated_dataset = lr.create_scalar_index(
   uri="path/to/dataset",
   column="text",
   index_type="INVERTED",
   name="my_text_index",
   num_workers=4
)

# Try to create another index with the same name (will replace by default)
updated_dataset = lr.create_scalar_index(
   uri="path/to/dataset",
   column="text",
   index_type="INVERTED",
   name="my_text_index",  # Same name as before
   replace=True,          # Explicitly allow replacement (default behavior)
   num_workers=4
)

# Prevent index replacement
import lance_ray as lr

try:
    updated_dataset = lr.create_scalar_index(
       uri="path/to/dataset",
       column="text",
       index_type="INVERTED",
       name="my_text_index",  # Same name as existing index
       replace=False,         # Prevent replacement
       num_workers=4
    )
except ValueError as e:
    print(f"Index creation failed: {e}")
    # Handle the error appropriately
```

### Performance Considerations

- For very large datasets, it's recommended to use more powerful CPU/memory ray worker nodes. Increasing `num_workers` can improve index building speed, but requires more computational nodes.
- Too many num_workers can cause large number of partitions, which cause FTS queries slowness as lots of index partitions need to be loaded when searching.
- If `num_workers` is greater than the number of fragments, it will be automatically adjusted to match the fragment count

### Important Notes

- **Index Type Support**: For distributed indexing, currently only `"INVERTED"`/`"FTS"`/`"BTREE"`/`"BITMAP"` index types are supported, even though the function signature accepts other index types.
- **Default Behavior**: The `replace` parameter defaults to `True`, meaning existing indices with the same name will be replaced without warning. Set `replace=False` to prevent accidental overwrites.
- **Fragment Selection**: Use `fragment_ids` parameter to build indices on specific fragments only. This is useful for incremental index building or testing.
- **Error Handling**: When `replace=False` and an index with the same name exists, a `ValueError` or `RuntimeError` will be raised depending on the execution context.
