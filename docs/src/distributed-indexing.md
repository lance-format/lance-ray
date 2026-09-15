
# Distributed Index Building

Lance-Ray provides distributed index building functionality that leverages Ray's distributed computing capabilities to efficiently create indices for Lance datasets. This is particularly useful for large-scale datasets as it can distribute index building work across multiple Ray worker nodes.

## Distributed APIs

### Scalar Indexing

`create_scalar_index()` - Distributedly create scalar index using ray. Currently Inverted/FTS/BTREE/BITMAP/LABEL_LIST/NGRAM/ZONEMAP/BLOOMFILTER/RTREE are supported. Will add more index type support in the future.

To construct GeoArrow data for an RTREE index, install the PyLance geo extra:

```shell
pip install "pylance[geo]"
```

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
        Literal["BLOOMFILTER"],
        Literal["RTREE"],
        IndexConfig,
    ],
    table_id: Optional[list[str]] = None,
    name: Optional[str] = None,
    replace: bool = True,
    train: bool = True,
    fragment_ids: Optional[list[int]] = None,
    index_uuid: Optional[str] = None,
    num_workers: int = 4,
    num_segments: Optional[int] = None,
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
| `index_type` | `str` or `IndexConfig` | Index type, can be `"INVERTED"`, `"FTS"`, `"BTREE"`, `"BITMAP"`, `"LABEL_LIST"`, `"NGRAM"`, `"ZONEMAP"`, `"BLOOMFILTER"`, `"RTREE"`, or `IndexConfig` object |
| `table_id` | `list[str]`, optional | The table identifier as a list of strings. |
| `name` | `str`, optional | Index name, auto-generated if not provided |
| `replace` | `bool`, optional | Whether to replace existing index with the same name, default is `True` |
| `train` | `bool`, optional | Whether to train the index, default is `True` |
| `fragment_ids` | `list[int]`, optional | Optional list of fragment IDs to build index on |
| `index_uuid` | `str`, optional | Optional fragment UUID for distributed indexing |
| `num_workers` | `int`, optional | Maximum number of Ray Pool workers to use, default is 4 |
| `num_segments` | `int`, optional | Number of fragment batches / index segments to create. Defaults to `num_workers` for backwards compatibility |
| `storage_options` | `Dict[str, str]`, optional | Storage options for the dataset |
| `block_size` | `int`, optional | Block size in bytes to use when loading the dataset |
| `namespace_impl` | `str`, optional | The namespace implementation type (e.g., `"rest"`, `"dir"`) |
| `namespace_properties` | `Dict[str, str]`, optional | Properties for connecting to the namespace |
| `ray_remote_args` | `Dict[str, Any]`, optional | Ray task options (e.g., `num_cpus`, `resources`) |
| `**kwargs` | `Any` | Additional arguments passed to `create_scalar_index` |

**Note:** For distributed scalar indexing, currently `"INVERTED"`, `"FTS"`, `"BTREE"`, `"BITMAP"`, `"LABEL_LIST"`, `"NGRAM"`, `"ZONEMAP"`, `"BLOOMFILTER"`, and `"RTREE"` index types are supported.

#### Return Value

The function returns an updated Lance dataset with the newly created index.

### Vector Indexing

`create_index()` - Distributedly create vector indices using Ray. It leverages Ray to parallelize the index building process across multiple workers.

#### Supported Index Types
The following vector index types are supported for distributed building:
- `IVF_FLAT`
- `IVF_PQ`
- `IVF_RQ`
- `IVF_SQ`
- `IVF_HNSW_FLAT`
- `IVF_HNSW_PQ`
- `IVF_HNSW_SQ`

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
    num_segments: Optional[int] = None,
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
    rabitq_model: Optional[str] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uri` | `str` or `lance.LanceDataset`, optional | Lance dataset object, or its URI. Either `uri` OR (`namespace_impl` + `table_id`) must be provided when using URI mode. If you pass a `lance.LanceDataset` object, namespace parameters are ignored. |
| `column` | `str` | Vector column name to index |
| `index_type` | `str` | Vector index type (e.g., `"IVF_PQ"`, `"IVF_RQ"`, `"IVF_SQ"`, `"IVF_FLAT"`) |
| `name` | `str`, optional | Index name, auto-generated if not provided |
| `replace` | `bool`, optional | Whether to replace existing index, default is `True` |
| `num_workers` | `int`, optional | Maximum number of Ray Pool workers to use, default is 4 |
| `num_segments` | `int`, optional | Number of fragment batches / index segments to create. Defaults to `num_workers` for backwards compatibility |
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
| `rabitq_model` | `str`, optional | Pre-built RaBitQ model for IVF_RQ. If omitted for IVF_RQ, Lance-Ray builds one shared model on the driver |
| `num_bits` | `int`, optional | RaBitQ bits per vector dimension for IVF_RQ, default is 1. Passed through to Lance for validation |
| `**kwargs` | `Any` | Additional arguments to pass through to Lance index creation |

For `IVF_RQ`, Lance-Ray builds one shared RaBitQ rotation model on the driver
when `rabitq_model` is not provided, then passes that same model to every
fragment worker. To pin or reuse a model yourself, pass the JSON string returned
by `lance.lance.indices.build_rq_model(...)` as `rabitq_model`.

The RaBitQ model dimension is the vector column width and must be divisible by
8. `num_bits` controls how many RaBitQ code bits are used per vector dimension:
larger values can increase quantized-code fidelity at the cost of more index
storage and memory. The default is 1, matching Lance's IVF_RQ default, and
supported values are validated by Lance.

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
| `index_name` | `str`, optional | Vector index name to use. If provided and not found, `vector_search()` raises `ValueError` instead of silently falling back. If omitted, Lance-Ray uses the first vector index covering `nearest["column"]` with a compatible metric; if none exists, the search uses flat fallback plans unless `fast_search=True`. |
| `columns` | `list[str]` or `dict[str, str]`, optional | Projection passed to the Lance scanner. When a list is provided and `_distance` is missing, Lance-Ray appends `_distance` automatically because the driver needs it for global top-k merge. |
| `filter` | `Any`, optional | Filter passed unchanged to every worker scanner. |
| `storage_options` | `Dict[str, Any]`, optional | Storage options for the dataset. These are merged with namespace storage options when available. |
| `block_size` | `int`, optional | Block size in bytes to use when loading the dataset on the driver and workers. |
| `namespace_impl` | `str`, optional | Namespace implementation type, such as `"dir"` or `"rest"`. |
| `namespace_properties` | `Dict[str, str]`, optional | Namespace connection properties used with `namespace_impl`. |
| `table_id` | `list[str]`, optional | Table identifier used with namespace parameters. Must be provided together with `namespace_impl` in namespace mode. |
| `num_workers` | `int`, optional | Maximum number of Ray actors to use. Lance-Ray may create fewer actors when there are fewer search plans. |
| `ray_remote_args` | `Dict[str, Any]`, optional | Ray actor options, such as `num_cpus` or custom resources. |
| `oversample_factor` | `float`, optional | Multiplier for local worker candidates. Each worker requests at least `nearest["k"] * oversample_factor` candidates before driver-side merge. Must be greater than or equal to 1. |
| `include_unindexed` | `bool`, optional | Include fragments not covered by vector index segments using separate flat-search fallback plans. Fallback plans use regular fragment scans and compute vector distance in Lance-Ray. Ignored when `fast_search=True`. |
| `fast_search` | `bool`, optional | Search only indexed data. When enabled, Lance-Ray does not schedule flat-search fallback plans for fragments not covered by vector index segments. |
| `analyze_plan` | `bool`, optional | If `True`, execute `LanceScanner.analyze_plan()` for each planned shard and return runtime metrics as a string. This skips Lance-Ray's fallback distance computation and global top-k merge, but still executes the underlying scanners. |
| `scanner_options` | `Dict[str, Any]`, optional | Extra Lance scanner options, such as `batch_size`, `prefilter`, `with_row_id`, or `late_materialization`. Lance-Ray manages `nearest`, `fragments`, `index_segments`, `fast_search`, `limit`, and `offset` internally, so those options cannot be supplied here. Disabling prefilter is not supported for search results. |

#### Return Value

The function returns a `pyarrow.Table` containing the global top-k rows for each query. Single-query results omit `query_index`; explicit batches include a non-null Int64 `query_index` starting at zero for this call. Results are sorted by `query_index` (for batches), `_distance`, and `_rowid`; the internal `_rowid` is removed unless requested. Each query returns at most k rows. If `analyze_plan=True`, the function returns a `str` containing one Lance scanner analysis section per planned shard.

Each ordinary call creates and closes a short-lived search instance using the
same execution core as `open_vector_search()`. Use `open_vector_search()` to reuse
actors and Lance caches across requests; ordinary `vector_search()` calls no
longer reuse the global Ray Pool. `analyze_plan=True` retains the existing
scanner-analysis path.

### Reusable Vector Search

`open_vector_search()` creates a long-lived search instance that pins the dataset
snapshot, vector column, metric, and selected index. Actors reuse their Lance
Session, dataset, and index cache across requests. Lance-Ray does not depend on
Ray Serve. Online services and offline jobs can be deployed separately while
using the same execution core.

#### Synchronous and Asynchronous Requests

```python
import lance_ray as lr

with lr.open_vector_search(
    "path/to/dataset.lance",
    column="embedding",
    metric="cosine",
    max_concurrent_requests=4,
    actor_options=lr.VectorSearchActorOptions(num_actors=4),
) as search:
    result = search.search(
        query_vectors,
        nearest={"k": 20, "nprobes": 16},
        filter="category = 'books'",
        columns=["id"],
    )
```

Calls on the same instance can independently specify `k`, `nprobes`,
`refine_factor`, filters, and projections. All queries within a call use the same
parameters. `nearest` excludes `q`, `column`, and `metric`: the query is a separate
call argument, while the instance supplies the column and metric.

An online wrapper creates the instance at startup, awaits `search_async()` in its
request handler, and awaits `aclose()` at shutdown. A request can complete without
waiting for another request to arrive. The wrapper handles serialization of Arrow
results into HTTP responses.

```python
# search is the long-lived instance created at service startup.
async def handle_query(query_vectors, k):
    return await search.search_async(
        query_vectors,
        nearest={"k": k, "nprobes": 16, "refine_factor": 2},
        columns=["id"],
    )

async def shutdown():
    await search.aclose()
```

Synchronous and asynchronous calls can originate from different threads or event
loops and share the instance's request limit. The instance also supports
`async with`, which waits for accepted requests to finish on exit.

#### Offline Batch and Streaming Input

```python
with lr.open_vector_search(
    "path/to/dataset.lance",
    column="embedding",
    max_concurrent_requests=4,
) as search:
    for result in search.map_batches(
        query_batch_reader,
        nearest={"k": 20, "nprobes": 16},
        columns=["id"],
    ):
        write_results(result)
```

The input is an iterable, so the complete query set does not need to fit in memory.
Each input batch produces one result table, delivered in input order. Empty input
batches and batches with no matches produce empty tables. Reading the next input
batch is independent of delivering completed results, so a blocked input source
does not delay earlier results. Close the iterator when stopping consumption early:

```python
from contextlib import closing

with closing(search.map_batches(query_batch_reader, nearest={"k": 20})) as results:
    first_result = next(results)
```

Each input batch is copied before requesting the next item, so input sources may
reuse a buffer. The caller manages blocking I/O in the input source. Closing the
result iterator does not forcibly interrupt the source iterator's `next()` call.

#### Input Shapes and query_index

| Vector column | Input shape | Meaning |
|---|---|---|
| `FixedSizeList<D>` | `[D]` | One query |
| `FixedSizeList<D>` | `[B,D]` | B queries |
| `List<FixedSizeList<D>>` | `[M,D]` | One multivector query |
| `List<FixedSizeList<D>>` | `[B,M,D]` or a sequence of `[Mi,D]` | B multivector queries |

Inputs can be NumPy arrays, Python lists, or corresponding Arrow arrays or tables.
An Arrow query table must contain exactly one vector column. The instance's column
schema determines how to interpret two-dimensional input; no separate batch or
multivector mode flag is needed.

Results are flat `pyarrow.Table` objects. A single query omits `query_index`;
an explicit batch includes it even when the batch contains only one query.
The batch result's `query_index` is a non-null Int64 column. Numbering starts at
zero for each independent call and accumulates across the complete input stream
within one `map_batches()` call. Queries with no matches do not change subsequent
query indices. A multivector index identifies the complete query, not an individual
subvector. Each query returns at most k rows and may return fewer.

Candidates are merged independently for each query and sorted by
`query_index → _distance → _rowid`. The internal `_rowid` column is removed unless
requested. `query_index` is reserved and cannot be used as a dataset or projection
field name. Empty results retain the output schema for the request.

#### Parameter Scope and Search Coverage

| Parameter | Scope and semantics |
|---|---|
| `column`, `metric`, `index_name` | Fixed when opening the instance. If metric is omitted, use the selected index's metric, or L2 when no index exists. |
| `branch` / `version` | Resolved and pinned at open time; mutually exclusive. An already checked-out dataset retains its snapshot. |
| `storage_options`, namespace parameters, `base_store_params`, `block_size` | Dataset opening options passed to actors. |
| `actor_options` | `VectorSearchActorOptions` configures actor count and resources, index and metadata cache sizes, and `prewarm_index`. |
| `nearest` | Per-request PyLance options such as k, nprobes, refine_factor, distance_range, and use_index. |
| `filter`, `columns` | Per-request scalar prefilter and projection. columns accepts a list or a dictionary of expressions. |
| `fast_search` | Determines whether this request skips unindexed fragments. |
| `scanner_options` | Additional scanner options. Cannot override nearest, fragments, index_segments, fast_search, limit, or offset. prefilter=False is unsupported. |

| use_index (in nearest) | fast_search | Search coverage |
|---|---|---|
| True (default) | False (default) | Indexed data plus flat fallback over uncovered fragments |
| True | True | Indexed data only; returns an empty table when no usable index exists |
| False | False | Flat search over the entire snapshot, including indexed data |
| False | True | Invalid parameter combination |

All paths apply scalar prefiltering. L2 is squared Euclidean distance, cosine is
`1 - cosine_similarity`, dot is `1 - dot`, and Hamming counts differing bits in
packed bytes. Multivector search uses additive MaxSim distance. All distances are
sorted in ascending order.

ANN global Top-K merges the candidates returned by all shards. It does not
guarantee exact Top-K over the entire dataset, and approximate index scores may
differ from exact flat-search distances. `refine_factor` retains Lance Core's
reranking semantics. `nprobes=N` is not equivalent to setting only
`minimum_nprobes=N`: the latter sets a lower bound and allows more partitions to
be searched.

#### Concurrency Limits and Shutdown

`max_concurrent_requests` defaults to 4 and is shared by all entry points. One
search/search_async call, or one input batch in a stream, counts as one request.
Requests wait when capacity is full. Completed streaming results and errors retain
their capacity until delivered in order. Large batches are split internally along
logical query boundaries, with chunks from different requests submitted in an
interleaved order. Cross-request batching is not required.

The request limit does not provide a strict memory or latency bound. A single call
still holds its complete input and result, so callers should control batch and
output sizes. This version does not impose byte or candidate-count quotas.

If a required shard fails, the entire request fails; it does not return a successful
result with missing shards. An ordinary query error affects only that request.
Once a required actor is confirmed dead, the instance stops accepting new requests;
the caller must close and reopen it. A streaming error stops iteration, while
previously delivered results remain valid.

Cancellation stops submission of subsequent chunks and attempts to cancel work
already submitted. It does not kill shared actors or release capacity before the
work actually finishes. `close()` / `aclose()` stop accepting new requests, wait for
accepted requests to finish, and then release actors. Closing is idempotent.
Lance-Ray does not add automatic recovery, retries, or a shutdown timeout.


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
indices = updated_dataset.describe_indices()
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

### Vector Index (IVF_PQ / IVF_RQ / IVF_SQ / IVF_FLAT)
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

# Build a distributed IVF_RQ index
updated_dataset = lr.create_index(
    uri="path/to/dataset.lance",
    column="vector",
    index_type="IVF_RQ",
    name="idx_ivf_rq",
    num_workers=4,
    num_partitions=256,
)

# Or provide a pre-built shared RaBitQ model explicitly.
from lance.lance import indices

rabitq_model = indices.build_rq_model(dimension=128, num_bits=1)
updated_dataset = lr.create_index(
    uri="path/to/dataset.lance",
    column="vector",
    index_type="IVF_RQ",
    name="idx_ivf_rq",
    num_workers=4,
    num_partitions=256,
    num_bits=1,
    rabitq_model=rabitq_model,
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

### Binary Vector Search

For binary vectors, set `nearest["metric"] = "hamming"`. The fallback requires
list-like `uint8` vectors containing packed bits and a query of integer bytes
in `[0, 255]`. It uses the same bit-level Hamming distance as Lance:
`sum(popcount(q[i] ^ v[i]))`, not the number of unequal bytes. Distances are
returned as `float32` and sorted in ascending order. For example, the distance
between `[0, 0]` and `[255, 0]` is 8, not 1. Null vectors and null byte elements
are rejected by the fallback instead of silently producing a distance.

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

- For very large datasets, use Ray worker nodes with sufficient CPU and memory. Increasing `num_workers` can improve index build speed when there are enough segment batches to process, but requires more cluster resources.
- `num_segments` determines the number of index segments. More segments can slow FTS queries because more index segments need to be loaded during search.
- `num_segments` is capped at the number of fragments, and `num_workers` is capped at the number of non-empty segment batches.

### Important Notes

- **Index Type Support**: For distributed indexing, currently only `"INVERTED"`/`"FTS"`/`"BTREE"`/`"BITMAP"`/`"ZONEMAP"` index types are supported, even though the function signature accepts other index types.
- **Default Behavior**: The `replace` parameter defaults to `True`, meaning existing indices with the same name will be replaced without warning. Set `replace=False` to prevent accidental overwrites.
- **Fragment Selection**: Use `fragment_ids` parameter to build indices on specific fragments only. This is useful for incremental index building or testing.
- **Error Handling**: When `replace=False` and an index with the same name exists, a `ValueError` or `RuntimeError` will be raised depending on the execution context.
