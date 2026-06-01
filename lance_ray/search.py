# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import math
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
from lance.dataset import LanceDataset
from ray.util.multiprocessing import Pool

from .utils import (
    get_namespace_kwargs,
    get_or_create_namespace,
    validate_uri_or_namespace,
)

if TYPE_CHECKING:
    import lance

logger = logging.getLogger(__name__)


class _SearchPlan(NamedTuple):
    fragment_ids: list[int]
    index_segments: list[str]


class _SearchPlanAnalysis(NamedTuple):
    plan: _SearchPlan
    analysis: str


class _SearchPlanUnit(NamedTuple):
    fragment_ids: set[int]
    index_segments: list[str]
    weight: int


def _dataset_load_kwargs(
    storage_options: Optional[dict[str, Any]],
    namespace_kwargs: dict[str, Any],
    block_size: Optional[int],
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "storage_options": storage_options,
        **namespace_kwargs,
    }
    if block_size is not None:
        kwargs["block_size"] = block_size
    return kwargs


def _get_dataset_storage_options(dataset: LanceDataset) -> dict[str, Any]:
    try:
        return dataset.initial_storage_options or {}
    except AttributeError:
        return getattr(dataset, "_storage_options", None) or {}


def _get_fragment_id(fragment: Any) -> int:
    try:
        return fragment.fragment_id
    except AttributeError:
        return fragment.metadata.id


def _get_index_descriptions(dataset: LanceDataset) -> list[Any]:
    if hasattr(dataset, "describe_indices"):
        return dataset.describe_indices()

    descriptions = []
    for index in dataset.list_indices():
        descriptions.append(
            {
                "name": index["name"],
                "index_type": index.get("type"),
                "field_names": index.get("fields", []),
                "segments": [
                    {
                        "uuid": index["uuid"],
                        "fragment_ids": index.get("fragment_ids", set()),
                    }
                ],
            }
        )
    return descriptions


def _index_value(index: Any, name: str, default: Any = None) -> Any:
    if isinstance(index, dict):
        return index.get(name, default)
    return getattr(index, name, default)


def _segment_value(segment: Any, name: str, default: Any = None) -> Any:
    if isinstance(segment, dict):
        return segment.get(name, default)
    return getattr(segment, name, default)


def _select_vector_index(
    dataset: LanceDataset,
    *,
    column: str,
    index_name: Optional[str],
) -> Any | None:
    indices = _get_index_descriptions(dataset)
    for index in indices:
        name = _index_value(index, "name")
        field_names = _index_value(index, "field_names")
        if field_names is None:
            field_names = _index_value(index, "fields", [])

        if index_name is not None:
            if name == index_name:
                return index
            continue

        if column in field_names:
            return index

    if index_name is not None:
        available_names = [str(_index_value(index, "name")) for index in indices]
        raise ValueError(
            f"Vector index '{index_name}' was not found. "
            f"Available indices: {available_names}"
        )

    return None


def _plan_vector_search(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    num_workers: int,
    include_unindexed: bool,
) -> list[_SearchPlan]:
    fragment_ids = {_get_fragment_id(fragment) for fragment in fragments}
    if not fragment_ids:
        return []

    fragment_weights: dict[int, int] = {}
    for fragment in fragments:
        fragment_id = _get_fragment_id(fragment)
        try:
            fragment_weights[fragment_id] = fragment.count_rows()
        except Exception:  # pragma: no cover - defensive fallback
            fragment_weights[fragment_id] = 1

    indexed_units: list[_SearchPlanUnit] = []
    fallback_units: list[_SearchPlanUnit] = []
    indexed_fragment_ids: set[int] = set()

    if vector_index is not None:
        for segment in _index_value(vector_index, "segments", []):
            segment_fragment_ids = set(_segment_value(segment, "fragment_ids", set()))
            segment_fragment_ids &= fragment_ids
            if not segment_fragment_ids:
                continue
            segment_uuid = str(_segment_value(segment, "uuid"))
            indexed_fragment_ids.update(segment_fragment_ids)
            indexed_units.append(
                _SearchPlanUnit(
                    fragment_ids=segment_fragment_ids,
                    index_segments=[segment_uuid],
                    weight=sum(fragment_weights[fid] for fid in segment_fragment_ids),
                )
            )

    fallback_fragment_ids = fragment_ids - indexed_fragment_ids
    if include_unindexed:
        for fragment_id in fallback_fragment_ids:
            fallback_units.append(
                _SearchPlanUnit(
                    fragment_ids={fragment_id},
                    index_segments=[],
                    weight=fragment_weights[fragment_id],
                )
            )

    plans = [
        *_pack_search_plan_units(indexed_units, num_workers),
        *_pack_search_plan_units(fallback_units, num_workers),
    ]

    if not plans:
        return []

    included_fallback_count = len(fallback_fragment_ids) if include_unindexed else 0
    logger.info(
        "Planned distributed vector search across %d tasks, %d fragments, "
        "%d index segments, %d fallback fragments",
        len(plans),
        len(fragment_ids),
        sum(len(plan.index_segments) for plan in plans),
        included_fallback_count,
    )
    return plans


def _pack_search_plan_units(
    units: list[_SearchPlanUnit],
    num_workers: int,
) -> list[_SearchPlan]:
    if not units:
        return []

    plan_count = min(num_workers, len(units))
    worker_fragment_ids: list[set[int]] = [set() for _ in range(plan_count)]
    worker_index_segments: list[list[str]] = [[] for _ in range(plan_count)]
    worker_weights = [0] * plan_count

    for unit in sorted(units, key=lambda item: item.weight, reverse=True):
        worker_idx = min(range(plan_count), key=lambda idx: worker_weights[idx])
        worker_fragment_ids[worker_idx].update(unit.fragment_ids)
        worker_index_segments[worker_idx].extend(unit.index_segments)
        worker_weights[worker_idx] += unit.weight

    plans = [
        _SearchPlan(
            fragment_ids=sorted(worker_fragment_ids[idx]),
            index_segments=worker_index_segments[idx],
        )
        for idx in range(plan_count)
        if worker_fragment_ids[idx]
    ]
    return plans


def _execute_vector_search_plan(
    plan: _SearchPlan,
    *,
    dataset_uri: str,
    dataset_version: int,
    storage_options: Optional[dict[str, Any]],
    block_size: Optional[int],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    analyze_plan: bool,
) -> pa.Table | _SearchPlanAnalysis:
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    dataset = LanceDataset(
        dataset_uri,
        version=dataset_version,
        **_dataset_load_kwargs(storage_options, namespace_kwargs, block_size),
    )

    if not plan.index_segments:
        return _execute_flat_fallback_vector_search_plan(
            dataset,
            plan=plan,
            base_scanner_options=base_scanner_options,
            nearest=nearest,
            candidate_k=candidate_k,
            analyze_plan=analyze_plan,
        )

    scanner_options = dict(base_scanner_options)
    search_nearest = dict(nearest)
    search_nearest["k"] = candidate_k

    scanner_options["nearest"] = search_nearest
    scanner_options["index_segments"] = plan.index_segments
    scanner_options["fast_search"] = True

    logger.info(
        "Running indexed vector search plan: fragments=%d, index_segments=%d, k=%d",
        len(plan.fragment_ids),
        len(plan.index_segments),
        candidate_k,
    )
    scanner = dataset.scanner(**scanner_options)
    if analyze_plan:
        return _SearchPlanAnalysis(plan=plan, analysis=scanner.analyze_plan())
    return scanner.to_table()


def _execute_flat_fallback_vector_search_plan(
    dataset: LanceDataset,
    *,
    plan: _SearchPlan,
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    analyze_plan: bool,
) -> pa.Table | _SearchPlanAnalysis:
    vector_column = nearest["column"]
    vector_scan_column, drop_vector_column = _prepare_fallback_scan_columns(
        base_scanner_options,
        vector_column,
    )
    scanner_options = dict(base_scanner_options)
    scanner_options.pop("fast_search", None)
    scanner_options["fragments"] = [
        dataset.get_fragment(fragment_id) for fragment_id in plan.fragment_ids
    ]

    logger.info(
        "Running flat fallback vector search plan: fragments=%d, k=%d",
        len(plan.fragment_ids),
        candidate_k,
    )
    scanner = dataset.scanner(**scanner_options)
    if analyze_plan:
        return _SearchPlanAnalysis(plan=plan, analysis=scanner.analyze_plan())

    table = scanner.to_table()
    if table.num_rows == 0:
        table = table.append_column("_distance", pa.array([], type=pa.float32()))
        if drop_vector_column and vector_scan_column in table.column_names:
            table = table.drop_columns([vector_scan_column])
        return table

    distances = _compute_vector_distances(
        table[vector_scan_column],
        nearest["q"],
        _get_nearest_metric(nearest),
    )
    table = table.append_column("_distance", pa.array(distances, type=pa.float32()))
    table = _take_top_k(table, candidate_k)
    if drop_vector_column and vector_scan_column in table.column_names:
        table = table.drop_columns([vector_scan_column])
    return table


def _prepare_fallback_scan_columns(
    scanner_options: dict[str, Any],
    vector_column: str,
) -> tuple[str, bool]:
    requested_columns = scanner_options.get("columns")
    if requested_columns is None:
        return vector_column, False

    if isinstance(requested_columns, list):
        scan_columns = [column for column in requested_columns if column != "_distance"]
        if vector_column in scan_columns:
            scanner_options["columns"] = scan_columns
            return vector_column, False
        scanner_options["columns"] = [*scan_columns, vector_column]
        return vector_column, True

    if isinstance(requested_columns, dict):
        vector_scan_column = _unique_hidden_vector_column(requested_columns)
        scanner_options["columns"] = {
            **requested_columns,
            vector_scan_column: vector_column,
        }
        return vector_scan_column, True

    return vector_column, False


def _unique_hidden_vector_column(columns: dict[str, str]) -> str:
    vector_column = "__lance_ray_vector_search_vector"
    while vector_column in columns:
        vector_column = f"_{vector_column}"
    return vector_column


def _get_nearest_metric(nearest: dict[str, Any]) -> str:
    metric = nearest.get("metric") or nearest.get("distance_type") or "l2"
    return str(metric).lower()


def _compute_vector_distances(
    vector_column: pa.ChunkedArray,
    query: Any,
    metric: str,
) -> Any:
    import numpy as np

    matrix = _vector_column_to_numpy(vector_column)
    query_vector = np.asarray(query, dtype=np.float32)
    if query_vector.ndim != 1:
        raise ValueError("nearest['q'] must be a one-dimensional vector")
    if matrix.shape[1] != query_vector.shape[0]:
        raise ValueError(
            "Query vector dimension does not match fallback vector column "
            f"dimension: {query_vector.shape[0]} != {matrix.shape[1]}"
        )

    if metric in ("l2", "euclidean"):
        return np.linalg.norm(matrix - query_vector, axis=1).astype(np.float32)
    if metric == "cosine":
        query_norm = np.linalg.norm(query_vector)
        row_norms = np.linalg.norm(matrix, axis=1)
        denom = row_norms * query_norm
        similarities = np.divide(
            matrix @ query_vector,
            denom,
            out=np.zeros(matrix.shape[0], dtype=np.float32),
            where=denom != 0,
        )
        return (1.0 - similarities).astype(np.float32)
    if metric in ("dot", "ip", "inner_product"):
        return (-(matrix @ query_vector)).astype(np.float32)
    if metric == "hamming":
        return np.count_nonzero(matrix != query_vector, axis=1).astype(np.float32)

    raise ValueError(
        "Unsupported fallback vector search metric "
        f"{metric!r}. Supported metrics: l2, cosine, dot, hamming"
    )


def _vector_column_to_numpy(vector_column: pa.ChunkedArray) -> Any:
    import numpy as np

    values = vector_column.combine_chunks().to_pylist()
    if not values:
        return np.empty((0, 0), dtype=np.float32)
    if any(value is None for value in values):
        raise ValueError("Fallback vector search does not support null vectors")
    matrix = np.asarray(values, dtype=np.float32)
    if matrix.ndim != 2:
        raise ValueError("Fallback vector search requires a list-like vector column")
    return matrix


def _take_top_k(table: pa.Table, k: int) -> pa.Table:
    sort_indices = pc.sort_indices(table, sort_keys=[("_distance", "ascending")])
    return table.take(sort_indices.slice(0, k))


def _merge_vector_search_results(tables: list[pa.Table], k: int) -> pa.Table:
    non_empty_tables = [table for table in tables if table.num_rows > 0]
    if not non_empty_tables:
        return tables[0].slice(0, 0) if tables else pa.table({})

    table = pa.concat_tables(non_empty_tables, promote_options="default")
    if "_distance" not in table.column_names:
        raise RuntimeError(
            "Distributed vector search results must include a '_distance' column "
            "for global top-k merge"
        )

    return _take_top_k(table, k)


def _format_analyze_plan_results(results: list[_SearchPlanAnalysis]) -> str:
    sections = []
    for idx, result in enumerate(results):
        plan_kind = "indexed" if result.plan.index_segments else "flat_fallback"
        sections.append(
            "\n".join(
                [
                    f"== Lance-Ray vector search shard {idx} ({plan_kind}) ==",
                    f"fragments: {result.plan.fragment_ids}",
                    f"index_segments: {result.plan.index_segments}",
                    result.analysis,
                ]
            )
        )
    return "\n\n".join(sections)


def _validate_search_scanner_options(scanner_options: dict[str, Any]) -> None:
    reserved_options = {
        "fast_search",
        "fragments",
        "index_segments",
        "nearest",
        "limit",
        "offset",
    }
    conflicts = sorted(reserved_options & scanner_options.keys())
    if conflicts:
        raise ValueError(
            "scanner_options cannot include distributed search managed options: "
            + ", ".join(conflicts)
        )


def _candidate_k(nearest: dict[str, Any], oversample_factor: float) -> tuple[int, int]:
    try:
        global_k = int(nearest["k"])
    except KeyError as exc:
        raise ValueError(
            "nearest must include 'k' for distributed vector search"
        ) from exc

    if global_k <= 0:
        raise ValueError(f"nearest['k'] must be positive, got {global_k}")
    if oversample_factor < 1:
        raise ValueError(
            f"oversample_factor must be greater than or equal to 1, got {oversample_factor}"
        )

    return global_k, max(global_k, math.ceil(global_k * oversample_factor))


def vector_search(
    uri: Optional[Union[str, "lance.LanceDataset"]] = None,
    *,
    nearest: dict[str, Any],
    index_name: Optional[str] = None,
    columns: Optional[list[str] | dict[str, str]] = None,
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
) -> pa.Table | str:
    """Run a distributed Lance vector search and merge the global top-k.

    The driver opens a fixed dataset version, plans ownership by vector index
    segment coverage.  Indexed worker tasks search only their assigned
    ``index_segments``.  Unindexed fallback tasks scan their assigned fragments
    without ``nearest`` and compute distances locally.  Workers return local
    candidates and the driver sorts by ``_distance`` to produce the final top-k
    table.

    Args:
        uri: Lance dataset object or dataset URI.  In URI mode, provide either
            ``uri`` or namespace parameters (``namespace_impl`` + ``table_id``).
        nearest: Lance vector search options.  Must include ``column``, ``q``,
            and ``k``.  The worker-side ``k`` is raised to at least
            ``k * oversample_factor`` before the driver performs the final
            global top-k merge.
        index_name: Optional vector index name to use.  If specified and the
            index cannot be found, ``ValueError`` is raised.  If omitted,
            Lance-Ray uses the first vector index covering ``nearest["column"]``.
        columns: Projection passed to the Lance scanner.  When a list is
            provided, ``_distance`` is appended automatically because the driver
            needs it to merge global top-k results.
        filter: Filter passed to every worker scanner.
        storage_options: Storage options used to open the dataset.  In namespace
            mode these are merged with namespace-provided storage options.
        block_size: Optional block size in bytes used when loading the dataset.
        namespace_impl: Namespace implementation type, such as ``"dir"`` or
            ``"rest"``.
        namespace_properties: Properties used to connect to the namespace.
        table_id: Table identifier used with namespace parameters.
        num_workers: Maximum number of Ray Pool workers to use.
        ray_remote_args: Ray remote options for Pool workers, such as
            ``num_cpus`` or custom resources.
        oversample_factor: Multiplier for local worker candidates.  Each worker
            returns at least ``nearest["k"] * oversample_factor`` rows before
            driver-side merge.  Must be greater than or equal to 1.
        include_unindexed: Include fragments not covered by vector index
            segments using separate flat-search fallback plans.  Fallback plans
            use regular fragment scans and compute vector distance in Lance-Ray.
            Ignored when ``fast_search=True``.
        fast_search: Search only indexed data.  When enabled, Lance-Ray does
            not schedule flat-search fallback plans for unindexed fragments.
        analyze_plan: Return Lance scanner analyze plans instead of executing
            the query and returning a table.  The result is a string containing
            one section per planned shard.
        scanner_options: Additional Lance scanner options.  Lance-Ray manages
            ``nearest``, ``fragments``, ``index_segments``, ``fast_search``,
            ``limit``, and ``offset`` internally, so these options cannot be
            supplied here.

    Returns:
        A PyArrow table containing the global top-k rows sorted by ``_distance``.
        If ``analyze_plan=True``, returns a string containing per-shard Lance
        scanner analysis instead.
    """
    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")
    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    column = nearest.get("column")
    if not column:
        raise ValueError("nearest must include 'column' for distributed vector search")

    global_k, candidate_k = _candidate_k(nearest, oversample_factor)

    base_scanner_options = dict(scanner_options or {})
    _validate_search_scanner_options(base_scanner_options)
    if columns is not None:
        if isinstance(columns, list) and "_distance" not in columns:
            columns = [*columns, "_distance"]
        base_scanner_options["columns"] = columns
    if filter is not None:
        base_scanner_options["filter"] = filter
    base_scanner_options["fast_search"] = fast_search

    merged_storage_options: dict[str, Any] = {}
    if storage_options:
        merged_storage_options.update(storage_options)

    if isinstance(uri, str | type(None)):
        validate_uri_or_namespace(uri, namespace_impl, table_id)
        namespace = get_or_create_namespace(namespace_impl, namespace_properties)
        if namespace is not None and table_id is not None:
            from lance_namespace import DescribeTableRequest

            describe_response = namespace.describe_table(
                DescribeTableRequest(id=table_id)
            )
            uri = describe_response.location
            if describe_response.storage_options:
                merged_storage_options.update(describe_response.storage_options)

        dataset_uri = uri
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        worker_namespace_impl = namespace_impl
        worker_namespace_properties = namespace_properties
        worker_table_id = table_id
        dataset = LanceDataset(
            dataset_uri,
            **_dataset_load_kwargs(
                merged_storage_options, namespace_kwargs, block_size
            ),
        )
    else:
        dataset = uri
        dataset_uri = dataset.uri
        if not merged_storage_options:
            merged_storage_options.update(_get_dataset_storage_options(dataset))
        namespace_kwargs = {}
        worker_namespace_impl = None
        worker_namespace_properties = None
        worker_table_id = None

    try:
        dataset.schema.field(column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc

    dataset_version = dataset.version
    fragments = dataset.get_fragments()
    if not fragments:
        return pa.table({})

    vector_index = _select_vector_index(
        dataset,
        column=column,
        index_name=index_name,
    )
    if vector_index is None:
        logger.info(
            "No vector index found for column '%s'; distributed search will use flat scan",
            column,
        )

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=vector_index,
        num_workers=num_workers,
        include_unindexed=include_unindexed and not fast_search,
    )
    if not plans:
        return pa.table({})

    def run_plan(plan: _SearchPlan) -> pa.Table:
        return _execute_vector_search_plan(
            plan,
            dataset_uri=dataset_uri,
            dataset_version=dataset_version,
            storage_options=merged_storage_options,
            block_size=block_size,
            namespace_impl=worker_namespace_impl,
            namespace_properties=worker_namespace_properties,
            table_id=worker_table_id,
            base_scanner_options=base_scanner_options,
            nearest=nearest,
            candidate_k=candidate_k,
            analyze_plan=analyze_plan,
        )

    pool = Pool(processes=min(num_workers, len(plans)), ray_remote_args=ray_remote_args)
    try:
        results = pool.map_async(run_plan, plans, chunksize=1).get()
    except Exception as exc:  # pragma: no cover - exercised via integration tests
        raise RuntimeError(
            f"Failed to complete distributed vector search: {exc}"
        ) from exc
    finally:
        pool.close()
        pool.join()

    if analyze_plan:
        return _format_analyze_plan_results(results)

    return _merge_vector_search_results(results, global_k)
