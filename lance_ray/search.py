# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import pickle
import queue
import threading
from collections.abc import Coroutine, Generator, Iterable
from concurrent.futures import Future
from contextlib import suppress
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Optional, cast

import pyarrow as pa
import pyarrow.compute as pc
import ray
from lance.dataset import LanceDataset

from .field_path import canonical_field_path, resolve_arrow_field_path
from .pool import get_or_create_pool
from .utils import (
    get_namespace_kwargs,
    resolve_namespace_table,
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
        return cast(int, fragment.fragment_id)
    except AttributeError:
        return cast(int, fragment.metadata.id)


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
    indices = dataset.describe_indices()
    for index in indices:
        name = _index_value(index, "name")
        field_names = _index_value(index, "field_names")
        if field_names is None:
            field_names = _index_value(index, "fields", [])

        if index_name is not None:
            if name == index_name:
                return index
            continue

        if column in _canonical_index_field_names(field_names):
            return index

    if index_name is not None:
        available_names = [str(_index_value(index, "name")) for index in indices]
        raise ValueError(
            f"Vector index '{index_name}' was not found. "
            f"Available indices: {available_names}"
        )

    return None


def _canonical_index_field_names(field_names: Any) -> set[str]:
    canonical_names = set()
    for field_name in field_names or []:
        try:
            canonical_names.add(canonical_field_path(str(field_name)))
        except ValueError:
            canonical_names.add(str(field_name))
    return canonical_names


def _build_vector_search_plan_units(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    include_unindexed: bool,
) -> tuple[list[_SearchPlanUnit], list[_SearchPlanUnit], int, int]:
    fragment_ids = {_get_fragment_id(fragment) for fragment in fragments}
    if not fragment_ids:
        return [], [], 0, 0

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

    return (
        indexed_units,
        fallback_units,
        len(fragment_ids),
        len(fallback_fragment_ids),
    )


def _plan_vector_search(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    num_workers: int,
    include_unindexed: bool,
) -> list[_SearchPlan]:
    indexed_units, fallback_units, fragment_count, fallback_count = (
        _build_vector_search_plan_units(
            fragments=fragments,
            vector_index=vector_index,
            include_unindexed=include_unindexed,
        )
    )
    plans = [
        *_pack_search_plan_units(indexed_units, num_workers),
        *_pack_search_plan_units(fallback_units, num_workers),
    ]

    if not plans:
        return []

    included_fallback_count = fallback_count if include_unindexed else 0
    logger.info(
        "Planned distributed vector search across %d tasks, %d fragments, "
        "%d index segments, %d fallback fragments",
        len(plans),
        fragment_count,
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


@lru_cache(maxsize=16)
def _load_pickled_dataset(pickled_dataset: bytes) -> LanceDataset:
    return cast(LanceDataset, pickle.loads(pickled_dataset))


@lru_cache(maxsize=16)
def _load_pickled_dataset_ref(pickled_dataset_ref: Any) -> LanceDataset:
    return _load_pickled_dataset(ray.get(pickled_dataset_ref))


def _load_worker_dataset(pickled_dataset: Any) -> LanceDataset:
    if isinstance(pickled_dataset, ray.ObjectRef):
        return _load_pickled_dataset_ref(pickled_dataset)
    return _load_pickled_dataset(pickled_dataset)


def _share_pickled_dataset_for_workers(pickled_dataset: bytes) -> tuple[Any, bool]:
    if not ray.is_initialized():
        return pickled_dataset, False
    return ray.put(pickled_dataset), True


def _execute_vector_search_plan(
    plan: _SearchPlan,
    *,
    pickled_dataset: Any,
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    analyze_plan: bool,
) -> pa.Table | _SearchPlanAnalysis:
    dataset = _load_worker_dataset(pickled_dataset)

    if not plan.index_segments:
        return _execute_flat_fallback_vector_search_plan(
            dataset,
            plan=plan,
            base_scanner_options=base_scanner_options,
            nearest=nearest,
            candidate_k=candidate_k,
            analyze_plan=analyze_plan,
        )

    if not analyze_plan:
        return _indexed_search(
            dataset,
            index_segments=plan.index_segments,
            base_scanner_options=base_scanner_options,
            nearest=nearest,
            candidate_k=candidate_k,
        )

    if not _scanner_accepts_index_segments(dataset):
        raise RuntimeError(
            "The installed pylance scanner does not support index_segments, "
            "which is required for distributed indexed vector search plans. "
            "Upgrade pylance or run without an indexed plan."
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
    return _SearchPlanAnalysis(plan=plan, analysis=scanner.analyze_plan())


def _scanner_accepts_index_segments(dataset: LanceDataset) -> bool:
    try:
        parameters = inspect.signature(dataset.scanner).parameters
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return True
    return "index_segments" in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )


def _execute_flat_fallback_vector_search_plan(
    dataset: LanceDataset,
    *,
    plan: _SearchPlan,
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    analyze_plan: bool,
) -> pa.Table | _SearchPlanAnalysis:
    if not analyze_plan:
        result = _search_vector_shard(
            dataset,
            index_segments=(),
            fallback_fragment_ids=tuple(plan.fragment_ids),
            base_scanner_options=base_scanner_options,
            nearest=nearest,
            candidate_k=candidate_k,
            per_query=True,
        )
        if "query_index" in result.column_names:
            result = result.drop_columns(["query_index"])
        return result

    vector_column = nearest["column"]
    _prepare_fallback_scan_columns(base_scanner_options, vector_column)
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
    return _SearchPlanAnalysis(plan=plan, analysis=scanner.analyze_plan())


def _prepare_fallback_scan_columns(
    scanner_options: dict[str, Any],
    vector_column: str,
    *,
    virtual_columns: Optional[set[str]] = None,
) -> tuple[str, bool]:
    requested_columns = scanner_options.get("columns")
    if requested_columns is None:
        return vector_column, False

    if isinstance(requested_columns, list):
        virtual_columns = virtual_columns or {"_distance"}
        scan_columns = [
            column for column in requested_columns if column not in virtual_columns
        ]
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


def _get_index_metric(dataset: LanceDataset, vector_index: Any) -> str:
    # Recent index metadata records the metric without opening the index files.
    details = _index_value(vector_index, "details") or {}
    metric = details.get("metric_type")
    if metric:
        return str(metric).lower()

    # Older persisted indexes may not have a metric in their manifest details.
    name = _index_value(vector_index, "name")
    statistics = dataset.stats.index_stats(name)
    metrics = {
        str(segment.get("metric_type") or "").lower()
        for segment in statistics.get("indices", [])
    }
    if len(metrics) != 1 or "" in metrics:
        raise ValueError(
            f"Cannot determine a consistent distance metric for vector index {name!r}"
        )
    return metrics.pop()


def _compute_vector_distances(
    vector_column: pa.ChunkedArray[Any],
    query: Any,
    metric: str,
) -> Any:
    matrix = _vector_column_to_numpy(vector_column, metric)
    return _compute_distance_matrix(matrix, query, metric)


def _compute_distance_matrix(matrix: Any, query: Any, metric: str) -> Any:
    """Compute distances using Lance Core's scalar distance conventions."""

    import numpy as np

    if metric == "hamming":
        query_vector = _validate_hamming_query_values(query)
    else:
        query_vector = np.asarray(query, dtype=np.float32)
    if query_vector.ndim != 1:
        raise ValueError("nearest['q'] must be a one-dimensional vector")
    if matrix.shape[1] != query_vector.shape[0]:
        raise ValueError(
            "Query vector dimension does not match fallback vector column "
            f"dimension: {query_vector.shape[0]} != {matrix.shape[1]}"
        )

    if metric in ("l2", "euclidean"):
        # Lance returns squared L2, including on the indexed search path.
        difference = matrix - query_vector
        return np.sum(difference * difference, axis=1).astype(np.float32)
    if metric == "cosine":
        query_norm = np.linalg.norm(query_vector)
        row_norms = np.linalg.norm(matrix, axis=1)
        denom = row_norms * query_norm
        similarities = np.full(matrix.shape[0], np.nan, dtype=np.float32)
        similarities = np.divide(
            matrix @ query_vector,
            denom,
            out=similarities,
            where=denom != 0,
        )
        return (1.0 - similarities).astype(np.float32)
    if metric in ("dot", "ip", "inner_product"):
        # Preserve Lance's offset so indexed and flat candidates are comparable.
        return (1.0 - matrix @ query_vector).astype(np.float32)
    if metric == "hamming":
        # Lance stores packed bits in uint8 elements: count differing bits, not
        # differing bytes. A lookup table avoids expanding the matrix with
        # unpackbits and works on NumPy versions without bitwise_count.
        popcounts = np.array(
            [value.bit_count() for value in range(256)], dtype=np.uint8
        )
        xor = np.bitwise_xor(matrix, query_vector.astype(np.uint8))
        return popcounts[xor].sum(axis=1, dtype=np.uint64).astype(np.float32)

    raise ValueError(
        "Unsupported fallback vector search metric "
        f"{metric!r}. Supported metrics: l2, cosine, dot, hamming"
    )


def _vector_column_to_numpy(vector_column: pa.ChunkedArray[Any], metric: str) -> Any:
    import numpy as np

    if metric == "hamming":
        vector_type = vector_column.type
        if not (
            pa.types.is_list(vector_type)
            or pa.types.is_large_list(vector_type)
            or pa.types.is_fixed_size_list(vector_type)
        ) or not pa.types.is_uint8(vector_type.value_type):
            raise ValueError(
                "Hamming fallback requires a list-like uint8 vector column"
            )
        if pc.list_flatten(vector_column).null_count:
            raise ValueError("Hamming fallback does not support null vector elements")

    dtype = np.uint8 if metric == "hamming" else np.float32
    values = vector_column.combine_chunks().to_pylist()
    if not values:
        return np.empty((0, 0), dtype=dtype)
    if any(value is None for value in values):
        raise ValueError("Fallback vector search does not support null vectors")
    matrix = np.asarray(values, dtype=dtype)
    if matrix.ndim != 2:
        raise ValueError("Fallback vector search requires a list-like vector column")
    return matrix


def _validate_hamming_query_values(query: Any) -> Any:
    import numpy as np

    query_array = np.asarray(query)
    if (
        query_array.dtype.kind not in "iu"
        or np.any(query_array < 0)
        or np.any(query_array > 255)
    ):
        raise ValueError("Hamming query must contain integers in the range 0 to 255")
    return query_array


def _take_top_k(table: pa.Table, k: int) -> pa.Table:
    sort_indices = pc.sort_indices(table, sort_keys=[("_distance", "ascending")])
    return table.take(sort_indices.slice(0, k))


def _merge_vector_search_results(
    tables: list[pa.Table],
    k: int,
    *,
    per_query: bool = False,
    deterministic: bool = False,
) -> pa.Table:
    non_empty_tables = [table for table in tables if table.num_rows > 0]
    if not non_empty_tables:
        return tables[0].slice(0, 0) if tables else pa.table({})

    table = pa.concat_tables(non_empty_tables, promote_options="default")
    if "_distance" not in table.column_names:
        raise RuntimeError(
            "Distributed vector search results must include a '_distance' column "
            "for global top-k merge"
        )

    if per_query:
        if "query_index" not in table.column_names:
            raise RuntimeError(
                "Distributed batch vector search results must include a "
                "'query_index' column for per-query top-k merge"
            )
        return _take_top_k_per_query(table, k)
    if deterministic:
        return _take_top_k_deterministic(table, k)
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
    uri: Optional[str | lance.LanceDataset] = None,
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
    table. Each call creates and closes its own actors; use
    ``open_vector_search()`` to reuse actors and caches across requests.

    Args:
        uri: Lance dataset object or dataset URI.  In URI mode, provide either
            ``uri`` or namespace parameters (``namespace_impl`` + ``table_id``).
        nearest: Lance vector search options.  Must include ``column``, ``q``,
            and ``k``.  The worker-side ``k`` is raised to at least
            ``k * oversample_factor`` before the driver performs the final
            global top-k merge.
            If ``metric`` is omitted, fallback plans use the selected index's
            metric, or L2 when no index exists.  L2 distances are squared and
            dot distances are ``1 - dot(q, v)``, matching Lance.
        index_name: Optional vector index name to use.  If specified and the
            index cannot be found, ``ValueError`` is raised.  If omitted,
            Lance-Ray uses the first vector index covering ``nearest["column"]``
            with a compatible metric.
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
        analyze_plan: Execute Lance scanner analyze plans and return runtime
            metrics instead of a result table.  The result has one section per
            planned shard.  This skips Lance-Ray's fallback distance computation
            and global top-k merge, but still executes the underlying scanners.
        scanner_options: Additional Lance scanner options.  Lance-Ray manages
            ``nearest``, ``fragments``, ``index_segments``, ``fast_search``,
            ``limit``, and ``offset`` internally, so these options cannot be
            supplied here.

    Returns:
        A PyArrow table containing the global top-k rows for each query.
        Single-query results are sorted by ``_distance``. Batch results include
        ``query_index`` and are sorted by query index, distance, and row ID.
        If ``analyze_plan=True``, returns a string containing per-shard Lance
        scanner analysis instead.
    """
    if not analyze_plan:
        request_nearest = dict(nearest)
        try:
            column = request_nearest.pop("column")
            query = request_nearest.pop("q")
        except KeyError as exc:
            raise ValueError("nearest must include 'column', 'q' and 'k'") from exc
        metric = request_nearest.pop("metric", None) or request_nearest.pop(
            "distance_type", None
        )
        request_nearest.pop("distance_type", None)
        with open_vector_search(
            uri,
            column=column,
            metric=metric,
            index_name=index_name,
            storage_options=storage_options,
            block_size=block_size,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            max_concurrent_requests=1,
            actor_options=VectorSearchActorOptions(
                num_actors=num_workers,
                ray_remote_args=ray_remote_args,
            ),
        ) as session:
            indexed_only = fast_search or not include_unindexed
            result, _ = session._submit_search(
                query,
                nearest=request_nearest,
                columns=columns,
                filter=filter,
                fast_search=indexed_only,
                scanner_options=scanner_options,
                oversample_factor=oversample_factor,
            ).result()
            return cast(pa.Table, result)

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")
    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    column = nearest.get("column")
    if not column:
        raise ValueError("nearest must include 'column' for distributed vector search")

    _, candidate_k = _candidate_k(nearest, oversample_factor)

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
        uri, merged_storage_options = resolve_namespace_table(
            uri, storage_options, namespace_impl, namespace_properties, table_id
        )

        dataset_uri = uri
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        dataset = LanceDataset(
            dataset_uri,
            **_dataset_load_kwargs(
                merged_storage_options, namespace_kwargs, block_size
            ),
        )
    else:
        dataset = uri
        if not merged_storage_options:
            merged_storage_options.update(_get_dataset_storage_options(dataset))

    try:
        resolved_column = resolve_arrow_field_path(dataset.schema, column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc
    column = resolved_column.path
    nearest = {**nearest, "column": column}

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

    pickled_dataset = pickle.dumps(dataset)

    try:
        with get_or_create_pool(
            processes=min(num_workers, len(plans)),
            ray_remote_args=ray_remote_args,
        ) as pool:
            worker_pickled_dataset, _ = _share_pickled_dataset_for_workers(
                pickled_dataset
            )

            def run_plan(plan: _SearchPlan) -> pa.Table | _SearchPlanAnalysis:
                return _execute_vector_search_plan(
                    plan,
                    pickled_dataset=worker_pickled_dataset,
                    base_scanner_options=base_scanner_options,
                    nearest=nearest,
                    candidate_k=candidate_k,
                    analyze_plan=analyze_plan,
                )

            results = pool.map_async(run_plan, plans, chunksize=1).get()
    except Exception as exc:  # pragma: no cover - exercised via integration tests
        raise RuntimeError(
            f"Failed to complete distributed vector search: {exc}"
        ) from exc

    return _format_analyze_plan_results(results)


def _resolve_vector_search_schema(
    dataset: LanceDataset,
    *,
    nearest: dict[str, Any],
    base_scanner_options: dict[str, Any],
    include_row_id: bool,
) -> pa.Schema:
    schema_options = dict(base_scanner_options)
    schema_options["nearest"] = nearest
    schema_options["with_row_id"] = True
    result_schema = dataset.scanner(**schema_options).projected_schema
    if not include_row_id:
        row_id_indices = result_schema.get_all_field_indices("_rowid")
        if row_id_indices:
            result_schema = result_schema.remove(row_id_indices[-1])

    return result_schema


def _projection_includes_row_id(
    columns: Optional[list[str] | dict[str, str]],
    scanner_options: dict[str, Any],
) -> bool:
    if scanner_options.get("with_row_id"):
        return True
    if columns is None:
        columns = scanner_options.get("columns")
    if isinstance(columns, list):
        return "_rowid" in columns
    if isinstance(columns, dict):
        return "_rowid" in columns
    return False


def _take_top_k_deterministic(table: pa.Table, k: int) -> pa.Table:
    sort_keys: list[tuple[str, Literal["ascending", "descending"]]] = [
        ("_distance", "ascending")
    ]
    if "_rowid" in table.column_names:
        sort_keys.append(("_rowid", "ascending"))
    sort_indices = pc.sort_indices(table, sort_keys=sort_keys)
    return table.take(sort_indices.slice(0, k))


def _apply_distance_range(table: pa.Table, nearest: dict[str, Any]) -> pa.Table:
    distance_range = nearest.get("distance_range")
    if distance_range is None:
        return table

    lower_bound, upper_bound = distance_range
    if lower_bound is not None:
        table = table.filter(pc.greater_equal(table["_distance"], lower_bound))
    if upper_bound is not None:
        table = table.filter(pc.less(table["_distance"], upper_bound))
    return table


def _take_top_k_per_query(table: pa.Table, k: int) -> pa.Table:
    import numpy as np

    sort_keys: list[tuple[str, Literal["ascending", "descending"]]] = [
        ("query_index", "ascending"),
        ("_distance", "ascending"),
    ]
    if "_rowid" in table.column_names:
        sort_keys.append(("_rowid", "ascending"))
    sort_indices = pc.sort_indices(table, sort_keys=sort_keys)
    table = table.take(sort_indices)
    if table.num_rows == 0:
        return table

    query_indices = table["query_index"].combine_chunks().to_numpy()
    row_indices = np.arange(table.num_rows)
    group_starts = np.empty(table.num_rows, dtype=np.int64)
    group_starts[0] = 0
    group_starts[1:] = np.where(
        query_indices[1:] != query_indices[:-1],
        row_indices[1:],
        0,
    )
    np.maximum.accumulate(group_starts, out=group_starts)
    return table.filter(pa.array(row_indices - group_starts < k))


@dataclass(frozen=True)
class VectorSearchActorOptions:
    """Controls Ray actors, their Lance sessions, and scanner execution."""

    num_actors: int = 4
    ray_remote_args: Optional[dict[str, Any]] = None
    index_cache_size_bytes: Optional[int] = None
    metadata_cache_size_bytes: Optional[int] = None
    prewarm_index: bool = False

    def __post_init__(self) -> None:
        if self.num_actors <= 0:
            raise ValueError("num_actors must be positive")
        if self.index_cache_size_bytes is not None and self.index_cache_size_bytes < 0:
            raise ValueError("index_cache_size_bytes must be non-negative")
        if (
            self.metadata_cache_size_bytes is not None
            and self.metadata_cache_size_bytes < 0
        ):
            raise ValueError("metadata_cache_size_bytes must be non-negative")


@dataclass(frozen=True)
class _DatasetSnapshot:
    uri: str
    version: int
    serialized_manifest: bytes
    storage_options: dict[str, Any]
    base_store_params: Optional[dict[str, dict[str, Any]]]
    block_size: Optional[int]
    namespace_impl: Optional[str]
    namespace_properties: Optional[dict[str, str]]
    table_id: Optional[list[str]]


@dataclass(frozen=True)
class _ActorPlan:
    index_segments: tuple[str, ...]
    fallback_fragment_ids: tuple[int, ...]
    flat_fragment_ids: tuple[int, ...]


def _plan_streaming_vector_search(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    num_actors: int,
) -> list[_ActorPlan]:
    indexed_units, fallback_units, _, _ = _build_vector_search_plan_units(
        fragments=fragments,
        vector_index=vector_index,
        include_unindexed=True,
    )
    units = [*indexed_units, *fallback_units]

    if not units:
        return []

    actor_count = min(num_actors, len(units))
    actor_weights = [0] * actor_count
    index_segments: list[list[str]] = [[] for _ in range(actor_count)]
    fallback_fragments: list[set[int]] = [set() for _ in range(actor_count)]

    for unit in sorted(units, key=lambda item: item.weight, reverse=True):
        actor_idx = min(range(actor_count), key=lambda idx: actor_weights[idx])
        if not unit.index_segments:
            fallback_fragments[actor_idx].update(unit.fragment_ids)
        else:
            index_segments[actor_idx].extend(unit.index_segments)
        actor_weights[actor_idx] += unit.weight

    # Flat ownership covers the full snapshot independently of ANN ownership.
    # Segment coverage can overlap: assign each fragment exactly once.
    flat_fragments: list[list[int]] = [[] for _ in range(actor_count)]
    flat_weights = [0] * actor_count
    for fragment in sorted(fragments, key=lambda item: item.count_rows(), reverse=True):
        idx = min(range(actor_count), key=lambda item: flat_weights[item])
        flat_fragments[idx].append(_get_fragment_id(fragment))
        flat_weights[idx] += fragment.count_rows()

    return [
        _ActorPlan(
            index_segments=tuple(index_segments[idx]),
            fallback_fragment_ids=tuple(sorted(fallback_fragments[idx])),
            flat_fragment_ids=tuple(sorted(flat_fragments[idx])),
        )
        for idx in range(actor_count)
    ]


def _open_snapshot(
    snapshot: _DatasetSnapshot,
    *,
    index_cache_size_bytes: Optional[int],
    metadata_cache_size_bytes: Optional[int],
) -> LanceDataset:
    import lance

    session = lance.Session(  # type: ignore[call-arg]
        index_cache_size_bytes=index_cache_size_bytes,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )
    namespace_kwargs = get_namespace_kwargs(
        snapshot.namespace_impl,
        snapshot.namespace_properties,
        snapshot.table_id,
    )
    kwargs: dict[str, Any] = {
        "storage_options": snapshot.storage_options,
        "session": session,
        **namespace_kwargs,
    }
    if snapshot.block_size is not None:
        kwargs["block_size"] = snapshot.block_size
    if snapshot.base_store_params is not None:
        kwargs["base_store_params"] = snapshot.base_store_params

    dataset = LanceDataset(
        snapshot.uri,
        version=snapshot.version,
        serialized_manifest=snapshot.serialized_manifest,
        **kwargs,
    )
    if dataset.version != snapshot.version:
        raise RuntimeError(
            f"Dataset snapshot changed: expected {snapshot.version}, "
            f"opened {dataset.version}"
        )
    return dataset


def _empty_fallback_table(
    scanner: Any,
    *,
    vector_column: str,
    drop_vector_column: bool,
) -> pa.Table:
    schema = getattr(scanner, "projected_schema", pa.schema([]))
    fields = list(schema)
    if drop_vector_column:
        fields = [field for field in fields if field.name != vector_column]
    fields = [
        field for field in fields if field.name not in {"query_index", "_distance"}
    ]
    schema = pa.schema(
        [
            pa.field("query_index", pa.int32(), nullable=False),
            *fields,
            pa.field("_distance", pa.float32()),
        ]
    )
    return pa.Table.from_batches([], schema=schema)


def _scanner_batches(scanner: Any) -> Iterable[pa.RecordBatch]:
    if hasattr(scanner, "to_batches"):
        return cast(Iterable[pa.RecordBatch], scanner.to_batches())
    return cast(Iterable[pa.RecordBatch], scanner.to_table().to_batches())


def _stream_flat_fallback(
    dataset: LanceDataset,
    *,
    fragment_ids: tuple[int, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
) -> pa.Table:
    vector_column = nearest["column"]
    scanner_options = dict(base_scanner_options)
    vector_scan_column, drop_vector_column = _prepare_fallback_scan_columns(
        scanner_options,
        vector_column,
        virtual_columns={"_distance", "query_index"},
    )
    scanner_options.pop("fast_search", None)
    scanner_options["fragments"] = [
        dataset.get_fragment(fragment_id) for fragment_id in fragment_ids
    ]
    scanner = dataset.scanner(**scanner_options)

    metric = _get_nearest_metric(nearest)
    query_vectors = _canonical_query_batch(nearest["q"], metric, copy=False)
    running: Optional[pa.Table] = None
    for batch in _scanner_batches(scanner):
        table = pa.Table.from_batches([batch])
        if table.num_rows == 0:
            continue
        table = table.filter(pc.invert(pc.is_null(table[vector_scan_column])))
        if table.num_rows == 0:
            continue

        vector_matrix = _vector_column_to_numpy(table[vector_scan_column], metric)
        query_results = []
        for query_index, query_vector in enumerate(query_vectors):
            import numpy as np

            distances = _compute_distance_matrix(
                vector_matrix,
                query_vector,
                metric,
            )
            valid = np.invert(np.isnan(distances))
            query_result = table.filter(pa.array(valid, type=pa.bool_()))
            query_result = query_result.append_column(
                "_distance",
                pa.array(distances[valid], type=pa.float32()),
            )
            query_result = _apply_distance_range(query_result, nearest)
            query_result = _take_top_k_deterministic(query_result, candidate_k)
            if drop_vector_column and vector_scan_column in query_result.column_names:
                query_result = query_result.drop_columns([vector_scan_column])
            query_result = query_result.add_column(
                0,
                pa.field("query_index", pa.int32(), nullable=False),
                pa.array(
                    [query_index] * query_result.num_rows,
                    type=pa.int32(),
                ),
            )
            query_results.append(query_result)

        current = pa.concat_tables(query_results, promote_options="default")
        running = (
            current
            if running is None
            else _merge_vector_search_results(
                [running, current],
                candidate_k,
                per_query=True,
            )
        )

    if running is not None:
        return running
    return _empty_fallback_table(
        scanner,
        vector_column=vector_scan_column,
        drop_vector_column=drop_vector_column,
    )


def _indexed_search(
    dataset: LanceDataset,
    *,
    index_segments: list[str] | tuple[str, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
) -> pa.Table:
    if not _scanner_accepts_index_segments(dataset):
        raise RuntimeError(
            "The installed pylance scanner does not support index_segments"
        )
    scanner_options = dict(base_scanner_options)
    search_nearest = dict(nearest)
    search_nearest["k"] = candidate_k
    scanner_options.update(
        nearest=search_nearest,
        index_segments=index_segments,
        fast_search=True,
        prefilter=True,
    )
    return dataset.scanner(**scanner_options).to_table()


def _search_vector_shard(
    dataset: LanceDataset,
    *,
    index_segments: tuple[str, ...],
    fallback_fragment_ids: tuple[int, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    per_query: bool,
    is_multivector: bool = False,
) -> pa.Table:
    """Search the indexed and flat portions owned by one worker."""

    tables = []
    if index_segments:
        tables.append(
            _indexed_search(
                dataset,
                index_segments=index_segments,
                base_scanner_options=base_scanner_options,
                nearest=nearest,
                candidate_k=candidate_k,
            )
        )
    if fallback_fragment_ids:
        fallback_search = (
            _multivector_fallback_search if is_multivector else _stream_flat_fallback
        )
        tables.append(
            fallback_search(
                dataset,
                fragment_ids=fallback_fragment_ids,
                base_scanner_options=base_scanner_options,
                nearest=nearest,
                candidate_k=candidate_k,
            )
        )
    if not per_query:
        tables = [
            table.drop_columns(["query_index"])
            if "query_index" in table.column_names
            else table
            for table in tables
        ]
    return _merge_vector_search_results(
        tables,
        candidate_k,
        per_query=per_query,
        deterministic=not per_query,
    )


def _offset_query_index(
    table: pa.Table,
    offset: int,
    *,
    output_type: pa.DataType,
) -> pa.Table:
    if "query_index" not in table.column_names:
        raise RuntimeError("Batch search result is missing query_index")
    values = pc.cast(table["query_index"], output_type)
    if offset:
        values = pc.add(values, offset)
    return table.set_column(
        table.schema.get_field_index("query_index"),
        pa.field("query_index", output_type, nullable=False),
        values,
    )


def _canonical_query_batch(
    query: Any,
    metric: str,
    *,
    copy: bool = True,
) -> Any:
    import numpy as np

    if isinstance(query, pa.RecordBatch | pa.Table):
        if len(query.column_names) != 1:
            raise ValueError(
                "Arrow query batches must contain exactly one vector column"
            )
        query = query.column(0)
    if isinstance(query, pa.ChunkedArray):
        query = query.combine_chunks()
    if isinstance(query, pa.Array):
        query = query.to_pylist()
    if metric == "hamming":
        query = _validate_hamming_query_values(query)
    dtype = np.uint8 if metric == "hamming" else np.float32
    if copy:
        array = np.array(query, dtype=dtype, copy=True, order="C")
    else:
        array = np.asarray(query, dtype=dtype, order="C")
    if array.size == 0:
        if array.ndim == 2:
            return array
        return np.empty((0, 0), dtype=dtype)
    if array.ndim == 1:
        array = array.reshape(1, -1)
    if array.ndim != 2:
        raise ValueError("Each query batch must be a two-dimensional array")
    return array


def _streaming_is_multivector_type(data_type: pa.DataType) -> bool:
    if not (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)):
        return False
    return pa.types.is_fixed_size_list(data_type.value_type)


def _canonical_multivector_batch(
    query: Any,
    metric: str,
) -> tuple[Any, ...]:
    import numpy as np

    dtype = np.uint8 if metric == "hamming" else np.float32
    if isinstance(query, pa.RecordBatch | pa.Table):
        if len(query.column_names) != 1:
            raise ValueError(
                "Arrow query batches must contain exactly one multivector column"
            )
        query = query.column(0)
    if isinstance(query, pa.ChunkedArray):
        query = query.combine_chunks()
    if isinstance(query, pa.Array):
        query = query.to_pylist()

    try:
        array = np.asarray(query, dtype=dtype)
    except ValueError:
        array = None

    if array is not None and array.ndim <= 3:
        if array.size == 0:
            if array.ndim == 2 or (array.ndim == 3 and array.shape[0] > 0):
                raise ValueError(
                    "Each multivector query must contain at least one vector"
                )
            return ()
        if array.ndim == 1:
            return (np.array(array.reshape(1, -1), copy=True, order="C"),)
        if array.ndim == 2:
            return (np.array(array, copy=True, order="C"),)
        return tuple(np.array(item, copy=True, order="C") for item in array)

    queries = []
    for item in query:
        item_array = np.array(item, dtype=dtype, copy=True, order="C")
        if item_array.ndim == 1:
            item_array = item_array.reshape(1, -1)
        if item_array.ndim != 2:
            raise ValueError("Each multivector query must have shape [M, D]")
        queries.append(item_array)
    return tuple(queries)


def _add_query_index(table: pa.Table, query_index: int) -> pa.Table:
    return table.add_column(
        0,
        pa.field("query_index", pa.int32(), nullable=False),
        pa.array([query_index] * table.num_rows, type=pa.int32()),
    )


def _multivector_fallback_search(
    dataset: LanceDataset,
    *,
    fragment_ids: tuple[int, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
) -> pa.Table:
    query = nearest["q"]
    scanner_options = dict(base_scanner_options)
    scanner_options.pop("fast_search", None)
    scanner_options["fragments"] = [
        dataset.get_fragment(fragment_id) for fragment_id in fragment_ids
    ]
    # Core requires prefilter for nearest scans scoped to explicit fragments.
    scanner_options["prefilter"] = True

    search_nearest = {**nearest, "k": candidate_k, "use_index": False}
    distance_range = search_nearest.pop("distance_range", None)
    scanner_options["nearest"] = search_nearest
    table = dataset.scanner(**scanner_options).to_table()

    query_count = len(query)
    if query_count > 1 and table.num_rows:
        # Remove this compatibility offset once the minimum Core version uses
        # M - sum(MaxSim) for flat multivector distance.
        distances = pc.add(
            table["_distance"],
            pa.scalar(float(query_count - 1), pa.float32()),
        )
        table = table.set_column(
            table.schema.get_field_index("_distance"),
            pa.field("_distance", pa.float32()),
            distances,
        )
    if distance_range is not None:
        table = _apply_distance_range(
            table,
            {"distance_range": distance_range},
        )
    return _take_top_k_deterministic(table, candidate_k)


@ray.remote
class _VectorSearchActor:
    def __init__(
        self,
        snapshot: _DatasetSnapshot,
        plan: _ActorPlan,
        index_name: Optional[str],
        is_multivector: bool,
        actor_options: VectorSearchActorOptions,
    ):
        self._dataset = _open_snapshot(
            snapshot,
            index_cache_size_bytes=actor_options.index_cache_size_bytes,
            metadata_cache_size_bytes=actor_options.metadata_cache_size_bytes,
        )
        self._plan = plan
        self._index_name = index_name
        self._is_multivector = is_multivector

    def ready(self) -> dict[str, Any]:
        return {
            "version": self._dataset.version,
            "index_segments": len(self._plan.index_segments),
            "fallback_fragments": len(self._plan.fallback_fragment_ids),
        }

    def prewarm(self) -> dict[str, Any]:
        if not self._plan.index_segments or self._index_name is None:
            return {"index_segments": 0, "skipped": True}
        before = self._dataset.io_stats_snapshot()
        self._dataset.prewarm_index(
            self._index_name,
            index_segments=self._plan.index_segments,
        )
        after = self._dataset.io_stats_snapshot()
        return {
            "index_segments": len(self._plan.index_segments),
            "skipped": False,
            "read_bytes": after.read_bytes - before.read_bytes,
            "read_iops": after.read_iops - before.read_iops,
        }

    def search(
        self,
        query_batch: Any,
        nearest: dict[str, Any],
        candidate_k: int,
        scanner_options: dict[str, Any],
        fast_search: bool,
    ) -> pa.Table:
        metric = _get_nearest_metric(nearest)
        if self._is_multivector:
            queries = _canonical_multivector_batch(query_batch, metric)
        else:
            queries = _canonical_query_batch(query_batch, metric, copy=False)
        return self._search_micro_batch(
            queries, nearest, candidate_k, scanner_options, fast_search
        )

    def _search_micro_batch(
        self,
        query_batch: Any,
        nearest: dict[str, Any],
        candidate_k: int,
        scanner_options: dict[str, Any],
        fast_search: bool,
    ) -> pa.Table:
        use_index = nearest.get("use_index", True)
        index_segments = self._plan.index_segments if use_index else ()
        fragments = (
            self._plan.fallback_fragment_ids
            if use_index
            else self._plan.flat_fragment_ids
        )
        if fast_search:
            fragments = ()
        if not index_segments and not fragments:
            return pa.table({})
        # PyLance's 2-D query conversion uses float32. Keep packed Hamming
        # vectors uint8 by issuing native single-query searches inside the actor.
        if self._is_multivector or _get_nearest_metric(nearest) == "hamming":
            results = [
                _add_query_index(
                    _search_vector_shard(
                        self._dataset,
                        index_segments=index_segments,
                        fallback_fragment_ids=fragments,
                        base_scanner_options=scanner_options,
                        nearest={**nearest, "q": query},
                        candidate_k=candidate_k,
                        per_query=False,
                        is_multivector=self._is_multivector,
                    ),
                    query_index,
                )
                for query_index, query in enumerate(query_batch)
            ]
            return pa.concat_tables(results, promote_options="default")

        return _search_vector_shard(
            self._dataset,
            index_segments=index_segments,
            fallback_fragment_ids=fragments,
            base_scanner_options=scanner_options,
            nearest={**nearest, "q": query_batch},
            candidate_k=candidate_k,
            per_query=True,
        )


@dataclass(frozen=True)
class _SearchRequest:
    nearest: dict[str, Any]
    scanner_options: dict[str, Any]
    k: int
    candidate_k: int
    include_row_id: bool
    fast_search: bool


@dataclass(eq=False)
class _RequestSlot:
    task: Optional[asyncio.Task[Any]] = None
    input_consumed: threading.Event = field(default_factory=threading.Event)
    abandoned: bool = False
    released: bool = False


class VectorSearchSession:
    """A snapshot-pinned, actor-backed vector search session."""

    # Scheduling granularity, not a public memory budget. Never split one
    # multivector query into separate queries.
    _QUERY_CHUNK_SIZE = 128

    def __init__(
        self,
        *,
        dataset: LanceDataset,
        vector_type: pa.DataType,
        snapshot: _DatasetSnapshot,
        column: str,
        metric: str,
        index_name: Optional[str],
        plans: list[_ActorPlan],
        max_concurrent_requests: int,
        actor_options: VectorSearchActorOptions,
    ):
        self._dataset = dataset
        self.vector_type = vector_type
        self._column = column
        self._metric = metric
        self._version = snapshot.version
        self._is_multivector = _streaming_is_multivector_type(vector_type)
        self._max_concurrent_requests = max_concurrent_requests
        self._slots: set[_RequestSlot] = set()
        self._capacity = asyncio.Condition()
        self._closing = threading.Event()
        self._closed = threading.Event()
        self._close_lock = threading.Lock()
        self._failure: Optional[BaseException] = None
        self.actor_states: list[dict[str, Any]] = []
        self.prewarm_results: list[dict[str, Any]] = []
        self._actors: list[Any] = []

        remote_args = dict(actor_options.ray_remote_args or {})
        remote_args.setdefault("num_cpus", 1)
        actor_class = cast(Any, _VectorSearchActor).options(**remote_args)
        try:
            for plan in plans:
                self._actors.append(
                    actor_class.remote(
                        snapshot, plan, index_name, self._is_multivector, actor_options
                    )
                )
            if self._actors:
                self.actor_states = ray.get(
                    [actor.ready.remote() for actor in self._actors]
                )
                if actor_options.prewarm_index:
                    self.prewarm_results = ray.get(
                        [actor.prewarm.remote() for actor in self._actors]
                    )
        except BaseException:
            for actor in self._actors:
                ray.kill(actor, no_restart=True)
            raise

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop, name="lance-ray-search", daemon=True
        )
        self._thread.start()

    @property
    def column(self) -> str:
        return self._column

    @property
    def metric(self) -> str:
        return self._metric

    @property
    def version(self) -> int:
        return self._version

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def __enter__(self) -> VectorSearchSession:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    async def __aenter__(self) -> VectorSearchSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.aclose()

    def _schedule(self, coroutine: Coroutine[Any, Any, Any]) -> Future[Any]:
        # The lock also prevents submitting a coroutine to a stopped loop.
        with self._close_lock:
            if self._closing.is_set():
                coroutine.close()
                raise RuntimeError("VectorSearchSession is closing or closed")
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop)

    async def _acquire(self, stop: Optional[threading.Event] = None) -> _RequestSlot:
        async with self._capacity:
            while True:
                if stop is not None and stop.is_set():
                    raise asyncio.CancelledError
                if self._closing.is_set():
                    raise RuntimeError("VectorSearchSession is closing or closed")
                if self._failure is not None:
                    raise RuntimeError(
                        "A search actor died; close and reopen VectorSearchSession"
                    ) from self._failure
                if len(self._slots) < self._max_concurrent_requests:
                    slot = _RequestSlot()
                    self._slots.add(slot)
                    return slot
                await self._capacity.wait()

    async def _release(self, slot: _RequestSlot) -> None:
        async with self._capacity:
            slot.released = True
            slot.input_consumed.set()
            self._slots.discard(slot)
            self._capacity.notify_all()

    async def _abandon(self, slot: _RequestSlot) -> None:
        if slot.abandoned:
            return
        slot.abandoned = True
        if slot.task is not None and not slot.task.done():
            slot.task.cancel()
        else:
            await self._release(slot)

    def _cleanup_slot(self, slot: _RequestSlot, *, abandon: bool = False) -> None:
        with self._close_lock:
            if self._closing.is_set():
                return  # close() already owns draining and releasing these slots.
            operation = self._abandon(slot) if abandon else self._release(slot)
            future = asyncio.run_coroutine_threadsafe(operation, self._loop)
        future.result()

    async def _blocking(self, function: Any, *args: Any) -> Any:
        task = asyncio.create_task(asyncio.to_thread(function, *args))
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                if task.cancelled():
                    raise
                # Repeated cancellation must not detach running native work.
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _actor_results(self, refs: list[Any]) -> list[pa.Table]:
        pending = asyncio.gather(
            *(asyncio.wrap_future(ref.future()) for ref in refs),
            return_exceptions=True,
        )
        cancelled = False
        while True:
            try:
                results = await asyncio.shield(pending)
                break
            except asyncio.CancelledError:
                if pending.cancelled():
                    raise
                if not cancelled:
                    for ref in refs:
                        # A synchronous actor may already be running native code.
                        with suppress(Exception):
                            ray.cancel(ref, force=False)
                cancelled = True
        for result in results:
            if isinstance(result, ray.exceptions.ActorDiedError):
                async with self._capacity:
                    self._failure = result
                    self._capacity.notify_all()
        if cancelled:
            raise asyncio.CancelledError
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return cast(list[pa.Table], results)

    def _prepare(
        self, query: Any, nearest: dict[str, Any], options: dict[str, Any], batch: bool
    ) -> tuple[Any, pa.Schema, bool]:
        import numpy as np

        is_batch = batch or _is_batch_query(query, self._is_multivector)
        queries: Any = (
            _canonical_multivector_batch(query, self.metric)
            if self._is_multivector
            else _canonical_query_batch(query, self.metric)
        )
        vector_type: Any = self.vector_type
        if self._is_multivector:
            vector_type = vector_type.value_type
        dimension = vector_type.list_size
        if not is_batch and len(queries) == 0:
            raise ValueError("A single query must not be empty")
        if self._is_multivector:
            for item in queries:
                if len(item) == 0 or item.shape[1] != dimension:
                    raise ValueError(
                        "Each multivector query must have shape [M, D], M > 0"
                    )
        elif len(queries) and queries.shape[1] != dimension:
            raise ValueError(f"Query vector dimension must be {dimension}")

        dtype = np.uint8 if self.metric == "hamming" else np.float32
        # Schema depends on the projection and column, not the query count.
        # Avoid converting an entire large batch just to discover its schema.
        schema_query = (
            queries[0]
            if len(queries)
            else np.ones(
                (1, dimension) if self._is_multivector else (dimension,), dtype=dtype
            )
        )
        schema = _resolve_vector_search_schema(
            self._dataset,
            nearest={**nearest, "q": schema_query},
            base_scanner_options=options,
            include_row_id=True,
        )
        fields = [field for field in schema if field.name != "query_index"]
        schema = pa.schema(
            [pa.field("query_index", pa.int64(), nullable=False), *fields]
        )
        return queries, schema, is_batch

    def _request_options(
        self,
        nearest: dict[str, Any],
        columns: Optional[list[str] | dict[str, str]],
        filter: Optional[Any],
        fast_search: bool,
        scanner_options: Optional[dict[str, Any]],
        oversample_factor: float = 1.0,
    ) -> _SearchRequest:
        conflicts = {"q", "column", "metric", "distance_type"} & nearest.keys()
        if conflicts:
            raise ValueError(
                "Query input and instance options cannot be supplied in nearest: "
                + ", ".join(sorted(conflicts))
            )
        nearest = dict(nearest)
        global_k, candidate_k = _candidate_k(nearest, oversample_factor)
        if fast_search and not nearest.get("use_index", True):
            raise ValueError("use_index=False cannot be combined with fast_search=True")
        options = dict(scanner_options or {})
        _validate_search_scanner_options(options)
        if options.get("prefilter", True) is not True:
            raise ValueError("Distributed vector search only supports prefilter=True")
        include_row_id = _projection_includes_row_id(columns, options)
        columns = columns if columns is not None else options.get("columns")
        if columns is not None:
            if "query_index" in columns:
                raise ValueError("query_index is managed by batch vector search")
            columns = columns.copy()
            if isinstance(columns, list) and "_distance" not in columns:
                columns.append("_distance")
            options["columns"] = columns
        if filter is not None:
            options["filter"] = filter
        options.update(prefilter=True, with_row_id=True)
        nearest.update(column=self.column, metric=self.metric)
        return _SearchRequest(
            nearest=nearest,
            scanner_options=options,
            k=global_k,
            candidate_k=candidate_k,
            include_row_id=include_row_id,
            fast_search=fast_search,
        )

    def _submit_search(
        self,
        query: Any,
        *,
        nearest: dict[str, Any],
        columns: Optional[list[str] | dict[str, str]],
        filter: Optional[Any],
        fast_search: bool,
        scanner_options: Optional[dict[str, Any]],
        oversample_factor: float = 1.0,
    ) -> Future[Any]:
        request = self._request_options(
            nearest, columns, filter, fast_search, scanner_options, oversample_factor
        )
        return self._schedule(self._execute(query, request))

    async def _execute(
        self,
        query: Any,
        request: _SearchRequest,
        *,
        batch: bool = False,
        slot: Optional[_RequestSlot] = None,
    ) -> tuple[pa.Table, int]:
        hold_for_delivery = slot is not None
        slot = slot if slot is not None else await self._acquire()
        if slot.released:
            raise RuntimeError("Search request was cancelled before execution")
        slot.task = asyncio.current_task()
        try:
            if self._failure is not None:
                raise RuntimeError(
                    "A search actor died; reopen the session"
                ) from self._failure
            try:
                queries, schema, is_batch = await self._blocking(
                    self._prepare,
                    query,
                    request.nearest,
                    request.scanner_options,
                    batch,
                )
            finally:
                # The source may reuse its buffer when next() is called again.
                slot.input_consumed.set()
            pieces = []
            for offset in range(0, len(queries), self._QUERY_CHUNK_SIZE):
                if self._failure is not None:
                    raise RuntimeError(
                        "A search actor died; reopen the session"
                    ) from self._failure
                query_ref = ray.put(queries[offset : offset + self._QUERY_CHUNK_SIZE])
                refs = []
                try:
                    for actor in self._actors:
                        refs.append(
                            actor.search.remote(
                                query_ref,
                                request.nearest,
                                request.candidate_k,
                                request.scanner_options,
                                request.fast_search,
                            )
                        )
                except BaseException:
                    # Retain admission until already submitted work is drained.
                    await self._actor_results(refs)
                    raise
                tables = await self._actor_results(refs)
                piece = await self._blocking(
                    self._merge_chunk, tables, request.k, schema, offset
                )
                pieces.append(piece)
                # Each request submits at most one fan-out at a time.
                await asyncio.sleep(0)
            result = await self._blocking(
                self._assemble, pieces, schema, is_batch, request.include_row_id
            )
            return result, len(queries)
        finally:
            slot.input_consumed.set()
            # Both results and errors retain streaming capacity until delivery.
            if not hold_for_delivery or slot.abandoned:
                await self._release(slot)

    @staticmethod
    def _merge_chunk(
        tables: list[pa.Table], k: int, schema: pa.Schema, offset: int
    ) -> pa.Table:
        result = _merge_vector_search_results(tables, k, per_query=True)
        if not result.num_rows:
            return pa.Table.from_batches([], schema=schema)
        result = _offset_query_index(result, offset, output_type=pa.int64())
        return result.select(schema.names)

    @staticmethod
    def _assemble(
        pieces: list[pa.Table], schema: pa.Schema, batch: bool, include_row_id: bool
    ) -> pa.Table:
        result = (
            pa.concat_tables(pieces, promote_options="default")
            if pieces
            else pa.Table.from_batches([], schema=schema)
        )
        if not batch:
            result = result.drop_columns(["query_index"])
        if not include_row_id and "_rowid" in result.column_names:
            result = result.drop_columns(["_rowid"])
        return result

    def search(
        self,
        query: Any,
        *,
        nearest: dict[str, Any],
        columns: Optional[list[str] | dict[str, str]] = None,
        filter: Optional[Any] = None,
        fast_search: bool = False,
        scanner_options: Optional[dict[str, Any]] = None,
    ) -> pa.Table:
        """Search one query or finite batch, blocking until its complete result."""
        result, _ = self._submit_search(
            query,
            nearest=nearest,
            columns=columns,
            filter=filter,
            fast_search=fast_search,
            scanner_options=scanner_options,
        ).result()
        return cast(pa.Table, result)

    async def search_async(
        self,
        query: Any,
        *,
        nearest: dict[str, Any],
        columns: Optional[list[str] | dict[str, str]] = None,
        filter: Optional[Any] = None,
        fast_search: bool = False,
        scanner_options: Optional[dict[str, Any]] = None,
    ) -> pa.Table:
        """Search independently of other arrivals, without blocking the caller's loop."""
        future = self._submit_search(
            query,
            nearest=nearest,
            columns=columns,
            filter=filter,
            fast_search=fast_search,
            scanner_options=scanner_options,
        )
        result, _ = await asyncio.wrap_future(future)
        return cast(pa.Table, result)

    def map_batches(
        self,
        query_batches: Iterable[Any],
        *,
        nearest: dict[str, Any],
        columns: Optional[list[str] | dict[str, str]] = None,
        filter: Optional[Any] = None,
        fast_search: bool = False,
        scanner_options: Optional[dict[str, Any]] = None,
    ) -> Generator[pa.Table, None, None]:
        """Search an iterable of query batches with bounded requests in flight.

        The driver canonicalizes each input batch, places each query chunk in
        Ray's object store once, broadcasts the resulting reference to all search
        actors, merges their local candidates per query, and yields the completed
        global top-k table. Requests share the session's
        ``max_concurrent_requests`` limit. Close the iterator when abandoning a
        stream.

        Args:
            query_batches: Iterable of regular vector batches shaped ``[B, D]``
                or multivector batches shaped ``[B, M, D]`` or a sequence of
                ``[M_i, D]`` arrays.

        Yields:
            PyArrow tables in input-batch order. ``query_index`` is an Int64
            position in the complete stream, not an index local to the batch.
        """
        request = self._request_options(
            nearest, columns, filter, fast_search, scanner_options
        )
        completed: queue.Queue[Any] = queue.Queue()
        stop = threading.Event()
        lock = threading.Lock()
        owned: set[_RequestSlot] = set()
        end = object()

        def producer() -> None:
            iterator: Any = None
            held_slot = None
            try:
                iterator = iter(query_batches)
                while not stop.is_set():
                    with lock:
                        if stop.is_set():
                            break
                        reservation = self._schedule(self._acquire(stop))
                    slot = reservation.result()
                    held_slot = slot
                    with lock:
                        owned.add(slot)
                    if stop.is_set():
                        break
                    try:
                        query = next(iterator)
                    except StopIteration:
                        self._cleanup_slot(slot)
                        with lock:
                            owned.discard(slot)
                        held_slot = None
                        break
                    if stop.is_set():
                        break
                    future = self._schedule(
                        self._execute(query, request, batch=True, slot=slot)
                    )
                    completed.put((slot, future))
                    held_slot = None
                    slot.input_consumed.wait()
            except BaseException as exc:
                completed.put(exc)
            finally:
                # Do not join this thread on close: next(iterator) can block in
                # caller-owned I/O. Abandoned reservations are revoked below.
                try:
                    if held_slot is not None:
                        self._cleanup_slot(held_slot, abandon=True)
                    close_iterator = getattr(iterator, "close", None)
                    if close_iterator is not None:
                        close_iterator()
                except BaseException as exc:
                    completed.put(exc)
                finally:
                    completed.put(end)

        thread = threading.Thread(
            target=producer, name="lance-ray-query-input", daemon=True
        )
        thread.start()
        offset = 0
        try:
            while True:
                item = completed.get()
                if item is end:
                    break
                if isinstance(item, BaseException):
                    raise item
                slot, future = item
                result, count = future.result()
                result = _offset_query_index(result, offset, output_type=pa.int64())
                offset += count
                self._cleanup_slot(slot)
                with lock:
                    owned.discard(slot)
                yield result
        finally:
            stop.set()
            with lock:
                slots = list(owned)
            for slot in slots:
                self._cleanup_slot(slot, abandon=True)

    async def _drain(self) -> None:
        async with self._capacity:
            self._capacity.notify_all()
        # Queued acquisitions wake and reject admission. Accepted requests keep
        # running, even if a stream consumer is no longer pulling results.
        tasks = [slot.task for slot in self._slots if slot.task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for slot in list(self._slots):
            await self._release(slot)
        await asyncio.sleep(0)
        for actor in self._actors:
            ray.kill(actor, no_restart=True)
        self._actors.clear()
        await self._loop.shutdown_default_executor()

    def close(self) -> None:
        """Reject new requests, drain accepted work, then release owned actors."""
        with self._close_lock:
            if self._closed.is_set():
                return
            already_closing = self._closing.is_set()
            if not already_closing:
                self._closing.set()
                future = asyncio.run_coroutine_threadsafe(self._drain(), self._loop)
        if already_closing:
            self._closed.wait()
            return
        try:
            future.result()
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            self._loop.close()
            self._closed.set()

    async def aclose(self) -> None:
        """Drain and close without blocking the caller's event loop."""
        await asyncio.to_thread(self.close)


def _is_batch_query(query: Any, multivector: bool) -> bool:
    import numpy as np

    if isinstance(query, pa.Table | pa.RecordBatch):
        return True
    if isinstance(query, pa.Array | pa.ChunkedArray):
        data_type = query.type
        list_like = (
            pa.types.is_fixed_size_list(data_type)
            or pa.types.is_list(data_type)
            or pa.types.is_large_list(data_type)
        )
        if multivector:
            return list_like and pa.types.is_fixed_size_list(data_type.value_type)
        return list_like
    try:
        array = np.asarray(query)
    except ValueError:
        return True
    return array.ndim >= (3 if multivector else 2) or array.size == 0


def _build_driver_dataset(
    uri: str | LanceDataset | None,
    *,
    storage_options: Optional[dict[str, Any]],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    block_size: Optional[int],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    branch: Optional[str],
    version: int | str | None,
) -> tuple[LanceDataset, _DatasetSnapshot]:
    if branch is not None and version is not None:
        raise ValueError("branch and version are mutually exclusive")

    merged_storage_options = dict(storage_options or {})
    if isinstance(uri, LanceDataset):
        dataset = uri
        dataset_uri = dataset.uri
        if branch is not None:
            dataset = dataset.checkout_version((branch, None))
            dataset_uri = (
                f"{dataset_uri.partition('/tree/')[0].rstrip('/')}/tree/{branch}"
            )
        elif version is not None:
            dataset = dataset.checkout_version(version)
        if not merged_storage_options:
            merged_storage_options.update(_get_dataset_storage_options(dataset))
    else:
        validate_uri_or_namespace(uri, namespace_impl, table_id)
        dataset_uri, merged_storage_options = resolve_namespace_table(
            uri,
            storage_options,
            namespace_impl,
            namespace_properties,
            table_id,
        )
        kwargs: dict[str, Any] = {
            "storage_options": merged_storage_options,
            **get_namespace_kwargs(
                namespace_impl,
                namespace_properties,
                table_id,
            ),
        }
        if block_size is not None:
            kwargs["block_size"] = block_size
        if base_store_params is not None:
            kwargs["base_store_params"] = base_store_params
        dataset = LanceDataset(dataset_uri, **kwargs)
        if branch is not None:
            dataset = dataset.checkout_version((branch, None))
            dataset_uri = f"{dataset_uri.rstrip('/')}/tree/{branch}"
        elif version is not None:
            dataset = dataset.checkout_version(version)

    snapshot = _DatasetSnapshot(
        uri=dataset_uri,
        version=dataset.version,
        serialized_manifest=dataset._ds.serialized_manifest(),
        storage_options=merged_storage_options,
        base_store_params=base_store_params,
        block_size=block_size,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
    )
    if isinstance(uri, LanceDataset):
        # A caller can mutate its LanceDataset (e.g. add_columns). Keep the
        # driver's schema planning pinned just like the actors' data scans.
        dataset = _open_snapshot(
            snapshot, index_cache_size_bytes=None, metadata_cache_size_bytes=None
        )
    return dataset, snapshot


def open_vector_search(
    uri: str | LanceDataset | None = None,
    *,
    column: str,
    metric: Optional[str] = None,
    index_name: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    branch: Optional[str] = None,
    version: int | str | None = None,
    max_concurrent_requests: int = 4,
    actor_options: Optional[VectorSearchActorOptions] = None,
) -> VectorSearchSession:
    """Open a reusable distributed vector search session.

    The session pins the dataset manifest when it opens, assigns index segments
    and uncovered fragments to persistent Ray actors, and reuses each actor's
    Lance session and index cache across requests. Use the returned object
    as a context manager so the actors are stopped when the session closes.

    Queries and per-request options are supplied later through
    :meth:`VectorSearchSession.search`, :meth:`VectorSearchSession.search_async`,
    or :meth:`VectorSearchSession.map_batches`.

    Args:
        uri: Lance dataset object or dataset URI. In URI mode, provide either
            ``uri`` or namespace parameters (``namespace_impl`` + ``table_id``).
            An already checked-out dataset retains its exact manifest.
        column: Vector column to search for the lifetime of the session.
        metric: Distance metric. If omitted, use the selected index's metric,
            or L2 when no index exists.
        index_name: Optional vector index name. If omitted, the first vector
            index covering ``column`` with a compatible metric is selected.
        storage_options: Storage options used to open the dataset.
        base_store_params: Runtime options for registered external base paths.
        block_size: Optional dataset I/O block size in bytes.
        namespace_impl: Namespace implementation, such as ``"dir"`` or
            ``"rest"``.
        namespace_properties: Properties used to connect to the namespace.
        table_id: Table identifier used with namespace parameters.
        branch: Branch resolved and pinned when the session opens. Mutually
            exclusive with ``version``.
        version: Dataset version or tag to pin. Mutually exclusive with
            ``branch``.
        max_concurrent_requests: Maximum number of requests in flight across
            all entry points. Each stream input batch counts as one request,
            retaining capacity until its result or error is delivered. Defaults
            to 4. This limits request count, not bytes or result size.
        actor_options: Actor count, Ray resources, cache sizes, and optional
            index prewarming.

    Returns:
        A snapshot-pinned :class:`VectorSearchSession`.

    Example:
        >>> with open_vector_search(
        ...     "dataset.lance",
        ...     column="vector",
    ...     max_concurrent_requests=2,
    ... ) as search:
        ...     for result in search.map_batches(
    ...         query_batches, nearest={"k": 10, "nprobes": 8}
    ...     ):
        ...         write_result(result)
    """
    actor_options = actor_options or VectorSearchActorOptions()
    if not isinstance(max_concurrent_requests, int) or max_concurrent_requests <= 0:
        raise ValueError("max_concurrent_requests must be a positive integer")
    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")
    dataset, snapshot = _build_driver_dataset(
        uri,
        storage_options=storage_options,
        base_store_params=base_store_params,
        block_size=block_size,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        branch=branch,
        version=version,
    )
    if "query_index" in dataset.schema.names:
        raise ValueError(
            "Batch vector search cannot use a dataset containing column 'query_index'"
        )
    try:
        field = resolve_arrow_field_path(dataset.schema, column)
    except KeyError as exc:
        raise ValueError(f"Column {column!r} not found in dataset") from exc
    vector_type = field.field.type
    if not (
        pa.types.is_fixed_size_list(vector_type)
        or _streaming_is_multivector_type(vector_type)
    ):
        raise ValueError(
            "Vector column must be FixedSizeList<D> or List<FixedSizeList<D>>"
        )
    metric = _normalize_metric(metric) if metric is not None else None
    selected = None
    for index in dataset.describe_indices():
        name = str(_index_value(index, "name"))
        if index_name is not None and name != index_name:
            continue
        fields = _index_value(index, "field_names")
        if fields is None:
            fields = _index_value(index, "fields", [])
        if field.path not in _canonical_index_field_names(fields):
            if index_name is not None:
                raise ValueError(f"Index {index_name!r} does not cover {column!r}")
            continue
        # Scalar indexes on the same column cannot provide vector candidates.
        if not str(_index_value(index, "index_type", "")).upper().startswith("IVF"):
            if index_name is not None:
                raise ValueError(f"Index {index_name!r} is not a vector index")
            continue
        index_metric = _normalize_metric(_get_index_metric(dataset, index))
        if metric is not None and metric != index_metric:
            if index_name is not None:
                raise ValueError(
                    f"Index metric {index_metric!r} does not match {metric!r}"
                )
            continue
        selected = index
        metric = index_metric
        break
    if index_name is not None and selected is None:
        raise ValueError(f"Vector index {index_name!r} was not found")
    metric = metric or "l2"
    plans = _plan_streaming_vector_search(
        fragments=dataset.get_fragments(),
        vector_index=selected,
        num_actors=actor_options.num_actors,
    )
    return VectorSearchSession(
        dataset=dataset,
        vector_type=vector_type,
        snapshot=snapshot,
        column=field.path,
        metric=metric,
        index_name=str(_index_value(selected, "name"))
        if selected is not None
        else None,
        plans=plans,
        max_concurrent_requests=max_concurrent_requests,
        actor_options=actor_options,
    )


def _normalize_metric(metric: str) -> str:
    metric = metric.lower()
    metric = {"euclidean": "l2", "ip": "dot", "inner_product": "dot"}.get(
        metric, metric
    )
    if metric not in {"l2", "cosine", "dot", "hamming"}:
        raise ValueError(f"Unsupported vector search metric: {metric!r}")
    return metric
