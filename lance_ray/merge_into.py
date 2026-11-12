# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Distributed merge for Lance datasets on Ray.

This module provides a fragment-parallel, atomic merge: every source row that
matches a target row on the join key replaces that row (all columns). If
the same key appears more than once in the target, every matching target
row is updated (join-all, matching pylance ``merge_insert``). Every
source row with no match is inserted. The plan executes across Ray
workers so that neither the source rows nor the rewritten fragments ever have
to fit on the driver:

0. DEDUPE (distributed): source rows are cast to the target schema first,
   so key equality uses the type the lookup will see. The cast source is
   then range-partitioned with a Ray Data sort on the join key -- all
   copies of a key land adjacent in exactly one block -- and one arbitrary
   row per key is kept by dropping adjacent duplicates per block. The sort
   also gives the plan phase contiguous key slices, so each chunk's index
   lookups stay within few BTREE leaf pages.
1. PLAN (distributed): each plan task takes one source chunk and maps every
   join key to its target fragment id using batched ``key IN (...)`` lookups
   against the target dataset (``_rowaddr >> 32`` = fragment id; served by the
   scalar index on the key column when one exists). Each plan task then
   hash-partitions its rows into per-apply-task buckets with a static
   ownership function -- ``owner(fragment_id) = crc32(fragment_id) % num_workers``
   -- and yields only the non-empty buckets as separate Ray objects (a
   map-side shuffle). ``num_partitions`` only controls how many source
   chunks (plan tasks) run; it does not multiply the apply fan-out. The
   driver only receives small metadata; bucket bytes move from plan node to
   apply node directly through the Ray object store. Apply tasks pull those
   buckets one at a time, so a larger ``num_partitions`` bounds the
   deserialized source held inside one worker. The object store still keeps
   the deduplicated source and every bucket until commit.
2. APPLY (distributed): each apply task owns a disjoint set of target
   fragments (guaranteed by the ownership function). Updates are
   merge-on-read: for every owned fragment the task writes a *deletion file*
   marking the matched rows dead (addressed by the physical offsets from
   ``_rowaddr``, so no fragment data is rescanned), and the replacement
   values, together with the rows that had no match, are appended as
   brand-new fragments. On datasets with stable row IDs, replacement
   fragments keep the matched rows' logical ``_rowid`` values; inserts
   receive newly assigned IDs.
3. COMMIT (driver): the per-task results are unioned into a single
   ``lance.LanceOperation.Update`` and committed once, so the whole merge is
   one atomic version change (all-or-nothing). Concurrent appends are
   rebased inside ``LanceDataset.commit``. If that call raises after the
   operation is already visible in the latest manifest, the driver returns
   the latest dataset instead of failing (so a caller retry cannot
   double-insert). A concurrent rewrite, remove, or merge-on-read update of
   a fragment this merge modifies still fails. Every writer, including a
   plain append, must stay serialized with this merge: an append of a key
   this merge also inserts is invisible at ``read_version`` and both commits
   succeed. Data files and deletion files written before a failed apply or
   commit stay in storage until ``cleanup_old_versions``.

Example:
    >>> import lance_ray as lr
    >>> dataset = lr.merge_into(source, "/path/to/table.lance", on="id", num_workers=4)
    >>> dataset.version
    5
"""

from __future__ import annotations

import collections
import datetime
import json
import logging
import math
import pickle
import time
import zlib
from collections.abc import Callable, Iterator
from decimal import Context, Decimal, InvalidOperation, localcontext
from functools import partial
from typing import Any, Optional, Protocol, TypeVar

import lance
import pyarrow as pa
import pyarrow.compute as pc
import ray
import ray.data

from .utils import (
    get_namespace_kwargs,
    get_write_fragments_kwargs,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

__all__ = [
    "merge_into",
]

logger = logging.getLogger(__name__)

# Number of join keys per ``IN (...)`` index lookup in the plan phase.
_LOOKUP_BATCH_SIZE = 10_000

# Preferred helper column names shipped inside the plan buckets. If the
# target schema already uses a name, ``_helper_column_names`` picks the
# next free ``_2`` / ``_3`` / ... suffix from a shared taken set.
# ``_rowaddr``'s low 32 bits are the local physical offsets
# ``LanceFragment.delete_rows`` consumes. Logical ``_rowid`` is kept so
# stable-row-id replacements can reuse it.
_ROWID_COLUMN = "__merge_into_rowid"
_OFFSET_COLUMN = "__merge_into_offset"
_SORT_COLUMN = "__merge_into_sort_key"

_T = TypeVar("_T")
_NamespaceArgs = tuple[str | None, dict[str, str] | None, list[str] | None]


class _RemoteTask(Protocol):
    @property
    def remote(self) -> Callable[..., ray.ObjectRef[Any] | ray.ObjectRefGenerator]: ...


def _unused_column_name(preferred: str, taken: set[str]) -> str:
    if preferred not in taken:
        return preferred
    suffix = 2
    while True:
        candidate = f"{preferred}_{suffix}"
        if candidate not in taken:
            return candidate
        suffix += 1


def _helper_column_names(schema: pa.Schema) -> tuple[str, str]:
    """Pick rowid/offset helper names that do not collide with ``schema``.

    Both names are reserved from the same ``taken`` set, in order, so they
    cannot collide with user fields or with each other.
    """
    taken = set(schema.names)
    rowid_column = _unused_column_name(_ROWID_COLUMN, taken)
    taken.add(rowid_column)
    offset_column = _unused_column_name(_OFFSET_COLUMN, taken)
    return rowid_column, offset_column


# ---------------------------------------------------------------------------
# helpers shared by driver and workers
# ---------------------------------------------------------------------------


_JOIN_KEY_TYPE_HELP = (
    "boolean, integer, floating, string, date, timestamp, time, decimal, or binary"
)


def _unwrap_dictionary_type(arrow_type: pa.DataType) -> pa.DataType:
    while pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type
    return arrow_type


def _is_string_type(arrow_type: pa.DataType) -> bool:
    return bool(
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or getattr(pa.types, "is_string_view", lambda _t: False)(arrow_type)
    )


def _is_binary_type(arrow_type: pa.DataType) -> bool:
    return bool(
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
        or getattr(pa.types, "is_binary_view", lambda _t: False)(arrow_type)
    )


def _is_supported_join_key_type(arrow_type: pa.DataType) -> bool:
    arrow_type = _unwrap_dictionary_type(arrow_type)
    return bool(
        pa.types.is_boolean(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or _is_string_type(arrow_type)
        or pa.types.is_date(arrow_type)
        or pa.types.is_timestamp(arrow_type)
        or pa.types.is_time(arrow_type)
        or pa.types.is_decimal(arrow_type)
        or _is_binary_type(arrow_type)
    )


def _raise_unless_supported_join_key(on: str, arrow_type: pa.DataType) -> None:
    if _is_supported_join_key_type(arrow_type):
        return
    raise TypeError(
        f"Join key column {on!r} has unsupported type {arrow_type}; "
        f"merge_into supports {_JOIN_KEY_TYPE_HELP} keys."
    )


def _sql_string_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_identifier(name: str) -> str:
    """Quote a Lance SQL identifier, doubling any embedded backticks."""
    return "`" + name.replace("`", "``") + "`"


def _sql_date_literal(value: Any, arrow_type: pa.DataType) -> str:
    """Render a date key, using integer ticks outside Python's year range.

    ``datetime.date`` only accepts years 1 through 9999. Lance date32/date64
    values outside that range cannot survive ``to_pylist()`` or a ``DATE``
    literal, so the plan passes the underlying day or millisecond count and
    ``arrow_cast`` rebuilds the exact Arrow value.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        type_name = "Date64" if pa.types.is_date64(arrow_type) else "Date32"
        return f"arrow_cast({int(value)}, {_sql_string_literal(type_name)})"
    if isinstance(value, datetime.datetime):
        value = value.date()
    if not isinstance(value, datetime.date):
        value = pa.scalar(value, type=pa.date32()).as_py()
    return f"DATE '{value.isoformat()}'"


def _sql_temporal_literal(value: Any, arrow_type: pa.DataType) -> str:
    """Cast integer ticks to an exact temporal scalar, including its timezone.

    Python ``time`` loses nanoseconds, bare SQL TIMESTAMP uses microseconds,
    and Lance does not support SQL TIME literals. ``arrow_cast`` retains the
    Arrow unit/timezone and is constant-folded for scalar index lookups.
    """
    if not isinstance(arrow_type, pa.TimestampType | pa.Time32Type | pa.Time64Type):
        raise TypeError(f"Expected a temporal join key type, got {arrow_type}")
    unit = {
        "s": "Second",
        "ms": "Millisecond",
        "us": "Microsecond",
        "ns": "Nanosecond",
    }[arrow_type.unit]
    scalar = pa.scalar(value, type=arrow_type)
    if pa.types.is_time32(arrow_type):
        ticks = f"CAST({scalar.cast(pa.int32()).as_py()} AS INT)"
        type_name = f"Time32({unit})"
    else:
        ticks = str(scalar.cast(pa.int64()).as_py())
        if pa.types.is_timestamp(arrow_type):
            timezone = f"Some({json.dumps(arrow_type.tz)})" if arrow_type.tz else "None"
            type_name = f"Timestamp({unit}, {timezone})"
        else:
            type_name = f"Time64({unit})"
    return f"arrow_cast({ticks}, {_sql_string_literal(type_name)})"


def _join_key_values(column: pa.ChunkedArray[Any]) -> list[Any]:
    """Keep temporal and date keys as integer ticks on both sides of the lookup."""
    arrow_type = _unwrap_dictionary_type(column.type)
    if pa.types.is_timestamp(arrow_type) or pa.types.is_time(arrow_type):
        integer_type = pa.int32() if pa.types.is_time32(arrow_type) else pa.int64()
        return column.cast(arrow_type).cast(integer_type).to_pylist()
    if pa.types.is_date32(arrow_type):
        return column.cast(arrow_type).cast(pa.int32()).to_pylist()
    if pa.types.is_date64(arrow_type):
        return column.cast(arrow_type).cast(pa.int64()).to_pylist()
    return column.to_pylist()


def _integer_column_values(column: pa.ChunkedArray[Any]) -> list[int]:
    """Read non-null integer metadata without accepting missing row identities."""
    values: list[int] = []
    for value in column.to_pylist():
        if not isinstance(value, int):
            raise TypeError("Merge row identities must be non-null integers")
        values.append(value)
    return values


def _decimal_type_name(arrow_type: pa.DataType, precision: int, scale: int) -> str:
    family = "Decimal256" if pa.types.is_decimal256(arrow_type) else "Decimal128"
    return f"{family}({precision}, {scale})"


# 10**75 is the largest power of ten that fits in Decimal256 (76 digits).
_MAX_DECIMAL_POWER_EXPONENT = 75


def _negative_scale_power_factor(arrow_type: pa.DataType, exponent: int) -> str:
    """Precision-1 decimal equal to ``10**exponent``, stored at scale ``-exponent``.

    The scale-0 spelling of ``10**exponent`` has ``exponent + 1`` digits. A
    single factor stays within Decimal256. Callers split larger exponents.
    """
    power_text = "1" + ("0" * exponent)
    power_digits = len(power_text)
    power_family = "Decimal256" if power_digits > 38 else "Decimal128"
    scale0 = (
        f"arrow_cast({_sql_string_literal(power_text)}, "
        f"{_sql_string_literal(f'{power_family}({power_digits}, 0)')})"
    )
    return (
        f"arrow_cast({scale0}, "
        f"{_sql_string_literal(_decimal_type_name(arrow_type, 1, -exponent))})"
    )


def _negative_scale_power(arrow_type: pa.DataType, power: int) -> str:
    """Product of precision-1 factors equal to ``10**power``.

    ``decimal256(76, -76)`` needs ``10**76``, which is 77 digits and does not
    fit in one scale-0 Decimal256. ``10**75 * 10**1`` keeps every factor
    inside that limit, and each factor's scale is already negative so the
    coefficient multiply does not widen.
    """
    factors: list[str] = []
    remaining = power
    while remaining:
        chunk = min(remaining, _MAX_DECIMAL_POWER_EXPONENT)
        factors.append(_negative_scale_power_factor(arrow_type, chunk))
        remaining -= chunk
    return " * ".join(factors)


def _sql_negative_scale_decimal(
    quantized: Decimal, arrow_type: pa.DataType, precision: int, scale: int
) -> str:
    """Render a negative-scale decimal without widening the coefficient.

    Lance cannot cast a string onto a negative scale, and a bare number is
    parsed as Float64 once it no longer fits in an integer token. Multiplying
    the coefficient by a scale-0 power of ten overflows a full-precision value
    before the final cast. The power of ten is a precision-1 decimal, or a
    product of them when ``10**(-scale)`` has more than 76 digits. The product
    coefficient stays inside ``precision``.
    """
    coefficient = format(quantized.scaleb(scale), "f")
    coefficient_literal = (
        f"arrow_cast({_sql_string_literal(coefficient)}, "
        f"{_sql_string_literal(_decimal_type_name(arrow_type, precision, 0))})"
    )
    target_name = _decimal_type_name(arrow_type, precision, scale)
    return (
        f"arrow_cast({coefficient_literal} * {_negative_scale_power(arrow_type, -scale)}, "
        f"{_sql_string_literal(target_name)})"
    )


def _sql_decimal_literal(value: Any, arrow_type: pa.DataType) -> str:
    if not pa.types.is_decimal(arrow_type):
        raise TypeError(f"Expected a decimal join key type, got {arrow_type}")
    precision = int(arrow_type.precision)
    scale = int(arrow_type.scale)
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    # The process-wide decimal context defaults to precision 28. A negative
    # scale makes the scaled integer longer than ``precision`` digits.
    scaled_digits = precision + max(-scale, 0)
    with localcontext(Context(prec=max(scaled_digits, 1))):
        try:
            quantized = value.quantize(Decimal(1).scaleb(-scale))
        except InvalidOperation as exc:
            raise ValueError(
                f"Decimal join key {value} does not fit DECIMAL({precision},{scale})"
            ) from exc
        if scale < 0:
            return _sql_negative_scale_decimal(quantized, arrow_type, precision, scale)
    rendered = format(quantized, "f")
    # Lance 12 parses a SQL DECIMAL literal as Decimal128, so a Decimal256
    # value with more than 38 digits cannot use that syntax. arrow_cast keeps
    # the full precision.
    if pa.types.is_decimal256(arrow_type):
        type_name = _decimal_type_name(arrow_type, precision, scale)
        return (
            f"arrow_cast({_sql_string_literal(rendered)}, "
            f"{_sql_string_literal(type_name)})"
        )
    return f"DECIMAL({precision},{scale}) '{rendered}'"


def _sql_binary_literal(value: Any) -> str:
    raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    return "X'" + raw.hex() + "'"


def _sql_literal(value: Any, arrow_type: pa.DataType | None = None) -> str:
    """Render a join-key value as a Lance SQL literal for ``IN (...)``."""
    if arrow_type is None:
        if isinstance(value, bool):
            arrow_type = pa.bool_()
        elif isinstance(value, int):
            arrow_type = pa.int64()
        elif isinstance(value, float):
            arrow_type = pa.float64()
        elif isinstance(value, str):
            arrow_type = pa.string()
        elif isinstance(value, datetime.datetime):
            arrow_type = pa.timestamp("us")
        elif isinstance(value, datetime.date):
            arrow_type = pa.date32()
        elif isinstance(value, datetime.time):
            arrow_type = pa.time64("us")
        elif isinstance(value, Decimal):
            exponent = value.as_tuple().exponent
            if not isinstance(exponent, int):
                raise ValueError("Non-finite decimal join keys are unsupported")
            arrow_type = pa.decimal128(38, max(-exponent, 0))
        elif isinstance(value, bytes | bytearray | memoryview):
            arrow_type = pa.binary()
        else:
            raise TypeError(
                f"Unsupported join key type {type(value).__name__!r}; "
                f"merge_into supports {_JOIN_KEY_TYPE_HELP} keys."
            )
    arrow_type = _unwrap_dictionary_type(arrow_type)
    if pa.types.is_boolean(arrow_type):
        return "TRUE" if value else "FALSE"
    if pa.types.is_integer(arrow_type):
        number = int(value)
        # Lance parses a bare SQL number as Float64 when it does not fit in a
        # signed 64-bit token. That token covers -(2**63 - 1) through 2**63 - 1.
        # Int64's minimum and every UInt64 value at or above 2**63 fall outside
        # it, and the rounded float cannot be cast back to the original integer.
        # A string arrow_cast keeps every digit.
        if not (-(2**63 - 1) <= number <= 2**63 - 1):
            bits = int(arrow_type.bit_width)
            type_name = (
                f"Int{bits}"
                if pa.types.is_signed_integer(arrow_type)
                else f"UInt{bits}"
            )
            return (
                f"arrow_cast({_sql_string_literal(str(number))}, "
                f"{_sql_string_literal(type_name)})"
            )
        return str(number)
    if pa.types.is_floating(arrow_type):
        as_float = float(value)
        if not math.isfinite(as_float):
            raise ValueError(
                "Floating join keys must be finite; NaN and infinity are rejected."
            )
        rendered = repr(as_float)
        # Lance parses an untyped SQL float as Float64 and cannot cast that
        # literal onto a Float16 column.
        if pa.types.is_float16(arrow_type):
            return f"arrow_cast({rendered}, 'Float16')"
        return rendered
    if _is_string_type(arrow_type):
        return _sql_string_literal(value)
    if pa.types.is_date(arrow_type):
        return _sql_date_literal(value, arrow_type)
    if pa.types.is_timestamp(arrow_type) or pa.types.is_time(arrow_type):
        return _sql_temporal_literal(value, arrow_type)
    if pa.types.is_decimal(arrow_type):
        return _sql_decimal_literal(value, arrow_type)
    if _is_binary_type(arrow_type):
        return _sql_binary_literal(value)
    raise TypeError(
        f"Unsupported join key type {arrow_type}; "
        f"merge_into supports {_JOIN_KEY_TYPE_HELP} keys."
    )


def _chunked(seq: list[_T], n: int) -> Iterator[list[_T]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _align_source_batch(batch: Any, *, target_schema: pa.Schema, on: str) -> pa.Table:
    """Cast one source batch to ``target_schema`` before the dedupe sort."""
    if isinstance(batch, pa.RecordBatch):
        batch = pa.Table.from_batches([batch])
    if not isinstance(batch, pa.Table):
        raise TypeError("Merge source batches must be Arrow tables")
    if batch.num_rows == 0:
        return pa.Table.from_batches([], schema=target_schema)
    return _align_chunk(batch, target_schema, on)


def _align_chunk(chunk: pa.Table, target_schema: pa.Schema, on: str) -> pa.Table:
    """Project/cast a source chunk to the target schema and validate the key."""
    missing = [name for name in target_schema.names if name not in chunk.column_names]
    if missing:
        raise ValueError(f"Source is missing target-table columns: {missing}")
    chunk = chunk.select(target_schema.names)
    if chunk.schema != target_schema:
        chunk = chunk.cast(target_schema)
    key_column = chunk.column(on)
    if key_column.null_count:
        raise ValueError(f"Source contains null values in join key column {on!r}")
    _raise_if_nonfinite_float_keys(key_column, on)
    return chunk


def _raise_if_nonfinite_float_keys(
    column: pa.Array[Any] | pa.ChunkedArray[Any], on: str
) -> None:
    """Reject NaN and infinity before dedupe and SQL rendering.

    ``pc.not_equal(NaN, NaN)`` is true, and ``float('nan')`` does not collapse
    inside a Python ``set``, so neither the adjacent-duplicate drop nor the
    plan-task duplicate check would merge those rows. ``repr`` of NaN or
    infinity is also not a Lance SQL numeric literal.
    """
    arrow_type = _unwrap_dictionary_type(column.type)
    if not pa.types.is_floating(arrow_type):
        return
    values = column.cast(arrow_type) if pa.types.is_dictionary(column.type) else column
    if pa.types.is_float16(arrow_type):
        # Older Arrow releases have no half-float is_nan/is_inf kernels.
        # Float32 represents every half value exactly; only validation widens.
        values = values.cast(pa.float32())
    if pc.any(pc.is_nan(values)).as_py() or pc.any(pc.is_inf(values)).as_py():
        raise ValueError(
            f"Join key column {on!r} contains NaN or infinity. "
            "Floating join keys must be finite."
        )


def _raise_on_duplicate_keys(keys: list[Any], on: str, context: str) -> None:
    if len(keys) != len(set(keys)):
        raise ValueError(
            f"Duplicate join keys detected ({context}). Source rows are "
            f"deduplicated to one row per {on!r} key, so this indicates "
            "an internal routing error."
        )


def _raise_if_duplicate_integers(chunks: list[pa.Array[Any]], context: str) -> None:
    """Reject duplicate ids without building a Python ``set`` of every value."""
    if not chunks:
        return
    values = pa.chunked_array(chunks)
    n = len(values)
    if n <= 1:
        return
    ordered = values.take(pc.sort_indices(values))
    adjacent = pc.equal(ordered.slice(0, n - 1), ordered.slice(1))
    if pc.any(adjacent).as_py():
        raise RuntimeError(
            f"Duplicate target row ids detected ({context}). Each matched "
            "target row must be updated at most once."
        )


class _FragmentMatchIds:
    """Packed integer identities for one target fragment.

    Row ids and offsets are copied into compact Arrow buffers (about eight
    bytes each). A sliced or deserialized source column often aliases the
    whole bucket allocation; the copy lets that bucket be released while the
    identities are still stored. Row ids are released after the duplicate
    check. Offsets are handed to ``delete_rows`` and then released, before
    any replacement bucket is written. Preserved stable row ids are read
    later from the single bucket being written.
    """

    __slots__ = ("row_ids", "offsets")

    def __init__(self) -> None:
        self.row_ids: list[pa.Array[Any]] = []
        self.offsets: list[pa.Array[Any]] = []

    def append(
        self,
        row_ids: pa.Array[Any] | pa.ChunkedArray[Any],
        offsets: pa.Array[Any] | pa.ChunkedArray[Any],
    ) -> None:
        self.row_ids.append(_owned_integer_array(row_ids))
        self.offsets.append(_owned_integer_array(offsets))

    def release_row_ids_after_duplicate_check(self, context: str) -> None:
        _raise_if_duplicate_integers(self.row_ids, context)
        self.row_ids.clear()

    def take_offsets_after_duplicate_check(self, context: str) -> pa.ChunkedArray[Any]:
        _raise_if_duplicate_integers(self.offsets, context)
        offsets = pa.chunked_array(self.offsets)
        self.offsets.clear()
        return offsets


def _offsets_for_delete_rows(offsets: pa.ChunkedArray[Any]) -> Any:
    """Return offsets that ``LanceFragment.delete_rows`` can pass to ``int()``.

    ``delete_rows`` does ``[int(o) for o in offsets]``. Iterating a ChunkedArray
    yields ``Int64Scalar``, and some PyArrow builds reject ``int()`` on that
    scalar. A NumPy buffer keeps the values packed until that one call.
    """
    if len(offsets) == 0:
        return []
    return offsets.combine_chunks().to_numpy(zero_copy_only=False)


def _integer_chunks(
    column: pa.Array[Any] | pa.ChunkedArray[Any],
) -> list[pa.Array[Any]]:
    """Keep an integer column's buffers without copying values into Python."""
    if isinstance(column, pa.Array):
        chunks = [column]
        null_count = column.null_count
        column_type = column.type
    else:
        chunks = list(column.chunks)
        null_count = column.null_count
        column_type = column.type
    if null_count or not pa.types.is_integer(column_type):
        raise TypeError("Merge row identities must be non-null integers")
    return chunks


def _owned_integer_array(
    column: pa.Array[Any] | pa.ChunkedArray[Any],
) -> pa.Array[Any]:
    """Copy integer identities into a buffer that does not alias ``column``.

    ``to_numpy(zero_copy_only=False)`` still returns a view when the values
    are contiguous inside a larger parent buffer. ``ndarray.copy()`` allocates
    storage that dies with this array, so dropping the source bucket can free
    the rest of that buffer.
    """
    if isinstance(column, pa.Array):
        null_count = column.null_count
        column_type = column.type
    else:
        null_count = column.null_count
        column_type = column.type
    if null_count or not pa.types.is_integer(column_type):
        raise TypeError("Merge row identities must be non-null integers")
    if len(column) == 0:
        return pa.array([], type=column_type)
    values = column.to_numpy(zero_copy_only=False).copy()
    owned = pa.array(values, type=column_type)
    del values
    return owned


def _with_rowid_column(
    table: pa.Table,
    row_ids: list[int],
    rowid_column: str,
    *,
    rowid_type: pa.DataType,
) -> pa.Table:
    return table.append_column(rowid_column, pa.array(row_ids, type=rowid_type))


def _with_insert_rowid_column(
    table: pa.Table, rowid_column: str, *, rowid_type: pa.DataType
) -> pa.Table:
    """Attach a placeholder row-id column that inserts drop before writing.

    ``-1`` does not fit in Lance's UInt64 ``_rowid``. Nulls are legal for
    both signed and unsigned types, and the column is removed before the
    insert fragment is written. Commit assigns new ids.
    """
    placeholder = pa.nulls(table.num_rows, type=pa.int64()).cast(rowid_type)
    return table.append_column(rowid_column, placeholder)


def _with_match_identity(
    table: pa.Table,
    row_ids: list[int],
    offsets: list[int],
    rowid_column: str,
    offset_column: str,
    *,
    rowid_type: pa.DataType,
) -> pa.Table:
    return _with_rowid_column(
        table, row_ids, rowid_column, rowid_type=rowid_type
    ).append_column(offset_column, pa.array(offsets, type=pa.int64()))


def _rowaddr_parts(rowaddr: int) -> tuple[int, int]:
    """Split a Lance ``_rowaddr`` into ``(fragment_id, local_offset)``."""
    addr = int(rowaddr)
    return addr >> 32, addr & 0xFFFFFFFF


def _pack_plan_buckets(
    n_apply: int,
    source_chunk: pa.Table,
    updates_by_owner: list[dict[int, list[tuple[int, int, int]]]],
    inserts_by_owner: list[list[int]],
    rowid_column: str,
    offset_column: str,
    *,
    rowid_type: pa.DataType,
) -> tuple[list[int], list[dict[str, Any]], list[int]]:
    """Build apply-owner payloads, omitting owners with no rows.

    Returns ``(owners, buckets, bucket_rows)``. ``owners`` and ``buckets``
    are parallel and contain only non-empty slots; ``bucket_rows`` is dense
    over ``range(n_apply)`` so callers can still log per-owner counts.
    """
    owners: list[int] = []
    buckets: list[dict[str, Any]] = []
    bucket_rows: list[int] = []
    for owner in range(n_apply):
        frags: dict[int, pa.Table] = {}
        for fragment_id, pairs in updates_by_owner[owner].items():
            indices = [index for index, _, _ in pairs]
            row_ids = [rowid for _, rowid, _ in pairs]
            offsets = [offset for _, _, offset in pairs]
            frags[fragment_id] = _with_match_identity(
                source_chunk.take(indices),
                row_ids,
                offsets,
                rowid_column,
                offset_column,
                rowid_type=rowid_type,
            )
        inserts = None
        if inserts_by_owner[owner]:
            insert_table = source_chunk.take(inserts_by_owner[owner])
            inserts = _with_insert_rowid_column(
                insert_table, rowid_column, rowid_type=rowid_type
            )
        rows = sum(t.num_rows for t in frags.values()) + (
            inserts.num_rows if inserts is not None else 0
        )
        bucket_rows.append(rows)
        if rows:
            owners.append(owner)
            buckets.append({"frags": frags, "inserts": inserts})
    return owners, buckets, bucket_rows


def _key_bucket(key: Any, n: int) -> int:
    """Deterministic key -> bucket assignment for the plan shuffle
    (apply-task ownership, bucket by fragment id). One property matters:

    - **Process-stable.** Python's built-in ``hash()`` is salted per process,
      so two tasks running in different Ray workers would disagree on the
      bucket of the same key; crc32 over ``repr`` is stable everywhere.
    """
    return zlib.crc32(repr(key).encode("utf-8")) % n


def _drop_adjacent_duplicate_keys(batch: pa.Table, on: str) -> pa.Table:
    """Keep one row per key within a sorted block (keep-any).

    After ``Dataset.sort(on)`` all copies of a key are adjacent *and* live in
    the same range partition (Ray's sort assigns each key to exactly one
    half-open boundary range), so dropping adjacent duplicates per block is a
    complete, global dedupe.
    """
    keys = batch.column(on)
    if keys.null_count:
        raise ValueError(f"Source contains null values in join key column {on!r}")
    n = batch.num_rows
    if n <= 1:
        return batch
    changed = pc.not_equal(keys.slice(1), keys.slice(0, n - 1))
    chunks = changed.chunks if isinstance(changed, pa.ChunkedArray) else [changed]
    mask = pa.chunked_array([pa.array([True]), *chunks])
    return batch.filter(mask)


def _sort_helper_type(arrow_type: pa.DataType) -> pa.DataType | None:
    """Return a sortable projection of ``arrow_type``, or None.

    Dictionary columns and ``float16`` have no Arrow sort or inequality
    kernel. Temporal values and dates are projected to integer ticks so Ray's
    boundary sample does not round them through Python ``datetime``. Dates
    outside year 1–9999 overflow ``to_pylist()``. Decimals with a negative
    scale have no inequality kernel; their helper is the unscaled coefficient
    (same order and equality, and it always fits in ``precision`` digits).
    The original column stays in the batch; only the helper is sorted and
    compared.
    """
    value_type = _unwrap_dictionary_type(arrow_type)
    if pa.types.is_float16(value_type):
        return pa.float32()
    if pa.types.is_time32(value_type):
        return pa.int32()
    if pa.types.is_time(value_type) or pa.types.is_timestamp(value_type):
        return pa.int64()
    if pa.types.is_date32(value_type):
        return pa.int32()
    if pa.types.is_date64(value_type):
        return pa.int64()
    if pa.types.is_decimal(value_type) and int(value_type.scale) < 0:
        precision = int(value_type.precision)
        if pa.types.is_decimal256(value_type):
            return pa.decimal256(precision, 0)
        return pa.decimal128(precision, 0)
    if pa.types.is_dictionary(arrow_type):
        return value_type
    return None


def _decimal_coefficient(
    column: pa.Array[Any] | pa.ChunkedArray[Any], arrow_type: pa.DataType
) -> pa.Array[Any] | pa.ChunkedArray[Any]:
    """Reinterpret a negative-scale decimal as its unscaled coefficient.

    Casting to scale 0 rescales the numeric value and can overflow
    ``precision``. The scale lives in the type, not the buffer, so the same
    bytes are already the coefficient.
    """
    if not pa.types.is_decimal(arrow_type):
        raise TypeError(f"Expected a decimal join key type, got {arrow_type}")
    precision = int(arrow_type.precision)
    if pa.types.is_decimal256(arrow_type):
        coefficient_type: pa.DataType = pa.decimal256(precision, 0)
    else:
        coefficient_type = pa.decimal128(precision, 0)
    chunks = list(column.chunks) if isinstance(column, pa.ChunkedArray) else [column]
    projected: list[pa.Array[Any]] = []
    for chunk in chunks:
        if chunk.offset != 0:
            chunk = pa.concat_arrays([chunk])
        projected.append(
            pa.Array.from_buffers(coefficient_type, len(chunk), chunk.buffers())
        )
    if isinstance(column, pa.ChunkedArray):
        return pa.chunked_array(projected)
    return projected[0]


def _add_sort_key(batch: Any, *, on: str, sort_column: str) -> pa.Table:
    """Append a comparable helper column and keep the original key type."""
    if not isinstance(batch, pa.Table):
        raise TypeError("Merge source batches must be Arrow tables")
    # Ray's sort can emit a zero-column table for an empty range. That block
    # has no join key to cast; leave it unchanged instead of raising KeyError.
    if batch.num_rows == 0 and on not in batch.column_names:
        return batch
    keys = batch.column(on)
    helper_type = _sort_helper_type(keys.type)
    if helper_type is None:
        return batch
    if batch.num_rows == 0:
        return batch.append_column(sort_column, pa.array([], type=helper_type))
    value_type = _unwrap_dictionary_type(keys.type)
    projected = keys.cast(value_type) if pa.types.is_dictionary(keys.type) else keys
    helper_column: Any
    if pa.types.is_decimal(value_type) and int(value_type.scale) < 0:
        helper_column = _decimal_coefficient(projected, value_type)
    elif not projected.type.equals(helper_type):
        helper_column = projected.cast(helper_type)
    else:
        helper_column = projected
    return batch.append_column(sort_column, helper_column)


def _dedupe_source_batch(batch: Any, *, on: str, sort_column: str | None) -> pa.Table:
    """Remove a temporary sorting column before source rows reach planning."""
    if not isinstance(batch, pa.Table):
        raise TypeError("Merge source batches must be Arrow tables")
    table: pa.Table = batch
    helper_name = (
        sort_column
        if sort_column is not None and sort_column in table.column_names
        else None
    )
    compare_on = helper_name if helper_name is not None else on
    if table.num_rows == 0:
        if helper_name is None:
            return table
        dropped = table.drop_columns([helper_name])
        if not isinstance(dropped, pa.Table):
            raise TypeError("Merge source batches must be Arrow tables")
        return dropped
    deduped = _drop_adjacent_duplicate_keys(table, compare_on)
    if helper_name is None:
        return deduped
    dropped = deduped.drop_columns([helper_name])
    if not isinstance(dropped, pa.Table):
        raise TypeError("Merge source batches must be Arrow tables")
    return dropped


# ---------------------------------------------------------------------------
# Ray tasks (module-level so they are picklable)
# ---------------------------------------------------------------------------


@ray.remote
def _plan_task(
    task_id: int,
    uri: str,
    read_version: int,
    on: str,
    storage_options: Optional[dict[str, Any]],
    namespace_args: _NamespaceArgs,
    source_chunk: pa.Table,
    target_schema: pa.Schema,
    n_apply: int,
) -> Iterator[dict[str, Any]]:
    """PLAN + map-side shuffle: map keys to target fragments, bucket by owner.

    This is a Ray generator task: it yields one payload per *non-empty*
    apply owner, then a small metadata dict. Empty owners are omitted so
    object count tracks occupied plan-to-apply edges, not ``n_plan * n_apply``.
    The driver only fetches the metadata; bucket bytes stay in the object
    store on this node until the owning apply task pulls them. Ownership is
    ``crc32(fragment_id) % n_apply`` (``n_apply`` is ``num_workers``), so
    every plan task routes a given fragment to the same apply task without
    coordination -- that is what makes the apply phase write-disjoint by
    construction.
    """
    t0 = time.perf_counter()
    # The dedupe sort can leave some range partitions empty (fewer rows than
    # partitions); such blocks materialize as zero-column tables, so bail
    # out before schema alignment.
    if source_chunk.num_rows == 0:
        yield {
            "label": f"plan-{task_id}",
            "rows": 0,
            "matched": 0,
            "touched_fragments": [],
            "bucket_owners": [],
            "elapsed_s": time.perf_counter() - t0,
        }
    else:
        namespace_kwargs = get_namespace_kwargs(*namespace_args)
        source_chunk = _align_chunk(source_chunk, target_schema, on)
        keys = _join_key_values(source_chunk.column(on))
        _raise_on_duplicate_keys(keys, on, f"plan task {task_id}")

        dataset = lance.LanceDataset(
            uri,
            version=read_version,
            storage_options=storage_options or None,
            **namespace_kwargs,
        )
        # Batched index lookups: only the key column plus _rowaddr and _rowid
        # are materialized. _rowaddr >> 32 is the physical fragment id (the
        # shuffle key); the low 32 bits are the local offset delete_rows uses.
        # _rowid is preserved for stable-row-id replacements. A key may hit
        # several target rows (join-all); every hit is kept.
        matches_of: dict[Any, list[tuple[int, int, int]]] = collections.defaultdict(
            list
        )
        key_type = target_schema.field(on).type
        # Lance stable row ids are UInt64. Values at or above 2**63 overflow
        # if this column is narrowed to Int64 while the buckets are built.
        rowid_type: pa.DataType = pa.uint64()
        for batch in _chunked(keys, _LOOKUP_BATCH_SIZE):
            in_list = ", ".join(_sql_literal(key, key_type) for key in batch)
            # Backticks are Lance's identifier quoting (double quotes would be
            # parsed as a string literal by the filter planner). An embedded
            # backtick in the column name is doubled so the identifier does
            # not end early.
            hit_table = dataset.to_table(
                columns=[on],
                filter=f"{_sql_identifier(on)} IN ({in_list})",
                with_row_address=True,
                with_row_id=True,
            )
            rowid_type = hit_table.schema.field("_rowid").type
            for key, rowaddr, rowid in zip(
                _join_key_values(hit_table.column(on)),
                _integer_column_values(hit_table.column("_rowaddr")),
                _integer_column_values(hit_table.column("_rowid")),
                strict=False,
            ):
                fragment_id, offset = _rowaddr_parts(rowaddr)
                matches_of[key].append((fragment_id, int(rowid), offset))

        # Map-side shuffle: each target match is routed to owner(fragment_id).
        # One source row can therefore appear in several buckets (or twice in
        # the same fragment bucket) when the target key is not unique. Inserts
        # are spread round-robin.
        updates_by_owner: list[dict[int, list[tuple[int, int, int]]]] = [
            collections.defaultdict(list) for _ in range(n_apply)
        ]
        inserts_by_owner: list[list[int]] = [[] for _ in range(n_apply)]
        num_matched = 0
        touched_fragments: set[int] = set()
        for i, key in enumerate(keys):
            hits = matches_of.get(key)
            if not hits:
                inserts_by_owner[i % n_apply].append(i)
                continue
            num_matched += len(hits)
            for fragment_id, rowid, offset in hits:
                touched_fragments.add(fragment_id)
                updates_by_owner[_key_bucket(fragment_id, n_apply)][fragment_id].append(
                    (i, rowid, offset)
                )

        rowid_column, offset_column = _helper_column_names(target_schema)
        owners, buckets, _bucket_rows = _pack_plan_buckets(
            n_apply,
            source_chunk,
            updates_by_owner,
            inserts_by_owner,
            rowid_column,
            offset_column,
            rowid_type=rowid_type,
        )
        # Ray sends acknowledgements into streaming generators. A delegated
        # generator supports send(); a plain list iterator does not.
        yield from (bucket for bucket in buckets)
        yield {
            "label": f"plan-{task_id}",
            "rows": len(keys),
            "matched": num_matched,
            "touched_fragments": sorted(touched_fragments),
            "bucket_owners": owners,
            "elapsed_s": time.perf_counter() - t0,
        }


def _collect_bucket_matches(
    payload: dict[str, Any],
    rowid_column: str,
    offset_column: str,
    matches_by_fragment: dict[int, _FragmentMatchIds],
) -> None:
    """Record packed per-fragment row ids and offsets from one bucket."""
    for fragment_id, table in payload["frags"].items():
        matches = matches_by_fragment.get(fragment_id)
        if matches is None:
            matches = _FragmentMatchIds()
            matches_by_fragment[fragment_id] = matches
        matches.append(table.column(rowid_column), table.column(offset_column))


def _write_bucket(
    payload: dict[str, Any],
    uri: str,
    storage_options: Optional[dict[str, Any]],
    write_kwargs: dict[str, Any],
    rowid_column: str,
    offset_column: str,
    *,
    enable_stable_row_ids: bool,
) -> tuple[list[Any], int, int]:
    """Write one plan bucket and return ``(fragments, updated_rows, inserted_rows)``.

    Replacement rows in this bucket are written before its inserts. Each
    replacement fragment carries only the logical ids of the rows it holds,
    so inserts do not have to be a suffix of one combined table.
    """
    replacement_parts: list[pa.Table] = []
    row_id_chunks: list[pa.Array[Any]] = []
    for table in payload["frags"].values():
        if enable_stable_row_ids:
            row_id_chunks.extend(_integer_chunks(table.column(rowid_column)))
        replacement_parts.append(table.drop_columns([rowid_column, offset_column]))

    written: list[Any] = []
    updated_rows = 0
    if replacement_parts:
        replacement_table = (
            replacement_parts[0]
            if len(replacement_parts) == 1
            else pa.concat_tables(replacement_parts)
        )
        fragments = _write_append_fragments(
            replacement_table,
            uri,
            storage_options,
            write_kwargs,
            enable_stable_row_ids=enable_stable_row_ids,
        )
        if enable_stable_row_ids:
            fragments = _attach_preserved_row_ids(
                fragments, pa.chunked_array(row_id_chunks)
            )
            del row_id_chunks
        written.extend(fragments)
        updated_rows = replacement_table.num_rows

    inserted_rows = 0
    inserts = payload["inserts"]
    if inserts is not None and inserts.num_rows:
        # Insert rows carry a null row-id placeholder; strip it so the
        # appended rows match the target schema. Commit assigns new ids.
        insert_table = inserts.drop_columns([rowid_column])
        written.extend(
            _write_append_fragments(
                insert_table,
                uri,
                storage_options,
                write_kwargs,
                enable_stable_row_ids=enable_stable_row_ids,
            )
        )
        inserted_rows = insert_table.num_rows
    return written, updated_rows, inserted_rows


@ray.remote
def _apply_task(
    task_id: int,
    uri: str,
    read_version: int,
    storage_options: Optional[dict[str, Any]],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    bucket_refs: list[ray.ObjectRef[dict[str, Any]]],
) -> dict[str, Any]:
    """APPLY: merge-on-read updates for a disjoint set of target fragments.

    ``bucket_refs`` are this task's bucket ObjectRefs from every plan task.
    They are nested inside a list on purpose so Ray does not resolve them on
    the driver -- this task fetches them here, i.e. the bytes move from the
    plan node to this node directly. Buckets are materialized one at a time.
    Match offsets are stored as packed Arrow integers and released after that
    fragment's deletion file is written. Stable row ids are not kept across
    buckets; each bucket supplies its own ids when its replacement rows are
    written.

    For each owned fragment the task writes
    a new *deletion file* marking the matched rows dead
    (``LanceFragment.delete_rows`` by local physical offset from
    ``_rowaddr``, gathered by the plan phase's index lookups, so no
    fragment data is rescanned and the data files are untouched); the
    replacement rows and the inserts are appended together as brand-new
    fragments. A fragment left empty by the deletion is removed instead.

    Returns the parts of the final ``LanceOperation.Update``: removed
    fragment ids (fully-emptied fragments), pickled metadata of the fragments
    that received a new deletion file, and pickled metadata of the new
    fragments it wrote.
    """
    t0 = time.perf_counter()
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    write_kwargs = get_write_fragments_kwargs(
        namespace_impl, namespace_properties, table_id
    )
    dataset = lance.LanceDataset(
        uri,
        version=read_version,
        storage_options=storage_options or None,
        **namespace_kwargs,
    )
    rowid_column, offset_column = _helper_column_names(dataset.schema)
    fragment_by_id = {f.fragment_id: f for f in dataset.get_fragments()}
    uses_stable_row_ids = bool(getattr(dataset, "has_stable_row_ids", False))
    storage_version = getattr(dataset, "data_storage_version", None)
    if storage_version:
        # pylance documents ``data_storage_version=None`` as file format 2.0.
        # When the destination dataset already exists, Lance loads that
        # manifest and uses its format if the argument is omitted, so passing
        # the open dataset's version does not change the bytes written today.
        write_kwargs["data_storage_version"] = storage_version

    # Pass 1 keeps packed integer identities and releases each Arrow payload
    # before the next bucket is fetched. Row-id buffers are dropped after the
    # duplicate check. Offset buffers are dropped as each fragment is deleted.
    # Pass 2 writes one bucket at a time and, for stable row ids, reads ids
    # only from that bucket. ``num_partitions`` shrinks both the payload and
    # the preserved-id buffer.
    matches_by_fragment: dict[int, _FragmentMatchIds] = {}
    for ref in bucket_refs:
        payload = ray.get(ref)
        _collect_bucket_matches(
            payload,
            rowid_column,
            offset_column,
            matches_by_fragment,
        )
        del payload

    for fragment_id, matches in matches_by_fragment.items():
        matches.release_row_ids_after_duplicate_check(f"target fragment {fragment_id}")

    removed_fragment_ids: list[int] = []
    updated_fragments: list[bytes] = []
    for fragment_id, matches in matches_by_fragment.items():
        offsets = matches.take_offsets_after_duplicate_check(
            f"target fragment {fragment_id} offsets"
        )
        # Mark the matched rows dead with a deletion file, addressed by the
        # physical offsets gathered in the plan phase. A key predicate would
        # force the delete to rescan and decode the fragment's key column.
        delete_offsets = _offsets_for_delete_rows(offsets)
        del offsets
        new_meta = fragment_by_id[fragment_id].delete_rows(delete_offsets)
        del delete_offsets
        if new_meta is None:
            removed_fragment_ids.append(fragment_id)
        else:
            updated_fragments.append(pickle.dumps(new_meta))
    matches_by_fragment.clear()

    new_fragments: list[bytes] = []
    updated_rows = 0
    inserted_rows = 0
    for ref in bucket_refs:
        payload = ray.get(ref)
        written, bucket_updated, bucket_inserted = _write_bucket(
            payload,
            uri,
            storage_options,
            write_kwargs,
            rowid_column,
            offset_column,
            enable_stable_row_ids=uses_stable_row_ids,
        )
        del payload
        new_fragments.extend(pickle.dumps(fragment) for fragment in written)
        updated_rows += bucket_updated
        inserted_rows += bucket_inserted

    return {
        "label": f"apply-{task_id}",
        "removed_fragment_ids": removed_fragment_ids,
        "updated_fragments": updated_fragments,
        "new_fragments": new_fragments,
        "updated_rows": updated_rows,
        "inserted_rows": inserted_rows,
        "rows": updated_rows + inserted_rows,
        "elapsed_s": time.perf_counter() - t0,
    }


# ---------------------------------------------------------------------------
# driver-side scheduling helpers
# ---------------------------------------------------------------------------


def _bounded_map(
    remote_fn: _RemoteTask, arg_tuples: list[tuple[Any, ...]], max_in_flight: int
) -> list[dict[str, Any]]:
    """Run one Ray task per arg tuple with at most ``max_in_flight`` in flight."""
    total = len(arg_tuples)
    results: list[dict[str, Any]] = []
    pending: list[ray.ObjectRef[dict[str, Any]]] = []
    i = 0

    def _submit(j: int) -> None:
        ref = remote_fn.remote(*arg_tuples[j])
        if not isinstance(ref, ray.ObjectRef):
            raise TypeError("Apply tasks must return a Ray ObjectRef")
        pending.append(ref)

    while i < total and len(pending) < max_in_flight:
        _submit(i)
        i += 1
    while pending:
        done, pending = ray.wait(pending, num_returns=1)
        result = ray.get(done[0])
        results.append(result)
        if i < total:
            _submit(i)
            i += 1
    return results


def _bounded_map_shuffle(
    remote_fn: _RemoteTask, arg_tuples: list[tuple[Any, ...]], max_in_flight: int
) -> list[dict[str, Any]]:
    """``_bounded_map`` for plan tasks that yield a variable number of buckets.

    Each plan task is a Ray generator: non-empty owner payloads followed by a
    metadata dict. The driver waits for the generator to finish, fetches only
    the metadata, and keeps the bucket ObjectRefs (task outputs, not
    worker-side ``ray.put``) so they survive idle-worker recycling. Those
    refs are attached as ``meta["bucket_refs"]``, parallel to
    ``meta["bucket_owners"]``.
    """
    total = len(arg_tuples)
    results: list[dict[str, Any]] = []
    pending: dict[ray.ObjectRef[Any], ray.ObjectRefGenerator] = {}
    i = 0

    def _submit(j: int) -> None:
        gen = remote_fn.remote(*arg_tuples[j])
        if not isinstance(gen, ray.ObjectRefGenerator):
            raise TypeError("Plan tasks must return a Ray ObjectRefGenerator")
        pending[gen.completed()] = gen

    while i < total and len(pending) < max_in_flight:
        _submit(i)
        i += 1
    while pending:
        ready, _ = ray.wait(list(pending), num_returns=1)
        gen = pending.pop(ready[0])
        refs = list(gen)
        if not refs:
            raise RuntimeError("Internal error: plan task returned no values")
        meta = ray.get(refs[-1])
        owners = meta.get("bucket_owners", [])
        bucket_refs = refs[:-1]
        if len(bucket_refs) != len(owners):
            raise RuntimeError(
                "Internal error: plan shuffle mismatch: "
                f"{len(bucket_refs)} bucket(s) vs {len(owners)} owner(s)"
            )
        meta["bucket_refs"] = bucket_refs
        results.append(meta)
        if i < total:
            _submit(i)
            i += 1
    return results


def _fragment_metadata(fragment: Any) -> Any:
    return fragment.metadata if hasattr(fragment, "metadata") else fragment


def _data_file_paths(fragment: Any) -> tuple[Any, ...]:
    files = getattr(_fragment_metadata(fragment), "files", None) or ()
    return tuple(getattr(data_file, "path", None) for data_file in files)


def _deletion_file_identity(deletion_file: Any) -> tuple[Any, ...] | None:
    if deletion_file is None:
        return None
    return (
        getattr(deletion_file, "read_version", None),
        getattr(deletion_file, "id", None),
        getattr(deletion_file, "num_deleted_rows", None),
        getattr(deletion_file, "file_type", None),
        getattr(deletion_file, "base_id", None),
    )


def _merge_operation_visible(
    dataset: lance.LanceDataset,
    *,
    new_fragments: list[Any],
    updated_fragments: list[Any],
    removed_fragment_ids: list[int],
) -> bool:
    """Return True if ``dataset`` already contains this merge's commit payload."""
    if not (new_fragments or updated_fragments or removed_fragment_ids):
        return False
    for fragment_id in removed_fragment_ids:
        if dataset.get_fragment(fragment_id) is not None:
            return False
    if new_fragments:
        # Uncommitted fragments from ``write_fragments`` carry a placeholder
        # id (typically 0); the real id is assigned at commit time. Match by
        # data file identity instead, which is stable across the commit.
        committed_paths = {
            _data_file_paths(fragment) for fragment in dataset.get_fragments()
        }
        for expected in new_fragments:
            if _data_file_paths(expected) not in committed_paths:
                return False
    for expected in updated_fragments:
        current = dataset.get_fragment(expected.id)
        if current is None:
            return False
        current_meta = _fragment_metadata(current)
        if _data_file_paths(current_meta) != _data_file_paths(expected):
            return False
        if _deletion_file_identity(
            getattr(current_meta, "deletion_file", None)
        ) != _deletion_file_identity(getattr(expected, "deletion_file", None)):
            return False
    return True


def _commit_update(
    uri: str,
    operation: lance.LanceOperation.Update,
    read_version: int,
    storage_options: dict[str, Any],
    namespace_kwargs: dict[str, Any],
    new_fragments: list[Any],
    updated_fragments: list[Any],
    removed_fragment_ids: list[int],
) -> lance.LanceDataset:
    """Commit ``operation``, treating a lost success ack as success.

    Does not retry by submitting a second transaction. If ``commit`` raises
    but the latest manifest already contains this operation's fragments,
    return that dataset so a caller retry cannot double-apply an insert.
    """
    try:
        return lance.LanceDataset.commit(
            uri,
            operation,
            read_version=read_version,
            storage_options=storage_options or None,
            **namespace_kwargs,
        )
    except Exception as exc:
        try:
            latest = lance.LanceDataset(
                uri,
                storage_options=storage_options or None,
                **namespace_kwargs,
            )
        except Exception:  # noqa: BLE001 - probe failed; surface the commit error
            raise exc from None
        if latest.version > read_version and _merge_operation_visible(
            latest,
            new_fragments=new_fragments,
            updated_fragments=updated_fragments,
            removed_fragment_ids=removed_fragment_ids,
        ):
            logger.info(
                "merge_into commit raised after the operation was already "
                "visible at version %d; returning the latest dataset",
                latest.version,
            )
            return latest
        raise exc


def _attach_preserved_row_ids(
    fragments: list[Any], row_ids: pa.Array[Any] | pa.ChunkedArray[Any]
) -> list[Any]:
    """Bind preserved ``_rowid`` values onto newly written fragments.

    ``row_ids`` follows the replacement rows in this bucket. Each fragment
    takes as many ids as it has physical rows, and a trailing fragment may
    receive fewer ids than rows. Lance fills those remaining rows with newly
    assigned ids at commit.
    """
    from lance.fragment import RowIdSequence

    offset = 0
    total = len(row_ids)
    for fragment in fragments:
        if offset >= total:
            break
        take = min(int(fragment.physical_rows), total - offset)
        fragment.row_id_meta = RowIdSequence(
            row_ids.slice(offset, take)
        ).to_inline_metadata()
        offset += take
    if offset != total:
        raise RuntimeError(
            "Internal error: leftover replacement row ids after attaching "
            f"to fragments ({total - offset})"
        )
    return fragments


def _write_append_fragments(
    table: pa.Table,
    uri: str,
    storage_options: Optional[dict[str, Any]],
    write_kwargs: dict[str, Any],
    *,
    enable_stable_row_ids: bool = False,
) -> list[Any]:
    """Append one in-memory table and return fragment metadata.

    ``lance_ray.fragment.write_fragment`` consumes a block stream and returns
    ``(fragment, schema)`` pairs for ``write_lance``. This path already holds
    one table, then attaches ``row_id_meta`` on the returned metadata, so it
    calls ``lance.fragment.write_fragments`` directly. Namespace credential
    kwargs and the dataset ``data_storage_version`` arrive in ``write_kwargs``.
    ``initial_bases`` is create-only. Base placement (``target_bases`` /
    ``target_all_bases``) and file-size limits stay at the writer defaults:
    ``merge_into`` has no write-option arguments, and the dataset object does
    not expose a base list to copy. The fragment writer's ``call_with_retry``
    defaults to a single attempt, which is the same as calling the writer once.
    """
    if table.num_rows == 0:
        return []
    fragments = lance.fragment.write_fragments(
        table,
        uri,
        mode="append",
        storage_options=storage_options or None,
        enable_stable_row_ids=enable_stable_row_ids,
        **write_kwargs,
    )
    if not isinstance(fragments, list):
        raise TypeError("Fragment append must return fragment metadata")
    return fragments


def _schema_field_ids(schema: Any) -> list[int]:
    """Return every Lance field id, parents before their children.

    ``LanceOperation.Update.fields_for_preserving_frag_bitmap`` is the set of
    fields whose values moved. An index is left stale for a rewritten fragment
    when its field id is in that set. Scalar and vector indexes on nested
    columns store the leaf id, so a top-level-only list lets those indexes
    treat the new fragment as already indexed and filter queries miss the
    updated rows. This walk matches Lance ``Schema::fields_pre_order``.
    """
    field_ids: list[int] = []

    def walk(fields: list[Any]) -> None:
        for field in fields:
            field_ids.append(int(field.id()))
            walk(field.children())

    walk(schema.fields())
    return field_ids


def _index_field_name(name: str) -> str:
    """Undo Lance's minimal backtick quoting of an index field path.

    ``format_field_path_minimal`` quotes with backticks, and only when the
    path contains ``.`` or a backtick. A dotted key such as ``user.id`` is
    therefore `` `user.id` ``, not a double-quoted identifier.
    """
    if len(name) >= 2 and name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    return name


def _has_scalar_index_on(dataset: lance.LanceDataset, column: str) -> bool:
    try:
        if hasattr(dataset, "describe_indices"):
            for index in dataset.describe_indices():
                names = getattr(index, "field_names", None) or []
                if any(_index_field_name(name) == column for name in names):
                    return True
            return False
        for legacy_index in dataset.list_indices():
            fields = (
                legacy_index.get("fields")
                if isinstance(legacy_index, dict)
                else getattr(legacy_index, "fields", None)
            )
            if fields and column in fields:
                return True
    except Exception:  # noqa: BLE001 - best effort, only used for a warning
        return True
    return False


def _require_ray_dataset(value: Any) -> ray.data.Dataset:
    """Keep a Ray Dataset binding when a stub types the call as ``Any``."""
    if isinstance(value, ray.data.Dataset):
        return value
    raise TypeError(
        f"Merge source must stay a ray.data.Dataset, got {type(value).__name__}"
    )


def _source_to_chunk_refs(
    source: ray.data.Dataset | pa.Table,
    on: str,
    num_partitions: int,
    target_schema: pa.Schema,
) -> list[ray.ObjectRef[pa.Table]]:
    """Cast, then sort-dedupe the source; return Arrow-table ObjectRefs.

    The cast happens before the sort so keys that become equal only after
    conversion to the target type (string ``"01"`` and ``"1"`` against an
    integer column) are one key for the global dedupe. The source is then
    range-partitioned with a Ray Data sort on that key: all copies land
    adjacent in exactly one block, so dropping adjacent duplicates per block
    is a complete global dedupe (one arbitrary row per key survives). The
    sort also hands the plan phase contiguous key slices, which keeps each
    chunk's index lookups within few BTREE leaf pages. The key type is the
    target field type: every block is cast to ``target_schema`` before the
    sort, so the driver does not execute the dataset to read its schema.
    """
    if isinstance(source, pa.Table):
        if source.num_rows == 0:
            return []
        # string_view and binary_view cast to the target string/binary types,
        # but Ray's Arrow serialization rejects the view types. Convert the
        # in-memory table before it is handed to ``from_arrow``.
        source = _align_chunk(source, target_schema, on)
        dataset = _require_ray_dataset(ray.data.from_arrow(source))
    elif isinstance(source, ray.data.Dataset):
        dataset = source
    else:
        raise TypeError(
            "source must be a ray.data.Dataset or a pyarrow.Table, got "
            f"{type(source).__name__}"
        )
    dataset = _require_ray_dataset(
        dataset.map_batches(
            partial(_align_source_batch, target_schema=target_schema, on=on),
            batch_size=None,
            batch_format="pyarrow",
        )
    )
    sort_column = None
    key_type = target_schema.field(on).type
    if _sort_helper_type(key_type) is not None:
        # Ray samples sort boundaries with Arrow's to_pylist(), and Arrow has
        # no sort or inequality kernel for dictionary or float16. A decoded
        # or widened helper is what gets sorted and deduplicated. Temporal
        # helpers are integer ticks so nanoseconds are not rounded through
        # Python. The original key column is kept.
        sort_column = _unused_column_name(_SORT_COLUMN, set(target_schema.names))
        dataset = _require_ray_dataset(
            dataset.map_batches(
                partial(_add_sort_key, on=on, sort_column=sort_column),
                batch_size=None,
                batch_format="pyarrow",
            )
        )
    deduped = _require_ray_dataset(
        dataset.repartition(num_partitions)
        .sort(sort_column or on)
        .map_batches(
            partial(_dedupe_source_batch, on=on, sort_column=sort_column),
            batch_size=None,
            batch_format="pyarrow",
        )
        .materialize()
    )
    return list(deduped.to_arrow_refs())  # pyright: ignore[reportReturnType]


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def merge_into(
    ds: ray.data.Dataset | pa.Table,
    uri: Optional[str] = None,
    *,
    on: str,
    table_id: Optional[list[str]] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    num_workers: int = 4,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
) -> lance.LanceDataset:
    """Distributed merge of ``ds`` into a Lance dataset.

    Every source row that matches a target row on the ``on`` key replaces
    that row entirely (all columns), and every source row with no match is
    inserted. All changes -- across every touched fragment -- are committed
    as one atomic version; on any failure before the commit, the visible
    table is untouched.

    This is the distributed counterpart of pylance's
    ``LanceDataset.merge_insert(on).when_matched_update_all()
    .when_not_matched_insert_all()``: the source rows are matched to their
    target fragments with distributed index lookups, and Ray workers apply
    the updates (each fragment owned by exactly one
    worker): matched rows are masked out with per-fragment deletion files,
    and replacement plus insert rows are
    appended as new fragments. If a join key matches several target rows,
    every matching row is updated. On datasets with stable row IDs, updated
    rows keep their logical ``_rowid``. All changes are committed as a
    single atomic version. Scans filter through the deletion vectors until
    the next compaction folds them away.

    Concurrency: conflict detection is fragment-level, and this function does
    not retry the plan. Serialize every writer to the table for the whole
    call, including ordinary appends. A concurrent append is rebased inside
    ``LanceDataset.commit`` and both commits succeed. If that append inserts
    a key this merge also inserts, the plan ran at ``read_version`` and never
    saw it, so the table keeps both rows. A concurrent commit that rewrote,
    removed, or updated-in-place (new deletion file / fragment metadata) any
    fragment this merge touches fails -- re-run against the latest version.
    If ``commit`` raises after this operation is already visible in the
    latest manifest (lost success ack), the call still returns that dataset
    so a job-level retry cannot double-insert. Data files and deletion files
    written before a failed apply task or a commit conflict remain in storage.
    ``LanceDataset.cleanup_old_versions()`` reclaims them, but the default
    keeps unverified failed-transaction files until they are 7 days old.

    Args:
        ds: The rows to merge into the target table, as a
            ``ray.data.Dataset`` or an in-memory ``pyarrow.Table``. The
            source must contain every column of the target schema (columns
            are reordered/cast as needed) and must not contain null join
            keys. Duplicate join keys are deduplicated after that cast,
            keeping one arbitrary occurrence per target-typed key (which
            copy survives is unspecified).
        uri: The URI of the target Lance dataset. Either ``uri`` OR
            (``namespace_impl`` + ``table_id``) must be provided.
        on: The join key column name. Supported types are boolean, integer,
            floating, string, date, timestamp, time, decimal, and binary
            (dictionary-encoded scalars unwrap to the value type). Floating
            keys must be finite; NaN and infinity are rejected. The Int64
            minimum and every UInt64 value at or above 2**63 are rendered as
            typed literals. Dates outside year 1–9999 are sorted and looked up
            as integer ticks. Decimals with a
            negative scale are sorted on their unscaled coefficient and looked
            up with a typed cast. A column name containing a backtick is
            rejected: Lance 12 cannot resolve that field in a filter. Nested
            types are rejected on the driver before any Ray task starts. A
            scalar index on this column is strongly recommended for large
            targets (the plan phase falls back to filtered scans without one).
            Every target row whose key matches a source row is updated
            (join-all).
        table_id: The table identifier as a list of strings. Must be provided
            together with ``namespace_impl``.
        namespace_impl: The namespace implementation type (e.g. ``"rest"``,
            ``"dir"``), used for resolving the dataset location and credential
            vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
        storage_options: Storage options for the dataset.
        num_workers: Maximum number of Ray tasks running concurrently in each
            phase, and the number of apply-side fragment owners (default: 4).
            Fragment ownership is ``crc32(fragment_id) % num_workers``. Lower
            it to reduce concurrent IO and shuffle fan-out.
        num_partitions: Number of source chunks in the plan phase (default:
            ``num_workers``). Raise it to shrink each plan chunk without
            creating more apply tasks (e.g. ``num_partitions=32`` with
            ``num_workers=8``). Apply tasks stream those chunks one at a time,
            so the same setting bounds the deserialized source rows and
            preserved row ids inside one worker by the largest chunk. It does
            not shrink the Ray object store: the deduplicated source and every
            bucket stay there until commit, and join-all can copy one source
            row into several buckets. Match offsets are packed integers and
            are released when that fragment's deletion file is written. A hot
            fragment stays on one owner. New fragments use the target
            dataset's ``data_storage_version``.
        ray_remote_args: Options for the Ray tasks (e.g. ``num_cpus``,
            ``resources``). ``_generator_backpressure_num_objects`` is rejected:
            the plan scheduler waits for generators to finish before consuming
            their outputs, so generator backpressure can deadlock it.

    Returns:
        The updated :class:`lance.LanceDataset` at the committed version.
        When the source produced no updates and no inserts, returns the
        dataset pinned at ``read_version`` (no empty commit).

    Example:
        >>> import lance_ray as lr
        >>> dataset = lr.merge_into(
        ...     daily_batch,                  # ray.data.Dataset or pyarrow.Table
        ...     "s3://bucket/users.lance",
        ...     on="user_id",
        ...     num_workers=8,
        ... )
        >>> dataset.version
        5
    """
    if not on:
        raise ValueError("merge_into requires a join key column name ('on')")
    if num_workers < 1:
        raise ValueError("num_workers must be >= 1")
    if num_partitions is not None and num_partitions < 1:
        raise ValueError("num_partitions must be >= 1")
    num_partitions = num_partitions or num_workers
    ray_remote_args = ray_remote_args or {}
    if "_generator_backpressure_num_objects" in ray_remote_args:
        raise ValueError(
            "merge_into does not support ray_remote_args option "
            "'_generator_backpressure_num_objects': the plan scheduler waits "
            "for generators to finish before consuming their outputs, so "
            "generator backpressure can deadlock it. Remove this option."
        )

    validate_uri_or_namespace(uri, namespace_impl, table_id)
    uri, storage_options = resolve_namespace_table(
        uri,
        storage_options,
        namespace_impl,
        namespace_properties,
        table_id,
    )
    namespace_kwargs = get_namespace_kwargs(
        namespace_impl, namespace_properties, table_id
    )

    dataset = lance.LanceDataset(
        uri,
        storage_options=storage_options or None,
        **namespace_kwargs,
    )
    read_version = dataset.version
    target_schema = dataset.schema
    # Full-row rewrites change every column, including nested leaves. Pass
    # the whole id tree so stable-row-id commits do not extend a nested
    # index's fragment bitmap over the rewritten fragment.
    field_ids = _schema_field_ids(dataset.lance_schema)
    if on not in target_schema.names:
        raise ValueError(
            f"Join key column {on!r} not found in target schema {target_schema.names}"
        )
    if "`" in on:
        raise ValueError(
            f"Join key column {on!r} contains a backtick. Lance 12 drops that "
            "field from its query schema, so a filter cannot read the column. "
            "merge_into rejects the key instead of planning a merge that "
            "cannot match rows."
        )
    _raise_unless_supported_join_key(on, target_schema.field(on).type)
    if not _has_scalar_index_on(dataset, on):
        logger.warning(
            "No scalar index found on join key column %r; the merge_into plan "
            "phase will fall back to filtered scans of the target table. "
            "Create a scalar index on the key column for large targets.",
            on,
        )

    # Phase 0: cast to the target schema, then sort-dedupe. The deduplicated,
    # range-partitioned blocks become the plan chunks; every downstream
    # duplicate check then passes by construction.
    chunk_refs = _source_to_chunk_refs(ds, on, num_partitions, target_schema)

    # Phase 1: PLAN + map-side shuffle. Chunk refs are passed as top-level
    # args (resolved on the worker). Each plan task yields one object per
    # non-empty apply owner (``num_workers`` owners) plus metadata.
    plan_remote = _plan_task.options(**ray_remote_args)
    plan_args = [
        (
            i,
            uri,
            read_version,
            on,
            storage_options,
            (namespace_impl, namespace_properties, table_id),
            chunk_ref,
            target_schema,
            num_workers,
        )
        for i, chunk_ref in enumerate(chunk_refs)
    ]
    plan_results = (
        _bounded_map_shuffle(plan_remote, plan_args, num_workers) if plan_args else []
    )
    touched_fragment_ids = {
        f for meta in plan_results for f in meta["touched_fragments"]
    }
    logger.info(
        "merge_into plan done: %d source rows, %d matched, %d fragment(s) touched",
        sum(meta["rows"] for meta in plan_results),
        sum(meta["matched"] for meta in plan_results),
        len(touched_fragment_ids),
    )

    # Phase 2: route each bucket's ObjectRef to its owning apply task. The
    # refs are nested in a list on purpose so Ray does not resolve them on
    # the driver; the apply task fetches them node-to-node itself.
    refs_by_owner: dict[int, list[ray.ObjectRef[dict[str, Any]]]] = (
        collections.defaultdict(list)
    )
    for meta in plan_results:
        for owner, ref in zip(meta["bucket_owners"], meta["bucket_refs"], strict=True):
            refs_by_owner[owner].append(ref)
    apply_args = [
        (
            owner,
            uri,
            read_version,
            storage_options,
            namespace_impl,
            namespace_properties,
            table_id,
            refs_by_owner[owner],
        )
        for owner in range(num_workers)
        if owner in refs_by_owner
    ]
    apply_remote = _apply_task.options(**ray_remote_args)
    apply_results = (
        _bounded_map(apply_remote, apply_args, num_workers) if apply_args else []
    )

    # Phase 3: union the parts into ONE atomic LanceOperation.Update.
    num_inserted_rows = sum(r["inserted_rows"] for r in apply_results)
    num_updated_rows = sum(r["updated_rows"] for r in apply_results)
    if not apply_results:
        logger.info("merge_into: nothing to update or insert; no commit")
        return lance.LanceDataset(
            uri,
            version=read_version,
            storage_options=storage_options or None,
            **namespace_kwargs,
        )

    removed = [f for r in apply_results for f in r["removed_fragment_ids"]]
    updated = [pickle.loads(f) for r in apply_results for f in r["updated_fragments"]]
    new_fragments = [pickle.loads(f) for r in apply_results for f in r["new_fragments"]]
    touched_ids = removed + [f.id for f in updated]
    if len(touched_ids) != len(set(touched_ids)):
        raise RuntimeError(
            "Internal error: fragment overlap across apply tasks -- the "
            "assignment is not fragment-disjoint"
        )
    operation = lance.LanceOperation.Update(
        removed_fragment_ids=removed,
        updated_fragments=updated,
        new_fragments=new_fragments,
        fields_modified=[],
        fields_for_preserving_frag_bitmap=field_ids,
        update_mode="rewrite_rows",
    )

    committed = _commit_update(
        uri,
        operation,
        read_version,
        storage_options,
        namespace_kwargs,
        new_fragments,
        updated,
        removed,
    )
    logger.info(
        "merge_into committed version %d: %d row(s) updated, %d row(s) "
        "inserted, %d fragment(s) deletion-vector-updated, %d removed",
        committed.version,
        num_updated_rows,
        num_inserted_rows,
        len(updated),
        len(removed),
    )
    return committed
