# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

from dataclasses import dataclass
from typing import Any

import pyarrow as pa


@dataclass(frozen=True)
class ResolvedFieldPath:
    path: str
    field: pa.Field


@dataclass(frozen=True)
class ResolvedDatasetFieldPath(ResolvedFieldPath):
    field_id: int


def parse_field_path(path: str) -> list[str]:
    segments: list[str] = []
    segment: list[str] = []
    in_quote = False
    quoted = False
    i = 0

    while i < len(path):
        char = path[i]
        if in_quote:
            if char == "`":
                if i + 1 < len(path) and path[i + 1] == "`":
                    segment.append("`")
                    i += 2
                    continue
                in_quote = False
                quoted = True
                i += 1
                continue
            segment.append(char)
            i += 1
            continue

        if char == ".":
            if not segment:
                raise ValueError(f"Invalid field path {path!r}: empty path segment")
            segments.append("".join(segment))
            segment = []
            quoted = False
            i += 1
            continue

        if quoted:
            raise ValueError(
                f"Invalid field path {path!r}: quoted path segments must be "
                "followed by '.' or the end of the path"
            )

        if char == "`":
            if segment:
                raise ValueError(
                    f"Invalid field path {path!r}: quotes must start a path segment"
                )
            in_quote = True
            i += 1
            continue

        segment.append(char)
        i += 1

    if in_quote:
        raise ValueError(f"Invalid field path {path!r}: unterminated quoted segment")
    if not segment:
        raise ValueError(f"Invalid field path {path!r}: empty path segment")

    segments.append("".join(segment))
    return segments


def canonical_field_path(path: str) -> str:
    return ".".join(_quote_segment(segment) for segment in parse_field_path(path))


def resolve_arrow_field_path(schema: pa.Schema, path: str) -> ResolvedFieldPath:
    segments = parse_field_path(path)
    field = _schema_field(schema, segments[0])

    for segment in segments[1:]:
        if not pa.types.is_struct(field.type):
            raise KeyError(
                f"Field path {path!r} cannot descend into non-struct field "
                f"{field.name!r}"
            )
        field = _struct_child(field.type, segment)

    return ResolvedFieldPath(path=_canonical_segments(segments), field=field)


def resolve_dataset_field_path(dataset: Any, path: str) -> ResolvedDatasetFieldPath:
    resolved = resolve_arrow_field_path(dataset.schema, path)
    lance_field = dataset.lance_schema.field(resolved.path)
    if lance_field is None:
        raise KeyError(f"Field path {path!r} not found in Lance schema")
    return ResolvedDatasetFieldPath(
        path=resolved.path,
        field=resolved.field,
        field_id=lance_field.id(),
    )


def _schema_field(schema: pa.Schema, name: str) -> pa.Field:
    try:
        return schema.field(name)
    except KeyError as exc:
        available = _field_names(schema)
        raise KeyError(f"Field {name!r} not found. Available: {available}") from exc


def _struct_child(data_type: pa.DataType, name: str) -> pa.Field:
    for child in data_type:
        if child.name == name:
            return child
    available = [child.name for child in data_type]
    raise KeyError(f"Field {name!r} not found. Available: {available}")


def _field_names(schema: pa.Schema) -> list[str]:
    return [field.name for field in schema]


def _canonical_segments(segments: list[str]) -> str:
    return ".".join(_quote_segment(segment) for segment in segments)


def _quote_segment(segment: str) -> str:
    if segment and all(char.isalnum() or char == "_" for char in segment):
        return segment
    return "`" + segment.replace("`", "``") + "`"
