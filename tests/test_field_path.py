import pyarrow as pa
import pytest
from lance_ray.field_path import (
    canonical_field_path,
    parse_field_path,
    resolve_arrow_field_path,
)


def _nested_schema():
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "meta",
                pa.struct(
                    [
                        pa.field("userId", pa.string()),
                        pa.field("a.b", pa.string()),
                        pa.field("a`b", pa.string()),
                    ]
                ),
            ),
            pa.field(
                "meta-data",
                pa.struct([pa.field("user-id", pa.string())]),
            ),
            pa.field("outer", pa.struct([pa.field("leaf", pa.int64())])),
            pa.field("other", pa.struct([pa.field("leaf", pa.int64())])),
        ]
    )


def test_parse_field_path_supports_quoted_literal_dot_segments():
    assert parse_field_path("meta.`a.b`") == ["meta", "a.b"]
    assert parse_field_path("`meta`.`userId`") == ["meta", "userId"]
    assert parse_field_path("meta.`a``b`") == ["meta", "a`b"]


def test_canonical_field_path_matches_lance_formatting_rules():
    assert canonical_field_path("`meta`.`userId`") == "meta.userId"
    assert canonical_field_path("meta.`a.b`") == "meta.`a.b`"
    assert canonical_field_path("`meta-data`.`user-id`") == "`meta-data`.`user-id`"
    assert canonical_field_path("meta.`a``b`") == "meta.`a``b`"


def test_resolve_arrow_field_path_supports_nested_and_literal_dot():
    schema = _nested_schema()

    assert resolve_arrow_field_path(schema, "meta.userId").field.name == "userId"
    resolved_literal = resolve_arrow_field_path(schema, "`meta`.`a.b`")

    assert resolved_literal.path == "meta.`a.b`"
    assert resolved_literal.field.name == "a.b"

    resolved_hyphen = resolve_arrow_field_path(schema, "`meta-data`.`user-id`")
    assert resolved_hyphen.path == "`meta-data`.`user-id`"
    assert resolved_hyphen.field.name == "user-id"


def test_resolve_arrow_field_path_requires_disambiguated_same_leaf_name():
    schema = _nested_schema()

    with pytest.raises(KeyError):
        resolve_arrow_field_path(schema, "leaf")

    assert resolve_arrow_field_path(schema, "outer.leaf").field.name == "leaf"
    assert resolve_arrow_field_path(schema, "other.leaf").field.name == "leaf"


def test_parse_field_path_rejects_malformed_quoted_paths():
    with pytest.raises(ValueError, match="unterminated"):
        parse_field_path("meta.`a.b")
    with pytest.raises(ValueError, match="empty path segment"):
        parse_field_path("meta..userId")
