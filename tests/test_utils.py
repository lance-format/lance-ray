import pytest
from lance_ray.utils import (
    get_or_create_namespace,
    has_namespace_params,
    validate_uri_or_namespace,
)


def test_has_namespace_params_treats_empty_values_as_not_provided() -> None:
    assert has_namespace_params("", ["table"]) is False
    assert has_namespace_params("dir", []) is False
    assert has_namespace_params("dir", ["table"]) is True
    assert has_namespace_params(None, None) is False


def test_validate_uri_or_namespace_rejects_empty_namespace_params() -> None:
    # An empty namespace_impl or table_id (e.g. from os.environ.get) must be
    # rejected at the API boundary, not deep inside lance_namespace.connect().
    with pytest.raises(ValueError, match="Must provide either 'uri' OR"):
        validate_uri_or_namespace(None, "", [])


def test_get_or_create_namespace_treats_empty_impl_as_not_provided() -> None:
    # The datasink/datasource constructors call get_or_create_namespace
    # directly, bypassing has_namespace_params. An empty impl must return None
    # (the uri path) instead of reaching lance_namespace.connect("").
    assert get_or_create_namespace("", None) is None
