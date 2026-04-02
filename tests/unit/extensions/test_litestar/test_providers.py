"""Unit tests for Litestar filter providers (issue #405)."""

from typing import Any

import pytest

from sqlspec.core import InCollectionFilter, NotInCollectionFilter
from sqlspec.extensions.litestar.providers import (
    FieldNameType,
    _create_filter_aggregate_function,
    _create_statement_filters,
    dep_cache,
)
from sqlspec.typing import LITESTAR_INSTALLED

if not LITESTAR_INSTALLED:
    pytest.skip("Litestar not installed", allow_module_level=True)


@pytest.fixture(autouse=True)
def _clear_dependency_cache() -> Any:
    dep_cache.dependencies.clear()
    yield
    dep_cache.dependencies.clear()


def test_in_fields_creates_provider_and_aggregate_param() -> None:
    """in_fields config creates both individual provider and aggregate parameter."""
    config: dict[str, Any] = {"in_fields": {FieldNameType(name="status", type_hint=str)}}
    deps = _create_statement_filters(config)

    assert "status_in_filter" in deps
    assert "filters" in deps


def test_in_fields_provider_returns_filter_when_values_present() -> None:
    """Individual in_fields provider returns InCollectionFilter when values are given."""
    config: dict[str, Any] = {"in_fields": {FieldNameType(name="status", type_hint=str)}}
    deps = _create_statement_filters(config)

    provider = deps["status_in_filter"]
    result = provider.dependency(values=["active", "archived"])

    assert isinstance(result, InCollectionFilter)
    assert result.field_name == "status"
    assert list(result.values) == ["active", "archived"]  # type: ignore[arg-type]


def test_in_fields_provider_returns_none_when_no_values() -> None:
    """Individual in_fields provider returns None when values is None."""
    config: dict[str, Any] = {"in_fields": {FieldNameType(name="status", type_hint=str)}}
    deps = _create_statement_filters(config)

    provider = deps["status_in_filter"]
    result = provider.dependency(values=None)
    assert result is None


def test_not_in_fields_provider_returns_filter_when_values_present() -> None:
    """Individual not_in_fields provider returns NotInCollectionFilter when values are given."""
    config: dict[str, Any] = {"not_in_fields": {FieldNameType(name="status", type_hint=str)}}
    deps = _create_statement_filters(config)

    provider = deps["status_not_in_filter"]
    result = provider.dependency(values=["deleted", "archived"])

    assert isinstance(result, NotInCollectionFilter)
    assert result.field_name == "status"
    assert list(result.values) == ["deleted", "archived"]  # type: ignore[arg-type]


def test_aggregate_function_includes_in_filter() -> None:
    """Aggregate function includes InCollectionFilter in results."""
    config: dict[str, Any] = {"in_fields": {FieldNameType(name="status", type_hint=str)}}
    agg_fn = _create_filter_aggregate_function(config)

    in_filter = InCollectionFilter(field_name="status", values=["active", "pending"])
    result = agg_fn(status_in_filter=in_filter)

    assert len(result) == 1
    assert isinstance(result[0], InCollectionFilter)
    assert result[0].field_name == "status"


def test_aggregate_function_excludes_none_in_filter() -> None:
    """Aggregate function excludes None in_filter values."""
    config: dict[str, Any] = {"in_fields": {FieldNameType(name="status", type_hint=str)}}
    agg_fn = _create_filter_aggregate_function(config)

    result = agg_fn(status_in_filter=None)
    assert result == []


def test_aggregate_function_multiple_in_fields() -> None:
    """Aggregate function handles multiple in_fields."""
    config: dict[str, Any] = {
        "in_fields": {FieldNameType(name="status", type_hint=str), FieldNameType(name="role", type_hint=str)}
    }
    agg_fn = _create_filter_aggregate_function(config)

    status_filter = InCollectionFilter(field_name="status", values=["active"])
    role_filter = InCollectionFilter(field_name="role", values=["admin", "user"])

    result = agg_fn(status_in_filter=status_filter, role_in_filter=role_filter)

    assert len(result) == 2
    field_names = {f.field_name for f in result}
    assert field_names == {"status", "role"}


def test_in_fields_with_string_config() -> None:
    """in_fields config with list of strings works correctly."""
    config: dict[str, Any] = {"in_fields": ["status"]}
    deps = _create_statement_filters(config)

    assert "status_in_filter" in deps
    assert "filters" in deps

    provider = deps["status_in_filter"]
    result = provider.dependency(values=["active"])
    assert isinstance(result, InCollectionFilter)
    assert result.field_name == "status"
