"""Unit tests for Litestar filter providers (issue #405)."""

import inspect
from typing import Any

import pytest
from litestar.exceptions import ValidationException

from sqlspec.core import InCollectionFilter, NotInCollectionFilter, OrderByFilter
from sqlspec.extensions.litestar.providers import (
    FieldNameType,
    FilterConfig,
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
    config = FilterConfig(in_fields={FieldNameType(name="status", type_hint=str)})
    deps = _create_statement_filters(config)

    assert "status_in_filter" in deps
    assert "filters" in deps


def test_in_fields_provider_returns_filter_when_values_present() -> None:
    """Individual in_fields provider returns InCollectionFilter when values are given."""
    config = FilterConfig(in_fields={FieldNameType(name="status", type_hint=str)})
    deps = _create_statement_filters(config)

    provider = deps["status_in_filter"]
    result = provider.dependency(**{"status_values": ["active", "archived"]})

    assert isinstance(result, InCollectionFilter)
    assert result.field_name == "status"
    assert list(result.values) == ["active", "archived"]  # type: ignore[arg-type]


def test_in_fields_provider_returns_none_when_no_values() -> None:
    """Individual in_fields provider returns None when values is None."""
    config = FilterConfig(in_fields={FieldNameType(name="status", type_hint=str)})
    deps = _create_statement_filters(config)

    provider = deps["status_in_filter"]
    result = provider.dependency(**{"status_values": None})
    assert result is None


def test_in_fields_provider_uses_unique_signature_param_per_field() -> None:
    """Each in_fields provider exposes a unique parameter name to prevent cross-binding (issue #435)."""
    import inspect as _inspect

    config = FilterConfig(
        in_fields={FieldNameType(name="status", type_hint=str), FieldNameType(name="role", type_hint=str)}
    )
    deps = _create_statement_filters(config)

    status_params = list(_inspect.signature(deps["status_in_filter"].dependency).parameters)
    role_params = list(_inspect.signature(deps["role_in_filter"].dependency).parameters)

    assert status_params == ["status_values"]
    assert role_params == ["role_values"]


def test_not_in_fields_provider_returns_filter_when_values_present() -> None:
    """Individual not_in_fields provider returns NotInCollectionFilter when values are given."""
    config = FilterConfig(not_in_fields={FieldNameType(name="status", type_hint=str)})
    deps = _create_statement_filters(config)

    provider = deps["status_not_in_filter"]
    result = provider.dependency(**{"status_values": ["deleted", "archived"]})

    assert isinstance(result, NotInCollectionFilter)
    assert result.field_name == "status"
    assert list(result.values) == ["deleted", "archived"]  # type: ignore[arg-type]


def test_aggregate_function_includes_in_filter() -> None:
    """Aggregate function includes InCollectionFilter in results."""
    config = FilterConfig(in_fields={FieldNameType(name="status", type_hint=str)})
    agg_fn = _create_filter_aggregate_function(config)

    in_filter = InCollectionFilter(field_name="status", values=["active", "pending"])
    result = agg_fn(status_in_filter=in_filter)

    assert len(result) == 1
    assert isinstance(result[0], InCollectionFilter)
    assert result[0].field_name == "status"


def test_aggregate_function_excludes_none_in_filter() -> None:
    """Aggregate function excludes None in_filter values."""
    config = FilterConfig(in_fields={FieldNameType(name="status", type_hint=str)})
    agg_fn = _create_filter_aggregate_function(config)

    result = agg_fn(status_in_filter=None)
    assert result == []


def test_aggregate_function_multiple_in_fields() -> None:
    """Aggregate function handles multiple in_fields."""
    config = FilterConfig(
        in_fields={FieldNameType(name="status", type_hint=str), FieldNameType(name="role", type_hint=str)}
    )
    agg_fn = _create_filter_aggregate_function(config)

    status_filter = InCollectionFilter(field_name="status", values=["active"])
    role_filter = InCollectionFilter(field_name="role", values=["admin", "user"])

    result = agg_fn(status_in_filter=status_filter, role_in_filter=role_filter)

    assert len(result) == 2
    field_names = {f.field_name for f in result if isinstance(f, (InCollectionFilter, NotInCollectionFilter))}
    assert field_names == {"status", "role"}


def test_in_fields_with_string_config() -> None:
    """in_fields config with list of strings works correctly."""
    config = FilterConfig(in_fields=["status"])
    deps = _create_statement_filters(config)

    assert "status_in_filter" in deps
    assert "filters" in deps

    provider = deps["status_in_filter"]
    result = provider.dependency(**{"status_values": ["active"]})
    assert isinstance(result, InCollectionFilter)
    assert result.field_name == "status"


def test_order_by_provider_allows_configured_sort_field() -> None:
    """orderBy provider allows fields configured in sort_field."""
    config = FilterConfig(sort_field={"created_at", "name"})
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    result = provider.dependency(field_name="name", sort_order="asc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "name"
    assert result.sort_order == "asc"


def test_order_by_provider_rejects_unconfigured_sort_field() -> None:
    """orderBy provider rejects fields outside the configured allowlist."""
    config = FilterConfig(sort_field={"created_at", "name"})
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    with pytest.raises(ValidationException) as exc_info:
        provider.dependency(field_name="password_hash", sort_order="asc")

    assert "Invalid orderBy field" in str(exc_info.value)


def test_order_by_provider_accepts_camelized_sort_field_alias() -> None:
    """orderBy provider accepts camelized aliases by default."""
    config = FilterConfig(sort_field=["created_at", "uploaded_collections"])
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    result = provider.dependency(field_name="uploadedCollections", sort_order="asc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "uploaded_collections"
    assert result.sort_order == "asc"


def test_order_by_provider_accepts_explicit_sort_field_alias() -> None:
    """orderBy provider accepts explicit aliases when configured."""
    config = FilterConfig(
        sort_field=["created_at", "uploaded_collections"],
        sort_field_aliases={"uploadedCollections": "uploaded_collections"},
    )
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    result = provider.dependency(field_name="uploadedCollections", sort_order="asc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "uploaded_collections"
    assert result.sort_order == "asc"


def test_order_by_provider_keeps_snake_case_sort_field_in_alias_mode() -> None:
    """Automatic alias mode preserves legacy snake_case query values."""
    config = FilterConfig(sort_field=["created_at", "uploaded_collections"])
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    result = provider.dependency(field_name="uploaded_collections", sort_order="desc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "uploaded_collections"
    assert result.sort_order == "desc"


def test_order_by_provider_reports_display_aliases_for_invalid_alias() -> None:
    """Validation errors expose API-facing display values by default."""
    config = FilterConfig(sort_field=["created_at", "uploaded_collections"])
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    with pytest.raises(ValidationException) as exc_info:
        provider.dependency(field_name="uploadedCollectionz", sort_order="asc")

    message = str(exc_info.value)
    assert "Invalid orderBy field 'uploadedCollectionz'" in message
    assert "Allowed fields: createdAt, uploadedCollections" in message
    assert "uploaded_collections" not in message


def test_order_by_provider_uses_display_alias_as_query_default() -> None:
    """The orderBy parameter default uses the display alias by default."""
    config = FilterConfig(sort_field=["created_at", "uploaded_collections"])
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    field_param = inspect.signature(provider.dependency).parameters["field_name"]

    assert field_param.default.default == "createdAt"


def test_order_by_provider_can_disable_camelized_sort_field_aliases() -> None:
    """sort_field_camelize=False keeps orderBy validation snake_case-only."""
    config = FilterConfig(sort_field=["created_at", "uploaded_collections"], sort_field_camelize=False)
    deps = _create_statement_filters(config)

    provider = deps["order_by_filter"]
    field_param = inspect.signature(provider.dependency).parameters["field_name"]

    assert field_param.default.default == "created_at"
    with pytest.raises(ValidationException) as exc_info:
        provider.dependency(field_name="uploadedCollections", sort_order="asc")

    message = str(exc_info.value)
    assert "Invalid orderBy field 'uploadedCollections'" in message
    assert "Allowed fields: created_at, uploaded_collections" in message
