# pyright: reportArgumentType=false, reportCallIssue=false
"""Unit tests for Litestar filter providers (issue #405)."""

import datetime
import inspect
from typing import Any, NoReturn, cast

import pytest
from litestar.config.app import AppConfig
from litestar.exceptions import ValidationException
from litestar.types import HTTPScope

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core import (
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    OrderByFilter,
    SearchFilter,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin
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
    assert result.values is not None
    assert list(result.values) == ["active", "archived"]


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
    assert result.values is not None
    assert list(result.values) == ["deleted", "archived"]


def test_compiled_annotation_pair_providers_build_and_call() -> None:
    """Providers with paired annotation locals instantiate and call under compiled builds."""
    before = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    after = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    deps = _create_statement_filters(
        FilterConfig(
            created_at=True,
            updated_at=True,
            pagination_type="limit_offset",
            pagination_size=25,
            search=["name", "email"],
            search_ignore_case=True,
            sort_field=["created_at", "name"],
            sort_order="asc",
        )
    )
    created_filter = deps["created_filter"].dependency(created_at_before=before, created_at_after=after)
    updated_filter = deps["updated_filter"].dependency(updated_at_before=before, updated_at_after=after)
    limit_offset_filter = deps["limit_offset_filter"].dependency(current_page=3, page_size=10)
    search_filter = deps["search_filter"].dependency(search_string="alice", ignore_case=False)
    order_by_filter = deps["order_by_filter"].dependency(field_name="name", sort_order="desc")
    assert isinstance(created_filter, BeforeAfterFilter)
    assert created_filter.field_name == "created_at"
    assert created_filter.before == before
    assert created_filter.after == after
    assert isinstance(updated_filter, BeforeAfterFilter)
    assert updated_filter.field_name == "updated_at"
    assert updated_filter.before == before
    assert updated_filter.after == after
    assert isinstance(limit_offset_filter, LimitOffsetFilter)
    assert limit_offset_filter.limit == 10
    assert limit_offset_filter.offset == 20
    assert isinstance(search_filter, SearchFilter)
    assert search_filter.field_name == {"name", "email"}
    assert search_filter.value == "alice"
    assert search_filter.ignore_case is False
    assert isinstance(order_by_filter, OrderByFilter)
    assert order_by_filter.field_name == "name"
    assert order_by_filter.sort_order == "desc"


def test_null_fields_list_creates_litestar_providers() -> None:
    config = FilterConfig(null_fields=["email", "deleted_at"])
    deps = _create_statement_filters(config)
    email_filter = deps["email_null_filter"].dependency(email_is_null=True)
    deleted_filter = deps["deleted_at_null_filter"].dependency(deleted_at_is_null=True)
    assert isinstance(email_filter, NullFilter)
    assert email_filter.field_name == "email"
    assert isinstance(deleted_filter, NullFilter)
    assert deleted_filter.field_name == "deleted_at"


def test_not_null_fields_list_creates_litestar_providers() -> None:
    config = FilterConfig(not_null_fields=["email", "updated_at"])
    deps = _create_statement_filters(config)
    email_filter = deps["email_not_null_filter"].dependency(email_is_not_null=True)
    updated_filter = deps["updated_at_not_null_filter"].dependency(updated_at_is_not_null=True)
    assert isinstance(email_filter, NotNullFilter)
    assert email_filter.field_name == "email"
    assert isinstance(updated_filter, NotNullFilter)
    assert updated_filter.field_name == "updated_at"


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
    assert field_param.default == "createdAt"


def test_order_by_provider_can_disable_camelized_sort_field_aliases() -> None:
    """sort_field_camelize=False keeps orderBy validation snake_case-only."""
    config = FilterConfig(sort_field=["created_at", "uploaded_collections"], sort_field_camelize=False)
    deps = _create_statement_filters(config)
    provider = deps["order_by_filter"]
    field_param = inspect.signature(provider.dependency).parameters["field_name"]
    assert field_param.default == "created_at"
    with pytest.raises(ValidationException) as exc_info:
        provider.dependency(field_name="uploadedCollections", sort_order="asc")
    message = str(exc_info.value)
    assert "Invalid orderBy field 'uploadedCollections'" in message
    assert "Allowed fields: created_at, uploaded_collections" in message


def test_boolean_fields_creates_provider_and_aggregate_param() -> None:
    """boolean_fields config creates both individual provider and aggregate parameter."""
    from sqlspec.core import BooleanFilter

    config = FilterConfig(boolean_fields={"is_active"})
    deps = _create_statement_filters(config)
    assert "is_active_boolean_filter" in deps
    assert "filters" in deps
    provider = deps["is_active_boolean_filter"]
    result = provider.dependency(**{"is_active_boolean": True})
    assert isinstance(result, BooleanFilter)
    assert result.field_name == "is_active"
    assert result.value is True


def test_choice_fields_creates_provider_and_aggregate_param() -> None:
    """choice_fields config creates both individual provider and aggregate parameter."""
    from enum import Enum

    class StatusEnum(Enum):
        ACTIVE = "active"
        PENDING = "pending"

    from sqlspec.core import ChoicesFilter
    from sqlspec.extensions.litestar.providers import ChoiceField

    config = FilterConfig(choice_fields={ChoiceField("status", StatusEnum)})
    deps = _create_statement_filters(config)
    assert "status_choices_filter" in deps
    assert "filters" in deps
    provider = deps["status_choices_filter"]
    result = provider.dependency(**{"status_choices": [StatusEnum.ACTIVE]})
    assert isinstance(result, ChoicesFilter)
    assert result.field_name == "status"
    assert result.values == [StatusEnum.ACTIVE]


def test_aggregate_function_uses_new_style_parameter_annotations() -> None:
    """Aggregate function uses NamedDependency and SkipValidation annotations for Litestar 3.0."""
    config = FilterConfig(created_at=True)
    aggregate_fn = _create_filter_aggregate_function(config)
    sig = inspect.signature(aggregate_fn)
    param = sig.parameters["created_filter"]
    annotation = param.annotation
    assert "NamedDependency" in repr(annotation) or "Dependency" in repr(annotation)
    assert "SkipValidation" in repr(annotation)
    assert param.default == inspect.Parameter.empty


def _build_plugin() -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


def test_raise_missing_connection_raise_missing_connection_always_raises() -> None:
    plugin = _build_plugin()
    try:
        plugin._raise_missing_connection("db_connection")
    except ImproperConfigurationError as exc:
        assert "db_connection" in str(exc)
    else:
        msg = "_raise_missing_connection should not return"
        raise AssertionError(msg)


def test_raise_missing_connection_get_plugin_state_raises_for_unknown_key() -> None:
    plugin = _build_plugin()
    plugin.on_app_init(AppConfig())
    try:
        plugin._get_plugin_state("unknown")
    except KeyError as exc:
        assert "unknown" in str(exc)
    else:
        msg = "_get_plugin_state should raise for unknown keys"
        raise AssertionError(msg)


def test_raise_missing_connection_provide_request_connection_raises_when_connection_missing() -> None:
    plugin = _build_plugin()
    state = AppConfig().state
    scope = cast("HTTPScope", {"type": "http"})
    try:
        plugin.provide_request_connection("db_connection", state, scope)
    except ImproperConfigurationError as exc:
        assert "db_connection" in str(exc)
    else:
        msg = "provide_request_connection should raise when no connection is scoped"
        raise AssertionError(msg)


def test_raise_missing_connection_provide_request_session_raises_when_connection_missing() -> None:
    plugin = _build_plugin()
    state = AppConfig().state
    scope = cast("HTTPScope", {"type": "http"})
    try:
        plugin.provide_request_session("db_connection", state, scope)
    except ImproperConfigurationError as exc:
        assert "db_connection" in str(exc)
    else:
        msg = "provide_request_session should raise when no connection is scoped"
        raise AssertionError(msg)


def test_raise_missing_connection_raise_missing_connection_annotation_is_noreturn() -> None:
    assert SQLSpecPlugin._raise_missing_connection.__annotations__["return"] is NoReturn
