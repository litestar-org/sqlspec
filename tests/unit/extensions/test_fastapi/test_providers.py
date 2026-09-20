"""Unit tests for FastAPI filter providers."""

import datetime
import inspect
from typing import Any, Literal, get_args
from uuid import UUID, uuid4

import pytest
from fastapi.exceptions import RequestValidationError

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
from sqlspec.extensions.fastapi.providers import (
    DependencyDefaults,
    FieldNameType,
    FilterConfig,
    dep_cache,
    provide_filters,
)


@pytest.fixture(autouse=True)
def _clear_dependency_cache() -> Any:
    dep_cache.dependencies.clear()
    yield
    dep_cache.dependencies.clear()


def _get_order_by_dependency(provider: Any) -> Any:
    signature = provider.__signature__
    annotation = signature.parameters["order_by_filter"].annotation
    for arg in get_args(annotation):
        if hasattr(arg, "dependency"):
            return arg.dependency
    msg = "order_by_filter dependency was not found"
    raise AssertionError(msg)


def _get_dependency(provider: Any, name: str) -> Any:
    signature = provider.__signature__
    annotation = signature.parameters[name].annotation
    for arg in get_args(annotation):
        if hasattr(arg, "dependency"):
            return arg.dependency
    msg = f"{name} dependency was not found"
    raise AssertionError(msg)


def test_provide_filters_returns_callable() -> None:
    """Test provide_filters returns a callable."""
    config: FilterConfig = {"id_filter": UUID}
    result = provide_filters(config)
    assert callable(result)


def test_provide_filters_empty_config_returns_empty_list() -> None:
    """Test provide_filters with empty config returns function that returns empty list."""
    config: FilterConfig = {}
    provider = provide_filters(config)
    filters = provider()
    assert filters == []


def test_provide_filters_id_filter() -> None:
    """Test ID filter generation."""
    config: FilterConfig = {"id_filter": UUID}
    provider = provide_filters(config)

    # Check signature
    assert hasattr(provider, "__signature__")

    # Simulate FastAPI calling with None (no query param)
    filters = provider(id_filter=None)
    assert filters == []

    # Simulate FastAPI calling with UUIDs
    test_ids = [uuid4(), uuid4()]
    filters = provider(id_filter=InCollectionFilter(field_name="id", values=test_ids))
    assert len(filters) == 1
    assert isinstance(filters[0], InCollectionFilter)
    assert filters[0].field_name == "id"
    assert filters[0].values == test_ids


def test_provide_filters_custom_id_field() -> None:
    """Test ID filter with custom field name."""
    config: FilterConfig = {"id_filter": int, "id_field": "user_id"}
    provider = provide_filters(config)

    test_ids = [1, 2, 3]
    filters = provider(id_filter=InCollectionFilter(field_name="user_id", values=test_ids))
    assert len(filters) == 1
    assert filters[0].field_name == "user_id"  # type: ignore[union-attr]


def test_provide_filters_created_at() -> None:
    """Test created_at filter generation."""
    config: FilterConfig = {"created_at": True}
    provider = provide_filters(config)

    # No dates provided
    filters = provider(created_filter=None)
    assert filters == []

    # Before date only
    before_dt = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    filters = provider(created_filter=BeforeAfterFilter(field_name="created_at", before=before_dt, after=None))
    assert len(filters) == 1
    assert isinstance(filters[0], BeforeAfterFilter)
    assert filters[0].field_name == "created_at"
    assert filters[0].before == before_dt

    # After date only
    after_dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    filters = provider(created_filter=BeforeAfterFilter(field_name="created_at", before=None, after=after_dt))
    assert len(filters) == 1
    assert filters[0].after == after_dt  # type: ignore[union-attr]

    # Both dates
    filters = provider(created_filter=BeforeAfterFilter(field_name="created_at", before=before_dt, after=after_dt))
    assert len(filters) == 1
    assert filters[0].before == before_dt  # type: ignore[union-attr]
    assert filters[0].after == after_dt  # type: ignore[union-attr]


def test_provide_filters_updated_at() -> None:
    """Test updated_at filter generation."""
    config: FilterConfig = {"updated_at": True}
    provider = provide_filters(config)

    before_dt = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    filters = provider(updated_filter=BeforeAfterFilter(field_name="updated_at", before=before_dt, after=None))
    assert len(filters) == 1
    assert isinstance(filters[0], BeforeAfterFilter)
    assert filters[0].field_name == "updated_at"


def test_provide_filters_pagination() -> None:
    """Test pagination filter generation."""
    config: FilterConfig = {"pagination_type": "limit_offset"}
    provider = provide_filters(config)

    # Default pagination (page 1, size 20)
    filters = provider(limit_offset_filter=LimitOffsetFilter(limit=20, offset=0))
    assert len(filters) == 1
    assert isinstance(filters[0], LimitOffsetFilter)
    assert filters[0].limit == 20
    assert filters[0].offset == 0

    # Page 2
    filters = provider(limit_offset_filter=LimitOffsetFilter(limit=20, offset=20))
    assert filters[0].offset == 20  # type: ignore[union-attr]

    # Custom page size
    filters = provider(limit_offset_filter=LimitOffsetFilter(limit=50, offset=0))
    assert filters[0].limit == 50  # type: ignore[union-attr]


def test_provide_filters_custom_pagination_size() -> None:
    """Test pagination with custom default size."""
    config: FilterConfig = {"pagination_type": "limit_offset", "pagination_size": 50}
    provider = provide_filters(config)

    filters = provider(limit_offset_filter=LimitOffsetFilter(limit=50, offset=0))
    assert filters[0].limit == 50  # type: ignore[union-attr]


def test_provide_filters_search_string() -> None:
    """Test search filter generation with string fields."""
    config: FilterConfig = {"search": "name,email"}
    provider = provide_filters(config)

    # No search string
    filters = provider(search_filter=None)
    assert filters == []

    # With search string
    filters = provider(search_filter=SearchFilter(field_name={"name", "email"}, value="test", ignore_case=False))
    assert len(filters) == 1
    assert isinstance(filters[0], SearchFilter)
    assert filters[0].field_name == {"name", "email"}
    assert filters[0].value == "test"
    assert filters[0].ignore_case is False


def test_provide_filters_search_set() -> None:
    """Test search filter generation with set of fields."""
    config: FilterConfig = {"search": {"name", "email", "username"}}
    provider = provide_filters(config)

    filters = provider(
        search_filter=SearchFilter(field_name={"name", "email", "username"}, value="john", ignore_case=False)
    )
    assert len(filters) == 1
    assert filters[0].field_name == {"name", "email", "username"}  # type: ignore[union-attr]


def test_provide_filters_search_list_dependency() -> None:
    """FastAPI search configuration accepts list field names."""
    config: FilterConfig = {"search": ["name", "email"]}
    provider = provide_filters(config)
    search_dependency = _get_dependency(provider, "search_filter")

    result = search_dependency(search_string="john", ignore_case=True)

    assert isinstance(result, SearchFilter)
    assert result.field_name == {"name", "email"}
    assert result.value == "john"
    assert result.ignore_case is True


def test_provide_filters_search_ignore_case() -> None:
    """Test search filter with case insensitive flag."""
    config: FilterConfig = {"search": "name", "search_ignore_case": True}
    provider = provide_filters(config)

    filters = provider(search_filter=SearchFilter(field_name={"name"}, value="JOHN", ignore_case=True))
    assert len(filters) == 1
    assert filters[0].ignore_case is True  # type: ignore[union-attr]


def test_provide_filters_order_by() -> None:
    """Test order by filter generation."""
    config: FilterConfig = {"sort_field": "created_at"}
    provider = provide_filters(config)

    # Default order (desc)
    filters = provider(order_by_filter=OrderByFilter(field_name="created_at", sort_order="desc"))
    assert len(filters) == 1
    assert isinstance(filters[0], OrderByFilter)
    assert filters[0].field_name == "created_at"
    assert filters[0].sort_order == "desc"

    # Ascending order
    filters = provider(order_by_filter=OrderByFilter(field_name="created_at", sort_order="asc"))
    assert filters[0].sort_order == "asc"  # type: ignore[union-attr]


def test_provide_filters_custom_sort_order() -> None:
    """Test order by with custom default sort order."""
    config: FilterConfig = {"sort_field": "name", "sort_order": "asc"}
    provider = provide_filters(config)

    filters = provider(order_by_filter=OrderByFilter(field_name="name", sort_order="asc"))
    assert filters[0].sort_order == "asc"  # type: ignore[union-attr]


def test_provide_filters_order_by_accepts_camelized_sort_field_alias() -> None:
    """OrderBy dependency accepts camelized aliases by default."""
    config: FilterConfig = {"sort_field": ["created_at", "uploaded_collections"]}
    provider = provide_filters(config)

    order_by_dependency = _get_order_by_dependency(provider)
    result = order_by_dependency(field_name="uploadedCollections", sort_order="asc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "uploaded_collections"
    assert result.sort_order == "asc"


def test_provide_filters_order_by_accepts_explicit_sort_field_alias() -> None:
    """OrderBy dependency accepts explicit aliases when configured."""
    config: FilterConfig = {
        "sort_field": ["created_at", "uploaded_collections"],
        "sort_field_aliases": {"uploadedCollections": "uploaded_collections"},
    }
    provider = provide_filters(config)

    order_by_dependency = _get_order_by_dependency(provider)
    result = order_by_dependency(field_name="uploadedCollections", sort_order="asc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "uploaded_collections"
    assert result.sort_order == "asc"


def test_provide_filters_order_by_keeps_snake_case_sort_field_in_alias_mode() -> None:
    """Automatic alias mode preserves legacy snake_case orderBy values."""
    config: FilterConfig = {"sort_field": ["created_at", "uploaded_collections"]}
    provider = provide_filters(config)

    order_by_dependency = _get_order_by_dependency(provider)
    result = order_by_dependency(field_name="uploaded_collections", sort_order="desc")

    assert isinstance(result, OrderByFilter)
    assert result.field_name == "uploaded_collections"
    assert result.sort_order == "desc"


def test_provide_filters_order_by_invalid_alias_uses_fastapi_error_location() -> None:
    """Invalid aliases raise RequestValidationError at query.orderBy."""
    config: FilterConfig = {"sort_field": ["created_at", "uploaded_collections"]}
    provider = provide_filters(config)

    order_by_dependency = _get_order_by_dependency(provider)
    with pytest.raises(RequestValidationError) as exc_info:
        order_by_dependency(field_name="uploadedCollectionz", sort_order="asc")

    errors = exc_info.value.errors()
    assert errors[0]["loc"] == ("query", "orderBy")
    assert "Invalid orderBy field 'uploadedCollectionz'" in errors[0]["msg"]
    assert "Allowed fields: createdAt, uploadedCollections" in errors[0]["msg"]
    assert "uploaded_collections" not in errors[0]["msg"]


def test_provide_filters_order_by_uses_display_alias_as_query_default() -> None:
    """The orderBy dependency default uses the display alias by default."""
    config: FilterConfig = {"sort_field": ["created_at", "uploaded_collections"]}
    provider = provide_filters(config)

    order_by_dependency = _get_order_by_dependency(provider)
    field_param = inspect.signature(order_by_dependency).parameters["field_name"]

    assert field_param.default == "createdAt"


def test_provide_filters_order_by_can_disable_camelized_sort_field_aliases() -> None:
    """sort_field_camelize=False keeps orderBy validation snake_case-only."""
    config: FilterConfig = {"sort_field": ["created_at", "uploaded_collections"], "sort_field_camelize": False}
    provider = provide_filters(config)

    order_by_dependency = _get_order_by_dependency(provider)
    field_param = inspect.signature(order_by_dependency).parameters["field_name"]

    assert field_param.default == "created_at"
    with pytest.raises(RequestValidationError) as exc_info:
        order_by_dependency(field_name="uploadedCollections", sort_order="asc")

    errors = exc_info.value.errors()
    assert errors[0]["loc"] == ("query", "orderBy")
    assert "Invalid orderBy field 'uploadedCollections'" in errors[0]["msg"]
    assert "Allowed fields: created_at, uploaded_collections" in errors[0]["msg"]


def test_provide_filters_in_fields() -> None:
    """Test in-collection filter generation."""
    config: FilterConfig = {"in_fields": FieldNameType(name="status", type_hint=str)}
    provider = provide_filters(config)

    # No values
    filters = provider(status_in_filter=None)
    assert filters == []

    # With values
    filters = provider(status_in_filter=InCollectionFilter(field_name="status", values=["active", "pending"]))
    assert len(filters) == 1
    assert isinstance(filters[0], InCollectionFilter)
    assert filters[0].field_name == "status"
    assert filters[0].values == ["active", "pending"]


def test_provide_filters_not_in_fields() -> None:
    """Test not-in-collection filter generation."""
    config: FilterConfig = {"not_in_fields": FieldNameType(name="status", type_hint=str)}
    provider = provide_filters(config)

    filters = provider(status_not_in_filter=NotInCollectionFilter(field_name="status", values=["deleted", "archived"]))
    assert len(filters) == 1
    assert isinstance(filters[0], NotInCollectionFilter)
    assert filters[0].field_name == "status"
    assert filters[0].values == ["deleted", "archived"]


def test_provide_filters_multiple_in_fields() -> None:
    """Test multiple in-collection filters."""
    config: FilterConfig = {
        "in_fields": {FieldNameType(name="status", type_hint=str), FieldNameType(name="role", type_hint=str)}
    }
    provider = provide_filters(config)

    filters = provider(
        status_in_filter=InCollectionFilter(field_name="status", values=["active"]),
        role_in_filter=InCollectionFilter(field_name="role", values=["admin", "user"]),
    )
    assert len(filters) == 2


def test_provide_filters_combined() -> None:
    """Test combining multiple filter types."""
    config: FilterConfig = {
        "id_filter": UUID,
        "search": "name",
        "pagination_type": "limit_offset",
        "sort_field": "created_at",
        "created_at": True,
    }
    provider = provide_filters(config)

    test_id = uuid4()
    before_dt = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    filters = provider(
        id_filter=InCollectionFilter(field_name="id", values=[test_id]),
        search_filter=SearchFilter(field_name={"name"}, value="test", ignore_case=False),
        limit_offset_filter=LimitOffsetFilter(limit=20, offset=0),
        order_by_filter=OrderByFilter(field_name="created_at", sort_order="desc"),
        created_filter=BeforeAfterFilter(field_name="created_at", before=before_dt, after=None),
    )

    assert len(filters) == 5
    filter_types = {type(f) for f in filters}
    assert filter_types == {InCollectionFilter, SearchFilter, LimitOffsetFilter, OrderByFilter, BeforeAfterFilter}


def test_provide_filters_caching() -> None:
    """Test that identical configs return cached dependencies."""
    config: FilterConfig = {"id_filter": UUID, "search": "name"}

    provider1 = provide_filters(config)
    provider2 = provide_filters(config)

    # Should return the same function object due to caching
    assert provider1 is provider2


def test_provide_filters_custom_defaults() -> None:
    """Test custom dependency defaults."""
    custom_defaults = DependencyDefaults()
    custom_defaults.DEFAULT_PAGINATION_SIZE = 100

    config: FilterConfig = {"pagination_type": "limit_offset"}
    provider = provide_filters(config, dep_defaults=custom_defaults)

    # Note: The default size is baked into the config at provider creation time
    # so this test verifies the pattern works, not runtime behavior
    assert callable(provider)


def test_field_name_type_defaults() -> None:
    """Test FieldNameType default type hint."""
    field = FieldNameType(name="test")
    assert field.name == "test"
    assert field.type_hint is str

    field_int = FieldNameType(name="count", type_hint=int)
    assert field_int.type_hint is int


def test_provide_filters_filters_none_values() -> None:
    """Test that None filter values are excluded from results."""
    config: FilterConfig = {"id_filter": UUID, "search": "name", "created_at": True}
    provider = provide_filters(config)

    # Only provide one filter
    filters = provider(
        id_filter=InCollectionFilter(field_name="id", values=[uuid4()]), search_filter=None, created_filter=None
    )

    # Should only have the ID filter
    assert len(filters) == 1
    assert isinstance(filters[0], InCollectionFilter)


def test_provide_filters_search_without_value_excluded() -> None:
    """Test that search filters without values are excluded."""
    config: FilterConfig = {"search": "name"}
    provider = provide_filters(config)

    # SearchFilter with None value should be excluded
    filters = provider(search_filter=SearchFilter(field_name={"name"}, value=None, ignore_case=False))  # type: ignore[arg-type]
    assert filters == []


def test_provide_filters_in_fields_with_string_list() -> None:
    """Test in_fields config with list of strings (issue #405)."""
    config: FilterConfig = {"in_fields": [FieldNameType(name="status", type_hint=str)]}  # type: ignore[typeddict-item]
    provider = provide_filters(config)

    filters = provider(status_in_filter=InCollectionFilter(field_name="status", values=["active"]))
    assert len(filters) == 1
    assert isinstance(filters[0], InCollectionFilter)
    assert filters[0].field_name == "status"


def test_provide_filters_null_fields() -> None:
    """Test null_fields filter generation."""
    config: FilterConfig = {"null_fields": {"email"}}
    provider = provide_filters(config)

    # No filter
    filters = provider(email_null_filter=None)
    assert filters == []

    # With filter
    filters = provider(email_null_filter=NullFilter(field_name="email"))
    assert len(filters) == 1
    assert isinstance(filters[0], NullFilter)
    assert filters[0].field_name == "email"


def test_provide_filters_null_fields_list() -> None:
    """FastAPI null_fields accepts list field names."""
    config: FilterConfig = {"null_fields": ["email", "deleted_at"]}
    provider = provide_filters(config)

    filters = provider(
        email_null_filter=NullFilter(field_name="email"), deleted_at_null_filter=NullFilter(field_name="deleted_at")
    )

    assert len(filters) == 2
    assert {filter_.field_name for filter_ in filters if isinstance(filter_, NullFilter)} == {"email", "deleted_at"}


def test_provide_filters_not_null_fields() -> None:
    """Test not_null_fields filter generation."""
    config: FilterConfig = {"not_null_fields": {"email"}}
    provider = provide_filters(config)

    filters = provider(email_not_null_filter=NotNullFilter(field_name="email"))
    assert len(filters) == 1
    assert isinstance(filters[0], NotNullFilter)
    assert filters[0].field_name == "email"


def test_provide_filters_not_null_fields_list() -> None:
    """FastAPI not_null_fields accepts list field names."""
    config: FilterConfig = {"not_null_fields": ["email", "updated_at"]}
    provider = provide_filters(config)

    filters = provider(
        email_not_null_filter=NotNullFilter(field_name="email"),
        updated_at_not_null_filter=NotNullFilter(field_name="updated_at"),
    )

    assert len(filters) == 2
    assert {filter_.field_name for filter_ in filters if isinstance(filter_, NotNullFilter)} == {"email", "updated_at"}


def test_provide_filters_boolean_fields() -> None:
    """Test boolean_fields filter generation in FastAPI."""
    from sqlspec.core import BooleanFilter

    config: FilterConfig = {"boolean_fields": {"is_active"}}
    provider = provide_filters(config)

    filters = provider(is_active_boolean_filter=BooleanFilter(field_name="is_active", value=True))
    assert len(filters) == 1
    assert isinstance(filters[0], BooleanFilter)
    assert filters[0].field_name == "is_active"
    assert filters[0].value is True


def test_provide_filters_boolean_fields_list() -> None:
    """FastAPI boolean_fields accepts and advertises list field names."""
    from sqlspec.core import BooleanFilter

    assert "list[str]" in repr(FilterConfig.__annotations__["boolean_fields"])

    config: FilterConfig = {"boolean_fields": ["is_active", "is_staff"]}
    provider = provide_filters(config)

    filters = provider(
        is_active_boolean_filter=BooleanFilter(field_name="is_active", value=True),
        is_staff_boolean_filter=BooleanFilter(field_name="is_staff", value=False),
    )

    assert len(filters) == 2
    assert {filter_.field_name for filter_ in filters if isinstance(filter_, BooleanFilter)} == {
        "is_active",
        "is_staff",
    }


def test_provide_filters_choice_fields() -> None:
    """Test choice_fields filter generation in FastAPI."""
    from enum import Enum

    class StatusEnum(Enum):
        ACTIVE = "active"
        PENDING = "pending"

    from sqlspec.core import ChoicesFilter
    from sqlspec.extensions.fastapi.providers import ChoiceField

    config: FilterConfig = {"choice_fields": [ChoiceField("status", StatusEnum)]}
    provider = provide_filters(config)

    filters = provider(status_choices_filter=ChoicesFilter(field_name="status", values=[StatusEnum.ACTIVE]))
    assert len(filters) == 1
    assert isinstance(filters[0], ChoicesFilter)
    assert filters[0].field_name == "status"
    assert filters[0].values == [StatusEnum.ACTIVE]


@pytest.mark.parametrize("maximum", [None, 50, 2000])
def test_page_size_limit_is_applied(maximum: int | None) -> None:
    from typing import get_args

    config = FilterConfig(pagination_type="limit_offset")
    if maximum is not None:
        config["pagination_max_size"] = maximum
    provider = _get_dependency(provide_filters(config), "limit_offset_filter")
    assert next(
        item.le
        for item in get_args(inspect.signature(provider).parameters["page_size"].annotation)[1].metadata
        if hasattr(item, "le")
    ) == (1000 if maximum is None else maximum)


@pytest.mark.parametrize(
    "size, maximum, message",
    [
        (21, 20, "pagination_size must not exceed"),
        (20, 0, "pagination_max_size must be at least 1"),
        (20, -1, "pagination_max_size must be at least 1"),
    ],
)
def test_page_size_invalid_configuration(size: int, maximum: int, message: str) -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    config = FilterConfig(pagination_type="limit_offset", pagination_size=size, pagination_max_size=maximum)
    with pytest.raises(ImproperConfigurationError, match=message):
        _get_dependency(provide_filters(config), "limit_offset_filter")


def test_cursor_mode_requires_cursor_keys() -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    with pytest.raises(ImproperConfigurationError, match="requires a non-empty 'cursor_keys'"):
        provide_filters(FilterConfig(pagination_type="cursor"))


def test_cursor_provider_builds_filter_and_preserves_key_order() -> None:
    from sqlspec.core import CursorFilter, CursorKey

    config = FilterConfig(
        pagination_type="cursor", cursor_keys=[CursorKey("name"), CursorKey("id")], pagination_size=2, search="name"
    )
    provider = _get_dependency(provide_filters(config), "cursor_filter")
    result = provider()
    assert isinstance(result, CursorFilter)
    assert result.keys == tuple(config["cursor_keys"])
    assert result.limit == 2
    assert provider(page_size=5).limit == 5
    config["cursor_keys"] = list(reversed(config["cursor_keys"]))
    reverse_provider = _get_dependency(provide_filters(config), "cursor_filter")
    assert reverse_provider().keys == tuple(config["cursor_keys"])
    assert reverse_provider is not provider
    assert list(inspect.signature(provide_filters(config)).parameters) == ["search_filter", "cursor_filter"]


@pytest.mark.parametrize("field_name, order", [(None, None), ("name", "asc"), ("identifier", "desc")])
def test_cursor_provider_composes_order_by(field_name: str | None, order: Literal["asc", "desc"] | None) -> None:
    from sqlspec.core import CursorKey

    config = FilterConfig(
        pagination_type="cursor",
        cursor_keys=[CursorKey("id", nulls="last", result_name="identifier")],
        sort_field=["name", "id"],
        sort_field_aliases={"identifier": "id"},
    )
    provider = _get_dependency(provide_filters(config), "cursor_filter")
    keys = provider(field_name=field_name, sort_order=order).keys
    if field_name == "identifier":
        assert keys == (CursorKey("id", "desc", nulls="last", result_name="identifier"),)
    else:
        assert keys == (CursorKey("name", order or "desc"), config["cursor_keys"][0])


def test_cursor_provider_rejects_unknown_order_by() -> None:
    from sqlspec.core import CursorKey

    config = FilterConfig(pagination_type="cursor", cursor_keys=[CursorKey("id")], sort_field="name")
    provider = _get_dependency(provide_filters(config), "cursor_filter")
    with pytest.raises(RequestValidationError, match="Invalid orderBy"):
        provider(field_name="unknown")


@pytest.mark.parametrize("token", ["garbage", ""])
def test_cursor_provider_invalid_cursor(token: str) -> None:
    from sqlspec.core import CursorKey

    config = FilterConfig(pagination_type="cursor", cursor_keys=[CursorKey("id")])
    provider = _get_dependency(provide_filters(config), "cursor_filter")
    with pytest.raises(RequestValidationError, match="Invalid pagination cursor"):
        provider(cursor=token)


def test_cursor_secret_signs_tokens_and_rejects_tampering() -> None:
    import base64
    import json

    from sqlspec.core import CursorKey

    config = FilterConfig(pagination_type="cursor", cursor_keys=[CursorKey("id")], cursor_secret="secret")
    provider = _get_dependency(provide_filters(config), "cursor_filter")
    token = provider().encode({"id": 1}, backward=False)
    assert provider(cursor=token).limit == 20
    unsigned_config = FilterConfig(pagination_type="cursor", cursor_keys=[CursorKey("id")])
    config = unsigned_config
    unsigned_provider = _get_dependency(provide_filters(config), "cursor_filter")
    with pytest.raises(RequestValidationError, match="Invalid pagination cursor"):
        unsigned_provider(cursor=token)
    payload, signature = token.split(".")
    decoded = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    decoded["k"][0][0] = "s"
    tampered = base64.urlsafe_b64encode(json.dumps(decoded).encode()).rstrip(b"=").decode() + "." + signature
    with pytest.raises(RequestValidationError, match="Invalid pagination cursor"):
        provider(cursor=tampered)


def test_cursor_sort_field_order_changes_cached_default() -> None:
    from sqlspec.core import CursorKey

    config = FilterConfig(pagination_type="cursor", cursor_keys=[CursorKey("id")], sort_field=["name", "id"])
    first = _get_dependency(provide_filters(config), "cursor_filter")
    config["sort_field"] = ["id", "name"]
    second = _get_dependency(provide_filters(config), "cursor_filter")
    assert first().keys[0].field_name == "name"
    assert second().keys[0].field_name == "id"


@pytest.mark.parametrize("secrets", [(b"secret", "b'secret'"), ("b'secret'", b"secret")])
def test_cursor_secret_cache_preserves_type(secrets: tuple[str | bytes, str | bytes]) -> None:
    from sqlspec.core import CursorFilter, CursorKey

    keys = [CursorKey("id")]
    providers = []
    for secret in secrets:
        config = FilterConfig(pagination_type="cursor", cursor_keys=keys, cursor_secret=secret)
        providers.append(_get_dependency(provide_filters(config), "cursor_filter"))
    for index, secret in enumerate(secrets):
        token = CursorFilter(keys, 20, secret=secret).encode({"id": 1}, backward=False)
        assert providers[index](cursor=token).limit == 20
        with pytest.raises(RequestValidationError, match="Invalid pagination cursor"):
            providers[1 - index](cursor=token)
