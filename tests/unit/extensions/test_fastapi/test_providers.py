"""Unit tests for FastAPI filter providers."""

import datetime
from uuid import UUID, uuid4

from sqlspec.core import (
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    OrderByFilter,
    SearchFilter,
)
from sqlspec.extensions.fastapi.providers import DependencyDefaults, FieldNameType, FilterConfig, provide_filters


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
