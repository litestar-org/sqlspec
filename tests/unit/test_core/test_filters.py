"""Unit tests for SQL statement filters.

This module tests the filter system that provides dynamic WHERE clauses,
ORDER BY, LIMIT/OFFSET, and other SQL modifications with proper parameter naming.
"""

from datetime import datetime

import pytest

from sqlspec.core import (
    SQL,
    AnyCollectionFilter,
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    OrderByFilter,
    SearchFilter,
    apply_filter,
)
from sqlspec.driver import CommonDriverAttributesMixin

pytestmark = pytest.mark.xdist_group("core")


def test_before_after_filter_uses_column_based_parameters() -> None:
    """Test that BeforeAfterFilter uses column-based parameter names."""
    before_date = datetime(2023, 12, 31)
    after_date = datetime(2023, 1, 1)

    filter_obj = BeforeAfterFilter("created_at", before=before_date, after=after_date)

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "created_at_before" in named
    assert "created_at_after" in named
    assert named["created_at_before"] == before_date
    assert named["created_at_after"] == after_date


def test_in_collection_filter_uses_column_based_parameters() -> None:
    """Test that InCollectionFilter uses column-based parameter names."""
    values = ["active", "pending", "completed"]

    filter_obj = InCollectionFilter("status", values)

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "status_in_0" in named
    assert "status_in_1" in named
    assert "status_in_2" in named
    assert named["status_in_0"] == "active"
    assert named["status_in_1"] == "pending"
    assert named["status_in_2"] == "completed"


def test_search_filter_uses_column_based_parameters() -> None:
    """Test that SearchFilter uses column-based parameter names."""
    filter_obj = SearchFilter("name", "john")

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "name_search" in named
    assert named["name_search"] == "%john%"


def test_any_collection_filter_uses_column_based_parameters() -> None:
    """Test that AnyCollectionFilter uses column-based parameter names."""
    values = [1, 2, 3]

    filter_obj = AnyCollectionFilter("user_id", values)

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "user_id_any_0" in named
    assert "user_id_any_1" in named
    assert "user_id_any_2" in named
    assert named["user_id_any_0"] == 1
    assert named["user_id_any_1"] == 2
    assert named["user_id_any_2"] == 3


def test_not_in_collection_filter_uses_column_based_parameters() -> None:
    """Test that NotInCollectionFilter uses column-based parameter names."""
    values = ["deleted", "archived"]

    filter_obj = NotInCollectionFilter("status", values)

    positional, named = filter_obj.extract_parameters()

    assert positional == []

    param_names = list(named.keys())
    assert len(param_names) == 2
    assert all("status_notin_" in name for name in param_names)
    assert "deleted" in named.values()
    assert "archived" in named.values()


def test_limit_offset_filter_uses_descriptive_parameters() -> None:
    """Test that LimitOffsetFilter uses descriptive parameter names."""
    filter_obj = LimitOffsetFilter(limit=25, offset=50)

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "limit" in named
    assert "offset" in named
    assert named["limit"] == 25
    assert named["offset"] == 50


def test_order_by_filter_no_parameters() -> None:
    """Test that OrderByFilter doesn't use parameters."""
    filter_obj = OrderByFilter("created_at", "desc")

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert named == {}


def test_filter_parameter_conflict_resolution() -> None:
    """Test that filters resolve parameter name conflicts."""
    sql_stmt = SQL("SELECT * FROM users WHERE name = :name_search", {"name_search": "existing"})

    filter_obj = SearchFilter("name", "new_value")

    result = apply_filter(sql_stmt, filter_obj)

    assert "name_search" in result.parameters
    assert result.parameters["name_search"] == "existing"

    new_param_keys = [k for k in result.parameters.keys() if k.startswith("name_search_") and k != "name_search"]
    assert len(new_param_keys) == 1
    assert result.parameters[new_param_keys[0]] == "%new_value%"


def test_multiple_filters_preserve_column_names() -> None:
    """Test that multiple filters maintain column-based parameter naming and merge properly."""
    sql_stmt = SQL("SELECT * FROM users")

    status_filter = InCollectionFilter("status", ["active", "pending"])
    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)

    result = sql_stmt
    result = apply_filter(result, status_filter)
    result = apply_filter(result, search_filter)
    result = apply_filter(result, limit_filter)

    params = result.parameters

    assert "status_in_0" in params
    assert "status_in_1" in params
    assert params["status_in_0"] == "active"
    assert params["status_in_1"] == "pending"

    assert "name_search" in params
    assert params["name_search"] == "%john%"

    assert "limit" in params
    assert "offset" in params
    assert params["limit"] == 10
    assert params["offset"] == 0

    sql_text = result.sql.upper()
    assert "SELECT" in sql_text
    assert "FROM" in sql_text
    assert "WHERE" in sql_text
    assert "STATUS IN" in sql_text
    assert "NAME LIKE" in sql_text
    assert "LIMIT" in sql_text
    assert "OFFSET" in sql_text


def test_filter_with_empty_values() -> None:
    """Test filters handle empty values correctly."""

    empty_in_filter: InCollectionFilter[str] = InCollectionFilter("status", [])
    positional, named = empty_in_filter.extract_parameters()
    assert positional == []
    assert named == {}

    none_in_filter: InCollectionFilter[str] = InCollectionFilter("status", None)
    positional, named = none_in_filter.extract_parameters()
    assert positional == []
    assert named == {}


def test_search_filter_multiple_fields() -> None:
    """Test SearchFilter with multiple field names."""
    fields = {"first_name", "last_name", "email"}
    filter_obj = SearchFilter(fields, "john")

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "search_value" in named
    assert named["search_value"] == "%john%"


def test_cache_key_generation() -> None:
    """Test that filters generate proper cache keys."""

    before_date = datetime(2023, 12, 31)
    after_date = datetime(2023, 1, 1)
    ba_filter = BeforeAfterFilter("created_at", before=before_date, after=after_date)

    cache_key = ba_filter.get_cache_key()
    assert cache_key[0] == "BeforeAfterFilter"
    assert cache_key[1] == "created_at"
    assert before_date in cache_key
    assert after_date in cache_key

    in_filter = InCollectionFilter("status", ["active", "pending"])
    cache_key = in_filter.get_cache_key()
    assert cache_key[0] == "InCollectionFilter"
    assert cache_key[1] == "status"
    assert cache_key[2] == ("active", "pending")


def test_filter_sql_generation_preserves_parameter_names() -> None:
    """Test that applying filters to SQL generates proper parameter placeholders."""
    sql_stmt = SQL("SELECT * FROM users")

    search_filter = SearchFilter("name", "john")
    result = apply_filter(sql_stmt, search_filter)

    assert ":name_search" in result.sql
    assert "name_search" in result.parameters
    assert result.parameters["name_search"] == "%john%"

    in_filter = InCollectionFilter("status", ["active", "pending"])
    result = apply_filter(result, in_filter)

    assert ":status_in_0" in result.sql
    assert ":status_in_1" in result.sql
    assert "status_in_0" in result.parameters
    assert "status_in_1" in result.parameters
    assert result.parameters["status_in_0"] == "active"
    assert result.parameters["status_in_1"] == "pending"


def test_find_filter_returns_matching_filter() -> None:
    """Test that find_filter returns the first matching filter of the specified type."""

    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)
    in_filter = InCollectionFilter("status", ["active", "pending"])
    order_filter = OrderByFilter("created_at", "desc")

    filters = [search_filter, limit_filter, in_filter, order_filter]

    found_search = CommonDriverAttributesMixin.find_filter(SearchFilter, filters)
    assert found_search is search_filter
    assert found_search is not None
    assert found_search.field_name == "name"
    assert found_search.value == "john"

    found_limit = CommonDriverAttributesMixin.find_filter(LimitOffsetFilter, filters)
    assert found_limit is limit_filter
    assert found_limit is not None
    assert found_limit.limit == 10
    assert found_limit.offset == 0

    found_in = CommonDriverAttributesMixin.find_filter(InCollectionFilter, filters)
    assert found_in is in_filter
    assert found_in is not None
    assert found_in.field_name == "status"
    assert found_in.values == ["active", "pending"]

    found_order = CommonDriverAttributesMixin.find_filter(OrderByFilter, filters)
    assert found_order is order_filter
    assert found_order is not None
    assert found_order.field_name == "created_at"
    assert found_order.sort_order == "desc"


def test_find_filter_returns_none_when_not_found() -> None:
    """Test that find_filter returns None when no matching filter is found."""

    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)

    filters = [search_filter, limit_filter]

    found_filter = CommonDriverAttributesMixin.find_filter(BeforeAfterFilter, filters)
    assert found_filter is None


def test_find_filter_returns_first_match_when_multiple_exist() -> None:
    """Test that find_filter returns the first matching filter when multiple of the same type exist."""

    filter1 = SearchFilter("name", "john")
    filter2 = SearchFilter("email", "test@example.com")
    other_filter = LimitOffsetFilter(10, 0)

    filters = [filter1, other_filter, filter2]

    found_filter = CommonDriverAttributesMixin.find_filter(SearchFilter, filters)
    assert found_filter is filter1
    assert found_filter is not None
    assert found_filter.field_name == "name"
    assert found_filter.value == "john"


def test_find_filter_with_empty_filters_list() -> None:
    """Test that find_filter returns None when given an empty filters list."""
    filters: list[object] = []

    found_filter = CommonDriverAttributesMixin.find_filter(SearchFilter, filters)
    assert found_filter is None


def test_find_filter_with_mixed_parameter_types() -> None:
    """Test that find_filter works with mixed filter and parameter types."""

    search_filter = SearchFilter("name", "john")
    some_parameter = {"key": "value"}
    limit_filter = LimitOffsetFilter(5, 10)

    filters: list[object] = [search_filter, some_parameter, limit_filter]

    found_search = CommonDriverAttributesMixin.find_filter(SearchFilter, filters)
    assert found_search is search_filter

    found_limit = CommonDriverAttributesMixin.find_filter(LimitOffsetFilter, filters)
    assert found_limit is limit_filter

    found_order = CommonDriverAttributesMixin.find_filter(OrderByFilter, filters)
    assert found_order is None


def test_null_filter_generates_is_null_clause() -> None:
    """Test that NullFilter generates correct IS NULL SQL clause."""
    sql_stmt = SQL("SELECT * FROM users")

    null_filter = NullFilter("email")
    result = apply_filter(sql_stmt, null_filter)

    sql_text = result.sql.upper()
    assert "SELECT" in sql_text
    assert "FROM" in sql_text
    assert "WHERE" in sql_text
    assert "EMAIL IS NULL" in sql_text


def test_not_null_filter_generates_is_not_null_clause() -> None:
    """Test that NotNullFilter generates correct IS NOT NULL SQL clause."""
    sql_stmt = SQL("SELECT * FROM users")

    not_null_filter = NotNullFilter("email_verified_at")
    result = apply_filter(sql_stmt, not_null_filter)

    sql_text = result.sql.upper()
    assert "SELECT" in sql_text
    assert "FROM" in sql_text
    assert "WHERE" in sql_text
    # NotNullFilter generates "NOT column IS NULL" which is equivalent to "column IS NOT NULL"
    assert "NOT EMAIL_VERIFIED_AT IS NULL" in sql_text


def test_null_filter_extract_parameters_returns_empty() -> None:
    """Test that NullFilter returns empty parameters since IS NULL needs no values."""
    null_filter = NullFilter("status")

    positional, named = null_filter.extract_parameters()

    assert positional == []
    assert named == {}


def test_not_null_filter_extract_parameters_returns_empty() -> None:
    """Test that NotNullFilter returns empty parameters since IS NOT NULL needs no values."""
    not_null_filter = NotNullFilter("status")

    positional, named = not_null_filter.extract_parameters()

    assert positional == []
    assert named == {}


def test_null_filter_cache_key() -> None:
    """Test that NullFilter generates proper cache key."""
    null_filter = NullFilter("email")

    cache_key = null_filter.get_cache_key()

    assert cache_key[0] == "NullFilter"
    assert cache_key[1] == "email"
    assert len(cache_key) == 2


def test_not_null_filter_cache_key() -> None:
    """Test that NotNullFilter generates proper cache key."""
    not_null_filter = NotNullFilter("email_verified_at")

    cache_key = not_null_filter.get_cache_key()

    assert cache_key[0] == "NotNullFilter"
    assert cache_key[1] == "email_verified_at"
    assert len(cache_key) == 2


def test_null_filter_field_name_property() -> None:
    """Test that NullFilter field_name property works correctly."""
    null_filter = NullFilter("deleted_at")

    assert null_filter.field_name == "deleted_at"


def test_not_null_filter_field_name_property() -> None:
    """Test that NotNullFilter field_name property works correctly."""
    not_null_filter = NotNullFilter("created_at")

    assert not_null_filter.field_name == "created_at"


def test_null_filter_with_other_filters() -> None:
    """Test that NullFilter can be combined with other filters."""
    sql_stmt = SQL("SELECT * FROM users")

    status_filter = InCollectionFilter("status", ["active", "pending"])
    null_filter = NullFilter("deleted_at")

    result = apply_filter(sql_stmt, status_filter)
    result = apply_filter(result, null_filter)

    sql_text = result.sql.upper()
    assert "STATUS IN" in sql_text
    assert "DELETED_AT IS NULL" in sql_text

    params = result.parameters
    assert "status_in_0" in params
    assert "status_in_1" in params
    assert params["status_in_0"] == "active"
    assert params["status_in_1"] == "pending"


def test_not_null_filter_with_other_filters() -> None:
    """Test that NotNullFilter can be combined with other filters."""
    sql_stmt = SQL("SELECT * FROM users")

    search_filter = SearchFilter("name", "john")
    not_null_filter = NotNullFilter("email_verified_at")

    result = apply_filter(sql_stmt, search_filter)
    result = apply_filter(result, not_null_filter)

    sql_text = result.sql.upper()
    assert "NAME LIKE" in sql_text
    # NotNullFilter generates "NOT column IS NULL" which is equivalent to "column IS NOT NULL"
    assert "NOT EMAIL_VERIFIED_AT IS NULL" in sql_text

    params = result.parameters
    assert "name_search" in params
    assert params["name_search"] == "%john%"


def test_null_and_not_null_filters_together() -> None:
    """Test that NullFilter and NotNullFilter can be used together for different columns."""
    sql_stmt = SQL("SELECT * FROM users")

    null_filter = NullFilter("deleted_at")
    not_null_filter = NotNullFilter("email_verified_at")

    result = apply_filter(sql_stmt, null_filter)
    result = apply_filter(result, not_null_filter)

    sql_text = result.sql.upper()
    assert "DELETED_AT IS NULL" in sql_text
    # NotNullFilter generates "NOT column IS NULL" which is equivalent to "column IS NOT NULL"
    assert "NOT EMAIL_VERIFIED_AT IS NULL" in sql_text


def test_null_filter_cache_key_uniqueness() -> None:
    """Test that NullFilter cache keys are unique per field name."""
    filter1 = NullFilter("email")
    filter2 = NullFilter("phone")
    filter3 = NullFilter("email")

    key1 = filter1.get_cache_key()
    key2 = filter2.get_cache_key()
    key3 = filter3.get_cache_key()

    assert key1 != key2
    assert key1 == key3


def test_not_null_filter_cache_key_uniqueness() -> None:
    """Test that NotNullFilter cache keys are unique per field name."""
    filter1 = NotNullFilter("email")
    filter2 = NotNullFilter("phone")
    filter3 = NotNullFilter("email")

    key1 = filter1.get_cache_key()
    key2 = filter2.get_cache_key()
    key3 = filter3.get_cache_key()

    assert key1 != key2
    assert key1 == key3
