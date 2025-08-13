"""Unit tests for SQL statement filters.

This module tests the filter system that provides dynamic WHERE clauses,
ORDER BY, LIMIT/OFFSET, and other SQL modifications with proper parameter naming.
"""

from datetime import datetime

from sqlspec.core.filters import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    OrderByFilter,
    SearchFilter,
    apply_filter,
)
from sqlspec.core.statement import SQL
from sqlspec.driver._common import CommonDriverAttributesMixin


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
    # NotInCollectionFilter includes object id for uniqueness
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

    # This should create a conflict with the existing name_search parameter
    filter_obj = SearchFilter("name", "new_value")

    result = apply_filter(sql_stmt, filter_obj)

    # Should have the original parameter plus a new unique one
    assert "name_search" in result.parameters
    assert result.parameters["name_search"] == "existing"

    # Find the new parameter (it will have a UUID suffix)
    new_param_keys = [k for k in result.parameters.keys() if k.startswith("name_search_") and k != "name_search"]
    assert len(new_param_keys) == 1
    assert result.parameters[new_param_keys[0]] == "%new_value%"


def test_multiple_filters_preserve_column_names() -> None:
    """Test that multiple filters maintain column-based parameter naming and merge properly."""
    sql_stmt = SQL("SELECT * FROM users")

    # Apply multiple filters in sequence
    status_filter = InCollectionFilter("status", ["active", "pending"])
    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)

    result = sql_stmt
    result = apply_filter(result, status_filter)
    result = apply_filter(result, search_filter)
    result = apply_filter(result, limit_filter)

    # Check that all parameters use descriptive names and are preserved
    params = result.parameters

    # Status filter parameters
    assert "status_in_0" in params
    assert "status_in_1" in params
    assert params["status_in_0"] == "active"
    assert params["status_in_1"] == "pending"

    # Search filter parameter
    assert "name_search" in params
    assert params["name_search"] == "%john%"

    # Pagination filter parameters
    assert "limit" in params
    assert "offset" in params
    assert params["limit"] == 10
    assert params["offset"] == 0

    # Verify final SQL contains all components
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
    # Empty IN filter
    empty_in_filter: InCollectionFilter[str] = InCollectionFilter("status", [])
    positional, named = empty_in_filter.extract_parameters()
    assert positional == []
    assert named == {}

    # None values
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
    assert "search_value" in named  # Uses generic name for multiple fields
    assert named["search_value"] == "%john%"


def test_cache_key_generation() -> None:
    """Test that filters generate proper cache keys."""
    # BeforeAfterFilter
    before_date = datetime(2023, 12, 31)
    after_date = datetime(2023, 1, 1)
    ba_filter = BeforeAfterFilter("created_at", before=before_date, after=after_date)

    cache_key = ba_filter.get_cache_key()
    assert cache_key[0] == "BeforeAfterFilter"
    assert cache_key[1] == "created_at"
    assert before_date in cache_key
    assert after_date in cache_key

    # InCollectionFilter
    in_filter = InCollectionFilter("status", ["active", "pending"])
    cache_key = in_filter.get_cache_key()
    assert cache_key[0] == "InCollectionFilter"
    assert cache_key[1] == "status"
    assert cache_key[2] == ("active", "pending")


def test_filter_sql_generation_preserves_parameter_names() -> None:
    """Test that applying filters to SQL generates proper parameter placeholders."""
    sql_stmt = SQL("SELECT * FROM users")

    # Apply a search filter
    search_filter = SearchFilter("name", "john")
    result = apply_filter(sql_stmt, search_filter)

    # Check that SQL contains the named parameter
    assert ":name_search" in result.sql
    assert "name_search" in result.parameters
    assert result.parameters["name_search"] == "%john%"

    # Apply an IN filter
    in_filter = InCollectionFilter("status", ["active", "pending"])
    result = apply_filter(result, in_filter)

    # Check both filters' parameters are preserved
    assert ":status_in_0" in result.sql
    assert ":status_in_1" in result.sql
    assert "status_in_0" in result.parameters
    assert "status_in_1" in result.parameters
    assert result.parameters["status_in_0"] == "active"
    assert result.parameters["status_in_1"] == "pending"


def test_find_filter_returns_matching_filter() -> None:
    """Test that find_filter returns the first matching filter of the specified type."""
    # Create a list of filters with different types
    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)
    in_filter = InCollectionFilter("status", ["active", "pending"])
    order_filter = OrderByFilter("created_at", "desc")

    filters = [search_filter, limit_filter, in_filter, order_filter]

    # Test finding each type of filter
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
    # Create a list of filters without BeforeAfterFilter
    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)

    filters = [search_filter, limit_filter]

    # Try to find a filter type that doesn't exist in the list
    found_filter = CommonDriverAttributesMixin.find_filter(BeforeAfterFilter, filters)
    assert found_filter is None


def test_find_filter_returns_first_match_when_multiple_exist() -> None:
    """Test that find_filter returns the first matching filter when multiple of the same type exist."""
    # Create multiple filters of the same type
    filter1 = SearchFilter("name", "john")
    filter2 = SearchFilter("email", "test@example.com")
    other_filter = LimitOffsetFilter(10, 0)

    filters = [filter1, other_filter, filter2]

    # Should return the first matching filter
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
    # Test with a mixture of filters and other objects (simulating StatementParameters)
    search_filter = SearchFilter("name", "john")
    some_parameter = {"key": "value"}  # Simulating StatementParameters
    limit_filter = LimitOffsetFilter(5, 10)

    # Mixed list with different types
    filters: list[object] = [search_filter, some_parameter, limit_filter]

    # Should find the filter even with mixed types
    found_search = CommonDriverAttributesMixin.find_filter(SearchFilter, filters)
    assert found_search is search_filter

    found_limit = CommonDriverAttributesMixin.find_filter(LimitOffsetFilter, filters)
    assert found_limit is limit_filter

    # Should return None for filter types not in the list
    found_order = CommonDriverAttributesMixin.find_filter(OrderByFilter, filters)
    assert found_order is None
