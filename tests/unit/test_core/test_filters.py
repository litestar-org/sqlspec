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
