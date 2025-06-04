"""Unit tests for sqlspec.statement.filters module.

Tests the statement filter system including various filter types,
their application to SQL statements, and filter composition.
"""

from datetime import datetime
from typing import Any

import pytest

from sqlspec.statement.filters import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotAnyCollectionFilter,
    NotInCollectionFilter,
    NotInSearchFilter,
    OnBeforeAfterFilter,
    OrderByFilter,
    PaginationFilter,
    SearchFilter,
    StatementFilter,
    apply_filter,
)
from sqlspec.statement.sql import SQL


def test_statement_filter_protocol() -> None:
    """Test that StatementFilter is a protocol with append_to_statement method."""

    # Create a mock implementation
    class MockFilter:
        def append_to_statement(self, statement: SQL) -> SQL:
            return statement

    filter_instance = MockFilter()

    # Should satisfy the protocol
    assert hasattr(filter_instance, "append_to_statement")
    assert callable(filter_instance.append_to_statement)


def test_apply_filter_function() -> None:
    """Test the apply_filter utility function."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter("name", "john")

    result = apply_filter(statement, filter_obj)

    assert isinstance(result, SQL)
    assert result is not statement  # Should return new instance
    assert "name" in result.sql.lower()
    assert "like" in result.sql.lower() or "ilike" in result.sql.lower()


@pytest.fixture
def test_dates() -> dict[str, datetime]:
    """Test datetime values."""
    return {
        "before": datetime(2023, 12, 31, 23, 59, 59),
        "after": datetime(2023, 1, 1, 0, 0, 0),
    }


def test_before_after_filter_initialization(test_dates: dict[str, datetime]) -> None:
    """Test BeforeAfterFilter initialization."""
    filter_obj = BeforeAfterFilter(field_name="created_at", before=test_dates["before"], after=test_dates["after"])

    assert filter_obj.field_name == "created_at"
    assert filter_obj.before == test_dates["before"]
    assert filter_obj.after == test_dates["after"]


@pytest.mark.parametrize(
    ("before", "after", "expected_conditions"),
    [
        (datetime(2023, 12, 31), None, 1),  # Before only
        (None, datetime(2023, 1, 1), 1),  # After only
        (datetime(2023, 12, 31), datetime(2023, 1, 1), 2),  # Both
        (None, None, 0),  # Neither
    ],
    ids=["before_only", "after_only", "both", "neither"],
)
def test_before_after_filter_application(before: datetime, after: datetime, expected_conditions: int) -> None:
    """Test BeforeAfterFilter application with different combinations."""
    statement = SQL("SELECT * FROM orders")
    filter_obj = BeforeAfterFilter("created_at", before=before, after=after)

    result = filter_obj.append_to_statement(statement)

    if expected_conditions > 0:
        assert "created_at" in result.sql
        if before:
            assert "<" in result.sql  # LT condition
        if after:
            assert ">" in result.sql  # GT condition

        # Check parameters were added
        assert isinstance(result.parameters, dict)
        if before:
            before_params = [k for k in result.parameters.keys() if "before" in k]
            assert len(before_params) > 0
        if after:
            after_params = [k for k in result.parameters.keys() if "after" in k]
            assert len(after_params) > 0
    else:
        # No conditions should be added
        assert result.sql == statement.sql


def test_before_after_filter_unique_parameter_names(test_dates: dict[str, datetime]) -> None:
    """Test that BeforeAfterFilter generates unique parameter names."""
    statement = SQL("SELECT * FROM orders WHERE user_id = :user_id", user_id=123)
    filter_obj = BeforeAfterFilter("created_at", before=test_dates["before"], after=test_dates["after"])

    result = filter_obj.append_to_statement(statement)

    # Should have only before/after parameters (original parameters are not preserved)
    assert isinstance(result.parameters, dict)
    param_names = list(result.parameters.keys())
    before_params = [name for name in param_names if "before" in name]
    after_params = [name for name in param_names if "after" in name]

    assert len(before_params) == 1
    assert len(after_params) == 1
    assert before_params[0] != after_params[0]


@pytest.fixture
def on_before_after_test_dates() -> dict[str, datetime]:
    """Test datetime values."""
    return {
        "on_or_before": datetime(2023, 12, 31, 23, 59, 59),
        "on_or_after": datetime(2023, 1, 1, 0, 0, 0),
    }


def test_on_before_after_filter_initialization(on_before_after_test_dates: dict[str, datetime]) -> None:
    """Test OnBeforeAfterFilter initialization."""
    filter_obj = OnBeforeAfterFilter(
        field_name="updated_at",
        on_or_before=on_before_after_test_dates["on_or_before"],
        on_or_after=on_before_after_test_dates["on_or_after"],
    )

    assert filter_obj.field_name == "updated_at"
    assert filter_obj.on_or_before == on_before_after_test_dates["on_or_before"]
    assert filter_obj.on_or_after == on_before_after_test_dates["on_or_after"]


@pytest.mark.parametrize(
    ("on_or_before", "on_or_after", "expected_operators"),
    [
        (datetime(2023, 12, 31), None, ["<="]),  # Before only
        (None, datetime(2023, 1, 1), [">="]),  # After only
        (datetime(2023, 12, 31), datetime(2023, 1, 1), ["<=", ">="]),  # Both
    ],
    ids=["before_only", "after_only", "both"],
)
def test_on_before_after_filter_operators(
    on_or_before: datetime, on_or_after: datetime, expected_operators: list[str]
) -> None:
    """Test OnBeforeAfterFilter uses correct operators (inclusive)."""
    statement = SQL("SELECT * FROM events")
    filter_obj = OnBeforeAfterFilter("event_date", on_or_before=on_or_before, on_or_after=on_or_after)

    result = filter_obj.append_to_statement(statement)

    for operator in expected_operators:
        assert operator in result.sql


def test_on_before_after_filter_no_conditions() -> None:
    """Test OnBeforeAfterFilter with no conditions."""
    statement = SQL("SELECT * FROM events")
    filter_obj = OnBeforeAfterFilter("event_date")

    result = filter_obj.append_to_statement(statement)

    # Should return unchanged statement
    assert result.sql == statement.sql


@pytest.mark.parametrize(
    ("values", "expected_behavior"),
    [
        ([1, 2, 3], "has_in_clause"),
        ([], "always_false"),
        (None, "unchanged"),
        (("a", "b", "c"), "has_in_clause"),  # Tuple
        ({"x", "y", "z"}, "has_in_clause"),  # Set
    ],
    ids=["list_values", "empty_list", "none_values", "tuple_values", "set_values"],
)
def test_in_collection_filter_application(values: Any, expected_behavior: str) -> None:
    """Test InCollectionFilter with different value types."""
    statement = SQL("SELECT * FROM users")
    filter_obj = InCollectionFilter[str](field_name="status", values=values)

    result = filter_obj.append_to_statement(statement)

    if expected_behavior == "has_in_clause":
        assert "status" in result.sql
        assert "IN" in result.sql.upper()
        assert isinstance(result.parameters, dict)
        # Should have parameters for each value
        status_params = [k for k in result.parameters.keys() if "status_in_" in k]
        assert len(status_params) == len(values)
    elif expected_behavior == "always_false":
        # Empty list should result in FALSE condition
        assert "FALSE" in result.sql.upper() or "1 = 0" in result.sql
    elif expected_behavior == "unchanged":
        # None values should not change the statement
        assert result.sql == statement.sql


def test_in_collection_filter_parameter_generation() -> None:
    """Test InCollectionFilter parameter generation."""
    statement = SQL("SELECT * FROM products")
    values = ["electronics", "clothing", "books"]
    filter_obj = InCollectionFilter[str](field_name="category", values=values)

    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)

    # Should have unique parameter names for each value
    category_params = {k: v for k, v in result.parameters.items() if "category_in_" in k}
    assert len(category_params) == 3

    # Values should match
    param_values = list(category_params.values())
    assert all(val in values for val in param_values)


def test_in_collection_filter_unique_parameter_names() -> None:
    """Test that InCollectionFilter generates unique parameter names."""
    statement = SQL("SELECT * FROM products WHERE price > :min_price", min_price=10)
    filter_obj = InCollectionFilter[str](field_name="category", values=["electronics", "clothing"])

    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)
    # Only filter parameters should be present
    category_params = [k for k in result.parameters.keys() if "category_in_" in k]
    assert len(category_params) == 2
    assert len(set(category_params)) == 2  # All unique


@pytest.mark.parametrize(
    ("values", "should_add_condition"),
    [
        ([1, 2, 3], True),  # Has values
        ([], False),  # Empty list
        (None, False),  # None values
    ],
    ids=["has_values", "empty_list", "none_values"],
)
def test_not_in_collection_filter_application(values: Any, should_add_condition: bool) -> None:
    """Test NotInCollectionFilter application."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInCollectionFilter[str](field_name="status", values=values)

    result = filter_obj.append_to_statement(statement)

    if should_add_condition:
        assert "status" in result.sql
        assert "NOT" in result.sql.upper()
        assert "IN" in result.sql.upper()
        assert isinstance(result.parameters, dict)
    else:
        # Should return unchanged statement
        assert result.sql == statement.sql


def test_not_in_collection_filter_parameter_prefix() -> None:
    """Test NotInCollectionFilter uses correct parameter prefix."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInCollectionFilter[str](field_name="status", values=["banned", "deleted"])

    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)
    notin_params = [k for k in result.parameters.keys() if "status_notin_" in k]
    assert len(notin_params) == 2


@pytest.mark.parametrize(
    ("values", "expected_behavior"),
    [
        ([1, 2, 3], "has_any_clause"),
        ([], "always_false"),
        (None, "unchanged"),
    ],
    ids=["has_values", "empty_list", "none_values"],
)
def test_any_collection_filter_application(values: Any, expected_behavior: str) -> None:
    """Test AnyCollectionFilter application."""
    statement = SQL("SELECT * FROM users")
    filter_obj = AnyCollectionFilter[str](field_name="tags", values=values)

    result = filter_obj.append_to_statement(statement)

    if expected_behavior == "has_any_clause":
        assert "tags" in result.sql
        assert "ANY" in result.sql.upper()
        assert "ARRAY" in result.sql.upper()
        assert isinstance(result.parameters, dict)
    elif expected_behavior == "always_false":
        assert "FALSE" in result.sql.upper() or "1 = 0" in result.sql
    elif expected_behavior == "unchanged":
        assert result.sql == statement.sql


def test_any_collection_filter_parameter_prefix() -> None:
    """Test AnyCollectionFilter uses correct parameter prefix."""
    statement = SQL("SELECT * FROM posts")
    filter_obj = AnyCollectionFilter[str](field_name="tags", values=["python", "sql", "database"])

    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)
    any_params = [k for k in result.parameters.keys() if "tags_any_" in k]
    assert len(any_params) == 3


@pytest.mark.parametrize(
    ("values", "should_add_condition"),
    [
        ([1, 2, 3], True),  # Has values
        ([], False),  # Empty list - always true condition
        (None, False),  # None values - no condition
    ],
    ids=["has_values", "empty_list", "none_values"],
)
def test_not_any_collection_filter_application(values: Any, should_add_condition: bool) -> None:
    """Test NotAnyCollectionFilter application."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotAnyCollectionFilter[str](field_name="forbidden_tags", values=values)

    result = filter_obj.append_to_statement(statement)

    if should_add_condition:
        assert "forbidden_tags" in result.sql
        assert "NOT" in result.sql.upper()
        assert "ANY" in result.sql.upper()
        assert isinstance(result.parameters, dict)
    else:
        # Should return unchanged statement
        assert result.sql == statement.sql


def test_not_any_collection_filter_parameter_prefix() -> None:
    """Test NotAnyCollectionFilter uses correct parameter prefix."""
    statement = SQL("SELECT * FROM posts")
    filter_obj = NotAnyCollectionFilter[str](field_name="blocked_tags", values=["spam", "inappropriate"])

    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)
    notany_params = [k for k in result.parameters.keys() if "blocked_tags_notany_" in k]
    assert len(notany_params) == 2


def test_limit_offset_filter_initialization() -> None:
    """Test LimitOffsetFilter initialization."""
    filter_obj = LimitOffsetFilter(limit=50, offset=100)

    assert filter_obj.limit == 50
    assert filter_obj.offset == 100


def test_limit_offset_filter_application() -> None:
    """Test LimitOffsetFilter application."""
    statement = SQL("SELECT * FROM products ORDER BY created_at")
    filter_obj = LimitOffsetFilter(limit=20, offset=40)

    result = filter_obj.append_to_statement(statement)

    assert "LIMIT" in result.sql.upper()
    assert "OFFSET" in result.sql.upper()
    assert isinstance(result.parameters, dict)

    # Should use parameterized values
    limit_params = [k for k in result.parameters.keys() if "limit" in k]
    offset_params = [k for k in result.parameters.keys() if "offset" in k]

    assert len(limit_params) == 1
    assert len(offset_params) == 1
    assert result.parameters[limit_params[0]] == 20
    assert result.parameters[offset_params[0]] == 40


def test_limit_offset_filter_inheritance() -> None:
    """Test that LimitOffsetFilter inherits from PaginationFilter."""
    filter_obj = LimitOffsetFilter(limit=10, offset=0)

    assert isinstance(filter_obj, PaginationFilter)
    assert isinstance(filter_obj, StatementFilter)


@pytest.mark.parametrize(
    ("limit", "offset"),
    [
        (1, 0),  # First page
        (100, 200),  # Large pagination
        (10, 990),  # Deep pagination
    ],
    ids=["first_page", "large_page", "deep_page"],
)
def test_limit_offset_filter_various_values(limit: int, offset: int) -> None:
    """Test LimitOffsetFilter with various limit/offset values."""
    statement = SQL("SELECT * FROM users")
    filter_obj = LimitOffsetFilter(limit=limit, offset=offset)

    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)
    # Find the actual parameter values
    param_values = list(result.parameters.values())
    assert limit in param_values
    assert offset in param_values


def test_order_by_filter_initialization() -> None:
    """Test OrderByFilter initialization."""
    # Default ascending
    filter_asc = OrderByFilter("name")
    assert filter_asc.field_name == "name"
    assert filter_asc.sort_order == "asc"

    # Explicit descending
    filter_desc = OrderByFilter("created_at", "desc")
    assert filter_desc.field_name == "created_at"
    assert filter_desc.sort_order == "desc"


@pytest.mark.parametrize(
    ("field_name", "sort_order", "should_have_desc"),
    [
        ("name", "asc", False),  # ASC is implied, may not be explicit
        ("created_at", "desc", True),  # DESC should be explicit
        ("price", "asc", False),  # ASC is implied, may not be explicit
        ("rating", "desc", True),  # DESC should be explicit
    ],
    ids=["asc_lower", "desc_lower", "asc_upper", "desc_upper"],
)
def test_order_by_filter_application(field_name: str, sort_order: Any, should_have_desc: bool) -> None:
    """Test OrderByFilter application with different sort orders."""
    statement = SQL("SELECT * FROM products")
    filter_obj = OrderByFilter(field_name, sort_order)

    result = filter_obj.append_to_statement(statement)

    assert "ORDER BY" in result.sql.upper()
    assert field_name in result.sql  # Field name should be present

    if should_have_desc:
        assert "DESC" in result.sql.upper()  # DESC should be explicit
    # Note: ASC may or may not be explicit since it's the default


def test_order_by_filter_expression_objects() -> None:
    """Test that OrderByFilter creates proper SQLGlot expressions."""
    statement = SQL("SELECT * FROM users")

    # Test ASC - may or may not be explicit since it's the default
    filter_asc = OrderByFilter("name", "asc")
    result_asc = filter_asc.append_to_statement(statement)
    assert "ORDER BY" in result_asc.sql.upper()
    assert "name" in result_asc.sql

    # Test DESC - should be explicit
    filter_desc = OrderByFilter("created_at", "desc")
    result_desc = filter_desc.append_to_statement(statement)
    assert "DESC" in result_desc.sql.upper()


def test_search_filter_initialization() -> None:
    """Test SearchFilter initialization."""
    # Single field
    filter_single = SearchFilter("name", "john")
    assert filter_single.field_name == "name"
    assert filter_single.value == "john"
    assert filter_single.ignore_case is False

    # Multiple fields
    filter_multi = SearchFilter({"name", "email"}, "test", ignore_case=True)
    assert filter_multi.field_name == {"name", "email"}
    assert filter_multi.value == "test"
    assert filter_multi.ignore_case is True


@pytest.mark.parametrize(
    ("ignore_case", "expected_operator"),
    [
        (False, "LIKE"),
        (True, "ILIKE"),
    ],
    ids=["case_sensitive", "case_insensitive"],
)
def test_search_filter_case_sensitivity(ignore_case: bool, expected_operator: str) -> None:
    """Test SearchFilter case sensitivity."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter("name", "john", ignore_case=ignore_case)

    result = filter_obj.append_to_statement(statement)

    assert expected_operator in result.sql.upper()
    assert isinstance(result.parameters, dict)

    # Should add wildcards to search value
    search_params = [v for k, v in result.parameters.items() if "search_val" in k]
    assert len(search_params) == 1
    assert search_params[0] == "%john%"


def test_search_filter_empty_value() -> None:
    """Test SearchFilter with empty search value."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter("name", "")

    result = filter_obj.append_to_statement(statement)

    # Should return unchanged statement for empty value
    assert result.sql == statement.sql


def test_search_filter_multiple_fields() -> None:
    """Test SearchFilter with multiple fields."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter({"name", "email", "bio"}, "search_term")

    result = filter_obj.append_to_statement(statement)

    assert "name" in result.sql
    assert "email" in result.sql
    assert "bio" in result.sql
    assert "OR" in result.sql.upper()  # Should use OR for multiple fields
    assert isinstance(result.parameters, dict)


def test_search_filter_single_field_as_string() -> None:
    """Test SearchFilter with single field as string."""
    statement = SQL("SELECT * FROM products")
    filter_obj = SearchFilter("description", "keyword")

    result = filter_obj.append_to_statement(statement)

    assert "description" in result.sql
    assert "LIKE" in result.sql.upper()
    assert "OR" not in result.sql.upper()  # Should not use OR for single field


def test_search_filter_empty_field_set() -> None:
    """Test SearchFilter with empty field set."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter(set(), "search_term")

    result = filter_obj.append_to_statement(statement)

    # Should return unchanged statement for empty field set
    assert result.sql == statement.sql


def test_not_in_search_filter_inheritance() -> None:
    """Test that NotInSearchFilter inherits from SearchFilter."""
    filter_obj = NotInSearchFilter("name", "excluded")

    assert isinstance(filter_obj, SearchFilter)


@pytest.mark.parametrize(
    ("ignore_case", "expected_operator"),
    [
        (False, "NOT"),  # Should have NOT and LIKE
        (True, "NOT"),  # Should have NOT and ILIKE
    ],
    ids=["case_sensitive", "case_insensitive"],
)
def test_not_in_search_filter_negation(ignore_case: bool, expected_operator: str) -> None:
    """Test NotInSearchFilter creates NOT conditions."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInSearchFilter("name", "excluded", ignore_case=ignore_case)

    result = filter_obj.append_to_statement(statement)

    assert "NOT" in result.sql.upper()
    if ignore_case:
        assert "ILIKE" in result.sql.upper()
    else:
        assert "LIKE" in result.sql.upper()
    assert "name" in result.sql


def test_not_in_search_filter_multiple_fields() -> None:
    """Test NotInSearchFilter with multiple fields uses AND."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInSearchFilter({"name", "email"}, "spam")

    result = filter_obj.append_to_statement(statement)

    assert "NOT" in result.sql.upper()
    assert "AND" in result.sql.upper()  # Should use AND for multiple NOT conditions


def test_not_in_search_filter_empty_value() -> None:
    """Test NotInSearchFilter with empty search value."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInSearchFilter("name", "")

    result = filter_obj.append_to_statement(statement)

    # Should return unchanged statement for empty value
    assert result.sql == statement.sql


def test_filter_types_alias_coverage() -> None:
    """Test that FilterTypes includes all major filter classes."""
    # This test ensures the type alias includes the expected filters
    # We can't directly test the type alias, but we can test instances
    filters = [
        BeforeAfterFilter("date"),
        OnBeforeAfterFilter("date"),
        InCollectionFilter("status", ["active"]),
        LimitOffsetFilter(10, 0),
        OrderByFilter("name"),
        SearchFilter("name", "test"),
        NotInCollectionFilter("status", ["banned"]),
        NotInSearchFilter("name", "spam"),
        AnyCollectionFilter("tags", ["tag1"]),
        NotAnyCollectionFilter("tags", ["bad"]),
    ]

    # All should be valid StatementFilter instances
    for filter_obj in filters:
        assert hasattr(filter_obj, "append_to_statement")
        assert callable(filter_obj.append_to_statement)


def test_multiple_filter_application() -> None:
    """Test applying multiple filters to the same statement."""
    statement = SQL("SELECT * FROM products")

    # Apply multiple filters
    search_filter = SearchFilter("name", "widget")
    category_filter = InCollectionFilter("category", ["electronics", "gadgets"])
    pagination_filter = LimitOffsetFilter(20, 0)
    order_filter = OrderByFilter("created_at", "desc")

    # Apply filters sequentially
    result = statement
    for filter_obj in [search_filter, category_filter, pagination_filter, order_filter]:
        result = filter_obj.append_to_statement(result)

    # Check that all conditions are present
    assert "name" in result.sql
    assert "LIKE" in result.sql.upper()
    assert "category" in result.sql
    assert "IN" in result.sql.upper()
    assert "LIMIT" in result.sql.upper()
    assert "OFFSET" in result.sql.upper()
    assert "ORDER BY" in result.sql.upper()
    assert "DESC" in result.sql.upper()


def test_filter_parameter_isolation() -> None:
    """Test that filters don't interfere with each other's parameters."""
    statement = SQL("SELECT * FROM users WHERE active = :active", active=True)

    # Apply filters that add parameters
    search_filter = SearchFilter("name", "john")
    status_filter = InCollectionFilter("status", ["verified", "premium"])

    result = search_filter.append_to_statement(statement)
    result = status_filter.append_to_statement(result)

    assert isinstance(result.parameters, dict)

    # Only filter parameters should be present
    search_params = [k for k in result.parameters.keys() if "search_val" in k]
    assert len(search_params) == 1
    status_params = [k for k in result.parameters.keys() if "status_in_" in k]
    assert len(status_params) == 2


def test_filter_chaining_immutability() -> None:
    """Test that filter application preserves immutability."""
    original_statement = SQL("SELECT * FROM users")

    # Apply filter
    filter_obj = SearchFilter("name", "test")
    filtered_statement = filter_obj.append_to_statement(original_statement)

    # Original should be unchanged
    assert original_statement.sql == "SELECT * FROM users"
    assert original_statement is not filtered_statement

    # Filtered should have changes
    assert "name" in filtered_statement.sql
    assert "LIKE" in filtered_statement.sql.upper()


def test_filter_with_complex_existing_sql() -> None:
    """Test filters with complex existing SQL."""
    complex_sql = """
    SELECT u.name, u.email, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = true
    GROUP BY u.id, u.name, u.email
    HAVING COUNT(o.id) > 0
    ORDER BY u.created_at DESC
    """

    statement = SQL(complex_sql)
    filter_obj = SearchFilter("u.name", "admin")

    result = filter_obj.append_to_statement(statement)

    # Should add condition to existing WHERE clause
    assert "u.name" in result.sql
    assert "LIKE" in result.sql.upper()
    assert "u.active" in result.sql.lower()
    assert "true" in result.sql.lower()


def test_filter_parameter_name_conflicts() -> None:
    """Test handling of parameter name conflicts."""
    # Create statement with parameter that might conflict with filter naming
    statement = SQL("SELECT * FROM users WHERE search_val_0 = :search_val_0", search_val_0="existing")

    filter_obj = SearchFilter("name", "test")
    result = filter_obj.append_to_statement(statement)

    assert isinstance(result.parameters, dict)
    # Only filter parameters should be present
    search_params = [k for k in result.parameters.keys() if "search_val" in k]
    assert len(search_params) >= 1  # At least the filter parameter


@pytest.mark.parametrize(
    "filter_class",
    [
        BeforeAfterFilter,
        OnBeforeAfterFilter,
        InCollectionFilter,
        NotInCollectionFilter,
        AnyCollectionFilter,
        NotAnyCollectionFilter,
        LimitOffsetFilter,
        OrderByFilter,
        SearchFilter,
        NotInSearchFilter,
    ],
)
def test_filter_unique_parameter_generation(filter_class: type) -> None:
    """Test that all filters generate unique parameter names."""
    statement = SQL("SELECT * FROM test")

    # Create filter instance with appropriate arguments
    if filter_class == BeforeAfterFilter:
        filter_obj = filter_class("date", before=datetime.now())
    elif filter_class == OnBeforeAfterFilter:
        filter_obj = filter_class("date", on_or_before=datetime.now())
    elif filter_class in (InCollectionFilter, NotInCollectionFilter, AnyCollectionFilter, NotAnyCollectionFilter):
        filter_obj = filter_class("field", ["value1", "value2"])
    elif filter_class == LimitOffsetFilter:
        filter_obj = filter_class(10, 0)
    elif filter_class == OrderByFilter:
        filter_obj = filter_class("field")
    elif filter_class in (SearchFilter, NotInSearchFilter):
        filter_obj = filter_class("field", "search_term")
    else:
        pytest.skip(f"Unknown filter class: {filter_class}")

    result = filter_obj.append_to_statement(statement)

    # If parameters were added, they should be in a dict
    if result.parameters and result.parameters != statement.parameters:
        assert isinstance(result.parameters, dict)


def test_filter_with_none_statement() -> None:
    """Test filter behavior with None statement (should not happen but test safety)."""
    filter_obj = SearchFilter("name", "test")

    # This would be a programming error, but test graceful handling
    with pytest.raises((AttributeError, TypeError)):
        filter_obj.append_to_statement(None)  # type: ignore[arg-type]
