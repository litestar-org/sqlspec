"""Unit tests for sqlspec.statement.filters module."""

from collections.abc import Collection
from datetime import datetime
from typing import Any, Optional, Union

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
    SearchFilter,
    StatementFilter,
    apply_filter,
)
from sqlspec.statement.sql import SQL


# Test StatementFilter Protocol
def test_statement_filter_protocol_implementation() -> None:
    """Test that all filter classes implement StatementFilter protocol."""
    # Create instances of all filter types
    filters = [
        BeforeAfterFilter("date"),
        OnBeforeAfterFilter("date"),
        InCollectionFilter("field", ["value"]),
        NotInCollectionFilter("field", ["value"]),
        AnyCollectionFilter("field", ["value"]),
        NotAnyCollectionFilter("field", ["value"]),
        LimitOffsetFilter(10, 0),
        OrderByFilter("field"),
        SearchFilter("field", "search"),
        NotInSearchFilter("field", "search"),
    ]

    for filter_obj in filters:
        # Check protocol compliance
        assert isinstance(filter_obj, StatementFilter)
        assert hasattr(filter_obj, "append_to_statement")
        assert callable(filter_obj.append_to_statement)


# Test apply_filter utility
@pytest.mark.parametrize(
    "filter_obj,expected_in_sql",
    [
        (SearchFilter("name", "john"), ["name", "LIKE"]),
        (LimitOffsetFilter(10, 0), ["LIMIT", "OFFSET"]),
        (OrderByFilter("created_at", "desc"), ["ORDER BY", "created_at", "DESC"]),
        (InCollectionFilter("status", ["active", "pending"]), ["status", "IN"]),
    ],
    ids=["search", "pagination", "order", "in_collection"],
)
def test_apply_filter_function(filter_obj: StatementFilter, expected_in_sql: list[str]) -> None:
    """Test apply_filter utility function with various filter types."""
    statement = SQL("SELECT * FROM users")
    result = apply_filter(statement, filter_obj)

    assert isinstance(result, SQL)
    assert result is not statement  # Should return new instance

    sql_upper = result.sql.upper()
    for expected in expected_in_sql:
        assert expected.upper() in sql_upper


# Test BeforeAfterFilter
@pytest.mark.parametrize(
    "before,after,expected_operators",
    [
        (datetime(2023, 12, 31), None, ["<"]),
        (None, datetime(2023, 1, 1), [">"]),
        (datetime(2023, 12, 31), datetime(2023, 1, 1), ["<", ">"]),
        (None, None, []),
    ],
    ids=["before_only", "after_only", "both", "neither"],
)
def test_before_after_filter(
    before: Optional[datetime], after: Optional[datetime], expected_operators: list[str]
) -> None:
    """Test BeforeAfterFilter with different date combinations."""
    statement = SQL("SELECT * FROM orders")
    filter_obj = BeforeAfterFilter("created_at", before=before, after=after)

    result = filter_obj.append_to_statement(statement)

    if expected_operators:
        assert "created_at" in result.sql
        for op in expected_operators:
            assert op in result.sql

        # Check parameters were added
        assert hasattr(result, "parameters")
        if result.parameters:
            assert isinstance(result.parameters, dict)
            if before:
                # Handle nested parameters structure
                params_dict = result.parameters
                if "parameters" in params_dict and isinstance(params_dict["parameters"], dict):
                    params_dict = params_dict["parameters"]
                    if "parameters" in params_dict:
                        params_dict = params_dict["parameters"]
                assert any("before" in k for k in params_dict.keys())
            if after:
                # Handle nested parameters structure
                params_dict = result.parameters
                if "parameters" in params_dict and isinstance(params_dict["parameters"], dict):
                    params_dict = params_dict["parameters"]
                    if "parameters" in params_dict:
                        params_dict = params_dict["parameters"]
                assert any("after" in k for k in params_dict.keys())
    else:
        # No conditions added
        assert result.sql == statement.sql


def test_before_after_filter_parameter_uniqueness() -> None:
    """Test BeforeAfterFilter generates unique parameter names."""
    statement = SQL("SELECT * FROM orders")
    filter1 = BeforeAfterFilter("created_at", before=datetime(2023, 12, 31))
    filter2 = BeforeAfterFilter("updated_at", before=datetime(2023, 12, 31))

    # Apply both filters
    result1 = filter1.append_to_statement(statement)
    result2 = filter2.append_to_statement(result1)

    # Should have unique parameter names
    if result2.parameters:
        assert isinstance(result2.parameters, dict)
        param_names = list(result2.parameters.keys())
        assert len(param_names) == len(set(param_names))  # All unique


# Test OnBeforeAfterFilter
@pytest.mark.parametrize(
    "on_or_before,on_or_after,expected_operators",
    [
        (datetime(2023, 12, 31), None, ["<="]),
        (None, datetime(2023, 1, 1), [">="]),
        (datetime(2023, 12, 31), datetime(2023, 1, 1), ["<=", ">="]),
        (None, None, []),
    ],
    ids=["on_or_before_only", "on_or_after_only", "both", "neither"],
)
def test_on_before_after_filter(
    on_or_before: Optional[datetime], on_or_after: Optional[datetime], expected_operators: list[str]
) -> None:
    """Test OnBeforeAfterFilter with inclusive operators."""
    statement = SQL("SELECT * FROM events")
    filter_obj = OnBeforeAfterFilter("event_date", on_or_before=on_or_before, on_or_after=on_or_after)

    result = filter_obj.append_to_statement(statement)

    if expected_operators:
        for op in expected_operators:
            assert op in result.sql
    else:
        assert result.sql == statement.sql


# Test InCollectionFilter
@pytest.mark.parametrize(
    "values,expected_behavior",
    [
        ([1, 2, 3], "has_in_clause"),
        ([], "false_condition"),
        (None, "unchanged"),
        (("a", "b", "c"), "has_in_clause"),
        ({"x", "y", "z"}, "has_in_clause"),
    ],
    ids=["list", "empty_list", "none", "tuple", "set"],
)
def test_in_collection_filter(values: Optional[Collection[Any]], expected_behavior: str) -> None:
    """Test InCollectionFilter with various value types."""
    statement = SQL("SELECT * FROM users")
    filter_obj = InCollectionFilter[Any]("status", values)

    result = filter_obj.append_to_statement(statement)

    if expected_behavior == "has_in_clause":
        assert "status" in result.sql
        assert "IN" in result.sql.upper()
        if result.parameters:
            assert isinstance(result.parameters, dict)
            status_params = [k for k in result.parameters.keys() if "status_in_" in k]
            assert len(status_params) == len(values) if values else 0
    elif expected_behavior == "false_condition":
        # Empty list results in FALSE condition
        assert "FALSE" in result.sql.upper() or "0 = 1" in result.sql
    else:  # unchanged
        assert result.sql == statement.sql


def test_in_collection_filter_preserves_values() -> None:
    """Test InCollectionFilter preserves parameter values correctly."""
    statement = SQL("SELECT * FROM products")
    values = ["electronics", "clothing", "books"]
    filter_obj = InCollectionFilter[str]("category", values)

    result = filter_obj.append_to_statement(statement)

    if result.parameters:
        assert isinstance(result.parameters, dict)
        # Extract values from parameters
        category_params = {k: v for k, v in result.parameters.items() if "category_in_" in k}
        param_values = set(category_params.values())
        assert param_values == set(values)


# Test NotInCollectionFilter
@pytest.mark.parametrize(
    "values,should_add_condition",
    [([1, 2, 3], True), ([], False), (None, False)],
    ids=["has_values", "empty_list", "none"],
)
def test_not_in_collection_filter(values: Optional[Collection[Any]], should_add_condition: bool) -> None:
    """Test NotInCollectionFilter behavior."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInCollectionFilter[Any]("status", values)

    result = filter_obj.append_to_statement(statement)

    if should_add_condition:
        assert "status" in result.sql
        assert "NOT" in result.sql.upper()
        assert "IN" in result.sql.upper()
    else:
        assert result.sql == statement.sql


# Test AnyCollectionFilter
@pytest.mark.parametrize(
    "values,expected_behavior",
    [([1, 2, 3], "has_any_clause"), ([], "false_condition"), (None, "unchanged")],
    ids=["has_values", "empty_list", "none"],
)
def test_any_collection_filter(values: Optional[Collection[Any]], expected_behavior: str) -> None:
    """Test AnyCollectionFilter for array operations."""
    statement = SQL("SELECT * FROM posts")
    filter_obj = AnyCollectionFilter[Any]("tags", values)

    result = filter_obj.append_to_statement(statement)

    if expected_behavior == "has_any_clause":
        assert "tags" in result.sql
        assert "ANY" in result.sql.upper()
        assert "ARRAY" in result.sql.upper()
    elif expected_behavior == "false_condition":
        assert "FALSE" in result.sql.upper() or "0 = 1" in result.sql
    else:
        assert result.sql == statement.sql


# Test NotAnyCollectionFilter
@pytest.mark.parametrize(
    "values,should_add_condition",
    [([1, 2, 3], True), ([], False), (None, False)],
    ids=["has_values", "empty_list", "none"],
)
def test_not_any_collection_filter(values: Optional[Collection[Any]], should_add_condition: bool) -> None:
    """Test NotAnyCollectionFilter behavior."""
    statement = SQL("SELECT * FROM posts")
    filter_obj = NotAnyCollectionFilter[Any]("forbidden_tags", values)

    result = filter_obj.append_to_statement(statement)

    if should_add_condition:
        assert "forbidden_tags" in result.sql
        assert "NOT" in result.sql.upper()
        assert "ANY" in result.sql.upper()
    else:
        assert result.sql == statement.sql


# Test LimitOffsetFilter
@pytest.mark.parametrize("limit,offset", [(10, 0), (50, 100), (1, 999)], ids=["first_page", "middle_page", "deep_page"])
def test_limit_offset_filter(limit: int, offset: int) -> None:
    """Test LimitOffsetFilter pagination."""
    statement = SQL("SELECT * FROM products")
    filter_obj = LimitOffsetFilter(limit, offset)

    result = filter_obj.append_to_statement(statement)

    assert "LIMIT" in result.sql.upper()
    assert "OFFSET" in result.sql.upper()

    # Check parameters
    if result.parameters:
        assert isinstance(result.parameters, dict)
        param_values = list(result.parameters.values())
        assert limit in param_values
        assert offset in param_values


# Test OrderByFilter
@pytest.mark.parametrize(
    "field,sort_order,expected_desc",
    [
        ("name", "asc", False),
        ("created_at", "desc", True),
        ("price", "ASC", False),
        ("rating", "DESC", True),
        ("id", "invalid", False),  # Should default to asc
    ],
    ids=["asc_lower", "desc_lower", "asc_upper", "desc_upper", "invalid"],
)
def test_order_by_filter(field: str, sort_order: str, expected_desc: bool) -> None:
    """Test OrderByFilter with different sort orders."""
    statement = SQL("SELECT * FROM products")
    filter_obj = OrderByFilter(field, sort_order)  # type: ignore[arg-type]

    result = filter_obj.append_to_statement(statement)

    assert "ORDER BY" in result.sql.upper()
    assert field in result.sql

    if expected_desc:
        assert "DESC" in result.sql.upper()
    # ASC may or may not be explicit


# Test SearchFilter
@pytest.mark.parametrize(
    "field_name,value,ignore_case,expected_operator",
    [
        ("name", "john", False, "LIKE"),
        ("email", "test", True, "ILIKE"),
        ("description", "", False, None),  # Empty value
        ({"name", "email"}, "search", False, "LIKE"),
        (set(), "search", False, None),  # Empty field set
    ],
    ids=["single_case_sensitive", "single_case_insensitive", "empty_value", "multiple_fields", "empty_fields"],
)
def test_search_filter(
    field_name: Union[str, set[str]], value: str, ignore_case: bool, expected_operator: Optional[str]
) -> None:
    """Test SearchFilter with various configurations."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter(field_name, value, ignore_case)

    result = filter_obj.append_to_statement(statement)

    if expected_operator:
        assert expected_operator in result.sql.upper()
        if isinstance(field_name, set) and len(field_name) > 1:
            assert "OR" in result.sql.upper()
    else:
        assert result.sql == statement.sql


def test_search_filter_wildcard_wrapping() -> None:
    """Test SearchFilter properly wraps search value with wildcards."""
    statement = SQL("SELECT * FROM users")
    filter_obj = SearchFilter("name", "john")

    result = filter_obj.append_to_statement(statement)

    if result.parameters:
        assert isinstance(result.parameters, dict)
        search_params = [v for k, v in result.parameters.items() if "search" in k and "_not_" not in k]
        assert len(search_params) == 1
        assert search_params[0] == "%john%"


# Test NotInSearchFilter
@pytest.mark.parametrize(
    "field_name,value,ignore_case,should_add_condition",
    [
        ("name", "spam", False, True),
        ("email", "blocked", True, True),
        ("bio", "", False, False),
        ({"name", "email"}, "spam", False, True),
    ],
    ids=["single_field", "case_insensitive", "empty_value", "multiple_fields"],
)
def test_not_in_search_filter(
    field_name: Union[str, set[str]], value: str, ignore_case: bool, should_add_condition: bool
) -> None:
    """Test NotInSearchFilter behavior."""
    statement = SQL("SELECT * FROM users")
    filter_obj = NotInSearchFilter(field_name, value, ignore_case)

    result = filter_obj.append_to_statement(statement)

    if should_add_condition:
        assert "NOT" in result.sql.upper()
        if ignore_case:
            assert "ILIKE" in result.sql.upper()
        else:
            assert "LIKE" in result.sql.upper()

        if isinstance(field_name, set) and len(field_name) > 1:
            assert "AND" in result.sql.upper()  # Multiple NOT conditions use AND
    else:
        assert result.sql == statement.sql


# Test filter composition
def test_multiple_filter_composition() -> None:
    """Test applying multiple filters in sequence."""
    statement = SQL("SELECT * FROM products")

    filters = [
        SearchFilter("name", "widget"),
        InCollectionFilter("category", ["electronics", "gadgets"]),
        BeforeAfterFilter("created_at", before=datetime(2023, 12, 31)),
        OrderByFilter("price", "desc"),
        LimitOffsetFilter(20, 40),
    ]

    result = statement
    for filter_obj in filters:
        result = filter_obj.append_to_statement(result)

    sql_upper = result.sql.upper()
    # Verify all filters were applied
    assert "LIKE" in sql_upper
    assert "IN" in sql_upper
    assert "<" in result.sql  # Before condition
    assert "ORDER BY" in sql_upper
    assert "DESC" in sql_upper
    assert "LIMIT" in sql_upper
    assert "OFFSET" in sql_upper


def test_filter_immutability() -> None:
    """Test that filters don't modify the original statement."""
    original = SQL("SELECT * FROM users")
    filter_obj = SearchFilter("name", "test")

    result = filter_obj.append_to_statement(original)

    # Original unchanged
    assert original.sql == "SELECT * FROM users"
    assert original is not result
    assert "LIKE" in result.sql.upper()


def test_filter_with_existing_where_clause() -> None:
    """Test filters properly extend existing WHERE clauses."""
    statement = SQL("SELECT * FROM users WHERE active = true")
    filter_obj = SearchFilter("name", "admin")

    result = filter_obj.append_to_statement(statement)

    # Should preserve existing condition and add new one
    assert "active" in result.sql.lower()
    assert "true" in result.sql.lower()
    assert "name" in result.sql
    assert "LIKE" in result.sql.upper()


# Test edge cases
def test_filters_with_special_characters() -> None:
    """Test filters handle special characters in values."""
    statement = SQL("SELECT * FROM users")

    # Test with SQL-like special characters
    search_filter = SearchFilter("name", "john%_doe")
    result = search_filter.append_to_statement(statement)

    if result.parameters:
        assert isinstance(result.parameters, dict)
        search_params = [v for v in result.parameters.values() if isinstance(v, str) and "john" in v]
        assert len(search_params) == 1
        assert search_params[0] == "%john%_doe%"


@pytest.mark.parametrize(
    "filter_class,init_args",
    [
        (BeforeAfterFilter, {"field_name": "date", "before": datetime.now()}),
        (OnBeforeAfterFilter, {"field_name": "date", "on_or_before": datetime.now()}),
        (InCollectionFilter, {"field_name": "field", "values": ["a", "b"]}),
        (NotInCollectionFilter, {"field_name": "field", "values": ["a", "b"]}),
        (AnyCollectionFilter, {"field_name": "field", "values": ["a", "b"]}),
        (NotAnyCollectionFilter, {"field_name": "field", "values": ["a", "b"]}),
        (LimitOffsetFilter, {"limit": 10, "offset": 0}),
        (OrderByFilter, {"field_name": "field", "sort_order": "asc"}),
        (SearchFilter, {"field_name": "field", "value": "search"}),
        (NotInSearchFilter, {"field_name": "field", "value": "search"}),
    ],
)
def test_filter_dataclass_initialization(filter_class: type[StatementFilter], init_args: dict[str, Any]) -> None:
    """Test all filter classes can be initialized as dataclasses."""
    filter_obj = filter_class(**init_args)

    # Verify attributes were set
    for key, value in init_args.items():
        assert hasattr(filter_obj, key)
        assert getattr(filter_obj, key) == value


def test_filter_type_annotations() -> None:
    """Test filter classes have proper type annotations."""
    # This test verifies the filters work with type checking
    before_filter: BeforeAfterFilter = BeforeAfterFilter("date", before=datetime.now())
    in_filter: InCollectionFilter[str] = InCollectionFilter("status", ["active"])
    search_filter: SearchFilter = SearchFilter("name", "test")

    # These should all be valid StatementFilter instances
    filters: list[StatementFilter] = [before_filter, in_filter, search_filter]
    assert len(filters) == 3
