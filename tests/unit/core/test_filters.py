"""Unit tests for SQL statement filters.

This module tests the filter system that provides dynamic WHERE clauses,
ORDER BY, LIMIT/OFFSET, and other SQL modifications with proper parameter naming.
"""

import tempfile
from dataclasses import dataclass
from datetime import datetime

import pytest

from sqlspec import sql as sql_builder
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
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
from sqlspec.core.filters import NotInSearchFilter
from sqlspec.driver import CommonDriverAttributesMixin
from sqlspec.service import SQLSpecAsyncService

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


# --- Bug #379 regression tests: multi-field placeholder independence ---


def test_search_filter_multi_field_placeholder_independence() -> None:
    """Regression test for bug #379: SearchFilter with multiple fields must produce
    independent placeholder nodes for each field condition.

    Previously, a single exp.Placeholder node was reused across multiple field
    conditions. Because sqlglot nodes have mutable parent pointers, the first
    parent would lose its child when the second parent claimed the same node,
    resulting in corrupted SQL output with missing LIKE conditions.
    """
    filter_obj = SearchFilter(field_name={"name", "description"}, value="oracle")
    sql_stmt = SQL("SELECT * FROM items")

    result = apply_filter(sql_stmt, filter_obj)
    rendered = result.sql.upper()

    # Both fields must appear with LIKE conditions
    assert "LIKE" in rendered
    like_count = rendered.count("LIKE")
    assert like_count == 2, f"Expected 2 LIKE conditions for 2 fields, got {like_count}. SQL: {result.sql}"

    # The placeholder should appear for each field condition
    param_name = filter_obj.get_param_name()
    assert param_name is not None
    placeholder_count = result.sql.count(f":{param_name}")
    assert placeholder_count == 2, (
        f"Expected 2 occurrences of :{param_name}, got {placeholder_count}. SQL: {result.sql}"
    )

    # Only 1 named parameter value (shared across both placeholders)
    positional, named = filter_obj.extract_parameters()
    assert positional == []
    assert len(named) == 1
    assert named[param_name] == "%oracle%"


def test_search_filter_three_fields_placeholder_independence() -> None:
    """Regression test for bug #379: SearchFilter with three fields must produce
    three independent LIKE conditions."""
    filter_obj = SearchFilter(field_name={"a", "b", "c"}, value="oracle")
    sql_stmt = SQL("SELECT * FROM items")

    result = apply_filter(sql_stmt, filter_obj)
    rendered = result.sql.upper()

    like_count = rendered.count("LIKE")
    assert like_count == 3, f"Expected 3 LIKE conditions for 3 fields, got {like_count}. SQL: {result.sql}"

    param_name = filter_obj.get_param_name()
    assert param_name is not None
    placeholder_count = result.sql.count(f":{param_name}")
    assert placeholder_count == 3, (
        f"Expected 3 occurrences of :{param_name}, got {placeholder_count}. SQL: {result.sql}"
    )

    positional, named = filter_obj.extract_parameters()
    assert positional == []
    assert len(named) == 1
    assert named[param_name] == "%oracle%"


def test_not_in_search_filter_multi_field_placeholder_independence() -> None:
    """Regression test for bug #379: NotInSearchFilter with multiple fields must
    produce independent placeholder nodes for each field condition."""
    filter_obj = NotInSearchFilter(field_name={"name", "description"}, value="oracle")
    sql_stmt = SQL("SELECT * FROM items")

    result = apply_filter(sql_stmt, filter_obj)
    rendered = result.sql.upper()

    # Both fields must appear with NOT LIKE conditions
    assert "LIKE" in rendered
    like_count = rendered.count("LIKE")
    assert like_count == 2, f"Expected 2 NOT LIKE conditions for 2 fields, got {like_count}. SQL: {result.sql}"

    param_name = filter_obj.get_param_name()
    assert param_name is not None
    placeholder_count = result.sql.count(f":{param_name}")
    assert placeholder_count == 2, (
        f"Expected 2 occurrences of :{param_name}, got {placeholder_count}. SQL: {result.sql}"
    )

    positional, named = filter_obj.extract_parameters()
    assert positional == []
    assert len(named) == 1
    assert named[param_name] == "%oracle%"


def test_search_filter_single_field_unchanged() -> None:
    """Non-regression test: SearchFilter with a single field (string, not set)
    continues to work correctly after the bug #379 fix."""
    filter_obj = SearchFilter(field_name="name", value="oracle")
    sql_stmt = SQL("SELECT * FROM items")

    result = apply_filter(sql_stmt, filter_obj)
    rendered = result.sql.upper()

    assert "LIKE" in rendered
    like_count = rendered.count("LIKE")
    assert like_count == 1, f"Expected 1 LIKE condition for single field, got {like_count}. SQL: {result.sql}"

    param_name = filter_obj.get_param_name()
    assert param_name == "name_search"
    assert f":{param_name}" in result.sql

    positional, named = filter_obj.extract_parameters()
    assert positional == []
    assert len(named) == 1
    assert named["name_search"] == "%oracle%"


# --- Task 1.1: NotInCollectionFilter deterministic param names ---


def test_not_in_collection_filter_deterministic_param_names() -> None:
    """Verify NotInCollectionFilter param names are deterministic (no id())."""
    f1 = NotInCollectionFilter("status", ["a", "b", "c"])
    f2 = NotInCollectionFilter("status", ["a", "b", "c"])

    assert f1.get_param_names() == f2.get_param_names()
    assert f1.get_param_names() == ["status_notin_0", "status_notin_1", "status_notin_2"]


def test_not_in_collection_filter_param_name_pattern() -> None:
    """Verify param names follow {field}_notin_{i} pattern."""
    f = NotInCollectionFilter("col", ["x"])
    names = f.get_param_names()
    assert names == ["col_notin_0"]


def test_not_in_collection_filter_extract_parameters_deterministic() -> None:
    """Verify extract_parameters uses deterministic names."""
    f = NotInCollectionFilter("status", ["deleted", "archived"])
    _, named = f.extract_parameters()
    assert named == {"status_notin_0": "deleted", "status_notin_1": "archived"}


# --- Task 1.2: SearchFilter ignore_case=None normalization ---


def test_search_filter_ignore_case_none_behaves_as_false() -> None:
    """ignore_case=None should normalize to False and use exp.Like."""

    f_none = SearchFilter("name", "test", ignore_case=None)
    f_false = SearchFilter("name", "test", ignore_case=False)

    assert f_none.ignore_case is False
    assert f_false.ignore_case is False

    sql_stmt = SQL("SELECT * FROM t")
    result_none = apply_filter(sql_stmt, f_none)
    result_false = apply_filter(sql_stmt, f_false)

    # Both should produce LIKE, not ILIKE
    assert "ILIKE" not in result_none.sql.upper()
    assert "LIKE" in result_none.sql.upper()
    assert result_none.sql == result_false.sql


def test_search_filter_ignore_case_true_uses_ilike() -> None:
    """ignore_case=True should use ILIKE."""
    f = SearchFilter("name", "test", ignore_case=True)
    assert f.ignore_case is True

    sql_stmt = SQL("SELECT * FROM t")
    result = apply_filter(sql_stmt, f)
    assert "ILIKE" in result.sql.upper()


# --- Task 1.4: InCollectionFilter edge cases ---


def test_in_collection_filter_empty_collection_returns_false() -> None:
    """Empty collection should add WHERE FALSE."""
    sql_stmt = SQL("SELECT * FROM t")
    f: InCollectionFilter[str] = InCollectionFilter("status", [])
    result = apply_filter(sql_stmt, f)
    assert "FALSE" in result.sql.upper()


def test_in_collection_filter_none_values_returns_unchanged() -> None:
    """None values should return statement unchanged."""
    sql_stmt = SQL("SELECT * FROM t")
    f: InCollectionFilter[str] = InCollectionFilter("status", None)
    result = apply_filter(sql_stmt, f)
    assert result.sql == sql_stmt.sql


# --- Task 1.4: NotInCollectionFilter edge cases ---


def test_not_in_collection_filter_empty_values_returns_unchanged() -> None:
    """Empty values should return statement unchanged."""
    sql_stmt = SQL("SELECT * FROM t")
    f: NotInCollectionFilter[str] = NotInCollectionFilter("status", [])
    result = apply_filter(sql_stmt, f)
    assert result.sql == sql_stmt.sql


def test_not_in_collection_filter_none_values_returns_unchanged() -> None:
    """None values should return statement unchanged."""
    sql_stmt = SQL("SELECT * FROM t")
    f: NotInCollectionFilter[str] = NotInCollectionFilter("status", None)
    result = apply_filter(sql_stmt, f)
    assert result.sql == sql_stmt.sql


# --- Task 1.5: Parameter conflict resolution and compound filters ---


def test_two_in_filters_same_column_no_collision() -> None:
    """Two InCollectionFilter instances on the same column should not collide."""
    sql_stmt = SQL("SELECT * FROM t")
    f1 = InCollectionFilter("status", ["a", "b"])
    f2 = InCollectionFilter("status", ["c", "d"])

    result = apply_filter(sql_stmt, f1)
    result = apply_filter(result, f2)

    params = result.parameters
    # First filter gets status_in_0, status_in_1
    assert params["status_in_0"] == "a"
    assert params["status_in_1"] == "b"
    # Second filter should get conflict-resolved names
    conflict_keys = [k for k in params if k.startswith("status_in_0_") or k.startswith("status_in_1_")]
    assert len(conflict_keys) == 2
    conflict_values = {params[k] for k in conflict_keys}
    assert conflict_values == {"c", "d"}


def test_multiple_filters_sequential_compound_where() -> None:
    """Multiple filters applied sequentially produce compound WHERE clauses."""
    sql_stmt = SQL("SELECT * FROM users")

    f1 = InCollectionFilter("status", ["active"])
    f2 = NotInCollectionFilter("role", ["admin"])
    f3 = SearchFilter("name", "john")

    result = apply_filter(sql_stmt, f1)
    result = apply_filter(result, f2)
    result = apply_filter(result, f3)

    sql_upper = result.sql.upper()
    assert "WHERE" in sql_upper
    assert "STATUS IN" in sql_upper
    assert "NOT" in sql_upper
    assert "ROLE" in sql_upper
    assert "LIKE" in sql_upper

    params = result.parameters
    assert params["status_in_0"] == "active"
    assert params["role_notin_0"] == "admin"
    assert params["name_search"] == "%john%"


# --- Task 2.4-2.5: QueryBuilder.apply_filters and prepare_statement with filters ---


def test_query_builder_apply_filters_in_collection() -> None:
    """QueryBuilder.apply_filters applies InCollectionFilter correctly (issue #405)."""
    from sqlspec.builder import Select

    builder = Select("*").from_("users")
    f = InCollectionFilter("status", ["active", "pending"])

    result = builder.apply_filters(f)

    sql_upper = result.sql.upper()
    assert "WHERE" in sql_upper
    assert "STATUS IN" in sql_upper
    assert result.parameters["status_in_0"] == "active"
    assert result.parameters["status_in_1"] == "pending"


def test_query_builder_apply_filters_multiple() -> None:
    """QueryBuilder.apply_filters handles multiple filters (issue #405)."""
    from sqlspec.builder import Select

    builder = Select("*").from_("users")
    f1 = InCollectionFilter("status", ["active"])
    f2 = SearchFilter("name", "john")
    f3 = LimitOffsetFilter(10, 0)

    result = builder.apply_filters(f1, f2, f3)

    sql_upper = result.sql.upper()
    assert "STATUS IN" in sql_upper
    assert "LIKE" in sql_upper
    assert "LIMIT" in sql_upper
    assert result.parameters["status_in_0"] == "active"
    assert result.parameters["name_search"] == "%john%"
    assert result.parameters["limit"] == 10


def test_query_builder_apply_filters_not_in_collection() -> None:
    """QueryBuilder.apply_filters applies NotInCollectionFilter correctly (issue #405)."""
    from sqlspec.builder import Select

    builder = Select("*").from_("users")
    f = NotInCollectionFilter("status", ["deleted", "archived"])

    result = builder.apply_filters(f)

    sql_upper = result.sql.upper()
    assert "WHERE" in sql_upper
    assert "NOT" in sql_upper
    assert "STATUS" in sql_upper
    assert result.parameters["status_notin_0"] == "deleted"
    assert result.parameters["status_notin_1"] == "archived"


def test_query_builder_apply_filters_empty() -> None:
    """QueryBuilder.apply_filters with no filters returns unmodified SQL."""
    from sqlspec.builder import Select

    builder = Select("*").from_("users")

    result = builder.apply_filters()

    assert "WHERE" not in result.sql.upper()


def test_search_filter_with_qualified_name_uses_sanitized_parameters() -> None:
    """Test that SearchFilter with a dotted name uses sanitized parameter names."""
    filter_obj = SearchFilter("users.name", "john")

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "users_name_search" in named
    assert named["users_name_search"] == "%john%"


def test_search_filter_with_qualified_name_appends_to_statement_correctly() -> None:
    """Test that SearchFilter with a dotted name appends to statement with qualified column."""
    from sqlspec.core import SQL

    statement = SQL("SELECT * FROM users u JOIN profiles p ON u.id = p.user_id")
    filter_obj = SearchFilter("u.name", "john")

    result = apply_filter(statement, filter_obj)

    sql_upper = result.sql.upper()
    assert "U.NAME LIKE" in sql_upper or '"U"."NAME" LIKE' in sql_upper or 'U."NAME" LIKE' in sql_upper
    assert "u_name_search" in result.named_parameters
    assert result.named_parameters["u_name_search"] == "%john%"


def test_in_collection_filter_with_qualified_name_uses_sanitized_parameters() -> None:
    """Test that InCollectionFilter with a dotted name uses sanitized parameter names."""
    values = ["active", "pending"]
    filter_obj = InCollectionFilter("u.status", values)

    positional, named = filter_obj.extract_parameters()

    assert positional == []
    assert "u_status_in_0" in named
    assert "u_status_in_1" in named


def test_order_by_filter_with_qualified_name_appends_to_statement_correctly() -> None:
    """Test that OrderByFilter with a dotted name appends to statement correctly."""
    from sqlspec.core import SQL

    statement = SQL("SELECT * FROM users u JOIN profiles p ON u.id = p.user_id")
    filter_obj = OrderByFilter("u.created_at", "desc")

    result = apply_filter(statement, filter_obj)

    sql_upper = result.sql.upper()
    assert "ORDER BY U.CREATED_AT DESC" in sql_upper or 'ORDER BY "U"."CREATED_AT" DESC' in sql_upper


def test_order_by_filter_with_expression_appends_to_statement_correctly() -> None:
    """Test that OrderByFilter with a SQLGlot expression appends to statement correctly."""
    from sqlglot import exp

    from sqlspec.core import SQL

    statement = SQL("SELECT id, lines, occurrences FROM stats")
    # COALESCE(lines, occurrences, 0)
    coalesce_expr = exp.func("COALESCE", exp.column("lines"), exp.column("occurrences"), exp.Literal.number(0))
    filter_obj = OrderByFilter(field_name=coalesce_expr, sort_order="desc")

    result = apply_filter(statement, filter_obj)

    sql_upper = result.sql.upper()
    assert "ORDER BY COALESCE(LINES, OCCURRENCES, 0) DESC" in sql_upper


def test_search_filter_with_expression_in_set_appends_to_statement_correctly() -> None:
    """Test that SearchFilter with a SQLGlot expression in a set appends to statement correctly."""
    from sqlglot import exp

    from sqlspec.core import SQL

    statement = SQL("SELECT name, email FROM users")
    # UPPER(name)
    upper_name = exp.func("UPPER", exp.column("name"))
    filter_obj = SearchFilter(field_name={upper_name, "email"}, value="john")

    result = apply_filter(statement, filter_obj)

    sql_upper = result.sql.upper()
    assert "UPPER(NAME) LIKE" in sql_upper
    assert "EMAIL LIKE" in sql_upper
    assert "OR" in sql_upper


def test_search_filter_like_pattern() -> None:
    """Test that SearchFilter.like_pattern returns the correct pattern."""
    filter_obj = SearchFilter("name", "john")
    assert filter_obj.like_pattern == "%john%"

    filter_no_value = SearchFilter("name", None)
    assert filter_no_value.like_pattern is None


def test_query_builder_apply_filters_produces_valid_sql_for_execution() -> None:
    """QueryBuilder.apply_filters produces SQL that can be used with prepare_statement (issue #405).

    This verifies the end-to-end path: QueryBuilder -> apply_filters -> SQL with parameters.
    """
    from sqlspec.builder import Select

    builder = Select("id", "name", "status").from_("users")
    f1 = InCollectionFilter("status", ["active", "pending"])
    f2 = OrderByFilter("name", "asc")
    f3 = LimitOffsetFilter(10, 0)

    result = builder.apply_filters(f1, f2, f3)

    sql_upper = result.sql.upper()
    assert "SELECT" in sql_upper
    assert "FROM USERS" in sql_upper.replace('"', "")
    assert "STATUS IN" in sql_upper
    assert "ORDER BY" in sql_upper
    assert "LIMIT" in sql_upper

    assert result.parameters["status_in_0"] == "active"
    assert result.parameters["status_in_1"] == "pending"
    assert result.parameters["limit"] == 10
    assert result.parameters["offset"] == 0


@dataclass
class User:
    id: int
    name: str


class UserService(SQLSpecAsyncService):
    pass


@pytest.mark.anyio
async def test_service_paginate_works() -> None:
    """Test that the base service paginate helper works correctly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sqlspec.add_config(config)

        async with sqlspec.provide_session(config) as session:
            await session.execute_script("""
                CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO users (id, name) VALUES (1, 'alice');
                INSERT INTO users (id, name) VALUES (2, 'bob');
                INSERT INTO users (id, name) VALUES (3, 'charlie');
            """)
            await session.commit()

            service = UserService(session)

            # Query all
            query = sql_builder.select("*").from_("users")

            # Paginate first 2
            pagination_filter = LimitOffsetFilter(limit=2, offset=0)
            result = await service.paginate(query, pagination_filter, schema_type=User)

            assert len(result.items) == 2
            assert result.total == 3
            assert result.limit == 2
            assert result.offset == 0
            assert result.items[0].name == "alice"
            assert result.items[1].name == "bob"

            # Paginate next
            pagination_filter2 = LimitOffsetFilter(limit=2, offset=2)
            result2 = await service.paginate(query, pagination_filter2, schema_type=User)

            assert len(result2.items) == 1
            assert result2.total == 3
            assert result2.limit == 2
            assert result2.offset == 2
            assert result2.items[0].name == "charlie"


@pytest.mark.anyio
async def test_service_exists_works() -> None:
    """Test that the base service exists helper works correctly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sqlspec.add_config(config)

        async with sqlspec.provide_session(config) as session:
            await session.execute_script("""
                CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO users (id, name) VALUES (1, 'alice');
            """)
            await session.commit()

            service = UserService(session)

            # Exists
            query = sql_builder.select("*").from_("users").where_eq("name", "alice")
            assert await service.exists(query) is True

            # Does not exist
            query2 = sql_builder.select("*").from_("users").where_eq("name", "bob")
            assert await service.exists(query2) is False
