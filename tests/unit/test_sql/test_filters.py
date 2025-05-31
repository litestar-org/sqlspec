from datetime import datetime

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
)
from sqlspec.statement.sql import SQL


def test_limit_offset_filter_apply() -> None:
    """Test applying LimitOffset filter to SQLStatement."""
    stmt = SQL("SELECT * FROM users")
    limit_offset_filter = LimitOffsetFilter(limit=10, offset=5)
    stmt = limit_offset_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM users LIMIT ? OFFSET ?"
    assert params == [10, 5]

    stmt_no_params = SQL("SELECT * FROM projects")
    # limit_offset_filter_no_params = LimitOffset(limit=20, offset=0) # Unused variable
    stmt_no_params = stmt_no_params.limit(20, use_parameter=False)
    stmt_no_params = stmt_no_params.offset(0, use_parameter=False)
    sql_no_params = stmt_no_params.to_sql(placeholder_style="qmark")
    assert sql_no_params == "SELECT * FROM projects LIMIT 20 OFFSET 0"


def test_order_by_filter_apply() -> None:
    """Test applying OrderBy filter to SQLStatement."""
    stmt_asc = SQL("SELECT * FROM products")
    order_by_asc_filter = OrderByFilter(field_name="name", sort_order="asc")
    stmt_asc = order_by_asc_filter.append_to_statement(stmt_asc)
    sql_asc = stmt_asc.to_sql()
    assert sql_asc == "SELECT * FROM products ORDER BY name"  # SQLGlot omits ASC (default)

    stmt_desc = SQL("SELECT * FROM products")
    order_by_desc_filter = OrderByFilter(field_name="category", sort_order="desc")
    stmt_desc = order_by_desc_filter.append_to_statement(stmt_desc)
    sql_desc = stmt_desc.to_sql()
    assert sql_desc == "SELECT * FROM products ORDER BY category DESC"

    stmt_invalid_sort = SQL("SELECT * FROM products")
    order_by_invalid_sort_filter = OrderByFilter(field_name="price", sort_order="INVALID_SORT")  # type: ignore[arg-type]
    stmt_invalid_sort = order_by_invalid_sort_filter.append_to_statement(stmt_invalid_sort)
    sql_invalid_sort = stmt_invalid_sort.to_sql()
    # Default to ASC if sort_order is invalid (SQLGlot omits ASC keyword)
    assert sql_invalid_sort == "SELECT * FROM products ORDER BY price"


def test_search_filter_apply() -> None:
    """Test applying SearchFilter to SQLStatement."""
    stmt = SQL("SELECT * FROM customers")
    search_filter = SearchFilter(field_name="email", value="domain.com", ignore_case=True)
    stmt = search_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM customers WHERE email ILIKE ?"
    assert params == ["%domain.com%"]

    stmt_multi_field = SQL("SELECT * FROM logs")
    search_filter_multi = SearchFilter(field_name={"message", "source"}, value="error", ignore_case=False)
    stmt_multi_field = search_filter_multi.append_to_statement(stmt_multi_field)
    sql_multi = stmt_multi_field.to_sql(placeholder_style="qmark")
    params_multi = stmt_multi_field.get_parameters("qmark")

    # The order of OR conditions might vary depending on set iteration order,
    # and parameter names might differ if not explicitly controlled in filter for multiple fields.
    # For simplicity, we check for presence of key components.
    assert "SELECT * FROM logs WHERE" in sql_multi
    assert ("message LIKE ?" in sql_multi and "source LIKE ?" in sql_multi) or (
        "source LIKE ?" in sql_multi and "message LIKE ?" in sql_multi
    )
    assert "OR" in sql_multi
    if params_multi is not None:
        if isinstance(params_multi, dict):
            assert list(params_multi.values()).count("%error%") == 2
        elif isinstance(params_multi, (list, tuple)):
            assert params_multi.count("%error%") == 2

    stmt_no_value = SQL("SELECT * FROM data")
    search_filter_no_value = SearchFilter(field_name="content", value="")
    stmt_no_value = search_filter_no_value.append_to_statement(stmt_no_value)
    sql_no_value = stmt_no_value.to_sql()
    assert sql_no_value == "SELECT * FROM data"


def test_not_in_search_filter_apply() -> None:
    """Test applying NotInSearchFilter to SQLStatement."""
    stmt = SQL("SELECT * FROM users")
    not_in_search_filter = NotInSearchFilter(field_name="username", value="admin", ignore_case=False)
    stmt = not_in_search_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM users WHERE NOT username LIKE ?"
    assert params == ["%admin%"]

    stmt_multi_field = SQL("SELECT * FROM logs")
    not_in_search_filter_multi = NotInSearchFilter(field_name={"message", "source"}, value="debug", ignore_case=True)
    stmt_multi_field = not_in_search_filter_multi.append_to_statement(stmt_multi_field)
    sql_multi = stmt_multi_field.to_sql(placeholder_style="qmark")
    params_multi = stmt_multi_field.get_parameters("qmark")

    assert "SELECT * FROM logs WHERE" in sql_multi
    assert ("NOT message ILIKE ?" in sql_multi and "NOT source ILIKE ?" in sql_multi) or (
        "NOT source ILIKE ?" in sql_multi and "NOT message ILIKE ?" in sql_multi
    )
    assert "AND" in sql_multi  # For NOT IN, conditions are typically ANDed
    if params_multi is not None:
        if isinstance(params_multi, dict):
            assert list(params_multi.values()).count("%debug%") == 2
        elif isinstance(params_multi, (list, tuple)):
            assert params_multi.count("%debug%") == 2

    stmt_no_value = SQL("SELECT * FROM data")
    not_in_search_filter_no_value = NotInSearchFilter(field_name="content", value="")
    stmt_no_value = not_in_search_filter_no_value.append_to_statement(stmt_no_value)
    sql_no_value = stmt_no_value.to_sql()
    assert sql_no_value == "SELECT * FROM data"


def test_collection_filter_apply() -> None:
    """Test applying CollectionFilter to SQLStatement."""
    stmt = SQL("SELECT * FROM items")
    collection_filter = InCollectionFilter[int](field_name="id", values=[1, 2, 3])
    stmt = collection_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM items WHERE id IN (?, ?, ?)"
    assert params == [1, 2, 3]

    stmt_empty_values = SQL("SELECT * FROM items")
    collection_filter_empty = InCollectionFilter[int](field_name="id", values=[])
    stmt_empty_values = collection_filter_empty.append_to_statement(stmt_empty_values)
    sql_empty_values = stmt_empty_values.to_sql()
    assert sql_empty_values == "SELECT * FROM items WHERE FALSE"

    stmt_none_values = SQL("SELECT * FROM items")
    collection_filter_none = InCollectionFilter[int](field_name="id", values=None)
    stmt_none_values = collection_filter_none.append_to_statement(stmt_none_values)
    sql_none_values = stmt_none_values.to_sql()
    assert sql_none_values == "SELECT * FROM items"


def test_not_in_collection_filter_apply() -> None:
    """Test applying NotInCollectionFilter to SQLStatement."""
    stmt = SQL("SELECT * FROM products")
    not_in_collection_filter = NotInCollectionFilter[int](field_name="category_id", values=[10, 20])
    stmt = not_in_collection_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM products WHERE category_id NOT IN (?, ?)"
    assert params == [10, 20]

    stmt_empty_values = SQL("SELECT * FROM products")
    not_in_collection_filter_empty = NotInCollectionFilter[int](field_name="category_id", values=[])
    stmt_empty_values = not_in_collection_filter_empty.append_to_statement(stmt_empty_values)
    sql_empty_values = stmt_empty_values.to_sql()
    assert sql_empty_values == "SELECT * FROM products"

    stmt_none_values = SQL("SELECT * FROM products")
    not_in_collection_filter_none = NotInCollectionFilter[int](field_name="category_id", values=None)
    stmt_none_values = not_in_collection_filter_none.append_to_statement(stmt_none_values)
    sql_none_values = stmt_none_values.to_sql()
    assert sql_none_values == "SELECT * FROM products"


def test_any_collection_filter_apply() -> None:
    """Test applying AnyCollectionFilter to SQLStatement."""
    stmt = SQL("SELECT * FROM tags")
    any_filter = AnyCollectionFilter[str](field_name="name", values=["urgent", "important"])
    stmt = any_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark", dialect="postgresql")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM tags WHERE name = ANY(ARRAY[?, ?])"
    assert params == ["urgent", "important"]

    stmt_empty = SQL("SELECT * FROM tags")
    any_filter_empty = AnyCollectionFilter[str](field_name="name", values=[])
    stmt_empty = any_filter_empty.append_to_statement(stmt_empty)
    sql_empty = stmt_empty.to_sql(dialect="postgresql")
    assert sql_empty == "SELECT * FROM tags WHERE FALSE"

    stmt_none = SQL("SELECT * FROM tags")
    any_filter_none = AnyCollectionFilter[str](field_name="name", values=None)
    stmt_none = any_filter_none.append_to_statement(stmt_none)
    sql_none = stmt_none.to_sql(dialect="postgresql")
    assert sql_none == "SELECT * FROM tags"


def test_not_any_collection_filter_apply() -> None:
    """Test applying NotAnyCollectionFilter to SQLStatement."""
    stmt = SQL("SELECT * FROM posts")
    not_any_filter = NotAnyCollectionFilter[str](field_name="status", values=["draft", "pending"])
    stmt = not_any_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark", dialect="postgresql")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM posts WHERE NOT status = ANY(ARRAY[?, ?])"
    assert params == ["draft", "pending"]

    stmt_empty = SQL("SELECT * FROM posts")
    not_any_filter_empty = NotAnyCollectionFilter[str](field_name="status", values=[])
    stmt_empty = not_any_filter_empty.append_to_statement(stmt_empty)
    sql_empty = stmt_empty.to_sql(dialect="postgresql")
    assert sql_empty == "SELECT * FROM posts"

    stmt_none = SQL("SELECT * FROM posts")
    not_any_filter_none = NotAnyCollectionFilter[str](field_name="status", values=None)
    stmt_none = not_any_filter_none.append_to_statement(stmt_none)
    sql_none = stmt_none.to_sql(dialect="postgresql")
    assert sql_none == "SELECT * FROM posts"


def test_before_after_filter_apply() -> None:
    """Test applying BeforeAfter filter to SQLStatement."""
    dt_before = datetime(2023, 1, 1, 10, 0, 0)
    dt_after = datetime(2023, 1, 1, 12, 0, 0)

    stmt = SQL("SELECT * FROM events")
    before_after_filter = BeforeAfterFilter(field_name="event_time", before=dt_before, after=dt_after)
    stmt = before_after_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM events WHERE event_time < ? AND event_time > ?"
    assert params == [dt_before, dt_after]

    stmt_before_only = SQL("SELECT * FROM events")
    before_filter = BeforeAfterFilter(field_name="event_time", before=dt_before)
    stmt_before_only = before_filter.append_to_statement(stmt_before_only)
    sql_before = stmt_before_only.to_sql(placeholder_style="qmark")
    params_before = stmt_before_only.get_parameters("qmark")
    assert sql_before == "SELECT * FROM events WHERE event_time < ?"
    assert params_before == [dt_before]

    stmt_after_only = SQL("SELECT * FROM events")
    after_filter = BeforeAfterFilter(field_name="event_time", after=dt_after)
    stmt_after_only = after_filter.append_to_statement(stmt_after_only)
    sql_after = stmt_after_only.to_sql(placeholder_style="qmark")
    params_after = stmt_after_only.get_parameters("qmark")
    assert sql_after == "SELECT * FROM events WHERE event_time > ?"
    assert params_after == [dt_after]


def test_on_before_after_filter_apply() -> None:
    """Test applying OnBeforeAfter filter to SQLStatement."""
    dt_on_before = datetime(2023, 5, 15, 0, 0, 0)
    dt_on_after = datetime(2023, 5, 10, 0, 0, 0)

    stmt = SQL("SELECT * FROM logs")
    on_before_after_filter = OnBeforeAfterFilter(
        field_name="timestamp", on_or_before=dt_on_before, on_or_after=dt_on_after
    )
    stmt = on_before_after_filter.append_to_statement(stmt)
    sql = stmt.to_sql(placeholder_style="qmark")
    params = stmt.get_parameters("qmark")
    assert sql == "SELECT * FROM logs WHERE timestamp <= ? AND timestamp >= ?"
    assert params == [dt_on_before, dt_on_after]

    stmt_on_before_only = SQL("SELECT * FROM logs")
    on_before_filter = OnBeforeAfterFilter(field_name="timestamp", on_or_before=dt_on_before)
    stmt_on_before_only = on_before_filter.append_to_statement(stmt_on_before_only)
    sql_on_before = stmt_on_before_only.to_sql(placeholder_style="qmark")
    params_on_before = stmt_on_before_only.get_parameters("qmark")
    assert sql_on_before == "SELECT * FROM logs WHERE timestamp <= ?"
    assert params_on_before == [dt_on_before]

    stmt_on_after_only = SQL("SELECT * FROM logs")
    on_after_filter = OnBeforeAfterFilter(field_name="timestamp", on_or_after=dt_on_after)
    stmt_on_after_only = on_after_filter.append_to_statement(stmt_on_after_only)
    sql_on_after = stmt_on_after_only.to_sql(placeholder_style="qmark")
    params_on_after = stmt_on_after_only.get_parameters("qmark")
    assert sql_on_after == "SELECT * FROM logs WHERE timestamp >= ?"
    assert params_on_after == [dt_on_after]
