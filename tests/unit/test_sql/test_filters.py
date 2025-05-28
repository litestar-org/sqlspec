from datetime import datetime

from sqlspec.sql.filters import (
    BeforeAfter,
    CollectionFilter,
    LimitOffset,
    NotInCollectionFilter,
    NotInSearchFilter,
    OnBeforeAfter,
    OrderBy,
    SearchFilter,
)
from sqlspec.sql.statement import SQLStatement


def test_limit_offset_filter_apply() -> None:
    """Test applying LimitOffset filter to SQLStatement."""
    stmt = SQLStatement("SELECT * FROM users")
    limit_offset_filter = LimitOffset(limit=10, offset=5)
    stmt = limit_offset_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM users LIMIT ? OFFSET ?"
    assert params == [10, 5]

    stmt_no_params = SQLStatement("SELECT * FROM projects")
    # limit_offset_filter_no_params = LimitOffset(limit=20, offset=0) # Unused variable
    stmt_no_params = stmt_no_params.limit(20, use_parameter=False)
    stmt_no_params = stmt_no_params.offset(0, use_parameter=False)
    sql_no_params = stmt_no_params.get_sql(placeholder_style="qmark")
    assert sql_no_params == "SELECT * FROM projects LIMIT 20 OFFSET 0"


def test_order_by_filter_apply() -> None:
    """Test applying OrderBy filter to SQLStatement."""
    stmt_asc = SQLStatement("SELECT * FROM products")
    order_by_asc_filter = OrderBy(field_name="name", sort_order="asc")
    stmt_asc = order_by_asc_filter.append_to_statement(stmt_asc)
    sql_asc = stmt_asc.get_sql()
    assert sql_asc == "SELECT * FROM products ORDER BY name"  # SQLGlot omits ASC (default)

    stmt_desc = SQLStatement("SELECT * FROM products")
    order_by_desc_filter = OrderBy(field_name="category", sort_order="desc")
    stmt_desc = order_by_desc_filter.append_to_statement(stmt_desc)
    sql_desc = stmt_desc.get_sql()
    assert sql_desc == "SELECT * FROM products ORDER BY category DESC"

    stmt_invalid_sort = SQLStatement("SELECT * FROM products")
    order_by_invalid_sort_filter = OrderBy(field_name="price", sort_order="INVALID_SORT")  # type: ignore
    stmt_invalid_sort = order_by_invalid_sort_filter.append_to_statement(stmt_invalid_sort)
    sql_invalid_sort = stmt_invalid_sort.get_sql()
    # Default to ASC if sort_order is invalid (SQLGlot omits ASC keyword)
    assert sql_invalid_sort == "SELECT * FROM products ORDER BY price"


def test_search_filter_apply() -> None:
    """Test applying SearchFilter to SQLStatement."""
    stmt = SQLStatement("SELECT * FROM customers")
    search_filter = SearchFilter(field_name="email", value="domain.com", ignore_case=True)
    stmt = search_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM customers WHERE email ILIKE ?"
    assert params == ["%domain.com%"]

    stmt_multi_field = SQLStatement("SELECT * FROM logs")
    search_filter_multi = SearchFilter(field_name={"message", "source"}, value="error", ignore_case=False)
    stmt_multi_field = search_filter_multi.append_to_statement(stmt_multi_field)
    sql_multi = stmt_multi_field.get_sql(placeholder_style="qmark")
    params_multi = stmt_multi_field.get_parameters_for_style("qmark")

    # The order of OR conditions might vary depending on set iteration order,
    # and parameter names might differ if not explicitly controlled in filter for multiple fields.
    # For simplicity, we check for presence of key components.
    assert "SELECT * FROM logs WHERE" in sql_multi
    assert ("message LIKE ?" in sql_multi and "source LIKE ?" in sql_multi) or (
        "source LIKE ?" in sql_multi and "message LIKE ?" in sql_multi
    )
    assert "OR" in sql_multi
    # Parameters are added for each field, so we expect two identical parameters
    assert params_multi.count("%error%") == 2  # type: ignore[union-attr]
    assert len(params_multi) == 2  # type: ignore[union-attr]

    stmt_no_value = SQLStatement("SELECT * FROM data")
    search_filter_no_value = SearchFilter(field_name="content", value="")
    stmt_no_value = search_filter_no_value.append_to_statement(stmt_no_value)
    sql_no_value = stmt_no_value.get_sql()
    assert sql_no_value == "SELECT * FROM data"  # No change if value is empty


def test_not_in_search_filter_apply() -> None:
    """Test applying NotInSearchFilter to SQLStatement."""
    stmt = SQLStatement("SELECT * FROM users")
    not_in_search_filter = NotInSearchFilter(field_name="username", value="admin", ignore_case=False)
    stmt = not_in_search_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM users WHERE NOT username LIKE ?"  # SQLGlot renders NOT without parentheses
    assert params == ["%admin%"]

    stmt_multi_field = SQLStatement("SELECT * FROM events")
    not_in_search_filter_multi = NotInSearchFilter(field_name={"type", "detail"}, value="debug", ignore_case=True)
    stmt_multi_field = not_in_search_filter_multi.append_to_statement(stmt_multi_field)
    sql_multi = stmt_multi_field.get_sql(placeholder_style="qmark")
    params_multi = stmt_multi_field.get_parameters_for_style("qmark")

    assert "SELECT * FROM events WHERE" in sql_multi
    assert ("NOT type ILIKE ?" in sql_multi and "NOT detail ILIKE ?" in sql_multi) or (
        "NOT detail ILIKE ?" in sql_multi and "NOT type ILIKE ?" in sql_multi
    )  # SQLGlot renders NOT without parentheses
    assert "AND" in sql_multi
    assert params_multi.count("%debug%") == 2  # pyright: ignore
    assert len(params_multi) == 2  # pyright: ignore

    stmt_no_value = SQLStatement("SELECT * FROM items")
    not_in_search_filter_no_value = NotInSearchFilter(field_name="description", value="")
    stmt_no_value = not_in_search_filter_no_value.append_to_statement(stmt_no_value)
    sql_no_value = stmt_no_value.get_sql()
    assert sql_no_value == "SELECT * FROM items"  # No change if value is empty


def test_collection_filter_apply() -> None:
    """Test applying CollectionFilter to SQLStatement."""
    stmt = SQLStatement("SELECT * FROM orders")
    collection_filter = CollectionFilter(field_name="status", values=["pending", "shipped"])
    stmt = collection_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM orders WHERE status IN (?, ?)"
    assert params == ["pending", "shipped"]

    stmt_empty_values = SQLStatement("SELECT * FROM orders")
    collection_filter_empty = CollectionFilter(field_name="status", values=[])
    stmt_empty_values = collection_filter_empty.append_to_statement(stmt_empty_values)
    sql_empty = stmt_empty_values.get_sql()
    # Assuming `WHERE FALSE` or similar for empty IN, or dialect specific behavior
    assert "WHERE FALSE" in sql_empty or "WHERE 1 = 0" in sql_empty  # Check common ways to represent this

    stmt_none_values = SQLStatement("SELECT * FROM orders")
    collection_filter_none = CollectionFilter(field_name="status", values=None)
    stmt_none_values = collection_filter_none.append_to_statement(stmt_none_values)
    sql_none = stmt_none_values.get_sql()
    assert sql_none == "SELECT * FROM orders"  # No change if values is None


def test_not_in_collection_filter_apply() -> None:
    """Test applying NotInCollectionFilter to SQLStatement."""
    stmt = SQLStatement("SELECT * FROM products")
    not_in_collection_filter = NotInCollectionFilter(field_name="category_id", values=[1, 2, 3])
    stmt = not_in_collection_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM products WHERE NOT category_id IN (?, ?, ?)"
    assert params == [1, 2, 3]

    stmt_empty_values = SQLStatement("SELECT * FROM products")
    not_in_collection_filter_empty = NotInCollectionFilter(field_name="category_id", values=[])
    stmt_empty_values = not_in_collection_filter_empty.append_to_statement(stmt_empty_values)
    sql_empty = stmt_empty_values.get_sql()
    assert sql_empty == "SELECT * FROM products"  # No change if values is empty

    stmt_none_values = SQLStatement("SELECT * FROM products")
    not_in_collection_filter_none = NotInCollectionFilter(field_name="category_id", values=None)
    stmt_none_values = not_in_collection_filter_none.append_to_statement(stmt_none_values)
    sql_none = stmt_none_values.get_sql()
    assert sql_none == "SELECT * FROM products"  # No change if values is None


def test_before_after_filter_apply() -> None:
    """Test applying BeforeAfter filter to SQLStatement."""
    dt_before = datetime(2023, 1, 1, 10, 0, 0)
    dt_after = datetime(2023, 1, 1, 12, 0, 0)

    stmt = SQLStatement("SELECT * FROM events")
    before_after_filter = BeforeAfter(field_name="event_time", before=dt_before, after=dt_after)
    stmt = before_after_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM events WHERE event_time < ? AND event_time > ?"
    assert params == [dt_before, dt_after]

    stmt_before_only = SQLStatement("SELECT * FROM events")
    before_filter = BeforeAfter(field_name="event_time", before=dt_before)
    stmt_before_only = before_filter.append_to_statement(stmt_before_only)
    sql_before = stmt_before_only.get_sql(placeholder_style="qmark")
    params_before = stmt_before_only.get_parameters_for_style("qmark")
    assert sql_before == "SELECT * FROM events WHERE event_time < ?"
    assert params_before == [dt_before]

    stmt_after_only = SQLStatement("SELECT * FROM events")
    after_filter = BeforeAfter(field_name="event_time", after=dt_after)
    stmt_after_only = after_filter.append_to_statement(stmt_after_only)
    sql_after = stmt_after_only.get_sql(placeholder_style="qmark")
    params_after = stmt_after_only.get_parameters_for_style("qmark")
    assert sql_after == "SELECT * FROM events WHERE event_time > ?"
    assert params_after == [dt_after]


def test_on_before_after_filter_apply() -> None:
    """Test applying OnBeforeAfter filter to SQLStatement."""
    dt_on_before = datetime(2023, 5, 15, 0, 0, 0)
    dt_on_after = datetime(2023, 5, 10, 0, 0, 0)

    stmt = SQLStatement("SELECT * FROM logs")
    on_before_after_filter = OnBeforeAfter(field_name="timestamp", on_or_before=dt_on_before, on_or_after=dt_on_after)
    stmt = on_before_after_filter.append_to_statement(stmt)
    sql = stmt.get_sql(placeholder_style="qmark")
    params = stmt.get_parameters_for_style("qmark")
    assert sql == "SELECT * FROM logs WHERE timestamp <= ? AND timestamp >= ?"
    assert params == [dt_on_before, dt_on_after]

    stmt_on_before_only = SQLStatement("SELECT * FROM logs")
    on_before_filter = OnBeforeAfter(field_name="timestamp", on_or_before=dt_on_before)
    stmt_on_before_only = on_before_filter.append_to_statement(stmt_on_before_only)
    sql_on_before = stmt_on_before_only.get_sql(placeholder_style="qmark")
    params_on_before = stmt_on_before_only.get_parameters_for_style("qmark")
    assert sql_on_before == "SELECT * FROM logs WHERE timestamp <= ?"
    assert params_on_before == [dt_on_before]

    stmt_on_after_only = SQLStatement("SELECT * FROM logs")
    on_after_filter = OnBeforeAfter(field_name="timestamp", on_or_after=dt_on_after)
    stmt_on_after_only = on_after_filter.append_to_statement(stmt_on_after_only)
    sql_on_after = stmt_on_after_only.get_sql(placeholder_style="qmark")
    params_on_after = stmt_on_after_only.get_parameters_for_style("qmark")
    assert sql_on_after == "SELECT * FROM logs WHERE timestamp >= ?"
    assert params_on_after == [dt_on_after]
