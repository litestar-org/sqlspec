"""Unit tests for QueryBuilder.to_sql() edge cases."""

from datetime import date, datetime
from decimal import Decimal

from sqlspec import sql


def test_to_sql_with_no_parameters() -> None:
    """Test to_sql() with query that has no parameters."""
    query = sql.select("*").from_("products")

    sql_str = query.to_sql()
    assert "SELECT" in sql_str
    assert "products" in sql_str


def test_to_sql_empty_parameters_dict() -> None:
    """Test to_sql(show_parameters=True) with empty parameters."""
    query = sql.select("*").from_("products")

    sql_str = query.to_sql(show_parameters=True)
    assert "SELECT" in sql_str


def test_to_sql_decimal_parameter() -> None:
    """Test to_sql() handles Decimal parameters."""
    query = sql.select("*").from_("products").where("price = :price")
    query.add_parameter(Decimal("19.99"), "price")

    sql_str = query.to_sql(show_parameters=True)
    assert "19.99" in sql_str


def test_to_sql_zero_parameter() -> None:
    """Test to_sql() handles zero value correctly."""
    query = sql.select("*").from_("products").where("stock = :stock")
    query.add_parameter(0, "stock")

    sql_str = query.to_sql(show_parameters=True)
    assert "0" in sql_str


def test_to_sql_negative_number() -> None:
    """Test to_sql() handles negative numbers."""
    query = sql.select("*").from_("products").where("discount = :discount")
    query.add_parameter(-10, "discount")

    sql_str = query.to_sql(show_parameters=True)
    assert "-10" in sql_str


def test_to_sql_float_parameter() -> None:
    """Test to_sql() handles float parameters."""
    query = sql.select("*").from_("products").where("rating = :rating")
    query.add_parameter(4.5, "rating")

    sql_str = query.to_sql(show_parameters=True)
    assert "4.5" in sql_str


def test_to_sql_string_with_quotes() -> None:
    """Test to_sql() escapes quotes in string parameters."""
    query = sql.select("*").from_("products").where("name = :name")
    query.add_parameter("Product's Name", "name")

    sql_str = query.to_sql(show_parameters=True)
    assert "Product" in sql_str


def test_to_sql_empty_string() -> None:
    """Test to_sql() handles empty string parameters."""
    query = sql.select("*").from_("products").where("description = :desc")
    query.add_parameter("", "desc")

    sql_str = query.to_sql(show_parameters=True)
    assert "''" in sql_str


def test_to_sql_datetime_parameter() -> None:
    """Test to_sql() handles datetime parameters."""
    query = sql.select("*").from_("products").where("created_at = :created")
    query.add_parameter(datetime(2025, 10, 31, 12, 0, 0), "created")

    sql_str = query.to_sql(show_parameters=True)
    assert "2025" in sql_str


def test_to_sql_date_parameter() -> None:
    """Test to_sql() handles date parameters."""
    query = sql.select("*").from_("products").where("created_date = :date")
    query.add_parameter(date(2025, 10, 31), "date")

    sql_str = query.to_sql(show_parameters=True)
    assert "2025" in sql_str


def test_to_sql_list_parameter() -> None:
    """Test to_sql() handles list parameters."""
    query = sql.select("*").from_("products").where("id IN :ids")
    query.add_parameter([1, 2, 3], "ids")

    sql_str = query.to_sql(show_parameters=True)
    assert "1" in sql_str or "[1, 2, 3]" in sql_str


def test_to_sql_false_boolean() -> None:
    """Test to_sql() handles False boolean correctly."""
    query = sql.select("*").from_("products").where("active = :active")
    query.add_parameter(False, "active")

    sql_str = query.to_sql(show_parameters=True)
    assert "FALSE" in sql_str.upper()


def test_to_sql_complex_query_with_subquery() -> None:
    """Test to_sql() with complex query containing subquery."""
    query = (
        sql.select("*").from_("products").where("price > (SELECT AVG(price) FROM products WHERE category = :category)")
    )
    query.add_parameter("electronics", "category")

    sql_str = query.to_sql(show_parameters=True)
    assert "'electronics'" in sql_str
    assert "AVG" in sql_str


def test_to_sql_join_query() -> None:
    """Test to_sql() with JOIN query."""
    query = (
        sql
        .select("p.name", "c.category_name")
        .from_("products p")
        .join("categories c", "p.category_id = c.id")
        .where("p.price > :min_price")
    )
    query.add_parameter(100, "min_price")

    sql_str = query.to_sql(show_parameters=True)
    assert "100" in sql_str
    assert "JOIN" in sql_str


def test_to_sql_update_statement() -> None:
    """Test to_sql() with UPDATE statement."""
    query = sql.update("products").set(price=29.99, stock=50).where("id = :id")
    query.add_parameter(1, "id")

    sql_str = query.to_sql(show_parameters=True)
    assert "29.99" in sql_str or "50" in sql_str
    assert "UPDATE" in sql_str


def test_to_sql_delete_statement() -> None:
    """Test to_sql() with DELETE statement."""
    query = sql.delete().from_("products").where("id = :id")
    query.add_parameter(123, "id")

    sql_str = query.to_sql(show_parameters=True)
    assert "123" in sql_str
    assert "DELETE" in sql_str


def test_to_sql_delete_statement_with_table_arg() -> None:
    """Test sql.delete(table) sets target table without explicit from()."""

    query = sql.delete("products").where("id = :id")
    query.add_parameter(999, "id")

    sql_str = query.to_sql(show_parameters=True)

    assert "products" in sql_str.lower()
    assert "delete" in sql_str.lower()
    assert "999" in sql_str


def test_to_sql_same_parameter_name_multiple_times() -> None:
    """Test to_sql() handles same parameter name used multiple times."""
    query = sql.select("*").from_("products").where("price >= :threshold").where("discount >= :threshold")
    query.add_parameter(10, "threshold")

    sql_str = query.to_sql(show_parameters=True)
    assert "10" in sql_str


def test_to_sql_very_long_string() -> None:
    """Test to_sql() handles very long string parameters."""
    long_string = "A" * 1000
    query = sql.select("*").from_("products").where("description = :desc")
    query.add_parameter(long_string, "desc")

    sql_str = query.to_sql(show_parameters=True)
    assert "A" in sql_str


def test_to_sql_unicode_string() -> None:
    """Test to_sql() handles unicode string parameters."""
    query = sql.select("*").from_("products").where("name = :name")
    query.add_parameter("Café ☕", "name")

    sql_str = query.to_sql(show_parameters=True)
    assert "Café" in sql_str or "Cafe" in sql_str


def test_to_sql_numeric_string() -> None:
    """Test to_sql() handles numeric strings correctly."""
    query = sql.select("*").from_("products").where("sku = :sku")
    query.add_parameter("12345", "sku")

    sql_str = query.to_sql(show_parameters=True)
    assert "'12345'" in sql_str


def test_to_sql_preserves_sql_structure() -> None:
    """Test to_sql() preserves SQL structure with parameter substitution."""
    query = sql.select("name", "price").from_("products").where("category = :cat").order_by("price DESC").limit(10)
    query.add_parameter("electronics", "cat")

    sql_without_params = query.to_sql()
    sql_with_params = query.to_sql(show_parameters=True)

    assert "ORDER BY" in sql_without_params
    assert "ORDER BY" in sql_with_params
    assert "LIMIT" in sql_without_params
    assert "LIMIT" in sql_with_params


def test_to_sql_with_cte() -> None:
    """Test to_sql() with Common Table Expression."""
    query = sql.select("*").from_("products").where("price > :price")
    query.add_parameter(100, "price")

    sql_str = query.to_sql(show_parameters=True)
    assert "100" in sql_str


def test_to_sql_bytes_parameter() -> None:
    """Test to_sql() handles bytes parameters."""
    query = sql.select("*").from_("products").where("image_data = :data")
    query.add_parameter(b"binary_data", "data")

    sql_str = query.to_sql(show_parameters=True)
    assert "binary" in sql_str or "bytes" in sql_str or "b'" in sql_str


def test_to_sql_none_vs_null() -> None:
    """Test to_sql() correctly shows NULL for None values."""
    query = sql.select("*").from_("products").where("deleted_at = :deleted")
    query.add_parameter(None, "deleted")

    sql_str = query.to_sql(show_parameters=True)
    null_str = sql_str.upper()
    assert "NULL" in null_str
    assert "NONE" not in null_str


def test_to_sql_mixed_case_placeholders() -> None:
    """Test to_sql() handles mixed case parameter names."""
    query = sql.select("*").from_("products").where("category = :CategoryName")
    query.add_parameter("electronics", "CategoryName")

    sql_str = query.to_sql(show_parameters=True)
    assert "'electronics'" in sql_str


def test_to_sql_aggregate_functions() -> None:
    """Test to_sql() with aggregate functions and parameters."""
    query = sql.select("COUNT(*) as total").from_("products").where("category = :cat").group_by("category")
    query.add_parameter("electronics", "cat")

    sql_str = query.to_sql(show_parameters=True)
    assert "COUNT" in sql_str
    assert "'electronics'" in sql_str


def test_to_sql_window_functions() -> None:
    """Test to_sql() with window functions and parameters."""
    query = (
        sql
        .select("name", "ROW_NUMBER() OVER (PARTITION BY category ORDER BY price)")
        .from_("products")
        .where("price > :min")
    )
    query.add_parameter(50, "min")

    sql_str = query.to_sql(show_parameters=True)
    assert "ROW_NUMBER" in sql_str
    assert "50" in sql_str


def test_to_sql_default_shows_placeholders() -> None:
    """Test to_sql() without parameters shows placeholders."""
    query = sql.select("name", "price").from_("products").where("id = :id")
    query.add_parameter(123, "id")

    sql_str = query.to_sql()
    assert ":id" in sql_str
    assert "123" not in sql_str


def test_to_sql_with_show_parameters_substitutes_values() -> None:
    """Test to_sql(show_parameters=True) substitutes actual values."""
    query = sql.select("name", "price").from_("products").where("id = :id")
    query.add_parameter(123, "id")

    sql_str = query.to_sql(show_parameters=True)
    assert "123" in sql_str
    assert ":id" not in sql_str


def test_to_sql_string_parameter_quoted() -> None:
    """Test to_sql() quotes string parameters."""
    query = sql.select("*").from_("products").where("name = :name")
    query.add_parameter("Product 1", "name")

    sql_str = query.to_sql(show_parameters=True)
    assert "'Product 1'" in sql_str


def test_to_sql_null_parameter() -> None:
    """Test to_sql() handles NULL parameters."""
    query = sql.select("*").from_("products").where("description = :desc")
    query.add_parameter(None, "desc")

    sql_str = query.to_sql(show_parameters=True)
    assert "NULL" in sql_str.upper()


def test_to_sql_boolean_parameters() -> None:
    """Test to_sql() handles boolean parameters."""
    query = sql.select("*").from_("products").where("active = :active")
    query.add_parameter(True, "active")

    sql_str = query.to_sql(show_parameters=True)
    assert "TRUE" in sql_str.upper()


def test_to_sql_multiple_parameters() -> None:
    """Test to_sql() handles multiple parameters."""
    query = sql.select("*").from_("products").where("price > :min_price").where("category = :category")
    query.add_parameter(100, "min_price")
    query.add_parameter("electronics", "category")

    sql_str = query.to_sql(show_parameters=True)
    assert "100" in sql_str
    assert "'electronics'" in sql_str
    assert ":min_price" not in sql_str
    assert ":category" not in sql_str


def test_to_sql_merge_builder() -> None:
    """Test to_sql() works with MERGE builder."""
    query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using({"id": 1, "name": "Product 1"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    sql_str = query.to_sql()
    assert "MERGE INTO" in sql_str

    sql_with_params = query.to_sql(show_parameters=True)
    assert "MERGE INTO" in sql_with_params


def test_to_sql_insert_builder() -> None:
    """Test to_sql() works with INSERT builder."""
    query = sql.insert("products").values(id=1, name="Product 1", price=19.99)

    sql_str = query.to_sql()
    assert "INSERT INTO" in sql_str
    assert ":id" in sql_str
    assert ":name" in sql_str
    assert ":price" in sql_str

    sql_with_params = query.to_sql(show_parameters=True)
    assert "INSERT INTO" in sql_with_params
    assert "1" in sql_with_params
    assert "'Product 1'" in sql_with_params
    assert "19.99" in sql_with_params
