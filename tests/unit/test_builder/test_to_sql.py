"""Unit tests for QueryBuilder.to_sql() method."""

from sqlspec import sql


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
