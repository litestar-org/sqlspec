"""Unit tests for build() and to_sql() dialect override parameter."""

from sqlspec import sql


def test_build_with_default_dialect() -> None:
    """Test build() uses builder's default dialect."""
    query = sql.select("*").from_("products").where("id = :id")
    query.add_parameter(1, "id")

    result = query.build()
    assert ":id" in result.sql or "$1" in result.sql


def test_build_with_dialect_override() -> None:
    """Test build() can override dialect at build time."""
    query = sql.select("*").from_("products").where("id = :id")
    query.add_parameter(1, "id")

    postgres_result = query.build(dialect="postgres")
    assert "SELECT" in postgres_result.sql
    assert "products" in postgres_result.sql.lower()

    mysql_result = query.build(dialect="mysql")
    assert "SELECT" in mysql_result.sql
    assert "`products`" in mysql_result.sql or "products" in mysql_result.sql.lower()


def test_build_dialect_override_preserves_parameters() -> None:
    """Test dialect override doesn't affect parameters."""
    query = sql.select("*").from_("products").where("price > :min_price")
    query.add_parameter(100, "min_price")

    result1 = query.build(dialect="postgres")
    result2 = query.build(dialect="mysql")

    assert result1.parameters == {"min_price": 100}
    assert result2.parameters == {"min_price": 100}


def test_to_sql_with_default_dialect() -> None:
    """Test to_sql() uses builder's default dialect."""
    query = sql.select("name", "price").from_("products")

    sql_str = query.to_sql()
    assert "SELECT" in sql_str
    assert "products" in sql_str.lower()


def test_to_sql_with_dialect_override() -> None:
    """Test to_sql() can override dialect."""
    query = sql.select("*").from_("products").where("active = :active")
    query.add_parameter(True, "active")

    postgres_sql = query.to_sql(dialect="postgres")
    assert "SELECT" in postgres_sql
    assert "products" in postgres_sql.lower()

    mysql_sql = query.to_sql(dialect="mysql")
    assert "SELECT" in mysql_sql
    assert "products" in mysql_sql.lower()


def test_to_sql_with_show_parameters_and_dialect_override() -> None:
    """Test to_sql() with both show_parameters and dialect override.

    Note: Parameter substitution only works when using default dialect since
    SQLGlot changes parameter formats (e.g., :id → %(id)s for postgres).
    This test just verifies the dialect override works.
    """
    query = sql.select("*").from_("products").where("id = :id")
    query.add_parameter(123, "id")

    postgres_sql = query.to_sql(show_parameters=False, dialect="postgres")
    assert "SELECT" in postgres_sql
    assert "products" in postgres_sql.lower()

    mysql_sql = query.to_sql(show_parameters=False, dialect="mysql")
    assert "SELECT" in mysql_sql
    assert "products" in mysql_sql.lower()


def test_build_dialect_override_with_merge() -> None:
    """Test dialect override works with MERGE builder."""
    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    postgres_result = query.build(dialect="postgres")
    assert "MERGE INTO" in postgres_result.sql

    oracle_result = query.build(dialect="oracle")
    assert "MERGE INTO" in oracle_result.sql


def test_build_dialect_override_with_insert() -> None:
    """Test dialect override works with INSERT builder."""
    query = sql.insert("products").values(id=1, name="Widget", price=19.99)

    postgres_sql = query.build(dialect="postgres")
    assert "INSERT INTO" in postgres_sql.sql

    mysql_sql = query.build(dialect="mysql")
    assert "INSERT INTO" in mysql_sql.sql


def test_to_sql_dialect_override_preserves_builder_state() -> None:
    """Test dialect override doesn't modify builder's default dialect."""
    query = sql.select("*", dialect="postgres").from_("products")

    query.to_sql(dialect="mysql")
    query.to_sql()

    assert query.dialect_name == "postgres"


def test_build_multiple_dialect_overrides() -> None:
    """Test builder can generate SQL for multiple dialects."""
    query = sql.select("id", "name", "price").from_("products").where("category = :category").order_by("price DESC")
    query.add_parameter("electronics", "category")

    dialects = ["postgres", "mysql", "sqlite", "oracle", "bigquery"]
    results = [query.build(dialect=d) for d in dialects]

    for result in results:
        assert "SELECT" in result.sql
        assert "products" in result.sql.lower()
        assert result.parameters == {"category": "electronics"}


def test_to_sql_dialect_override_with_complex_query() -> None:
    """Test dialect override with complex query (JOIN, WHERE, ORDER BY)."""
    query = (
        sql
        .select("p.id", "p.name", "c.name AS category_name")
        .from_("products AS p")
        .join("categories AS c", "p.category_id = c.id")
        .where("p.price > :min_price")
        .where("c.active = :active")
        .order_by("p.name")
    )
    query.add_parameter(100, "min_price")
    query.add_parameter(True, "active")

    postgres_sql = query.to_sql(dialect="postgres")
    mysql_sql = query.to_sql(dialect="mysql")

    assert "JOIN" in postgres_sql
    assert "JOIN" in mysql_sql
    assert "WHERE" in postgres_sql
    assert "WHERE" in mysql_sql
