"""Tests for DeleteBuilder."""

from sqlglot import exp

from sqlspec.statement.builder import DeleteBuilder


def test_basic_DeleteBuilder() -> None:
    """Test basic DELETE statement construction."""
    builder = DeleteBuilder()
    result = builder.from_("users").build()

    # Verify SQL structure
    assert "DELETE" in result.sql
    assert "users" in result.sql


def test_delete_with_where() -> None:
    """Test DELETE with WHERE clause."""
    builder = DeleteBuilder()
    result = builder.from_("users").where("age < 18").build()

    assert "DELETE" in result.sql
    assert "WHERE" in result.sql


def test_delete_with_where_eq() -> None:
    """Test DELETE with parameterized WHERE equality."""
    builder = DeleteBuilder()
    result = builder.from_("users").where_eq("id", 123).build()

    assert "DELETE" in result.sql
    assert "WHERE" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 1
    assert 123 in result.parameters.values()


def test_delete_with_where_in() -> None:
    """Test DELETE with WHERE IN clause."""
    builder = DeleteBuilder()
    result = builder.from_("users").where_in("status", ["inactive", "banned"]).build()

    assert "DELETE" in result.sql
    assert "WHERE" in result.sql
    assert "IN" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 2


def test_delete_parameter_binding() -> None:
    """Test that values are properly parameterized."""
    builder = DeleteBuilder()
    result = builder.from_("users").where_eq("name", "'; DROP TABLE users; --").build()

    # Verify SQL injection is prevented
    assert "DROP TABLE" not in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 1


def test_delete_with_expression_column() -> None:
    """Test DELETE with sqlglot expression as column."""
    builder = DeleteBuilder()
    col_expr = exp.column("status")
    result = builder.from_("users").where_eq(col_expr, "deleted").build()

    assert "DELETE" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 1


def test_delete_chaining() -> None:
    """Test method chaining returns builder instance."""
    builder = DeleteBuilder()

    assert isinstance(builder.from_("users"), DeleteBuilder)
    assert isinstance(builder.where("id = 1"), DeleteBuilder)
    assert isinstance(builder.where_eq("status", "active"), DeleteBuilder)
    assert isinstance(builder.where_in("id", [1, 2, 3]), DeleteBuilder)


def test_delete_multiple_where_conditions() -> None:
    """Test DELETE with multiple WHERE conditions."""
    builder = DeleteBuilder()
    result = (
        builder.from_("users")
        .where("age < 18")
        .where_eq("status", "inactive")
        .where_in("role", ["guest", "temp"])
        .build()
    )

    assert "DELETE" in result.sql
    assert "WHERE" in result.sql
    # Should have parameters for the parameterized conditions
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) >= 3


def test_delete_string_representation() -> None:
    """Test string representation of DeleteBuilder."""
    builder = DeleteBuilder()
    builder.from_("users")

    sql_str = str(builder)
    assert "DELETE" in sql_str
    assert "users" in sql_str


def test_delete_with_complex_conditions() -> None:
    """Test DELETE with complex WHERE conditions."""
    builder = DeleteBuilder()
    result = builder.from_("users").where("created_at < '2020-01-01' AND last_login IS NULL").build()

    assert "DELETE" in result.sql
    assert "WHERE" in result.sql
    assert "AND" in result.sql


def test_delete_table_storage() -> None:
    """Test that table name is stored internally."""
    builder = DeleteBuilder()
    builder.from_("users")

    assert builder._table == "users"


def test_delete_condition_chaining() -> None:
    """Test that WHERE conditions are properly chained."""
    builder = DeleteBuilder()
    result = builder.from_("users").where_eq("status", "inactive").where("age > 65").build()

    # Both conditions should be present
    assert "WHERE" in result.sql
    # Should have at least one parameter from where_eq
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) >= 1


def test_delete_where_in_with_tuples() -> None:
    """Test DELETE with WHERE IN using tuples."""
    builder = DeleteBuilder()
    result = builder.from_("users").where_in("id", (1, 2, 3, 4, 5)).build()

    assert "DELETE" in result.sql
    assert "IN" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 5


def test_delete_empty_where_in_list() -> None:
    """Test DELETE with empty WHERE IN list."""
    builder = DeleteBuilder()
    result = builder.from_("users").where_in("id", []).build()

    assert "DELETE" in result.sql
    # Empty IN clause should still be valid SQL
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 0
