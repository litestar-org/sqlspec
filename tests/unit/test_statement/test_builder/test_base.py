"""Comprehensive unit tests for QueryBuilder base class and WhereClauseMixin.

This module tests the foundational builder functionality including:
- QueryBuilder abstract base class behavior
- Parameter management and binding
- CTE (Common Table Expression) support
- SafeQuery construction and validation
- WhereClauseMixin helper methods
- Dialect handling
- Error handling and edge cases
"""

import math
from typing import Any, Optional
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import (
    CreateIndexBuilder,
    CreateSchemaBuilder,
    DropIndexBuilder,
    DropSchemaBuilder,
    DropTableBuilder,
    DropViewBuilder,
    TruncateTableBuilder,
)
from sqlspec.statement.builder.base import QueryBuilder, SafeQuery
from sqlspec.statement.builder.ddl import (
    AlterTableBuilder,
    CommentOnBuilder,
    CreateMaterializedViewBuilder,
    CreateTableAsSelectBuilder,
    CreateViewBuilder,
    RenameTableBuilder,
)
from sqlspec.statement.builder.mixins._where import WhereClauseMixin
from sqlspec.statement.builder.select import SelectBuilder
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


# Test implementation of abstract QueryBuilder for testing
class MockQueryBuilder(QueryBuilder[SQLResult[dict[str, Any]]]):
    """Concrete implementation of QueryBuilder for testing purposes."""

    def _create_base_expression(self) -> exp.Select:
        """Create a basic SELECT expression for testing."""
        return exp.Select()

    @property
    def _expected_result_type(self) -> "type[SQLResult[SQLResult[dict[str, Any]]]]":
        """Return the expected result type."""
        return SQLResult[SQLResult[dict[str, Any]]]  # type: ignore[arg-type]


# Helper implementation of WhereClauseMixin for testing
class WhereClauseMixinHelper(WhereClauseMixin):
    """Helper class implementing WhereClauseMixin for testing purposes."""

    def __init__(self) -> None:
        self._parameters: dict[str, Any] = {}
        self._parameter_counter = 0
        self.dialect_name = None

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple["WhereClauseMixinHelper", str]:
        """Add parameter implementation for testing."""
        if name and name in self._parameters:
            raise SQLBuilderError(f"Parameter name '{name}' already exists.")

        param_name = name or f"param_{self._parameter_counter + 1}"
        self._parameter_counter += 1
        self._parameters[param_name] = value
        return self, param_name

    def where(self, condition: Any) -> "WhereClauseMixinHelper":
        """Mock where implementation for testing."""
        return self

    def _raise_sql_builder_error(self, message: str, cause: Optional[Exception] = None) -> None:
        """Mock error raising for testing."""
        raise SQLBuilderError(message) from cause


# Fixtures
@pytest.fixture
def test_builder() -> MockQueryBuilder:
    """Fixture providing a test QueryBuilder instance."""
    return MockQueryBuilder()


@pytest.fixture
def where_mixin() -> WhereClauseMixinHelper:
    """Fixture providing a test WhereClauseMixin instance."""
    return WhereClauseMixinHelper()


@pytest.fixture
def sample_cte_query() -> str:
    """Fixture providing a sample CTE query."""
    return "SELECT id, name FROM active_users WHERE status = 'active'"


# SafeQuery tests
def test_safe_query_basic_construction() -> None:
    """Test basic SafeQuery construction with required fields."""
    query = SafeQuery(sql="SELECT * FROM users", parameters={"param_1": "value"})

    assert query.sql == "SELECT * FROM users"
    assert query.parameters == {"param_1": "value"}
    assert query.dialect is None


def test_safe_query_with_dialect() -> None:
    """Test SafeQuery construction with dialect specified."""
    query = SafeQuery(sql="SELECT * FROM users", parameters={}, dialect="postgresql")

    assert query.dialect == "postgresql"


def test_safe_query_default_parameters() -> None:
    """Test SafeQuery default parameters dictionary."""
    query = SafeQuery(sql="SELECT 1")

    assert isinstance(query.parameters, dict)
    assert len(query.parameters) == 0


def test_safe_query_immutability() -> None:
    """Test that SafeQuery is immutable (frozen dataclass)."""
    query = SafeQuery(sql="SELECT 1")

    with pytest.raises(Exception):  # Should be frozen
        query.sql = "SELECT 2"  # type: ignore[misc]


# QueryBuilder basic functionality tests
def test_query_builder_initialization(test_builder: MockQueryBuilder) -> None:
    """Test QueryBuilder initialization sets up required fields."""
    assert test_builder._expression is not None
    assert isinstance(test_builder._expression, exp.Select)
    assert isinstance(test_builder._parameters, dict)
    assert test_builder._parameter_counter == 0
    assert isinstance(test_builder._with_ctes, dict)


@pytest.mark.parametrize(
    "dialect,expected_name",
    [(None, None), ("postgresql", "postgresql"), ("mysql", "mysql"), ("sqlite", "sqlite")],
    ids=["no_dialect", "postgresql", "mysql", "sqlite"],
)
def test_query_builder_dialect_property(dialect: Any, expected_name: Any) -> None:
    """Test dialect property returns correct values."""
    builder = MockQueryBuilder(dialect=dialect)
    assert builder.dialect_name == expected_name


def test_query_builder_dialect_property_with_class() -> None:
    """Test dialect property with Dialect class."""
    mock_dialect_class = Mock()
    mock_dialect_class.__name__ = "PostgreSQL"

    builder = MockQueryBuilder(dialect=mock_dialect_class)
    assert builder.dialect_name == "postgresql"


def test_query_builder_dialect_property_with_instance() -> None:
    """Test dialect property with Dialect instance."""
    mock_dialect = Mock(spec=Dialect)
    type(mock_dialect).__name__ = "MySQL"

    builder = MockQueryBuilder(dialect=mock_dialect)
    assert builder.dialect_name == "mysql"


# Parameter management tests
@pytest.mark.parametrize(
    "value,explicit_name,expected_name_pattern",
    [
        ("test_value", None, r"param_\d+"),
        (42, None, r"param_\d+"),
        ("custom_value", "custom_param", "custom_param"),
        (True, "bool_param", "bool_param"),
    ],
    ids=["auto_name_string", "auto_name_int", "explicit_name", "explicit_bool"],
)
def test_query_builder_add_parameter(
    test_builder: MockQueryBuilder, value: Any, explicit_name: Any, expected_name_pattern: str
) -> None:
    """Test adding parameters with various configurations."""
    result_builder, param_name = test_builder.add_parameter(value, name=explicit_name)

    assert result_builder is test_builder
    assert param_name in test_builder._parameters
    assert test_builder._parameters[param_name] == value

    if explicit_name:
        assert param_name == expected_name_pattern
    else:
        assert param_name.startswith("param_")


def test_query_builder_add_parameter_duplicate_name_error(test_builder: MockQueryBuilder) -> None:
    """Test error when adding parameter with duplicate name."""
    test_builder.add_parameter("first_value", name="duplicate")

    with pytest.raises(SQLBuilderError, match="Parameter name 'duplicate' already exists"):
        test_builder.add_parameter("second_value", name="duplicate")


def test_query_builder_parameter_counter_increment(test_builder: MockQueryBuilder) -> None:
    """Test that parameter counter increments correctly."""
    initial_counter = test_builder._parameter_counter

    test_builder._add_parameter("value1")
    assert test_builder._parameter_counter == initial_counter + 1

    test_builder.add_parameter("value2")
    assert test_builder._parameter_counter == initial_counter + 2


@pytest.mark.parametrize(
    "parameter_value",
    ["string_value", 42, math.pi, True, None, [1, 2, 3], {"key": "value"}, {1, 2, 3}, ("tuple", "value")],
    ids=["string", "int", "float", "bool", "none", "list", "dict", "set", "tuple"],
)
def test_query_builder_parameter_types(test_builder: MockQueryBuilder, parameter_value: Any) -> None:
    """Test that various parameter types are handled correctly."""
    _, param_name = test_builder.add_parameter(parameter_value)
    assert test_builder._parameters[param_name] == parameter_value


# CTE (Common Table Expression) tests
def test_query_builder_with_cte_string_query(test_builder: MockQueryBuilder, sample_cte_query: str) -> None:
    """Test adding CTE with string query."""
    alias = "active_users"
    result = test_builder.with_cte(alias, sample_cte_query)

    assert result is test_builder
    assert alias in test_builder._with_ctes
    assert isinstance(test_builder._with_ctes[alias], exp.CTE)


def test_query_builder_with_cte_builder_query(test_builder: MockQueryBuilder) -> None:
    """Test adding CTE with QueryBuilder instance."""
    alias = "user_stats"
    cte_builder = MockQueryBuilder()
    cte_builder._parameters = {"status": "active"}

    result = test_builder.with_cte(alias, cte_builder)

    assert result is test_builder
    assert alias in test_builder._with_ctes
    # Parameters should be merged with CTE prefix
    assert any("active" in str(value) for value in test_builder._parameters.values())


def test_query_builder_with_cte_sqlglot_expression(test_builder: MockQueryBuilder) -> None:
    """Test adding CTE with sqlglot Select expression."""
    alias = "test_cte"
    select_expr = exp.Select().select("id").from_("users")

    result = test_builder.with_cte(alias, select_expr)

    assert result is test_builder
    assert alias in test_builder._with_ctes


def test_query_builder_with_cte_duplicate_alias_error(test_builder: MockQueryBuilder, sample_cte_query: str) -> None:
    """Test error when adding CTE with duplicate alias."""
    alias = "duplicate_cte"
    test_builder.with_cte(alias, sample_cte_query)

    with pytest.raises(SQLBuilderError, match=f"CTE with alias '{alias}' already exists"):
        test_builder.with_cte(alias, sample_cte_query)


@pytest.mark.parametrize(
    "invalid_query,error_match",
    [
        (42, "Invalid query type for CTE"),
        ([], "Invalid query type for CTE"),
        ({}, "Invalid query type for CTE"),
        ("INVALID SQL SYNTAX", "Failed to parse CTE query string"),
        ("INSERT INTO users VALUES (1, 'test')", "must parse to a SELECT statement"),
    ],
    ids=["int", "list", "dict", "invalid_sql", "non_select"],
)
def test_query_builder_with_cte_invalid_query(
    test_builder: MockQueryBuilder, invalid_query: Any, error_match: str
) -> None:
    """Test error when adding CTE with invalid query."""
    with pytest.raises(SQLBuilderError, match=error_match):
        test_builder.with_cte("invalid_cte", invalid_query)


def test_query_builder_with_cte_builder_without_expression(test_builder: MockQueryBuilder) -> None:
    """Test error when CTE builder has no expression."""
    alias = "no_expr_cte"
    invalid_builder = MockQueryBuilder()
    invalid_builder._expression = None

    with pytest.raises(SQLBuilderError, match="CTE query builder has no expression"):
        test_builder.with_cte(alias, invalid_builder)


def test_query_builder_with_cte_builder_wrong_expression_type(test_builder: MockQueryBuilder) -> None:
    """Test error when CTE builder has wrong expression type."""
    alias = "wrong_expr_cte"
    invalid_builder = MockQueryBuilder()
    invalid_builder._expression = exp.Insert()  # Wrong type

    with pytest.raises(SQLBuilderError, match="must be a Select"):
        test_builder.with_cte(alias, invalid_builder)


# Build method tests
def test_query_builder_build_basic(test_builder: MockQueryBuilder) -> None:
    """Test basic build method functionality."""
    query = test_builder.build()

    assert isinstance(query, SafeQuery)
    assert isinstance(query.sql, str)
    assert isinstance(query.parameters, dict)
    assert query.dialect == test_builder.dialect


def test_query_builder_build_with_parameters(test_builder: MockQueryBuilder) -> None:
    """Test build method includes parameters."""
    test_builder.add_parameter("value1", "param1")
    test_builder.add_parameter("value2", "param2")

    query = test_builder.build()

    assert "param1" in query.parameters
    assert "param2" in query.parameters
    assert query.parameters["param1"] == "value1"
    assert query.parameters["param2"] == "value2"


def test_query_builder_build_parameters_copy(test_builder: MockQueryBuilder) -> None:
    """Test that build method returns a copy of parameters."""
    test_builder.add_parameter("original_value", "test_param")
    query = test_builder.build()

    # Modify the returned parameters
    query.parameters["test_param"] = "modified_value"

    # Original should be unchanged
    assert test_builder._parameters["test_param"] == "original_value"


def test_query_builder_build_with_ctes(test_builder: MockQueryBuilder, sample_cte_query: str) -> None:
    """Test build method with CTEs."""
    test_builder.with_cte("test_cte", sample_cte_query)
    query = test_builder.build()

    assert "WITH" in query.sql or "test_cte" in query.sql


def test_query_builder_build_expression_not_initialized() -> None:
    """Test build error when expression is not initialized."""
    builder = MockQueryBuilder()
    builder._expression = None

    with pytest.raises(SQLBuilderError, match="expression not initialized"):
        builder.build()


@patch("sqlspec.statement.builder.base.logger")
def test_query_builder_build_sql_generation_error(mock_logger: Mock, test_builder: MockQueryBuilder) -> None:
    """Test build method handles SQL generation errors."""
    # Mock the expression to raise an error during SQL generation
    test_builder._expression = Mock()
    test_builder._expression.copy.return_value = test_builder._expression
    test_builder._expression.sql.side_effect = Exception("SQL generation failed")

    with pytest.raises(SQLBuilderError, match="Error generating SQL"):
        test_builder.build()

    # Verify that the error was logged
    mock_logger.exception.assert_called_once()


# to_statement method tests
def test_query_builder_to_statement_basic(test_builder: MockQueryBuilder) -> None:
    """Test basic to_statement method functionality."""
    statement = test_builder.to_statement()

    assert isinstance(statement, SQL)


def test_query_builder_to_statement_with_config(test_builder: MockQueryBuilder) -> None:
    """Test to_statement method with custom config."""
    config = SQLConfig()
    statement = test_builder.to_statement(config)

    assert isinstance(statement, SQL)


def test_query_builder_to_statement_includes_parameters(test_builder: MockQueryBuilder) -> None:
    """Test that to_statement includes parameters."""
    test_builder.add_parameter("test_value", "test_param")
    statement = test_builder.to_statement()

    # The SQL object should contain the parameters
    assert hasattr(statement, "_parameters") or hasattr(statement, "parameters")


# Error handling tests
def test_query_builder_raise_sql_builder_error() -> None:
    """Test _raise_sql_builder_error method."""
    with pytest.raises(SQLBuilderError, match="Test error message"):
        MockQueryBuilder._raise_sql_builder_error("Test error message")


def test_query_builder_raise_sql_builder_error_with_cause() -> None:
    """Test _raise_sql_builder_error method with cause."""
    original_error = ValueError("Original error")

    with pytest.raises(SQLBuilderError, match="Test error message") as exc_info:
        MockQueryBuilder._raise_sql_builder_error("Test error message", original_error)

    assert exc_info.value.__cause__ is original_error


# WhereClauseMixin tests
@pytest.mark.parametrize(
    "column,value",
    [("name", "John"), ("age", 25), ("active", True), (exp.column("status"), "active")],
    ids=["string_column", "int_value", "bool_value", "expression_column"],
)
def test_where_mixin_where_eq(where_mixin: WhereClauseMixinHelper, column: Any, value: Any) -> None:
    """Test where_eq functionality with various inputs."""
    result = where_mixin.where_eq(column, value)

    assert result is where_mixin
    assert value in where_mixin._parameters.values()


def test_where_mixin_where_between_basic(where_mixin: WhereClauseMixinHelper) -> None:
    """Test basic where_between functionality."""
    result = where_mixin.where_between("age", 18, 65)

    assert result is where_mixin
    assert 18 in where_mixin._parameters.values()
    assert 65 in where_mixin._parameters.values()


@pytest.mark.parametrize(
    "pattern,escape",
    [("John%", None), ("%@example.com", None), ("_test_", None), ("test\\_underscore", "\\")],
    ids=["prefix", "suffix", "wildcard", "escaped"],
)
def test_where_mixin_where_like(where_mixin: WhereClauseMixinHelper, pattern: str, escape: Any) -> None:
    """Test where_like functionality with various patterns."""
    if escape:
        result = where_mixin.where_like("name", pattern, escape)
    else:
        result = where_mixin.where_like("name", pattern)

    assert result is where_mixin
    assert pattern in where_mixin._parameters.values()


def test_where_mixin_where_not_like_basic(where_mixin: WhereClauseMixinHelper) -> None:
    """Test basic where_not_like functionality."""
    pattern = "test%"
    result = where_mixin.where_not_like("name", pattern)

    assert result is where_mixin
    assert pattern in where_mixin._parameters.values()


@pytest.mark.parametrize(
    "column",
    ["deleted_at", "email", "phone", exp.column("last_login")],
    ids=["deleted_at", "email", "phone", "expression"],
)
def test_where_mixin_null_checks(where_mixin: WhereClauseMixinHelper, column: Any) -> None:
    """Test NULL check methods."""
    # Test IS NULL
    result = where_mixin.where_is_null(column)
    assert result is where_mixin

    # Test IS NOT NULL
    result = where_mixin.where_is_not_null(column)
    assert result is where_mixin


def test_where_mixin_where_exists_with_string(where_mixin: WhereClauseMixinHelper) -> None:
    """Test where_exists with string subquery."""
    subquery = "SELECT 1 FROM orders WHERE user_id = users.id"
    result = where_mixin.where_exists(subquery)

    assert result is where_mixin


def test_where_mixin_where_exists_with_builder(where_mixin: WhereClauseMixinHelper) -> None:
    """Test where_exists with QueryBuilder subquery."""
    mock_builder = Mock()
    mock_builder._parameters = {"status": "active"}
    mock_builder.build.return_value = Mock()
    mock_builder.build.return_value.sql = "SELECT 1 FROM orders"

    result = where_mixin.where_exists(mock_builder)

    assert result is where_mixin
    # Parameters should be merged
    assert "active" in where_mixin._parameters.values()


@patch("sqlglot.exp.maybe_parse")
def test_where_mixin_where_exists_parse_error(mock_parse: Mock, where_mixin: WhereClauseMixinHelper) -> None:
    """Test where_exists handles parse errors."""
    mock_parse.return_value = None  # Simulate parse failure

    with pytest.raises(SQLBuilderError, match="Could not parse subquery for EXISTS"):
        where_mixin.where_exists("INVALID SQL")


def test_where_mixin_method_chaining(where_mixin: WhereClauseMixinHelper) -> None:
    """Test that all WhereClauseMixin methods support chaining."""
    result = (
        where_mixin.where_eq("name", "John")
        .where_between("age", 18, 65)
        .where_like("email", "%@example.com")
        .where_is_not_null("created_at")
    )

    assert result is where_mixin
    # Should have parameters for parameterized methods
    assert len(where_mixin._parameters) >= 4


# DDL Builder tests
def test_drop_table_builder_basic() -> None:
    """Test basic DROP TABLE functionality."""
    sql = DropTableBuilder().table("my_table").build().sql
    assert "DROP TABLE" in sql and "my_table" in sql


def test_drop_index_builder_basic() -> None:
    """Test basic DROP INDEX functionality."""
    sql = DropIndexBuilder().name("idx_name").on_table("my_table").build().sql
    assert "DROP INDEX" in sql and "idx_name" in sql


def test_drop_view_builder_basic() -> None:
    """Test basic DROP VIEW functionality."""
    sql = DropViewBuilder().name("my_view").build().sql
    assert "DROP VIEW" in sql and "my_view" in sql


def test_drop_schema_builder_basic() -> None:
    """Test basic DROP SCHEMA functionality."""
    sql = DropSchemaBuilder().name("my_schema").build().sql
    assert "DROP SCHEMA" in sql and "my_schema" in sql


def test_create_index_builder_basic() -> None:
    """Test basic CREATE INDEX functionality."""
    sql = CreateIndexBuilder().name("idx_col").on_table("my_table").columns("col1", "col2").build().sql
    assert "CREATE INDEX" in sql and "idx_col" in sql


def test_truncate_table_builder_basic() -> None:
    """Test basic TRUNCATE TABLE functionality."""
    sql = TruncateTableBuilder().table("my_table").build().sql
    assert "TRUNCATE TABLE" in sql


def test_create_schema_builder_basic() -> None:
    """Test basic CREATE SCHEMA functionality."""
    sql = CreateSchemaBuilder().name("myschema").build().sql
    assert "CREATE SCHEMA" in sql and "myschema" in sql

    sql_if_not_exists = CreateSchemaBuilder().name("myschema").if_not_exists().build().sql
    assert "IF NOT EXISTS" in sql_if_not_exists and "myschema" in sql_if_not_exists

    sql_auth = CreateSchemaBuilder().name("myschema").authorization("bob").build().sql
    assert "CREATE SCHEMA" in sql_auth and "myschema" in sql_auth


# Complex DDL tests
def test_create_table_as_select_builder_basic() -> None:
    """Test CREATE TABLE AS SELECT functionality."""

    select_builder = SelectBuilder().select("id", "name").from_("users").where_eq("active", True)
    builder = (
        CreateTableAsSelectBuilder().name("new_table").if_not_exists().columns("id", "name").as_select(select_builder)
    )
    result = builder.build()
    sql = result.sql

    assert "CREATE TABLE" in sql
    assert "IF NOT EXISTS" in sql
    assert "AS SELECT" in sql or "AS\nSELECT" in sql
    assert 'FROM "users"' in sql or "FROM users" in sql
    assert "id" in sql and "name" in sql
    assert True in result.parameters.values()


def test_create_materialized_view_basic() -> None:
    """Test CREATE MATERIALIZED VIEW functionality."""

    select_builder = SelectBuilder().select("id", "name").from_("users").where_eq("active", True)
    builder = (
        CreateMaterializedViewBuilder()
        .name("active_users_mv")
        .if_not_exists()
        .columns("id", "name")
        .as_select(select_builder)
    )
    result = builder.build()
    sql = result.sql

    assert "CREATE MATERIALIZED VIEW" in sql or "CREATE MATERIALIZED_VIEW" in sql
    assert "IF NOT EXISTS" in sql
    assert "AS SELECT" in sql or "AS\nSELECT" in sql
    assert 'FROM "users"' in sql or "FROM users" in sql
    assert True in result.parameters.values()


def test_create_view_basic() -> None:
    """Test CREATE VIEW functionality."""

    select_builder = SelectBuilder().select("id", "name").from_("users").where_eq("active", True)
    builder = CreateViewBuilder().name("active_users_v").if_not_exists().columns("id", "name").as_select(select_builder)
    result = builder.build()
    sql = result.sql

    assert "CREATE VIEW" in sql
    assert "IF NOT EXISTS" in sql
    assert "AS SELECT" in sql or "AS\nSELECT" in sql
    assert 'FROM "users"' in sql or "FROM users" in sql
    assert True in result.parameters.values()


# ALTER TABLE tests
def test_alter_table_add_column() -> None:
    """Test ALTER TABLE ADD COLUMN."""

    sql = AlterTableBuilder("users").add_column("age", "INT").build().sql
    assert "ALTER TABLE" in sql and "ADD COLUMN" in sql and "age" in sql and "INT" in sql


def test_alter_table_drop_column() -> None:
    """Test ALTER TABLE DROP COLUMN."""

    sql = AlterTableBuilder("users").drop_column("age").build().sql
    assert "ALTER TABLE" in sql and "DROP COLUMN" in sql and "age" in sql


def test_alter_table_rename_column() -> None:
    """Test ALTER TABLE RENAME COLUMN."""

    sql = AlterTableBuilder("users").rename_column("old_name", "new_name").build().sql
    assert "ALTER TABLE" in sql and "RENAME COLUMN" in sql and "old_name" in sql and "new_name" in sql


def test_alter_table_error_if_no_action() -> None:
    """Test ALTER TABLE raises error without action."""

    builder = AlterTableBuilder("users")
    with pytest.raises(Exception):
        builder.build()


# COMMENT ON tests
def test_comment_on_table_builder() -> None:
    """Test COMMENT ON TABLE functionality."""

    sql = CommentOnBuilder().on_table("users").is_("User table").build().sql
    assert "COMMENT ON TABLE \"users\" IS 'User table'" in sql or "COMMENT ON TABLE users IS 'User table'" in sql


def test_comment_on_column_builder() -> None:
    """Test COMMENT ON COLUMN functionality."""

    sql = CommentOnBuilder().on_column("users", "age").is_("User age").build().sql
    assert "COMMENT ON COLUMN users.age IS 'User age'" in sql


def test_comment_on_builder_error() -> None:
    """Test COMMENT ON raises error without comment."""

    with pytest.raises(Exception):
        CommentOnBuilder().on_table("users").build()


# RENAME TABLE test
def test_rename_table_builder() -> None:
    """Test RENAME TABLE functionality."""

    sql = RenameTableBuilder().table("users").to("customers").build().sql
    assert 'ALTER TABLE "users" RENAME TO "customers"' in sql or "ALTER TABLE users RENAME TO customers" in sql


def test_rename_table_builder_error() -> None:
    """Test RENAME TABLE raises error without new name."""

    with pytest.raises(Exception):
        RenameTableBuilder().table("users").build()


# Integration tests
def test_query_builder_full_workflow_integration(test_builder: MockQueryBuilder) -> None:
    """Test complete QueryBuilder workflow integration."""
    # Add parameters
    test_builder.add_parameter("active", "status_param")

    # Add CTE
    test_builder.with_cte("active_users", "SELECT * FROM users WHERE status = 'active'")

    # Build query
    query = test_builder.build()

    assert isinstance(query, SafeQuery)
    assert query.parameters["status_param"] == "active"
    assert "WITH" in query.sql or "active_users" in query.sql


def test_query_builder_large_parameter_count(test_builder: MockQueryBuilder) -> None:
    """Test QueryBuilder with large number of parameters."""
    # Add many parameters
    for i in range(100):
        test_builder.add_parameter(f"value_{i}", f"param_{i}")

    query = test_builder.build()

    assert len(query.parameters) == 100
    assert all(f"value_{i}" in query.parameters.values() for i in range(100))


def test_query_builder_complex_parameter_types(test_builder: MockQueryBuilder) -> None:
    """Test QueryBuilder with complex parameter types."""
    complex_params = {
        "list_param": [1, 2, 3],
        "dict_param": {"nested": {"key": "value"}},
        "none_param": None,
        "bool_param": True,
        "set_param": {4, 5, 6},
        "tuple_param": (7, 8, 9),
    }

    for name, value in complex_params.items():
        test_builder.add_parameter(value, name)

    query = test_builder.build()

    for name, expected_value in complex_params.items():
        assert query.parameters[name] == expected_value


def test_query_builder_str_fallback() -> None:
    """Test __str__ fallback when build fails."""
    builder = MockQueryBuilder()
    builder._expression = None
    # Should not raise, should return dataclass __str__
    result = str(builder)
    # Since QueryBuilder is a dataclass, it should show class name and fields
    assert "MockQueryBuilder" in result
    assert "dialect=" in result
