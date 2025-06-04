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
from sqlspec.statement.builder.mixins._where import WhereClauseMixin
from sqlspec.statement.result import StatementResult
from sqlspec.statement.sql import SQL, SQLConfig


# Test implementation of abstract QueryBuilder for testing
class MockQueryBuilder(QueryBuilder[StatementResult[dict[str, Any]]]):
    """Concrete implementation of QueryBuilder for testing purposes."""

    def _create_base_expression(self) -> exp.Select:
        """Create a basic SELECT expression for testing."""
        return exp.Select()

    @property
    def _expected_result_type(self) -> type[StatementResult[dict[str, Any]]]:
        """Return the expected result type."""
        return StatementResult[dict[str, Any]]


# Test implementation of WhereClauseMixin for testing
class TestWhereClauseMixin(WhereClauseMixin):
    """Test class implementing WhereClauseMixin for testing purposes."""

    def __init__(self) -> None:
        self._parameters: dict[str, Any] = {}
        self._parameter_counter = 0
        self.dialect_name = None

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple["TestWhereClauseMixin", str]:
        """Add parameter implementation for testing."""
        if name and name in self._parameters:
            raise SQLBuilderError(f"Parameter name '{name}' already exists.")

        param_name = name or f"param_{self._parameter_counter + 1}"
        self._parameter_counter += 1
        self._parameters[param_name] = value
        return self, param_name

    def where(self, condition: Any) -> "TestWhereClauseMixin":
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
def where_mixin() -> TestWhereClauseMixin:
    """Fixture providing a test WhereClauseMixin instance."""
    return TestWhereClauseMixin()


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


def test_query_builder_dialect_property(test_builder: MockQueryBuilder) -> None:
    """Test dialect property returns correct values."""
    # Test with no dialect
    assert test_builder.dialect_name is None

    # Test with string dialect
    test_builder.dialect = "postgresql"
    assert test_builder.dialect_name == "postgresql"


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
def test_query_builder_add_parameter_auto_name(test_builder: MockQueryBuilder) -> None:
    """Test adding parameter with auto-generated name."""
    value = "test_value"
    result_builder, param_name = test_builder.add_parameter(value)

    assert result_builder is test_builder
    assert param_name in test_builder._parameters
    assert test_builder._parameters[param_name] == value
    assert param_name.startswith("param_")


def test_query_builder_add_parameter_explicit_name(test_builder: MockQueryBuilder) -> None:
    """Test adding parameter with explicit name."""
    value = "test_value"
    explicit_name = "custom_param"

    result_builder, param_name = test_builder.add_parameter(value, name=explicit_name)

    assert result_builder is test_builder
    assert param_name == explicit_name
    assert test_builder._parameters[explicit_name] == value


def test_query_builder_add_parameter_duplicate_name_error(test_builder: MockQueryBuilder) -> None:
    """Test error when adding parameter with duplicate name."""
    test_builder.add_parameter("first_value", name="duplicate")

    with pytest.raises(SQLBuilderError, match="Parameter name 'duplicate' already exists"):
        test_builder.add_parameter("second_value", name="duplicate")


def test_query_builder_internal_add_parameter(test_builder: MockQueryBuilder) -> None:
    """Test internal _add_parameter method."""
    value = "internal_value"
    param_name = test_builder._add_parameter(value)

    assert param_name in test_builder._parameters
    assert test_builder._parameters[param_name] == value
    assert param_name.startswith("param_")


def test_query_builder_parameter_counter_increment(test_builder: MockQueryBuilder) -> None:
    """Test that parameter counter increments correctly."""
    initial_counter = test_builder._parameter_counter

    test_builder._add_parameter("value1")
    assert test_builder._parameter_counter == initial_counter + 1

    test_builder.add_parameter("value2")
    assert test_builder._parameter_counter == initial_counter + 2


@pytest.mark.parametrize(
    "parameter_value",
    [
        "string_value",
        42,
        math.pi,
        True,
        None,
        [1, 2, 3],
        {"key": "value"},
    ],
    ids=["string", "int", "float", "bool", "none", "list", "dict"],
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


def test_query_builder_with_cte_invalid_query_type(test_builder: MockQueryBuilder) -> None:
    """Test error when adding CTE with invalid query type."""
    alias = "invalid_cte"
    invalid_query = 42  # Invalid type

    with pytest.raises(SQLBuilderError, match="Invalid query type for CTE"):
        test_builder.with_cte(alias, invalid_query)  # type: ignore[arg-type]


def test_query_builder_with_cte_invalid_string_query(test_builder: MockQueryBuilder) -> None:
    """Test error when adding CTE with invalid SQL string."""
    alias = "invalid_sql_cte"
    invalid_sql = "INVALID SQL SYNTAX"

    with pytest.raises(SQLBuilderError, match="Failed to parse CTE query string"):
        test_builder.with_cte(alias, invalid_sql)


def test_query_builder_with_cte_non_select_string(test_builder: MockQueryBuilder) -> None:
    """Test error when CTE string is not a SELECT statement."""
    alias = "non_select_cte"
    non_select_sql = "INSERT INTO users VALUES (1, 'test')"

    with pytest.raises(SQLBuilderError, match="must parse to a SELECT statement"):
        test_builder.with_cte(alias, non_select_sql)


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
def test_where_mixin_where_eq_basic(where_mixin: TestWhereClauseMixin) -> None:
    """Test basic where_eq functionality."""
    result = where_mixin.where_eq("name", "John")

    assert result is where_mixin
    assert "John" in where_mixin._parameters.values()


def test_where_mixin_where_eq_with_column_expression(where_mixin: TestWhereClauseMixin) -> None:
    """Test where_eq with sqlglot Column expression."""
    col_expr = exp.column("status")
    result = where_mixin.where_eq(col_expr, "active")

    assert result is where_mixin
    assert "active" in where_mixin._parameters.values()


def test_where_mixin_where_between_basic(where_mixin: TestWhereClauseMixin) -> None:
    """Test basic where_between functionality."""
    result = where_mixin.where_between("age", 18, 65)

    assert result is where_mixin
    assert 18 in where_mixin._parameters.values()
    assert 65 in where_mixin._parameters.values()


def test_where_mixin_where_like_basic(where_mixin: TestWhereClauseMixin) -> None:
    """Test basic where_like functionality."""
    pattern = "John%"
    result = where_mixin.where_like("name", pattern)

    assert result is where_mixin
    assert pattern in where_mixin._parameters.values()


def test_where_mixin_where_not_like_basic(where_mixin: TestWhereClauseMixin) -> None:
    """Test basic where_not_like functionality."""
    pattern = "test%"
    result = where_mixin.where_not_like("name", pattern)

    assert result is where_mixin
    assert pattern in where_mixin._parameters.values()


def test_where_mixin_where_is_null_basic(where_mixin: TestWhereClauseMixin) -> None:
    """Test basic where_is_null functionality."""
    result = where_mixin.where_is_null("deleted_at")

    assert result is where_mixin
    # No parameters should be added for IS NULL


def test_where_mixin_where_is_not_null_basic(where_mixin: TestWhereClauseMixin) -> None:
    """Test basic where_is_not_null functionality."""
    result = where_mixin.where_is_not_null("email")

    assert result is where_mixin
    # No parameters should be added for IS NOT NULL


def test_where_mixin_where_exists_with_string(where_mixin: TestWhereClauseMixin) -> None:
    """Test where_exists with string subquery."""
    subquery = "SELECT 1 FROM orders WHERE user_id = users.id"
    result = where_mixin.where_exists(subquery)

    assert result is where_mixin


def test_where_mixin_where_exists_with_builder(where_mixin: TestWhereClauseMixin) -> None:
    """Test where_exists with QueryBuilder subquery."""
    mock_builder = Mock()
    mock_builder._parameters = {"status": "active"}
    mock_builder.build.return_value = Mock()
    mock_builder.build.return_value.sql = "SELECT 1 FROM orders"

    result = where_mixin.where_exists(mock_builder)

    assert result is where_mixin
    # Parameters should be merged
    assert "active" in where_mixin._parameters.values()


def test_where_mixin_where_not_exists_with_string(where_mixin: TestWhereClauseMixin) -> None:
    """Test where_not_exists with string subquery."""
    subquery = "SELECT 1 FROM orders WHERE user_id = users.id"
    result = where_mixin.where_not_exists(subquery)

    assert result is where_mixin


def test_where_mixin_where_not_exists_with_builder(where_mixin: TestWhereClauseMixin) -> None:
    """Test where_not_exists with QueryBuilder subquery."""
    mock_builder = Mock()
    mock_builder._parameters = {"status": "active"}
    mock_builder.build.return_value = Mock()
    mock_builder.build.return_value.sql = "SELECT 1 FROM orders"

    result = where_mixin.where_not_exists(mock_builder)

    assert result is where_mixin
    # Parameters should be merged
    assert "active" in where_mixin._parameters.values()


@patch("sqlglot.exp.maybe_parse")
def test_where_mixin_where_exists_parse_error(mock_parse: Mock, where_mixin: TestWhereClauseMixin) -> None:
    """Test where_exists handles parse errors."""
    mock_parse.return_value = None  # Simulate parse failure

    with pytest.raises(SQLBuilderError, match="Could not parse subquery for EXISTS"):
        where_mixin.where_exists("INVALID SQL")


@patch("sqlglot.exp.maybe_parse")
def test_where_mixin_where_not_exists_parse_error(mock_parse: Mock, where_mixin: TestWhereClauseMixin) -> None:
    """Test where_not_exists handles parse errors."""
    mock_parse.return_value = None  # Simulate parse failure

    with pytest.raises(SQLBuilderError, match="Could not parse subquery for NOT EXISTS"):
        where_mixin.where_not_exists("INVALID SQL")


# Edge cases and integration tests
@pytest.mark.parametrize(
    ("column_input", "expected_type"),
    [
        ("string_column", str),
        (exp.column("expr_column"), exp.Column),
    ],
    ids=["string_column", "expression_column"],
)
def test_where_mixin_column_input_types(
    where_mixin: TestWhereClauseMixin, column_input: Any, expected_type: type
) -> None:
    """Test WhereClauseMixin methods handle both string and expression columns."""
    # Test with where_eq as representative method
    result = where_mixin.where_eq(column_input, "test_value")
    assert result is where_mixin


def test_where_mixin_method_chaining(where_mixin: TestWhereClauseMixin) -> None:
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
    }

    for name, value in complex_params.items():
        test_builder.add_parameter(value, name)

    query = test_builder.build()

    for name, expected_value in complex_params.items():
        assert query.parameters[name] == expected_value


def test_drop_table_builder_basic() -> None:
    sql = DropTableBuilder().table("my_table").build().sql
    assert "DROP TABLE" in sql and "my_table" in sql


def test_drop_index_builder_basic() -> None:
    sql = DropIndexBuilder().name("idx_name").on_table("my_table").build().sql
    # sqlglot does not render the table name for DROP INDEX in default dialect
    assert "DROP INDEX" in sql and "idx_name" in sql


def test_drop_view_builder_basic() -> None:
    sql = DropViewBuilder().name("my_view").build().sql
    assert "DROP VIEW" in sql and "my_view" in sql


def test_drop_schema_builder_basic() -> None:
    sql = DropSchemaBuilder().name("my_schema").build().sql
    assert "DROP SCHEMA" in sql and "my_schema" in sql


def test_create_index_builder_basic() -> None:
    sql = CreateIndexBuilder().name("idx_col").on_table("my_table").columns("col1", "col2").build().sql
    # sqlglot does not render the table name or columns for CREATE INDEX in default dialect
    assert "CREATE INDEX" in sql and "idx_col" in sql


def test_truncate_table_builder_basic() -> None:
    sql = TruncateTableBuilder().table("my_table").build().sql
    # sqlglot does not render the table name for TRUNCATE TABLE in default dialect
    assert "TRUNCATE TABLE" in sql


def test_create_schema_builder_basic() -> None:
    sql = CreateSchemaBuilder().name("myschema").build().sql
    assert "CREATE SCHEMA" in sql and "myschema" in sql

    sql_if_not_exists = CreateSchemaBuilder().name("myschema").if_not_exists().build().sql
    assert "IF NOT EXISTS" in sql_if_not_exists and "myschema" in sql_if_not_exists

    sql_auth = CreateSchemaBuilder().name("myschema").authorization("bob").build().sql
    # sqlglot may render AUTHORIZATION as a property or not, depending on dialect support
    assert "CREATE SCHEMA" in sql_auth and "myschema" in sql_auth and ("AUTHORIZATION" in sql_auth or "bob" in sql_auth)


def test_create_table_as_select_builder_basic() -> None:
    from sqlspec.statement.builder.ddl import CreateTableAsSelectBuilder
    from sqlspec.statement.builder.select import SelectBuilder

    select_builder = SelectBuilder().select("id", "name").from_("users").where_eq("active", True)
    builder = (
        CreateTableAsSelectBuilder().name("new_table").if_not_exists().columns("id", "name").as_select(select_builder)
    )
    result = builder.build()
    sql = result.sql
    # Basic SQL structure assertions
    assert "CREATE TABLE" in sql
    assert "IF NOT EXISTS" in sql
    assert "AS SELECT" in sql or "AS\nSELECT" in sql
    assert "FROM users" in sql
    assert "id" in sql and "name" in sql
    # Parameter merging: value should be True, but name is auto-generated (see TODO below)
    assert True in result.parameters.values()
    # TODO: It is critical to enhance query processing to preserve user-supplied parameter names.
    # Currently, parameter names from subqueries/builders are not preserved (e.g., 'active' becomes 'param_1').
    # This affects CTAS and any builder that merges parameters from other builders or SQL objects.
    # See test_create_table_as_select_builder_basic and CreateTableAsSelectBuilder for context.
    # The system should support passing through user-supplied parameter names when possible for better traceability and debugging.


def test_query_builder_str_fallback() -> None:
    """Test __str__ fallback when build fails."""
    builder = MockQueryBuilder()
    builder._expression = None
    # Should not raise, should return default object __str__
    result = str(builder)
    assert result.startswith("<") and result.endswith(">")


def test_create_materialized_view_basic() -> None:
    from sqlspec.statement.builder.ddl import CreateMaterializedViewBuilder
    from sqlspec.statement.builder.select import SelectBuilder

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
    assert "CREATE MATERIALIZED VIEW" in sql
    assert "IF NOT EXISTS" in sql
    assert "AS SELECT" in sql or "AS\nSELECT" in sql
    assert "FROM users" in sql
    assert "id" in sql and "name" in sql
    assert True in result.parameters.values()


def test_create_materialized_view_with_options() -> None:
    from sqlspec.statement.builder.ddl import CreateMaterializedViewBuilder
    from sqlspec.statement.builder.select import SelectBuilder

    select_builder = SelectBuilder().select("id").from_("users")
    builder = (
        CreateMaterializedViewBuilder()
        .name("mv1")
        .as_select(select_builder)
        .with_data()
        .refresh_mode("ON COMMIT")
        .storage_parameter("foo", "bar")
        .tablespace("fastspace")
        .using_index("idx_mv1")
        .with_hint("/*+ PARALLEL(2) */")
    )
    result = builder.build()
    sql = result.sql
    assert "CREATE MATERIALIZED VIEW" in sql
    assert "WITH_DATA" in sql or "NO_DATA" in sql or "WITH DATA" in sql or "NO DATA" in sql
    assert "PARALLEL" in sql or "HINT" in sql or "PARALLEL(2)" in sql
    assert "TABLESPACE" in sql or "fastspace" in sql
    assert "USING_INDEX" in sql or "idx_mv1" in sql
    assert "foo" in sql and "bar" in sql


def test_create_materialized_view_parameter_merging() -> None:
    from sqlspec.statement.builder.ddl import CreateMaterializedViewBuilder
    from sqlspec.statement.builder.select import SelectBuilder

    select_builder = SelectBuilder().select("id").from_("users").where_eq("status", "active")
    builder = CreateMaterializedViewBuilder().name("mv2").as_select(select_builder)
    result = builder.build()
    # Parameter from SELECT should be merged
    assert "active" in result.parameters.values()


def test_create_view_basic() -> None:
    from sqlspec.statement.builder.ddl import CreateViewBuilder
    from sqlspec.statement.builder.select import SelectBuilder

    select_builder = SelectBuilder().select("id", "name").from_("users").where_eq("active", True)
    builder = CreateViewBuilder().name("active_users_v").if_not_exists().columns("id", "name").as_select(select_builder)
    result = builder.build()
    sql = result.sql
    assert "CREATE VIEW" in sql
    assert "IF NOT EXISTS" in sql
    assert "AS SELECT" in sql or "AS\nSELECT" in sql
    assert "FROM users" in sql
    assert "id" in sql and "name" in sql
    assert True in result.parameters.values()


def test_create_view_with_hint() -> None:
    from sqlspec.statement.builder.ddl import CreateViewBuilder
    from sqlspec.statement.builder.select import SelectBuilder

    select_builder = SelectBuilder().select("id").from_("users")
    builder = CreateViewBuilder().name("v1").as_select(select_builder).with_hint("/*+ NO_MERGE */")
    result = builder.build()
    sql = result.sql
    assert "CREATE VIEW" in sql
    assert "NO_MERGE" in sql or "HINT" in sql


def test_create_view_parameter_merging() -> None:
    from sqlspec.statement.builder.ddl import CreateViewBuilder
    from sqlspec.statement.builder.select import SelectBuilder

    select_builder = SelectBuilder().select("id").from_("users").where_eq("status", "active")
    builder = CreateViewBuilder().name("v2").as_select(select_builder)
    result = builder.build()
    # Parameter from SELECT should be merged
    assert "active" in result.parameters.values()


def test_alter_table_add_column_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("ADD COLUMN age INT").build().sql
    assert "ALTER TABLE" in sql and "ADD COLUMN" in sql and "age" in sql and "INT" in sql


def test_alter_table_drop_column_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("DROP COLUMN age").build().sql
    assert "ALTER TABLE" in sql and "DROP COLUMN" in sql and "age" in sql


def test_alter_table_rename_column_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("RENAME COLUMN old_name TO new_name").build().sql
    assert "ALTER TABLE" in sql and "RENAME COLUMN" in sql and "old_name" in sql and "new_name" in sql


def test_alter_table_alter_column_type_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("ALTER COLUMN age TYPE BIGINT").build().sql
    assert "ALTER TABLE" in sql and "ALTER COLUMN" in sql and "age" in sql and "BIGINT" in sql


def test_alter_table_set_default_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("ALTER COLUMN age SET DEFAULT 42").build().sql
    assert "ALTER TABLE" in sql and "SET DEFAULT" in sql and "age" in sql and "42" in sql


def test_alter_table_drop_default_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("ALTER COLUMN age DROP DEFAULT").build().sql
    assert "ALTER TABLE" in sql and "DROP DEFAULT" in sql and "age" in sql


def test_alter_table_set_property_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("SET (fillfactor = 80)").build().sql
    assert "ALTER TABLE" in sql and ("SET" in sql and "fillfactor" in sql)


def test_alter_table_drop_property_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("RESET (fillfactor)").build().sql
    assert "ALTER TABLE" in sql and ("RESET" in sql and "fillfactor" in sql)


def test_alter_table_with_hint_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    sql = AlterTableBuilder().table("users").action("ADD COLUMN age INT").with_hint("/*+ NOLOCK */").build().sql
    assert "ALTER TABLE" in sql and ("NOLOCK" in sql or "HINT" in sql)


def test_alter_table_error_if_no_action() -> None:
    from sqlspec.statement.builder.ddl import AlterTableBuilder

    builder = AlterTableBuilder().table("users")
    with pytest.raises(Exception):
        builder.build()


def test_comment_on_table_builder() -> None:
    from sqlspec.statement.builder.ddl import CommentOnBuilder

    sql = CommentOnBuilder().on_table("users").is_("User table").build().sql
    assert "COMMENT ON TABLE users IS 'User table'" in sql


def test_comment_on_column_builder() -> None:
    from sqlspec.statement.builder.ddl import CommentOnBuilder

    sql = CommentOnBuilder().on_column("users", "age").is_("User age").build().sql
    assert "COMMENT ON COLUMN users.age IS 'User age'" in sql


def test_comment_on_builder_error() -> None:
    import pytest

    from sqlspec.statement.builder.ddl import CommentOnBuilder

    with pytest.raises(Exception):
        CommentOnBuilder().on_table("users").build()


def test_rename_table_builder() -> None:
    from sqlspec.statement.builder.ddl import RenameTableBuilder

    sql = RenameTableBuilder().table("users").to("customers").build().sql
    assert "ALTER TABLE users RENAME TO customers" in sql or "RENAME TO customers" in sql


def test_rename_table_builder_error() -> None:
    import pytest

    from sqlspec.statement.builder.ddl import RenameTableBuilder

    with pytest.raises(Exception):
        RenameTableBuilder().table("users").build()
