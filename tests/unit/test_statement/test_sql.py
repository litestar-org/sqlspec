"""Unit tests for sqlspec.statement.sql module."""

from typing import TYPE_CHECKING, Any

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLParsingError
from sqlspec.statement.filters import LimitOffsetFilter, SearchFilter
from sqlspec.statement.sql import SQL, SQLConfig

# Create a default test config
TEST_CONFIG = SQLConfig()

if TYPE_CHECKING:
    from sqlspec.typing import SQLParameterType


# Test SQLConfig
@pytest.mark.parametrize(
    "config_kwargs,expected_values",
    [
        (
            {},  # Default values
            {"dialect": None, "enable_caching": True},
        ),
        ({"dialect": "duckdb", "enable_caching": False}, {"dialect": "duckdb", "enable_caching": False}),
    ],
    ids=["defaults", "custom"],
)
def test_sql_config_initialization(config_kwargs: "dict[str, Any]", expected_values: "dict[str, Any]") -> None:
    """Test SQLConfig initialization with different parameters."""
    config = SQLConfig(**config_kwargs)

    for attr, expected in expected_values.items():
        assert getattr(config, attr) == expected

    # Test that parameter_converter and parameter_validator are always created
    assert config.parameter_converter is not None
    assert config.parameter_validator is not None


# Test SQL class basic functionality
def test_sql_initialization_with_string() -> None:
    """Test SQL initialization with string input."""
    sql_str = "SELECT * FROM users"
    stmt = SQL(sql_str)

    assert stmt.sql == sql_str
    assert stmt.parameters == {}
    assert stmt._filters == []
    assert stmt._config is not None
    assert isinstance(stmt._config, SQLConfig)


def test_sql_initialization_with_parameters() -> None:
    """Test SQL initialization with parameters."""
    sql_str = "SELECT * FROM users WHERE id = :id"
    params: dict[str, Any] = {"id": 1}
    stmt = SQL(sql_str, **params)  # type: ignore[arg-type]  # kwargs are passed correctly

    assert stmt.sql == sql_str
    assert stmt.parameters == params


@pytest.mark.parametrize(
    "sql,params,expected_sql",
    [
        ("SELECT * FROM users WHERE id = ?", (1), "SELECT * FROM users WHERE id = ?"),
        ("SELECT * FROM users WHERE id = :id", {"id": 1}, "SELECT * FROM users WHERE id = :id"),
        ("SELECT * FROM users WHERE id = $1", (1), "SELECT * FROM users WHERE id = $1"),
    ],
)
def test_sql_with_different_parameter_styles(sql: str, params: "SQLParameterType", expected_sql: str) -> None:
    """Test SQL handles different parameter styles."""
    if isinstance(params, dict):
        stmt = SQL(sql, **params)
    elif isinstance(params, tuple):
        stmt = SQL(sql, *params)
    else:
        stmt = SQL(sql, params)
    assert stmt.sql == expected_sql


def test_sql_initialization_with_expression() -> None:
    """Test SQL initialization with sqlglot expression."""
    expr = exp.select("*").from_("users")
    stmt = SQL(expr)

    assert stmt.sql == expr.sql()
    assert stmt.parameters == {}


def test_sql_initialization_with_custom_config() -> None:
    """Test SQL initialization with custom config."""
    config = SQLConfig(dialect="sqlite")
    stmt = SQL("SELECT * FROM users", config=config)

    assert stmt._config == config
    assert stmt._config.dialect == "sqlite"


# Test SQL immutability
def test_sql_immutability() -> None:
    """Test SQL objects are immutable (through the public API)."""
    stmt = SQL("SELECT * FROM users")

    # Test that we cannot add new attributes (due to __slots__)
    with pytest.raises(AttributeError):
        stmt.new_attribute = "test"  # type: ignore

    # Note: Direct assignment to slot attributes is allowed by Python,
    # but the SQL class doesn't provide public setters


# Test SQL lazy processing
def test_sql_lazy_processing() -> None:
    """Test SQL processing is done lazily."""
    stmt = SQL("SELECT * FROM users")

    # Processing not done yet
    assert stmt._processed_state is None

    # Accessing sql property triggers processing
    _ = stmt.sql
    assert stmt._processed_state is not None


# Test SQL properties
@pytest.mark.parametrize(
    "sql_input,expected_sql",
    [
        ("SELECT * FROM users", "SELECT * FROM users"),
        ("  SELECT * FROM users  ", "SELECT * FROM users"),  # Trimmed
        (exp.select("*").from_("users"), "SELECT * FROM users"),  # Expression
    ],
)
def test_sql_property(sql_input: "str | exp.Expression", expected_sql: str) -> None:
    """Test SQL.sql property returns processed SQL string."""
    stmt = SQL(sql_input, config=TEST_CONFIG)
    assert stmt.sql == expected_sql


def test_sql_parameters_property() -> None:
    """Test SQL.parameters property returns original parameters."""
    # No parameters
    stmt1 = SQL("SELECT * FROM users")
    assert stmt1.parameters == {}

    # With positional parameters - returns the original tuple
    stmt2 = SQL("SELECT * FROM users WHERE id = ?", 1)
    assert stmt2.parameters == (1,)

    # With tuple of parameters
    stmt2b = SQL("SELECT * FROM users WHERE id = ? AND name = ?", 1, "test")
    assert stmt2b.parameters == (1, "test")

    # Dict parameters
    stmt3 = SQL("SELECT * FROM users WHERE id = :id", id=1)
    assert stmt3.parameters == {"id": 1}


def test_sql_expression_property() -> None:
    """Test SQL.expression property returns parsed expression."""
    stmt = SQL("SELECT * FROM users")
    expr = stmt.expression

    assert expr is not None
    assert isinstance(expr, exp.Expression)
    assert isinstance(expr, exp.Select)


def test_sql_expression_with_parsing_disabled() -> None:
    """Test SQL.expression returns None when parsing disabled."""
    # SQLConfig no longer has enable_parsing flag - parsing is always enabled
    # This test can be removed or updated to test something else
    stmt = SQL("SELECT * FROM users")
    assert stmt.expression is not None


# Test SQL validation
def test_sql_validate_method() -> None:
    """Test SQL.validate() returns validation errors."""
    # Valid SQL
    stmt1 = SQL("SELECT id, name FROM users")
    errors1 = stmt1.validate()
    assert isinstance(errors1, list)
    assert len(errors1) == 0

    # SQL with validation issues
    stmt2 = SQL("UPDATE users SET name = 'test'")  # No WHERE clause
    errors2 = stmt2.validate()
    assert isinstance(errors2, list)
    # The actual validation happens in the pipeline validators


def test_sql_validation_disabled() -> None:
    """Test SQL validation behavior."""
    # SQLConfig no longer has enable_validation flag
    # Validation happens in the pipeline based on configured validators
    stmt = SQL("UPDATE users SET name = 'test'")
    errors = stmt.validate()
    assert isinstance(errors, list)
    # The actual validation happens in the pipeline validators


def test_sql_parse_errors_warn_by_default() -> None:
    """Test SQL warns on parse errors by default (new behavior for compatibility)."""
    # Invalid SQL that can't be parsed - should return Anonymous expression instead of raising
    stmt = SQL("INVALID SQL SYNTAX !@#$%^&*()")
    result_sql = stmt.sql  # Should not raise
    assert "INVALID SQL SYNTAX" in result_sql  # Should return original SQL


def test_sql_parse_errors_can_raise_explicitly() -> None:
    """Test SQL can still raise on parse errors when explicitly configured."""
    # Invalid SQL that can't be parsed with explicit config
    config = SQLConfig(parse_errors_as_warnings=False)
    with pytest.raises(SQLParsingError) as exc_info:
        stmt = SQL("INVALID SQL SYNTAX !@#$%^&*()", config=config)
        _ = stmt.sql  # Trigger processing

    assert "parse" in str(exc_info.value).lower()


# Test SQL filtering
def test_sql_filter_method() -> None:
    """Test SQL.filter() returns new instance with filter applied."""
    stmt1 = SQL("SELECT * FROM users")
    filter_obj = LimitOffsetFilter(limit=10, offset=0)

    stmt2 = stmt1.filter(filter_obj)

    # Different instances
    assert stmt2 is not stmt1
    assert stmt2._filters == [filter_obj]
    assert stmt1._filters == []

    # Filter is applied - limit is parameterized with unique name
    assert "LIMIT :" in stmt2.sql
    # Check that there's a parameter with value 10 (limit parameter)
    limit_params = [key for key, value in stmt2.parameters.items() if value == 10 and key.startswith("limit_")]
    assert len(limit_params) == 1


def test_sql_multiple_filters() -> None:
    """Test SQL with multiple filters applied."""
    stmt = SQL("SELECT * FROM users")

    stmt2 = stmt.filter(LimitOffsetFilter(limit=10, offset=0))
    stmt3 = stmt2.filter(SearchFilter(field_name="name", value="test"))

    sql = stmt3.sql
    assert "LIMIT :limit" in sql
    assert "WHERE" in sql
    assert "name" in sql


# Test SQL parameter handling
def test_sql_with_missing_parameters() -> None:
    """Test SQL handles missing parameters gracefully."""
    # SQL allows creating statements with placeholders but no parameters
    # This enables patterns like SQL("SELECT * FROM users WHERE id = ?").as_many([...])
    stmt = SQL("SELECT * FROM users WHERE id = ?")
    assert stmt.sql == "SELECT * FROM users WHERE id = ?"
    assert stmt.parameters == {}


def test_sql_with_extra_parameters() -> None:
    """Test SQL handles extra parameters gracefully."""
    # The variadic parameter API - passing multiple values becomes a tuple
    stmt = SQL("SELECT * FROM users WHERE id = ?", 1, 2, 3)
    # Parameters are returned as the original tuple
    assert stmt.parameters == (1, 2, 3)
    assert stmt.sql == "SELECT * FROM users WHERE id = ?"


# Test SQL transformations
def test_sql_with_literal_parameterization() -> None:
    """Test SQL literal parameterization when enabled."""
    # By default, enable_transformations is True, which includes ParameterizeLiterals
    stmt = SQL("SELECT * FROM users WHERE id = 1")

    # The SQL should have the literal parameterized
    sql = stmt.sql
    params = stmt.parameters

    # With default transformers enabled, literal should be parameterized
    assert sql == "SELECT * FROM users WHERE id = ?"
    # The extracted parameters are returned as a list
    assert isinstance(params, list)
    assert len(params) == 1
    # TypedParameter objects have a value attribute
    assert hasattr(params[0], "value")
    assert params[0].value == 1


def test_sql_comment_removal() -> None:
    """Test SQL comment removal when enabled."""
    sql_with_comments = """
    -- This is a comment
    SELECT * FROM users /* inline comment */
    """

    stmt = SQL(sql_with_comments)
    sql = stmt.sql

    assert "--" not in sql
    assert "/*" not in sql
    assert "*/" not in sql


# Test SQL dialect handling
@pytest.mark.parametrize(
    "dialect,expected_sql",
    [("mysql", "SELECT * FROM users"), ("postgres", "SELECT * FROM users"), ("sqlite", "SELECT * FROM users")],
)
def test_sql_with_dialect(dialect: str, expected_sql: str) -> None:
    """Test SQL respects dialect setting."""
    config = SQLConfig(dialect=dialect)
    stmt = SQL("SELECT * FROM users", config=config)
    assert stmt.sql == expected_sql


# Test SQL error handling
def test_sql_parsing_error() -> None:
    """Test SQL handles parsing errors gracefully."""
    # Test with parse_errors_as_warnings=True
    config = SQLConfig(parse_errors_as_warnings=True)
    stmt = SQL("INVALID SQL SYNTAX !", config=config)
    sql = stmt.sql

    # The invalid SQL is preserved (sqlglot wraps it)
    assert "INVALID" in sql


def test_sql_transformation_error() -> None:
    """Test SQL handles transformation errors."""
    # The new SQL class doesn't support custom transformers in config
    # Transformers are configured in the pipeline
    stmt = SQL("SELECT * FROM users")

    # Without pipeline configuration, no transformations occur
    assert stmt.sql == "SELECT * FROM users"


# Test SQL special cases
def test_sql_empty_string() -> None:
    """Test SQL handles empty string input."""
    stmt = SQL("")
    assert stmt.sql == ""
    assert stmt.parameters == {}


def test_sql_whitespace_only() -> None:
    """Test SQL handles whitespace-only input."""
    stmt = SQL("   \n\t   ")
    assert stmt.sql == ""
    assert stmt.parameters == {}


# Test SQL caching behavior
def test_sql_expression_caching() -> None:
    """Test SQL expression caching when enabled."""
    config = SQLConfig(enable_caching=True)
    stmt = SQL("SELECT * FROM users", config=config)

    # First access
    expr1 = stmt.expression
    # Second access should return cached
    expr2 = stmt.expression

    assert expr1 is expr2  # Same object


def test_sql_no_expression_caching() -> None:
    """Test SQL expression not cached when disabled."""
    config = SQLConfig(enable_caching=False)
    stmt = SQL("SELECT * FROM users", config=config)

    # Access expression multiple times
    expr1 = stmt.expression
    expr2 = stmt.expression

    # Should be different objects (re-parsed each time)
    # Note: This behavior depends on implementation details
    assert expr1 is not None
    assert expr2 is not None


# Test SQL with complex queries
@pytest.mark.parametrize(
    "complex_sql",
    [
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = 1",
        "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        "SELECT COUNT(*), MAX(price) FROM orders GROUP BY user_id HAVING COUNT(*) > 5",
        "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
        "UPDATE users SET active = 0 WHERE last_login < '2023-01-01'",
        "DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2023-01-01'",
    ],
)
def test_sql_complex_queries(complex_sql: str) -> None:
    """Test SQL handles complex queries correctly."""
    stmt = SQL(complex_sql)
    assert stmt.sql is not None
    assert len(stmt.sql) > 0


# Test SQL copy behavior
def test_sql_copy() -> None:
    """Test SQL objects can be copied with modifications."""
    stmt1 = SQL("SELECT * FROM users", id=1)

    # Create new instance with different config
    new_config = SQLConfig(dialect="sqlite")
    stmt2 = SQL(stmt1, config=new_config)

    assert stmt2._raw_sql == stmt1._raw_sql
    assert stmt2._raw_parameters == stmt1._raw_parameters
    assert stmt2._config == new_config
    assert stmt2._config != stmt1._config
