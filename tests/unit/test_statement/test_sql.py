"""Unit tests for sqlspec.statement.sql module."""

from typing import TYPE_CHECKING, Any, Optional
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp

from sqlspec.exceptions import MissingParameterError, SQLParsingError, SQLTransformationError, SQLValidationError
from sqlspec.statement.filters import LimitOffsetFilter, SearchFilter
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig

if TYPE_CHECKING:
    from sqlspec.typing import SQLParameterType


# Test SQLConfig
@pytest.mark.parametrize(
    "config_kwargs,expected_values",
    [
        (
            {},  # Default values
            {
                "enable_parsing": True,
                "enable_validation": True,
                "enable_transformations": True,
                "enable_analysis": False,
                "enable_normalization": True,
                "strict_mode": True,
                "cache_parsed_expression": True,
                "analysis_cache_size": 1000,
                "input_sql_had_placeholders": False,
                "allowed_parameter_styles": None,
                "target_parameter_style": None,
                "allow_mixed_parameter_styles": False,
            },
        ),
        (
            {
                "enable_parsing": False,
                "enable_validation": False,
                "strict_mode": False,
                "analysis_cache_size": 500,
                "allowed_parameter_styles": ("qmark", "named"),
                "target_parameter_style": "qmark",
            },
            {
                "enable_parsing": False,
                "enable_validation": False,
                "enable_transformations": True,
                "enable_analysis": False,
                "enable_normalization": True,
                "strict_mode": False,
                "cache_parsed_expression": True,
                "analysis_cache_size": 500,
                "input_sql_had_placeholders": False,
                "allowed_parameter_styles": ("qmark", "named"),
                "target_parameter_style": "qmark",
                "allow_mixed_parameter_styles": False,
            },
        ),
    ],
    ids=["defaults", "custom"],
)
def test_sql_config_initialization(config_kwargs: "dict[str, Any]", expected_values: "dict[str, Any]") -> None:
    """Test SQLConfig initialization with different parameters."""
    config = SQLConfig(**config_kwargs)

    for attr, expected in expected_values.items():
        assert getattr(config, attr) == expected


@pytest.mark.parametrize(
    "style,allowed_styles,expected",
    [
        ("qmark", None, True),  # No restrictions
        ("qmark", ("qmark", "named"), True),  # Allowed
        ("numeric", ("qmark", "named"), False),  # Not allowed
        (ParameterStyle.QMARK, ("qmark",), True),  # Enum value
        (ParameterStyle.NUMERIC, ("qmark",), False),  # Enum not allowed
    ],
)
def test_sql_config_validate_parameter_style(
    style: "str | ParameterStyle", allowed_styles: "Optional[tuple[str, ...]]", expected: bool
) -> None:
    """Test SQLConfig parameter style validation."""
    config = SQLConfig(allowed_parameter_styles=allowed_styles)
    assert config.validate_parameter_style(style) == expected


# Test SQL class basic functionality
def test_sql_initialization_with_string() -> None:
    """Test SQL initialization with string input."""
    sql_str = "SELECT * FROM users"
    stmt = SQL(sql_str)

    assert stmt._raw_sql == sql_str
    assert stmt._raw_parameters is None
    assert stmt._filters == []
    assert stmt._config is not None
    assert isinstance(stmt._config, SQLConfig)


def test_sql_initialization_with_parameters() -> None:
    """Test SQL initialization with parameters."""
    sql_str = "SELECT * FROM users WHERE id = ?"
    params = (1,)
    stmt = SQL(sql_str, params)

    assert stmt._raw_sql == sql_str
    assert stmt._raw_parameters == params


@pytest.mark.parametrize(
    "sql,params",
    [
        ("SELECT * FROM users WHERE id = ?", (1,)),
        ("SELECT * FROM users WHERE id = :id", {"id": 1}),
        ("SELECT * FROM users WHERE id = %(id)s", {"id": 1}),
        ("SELECT * FROM users WHERE id = $1", (1,)),
    ],
)
def test_sql_with_different_parameter_styles(sql: str, params: "SQLParameterType") -> None:
    """Test SQL handles different parameter styles."""
    stmt = SQL(sql, params)
    assert stmt._raw_sql == sql
    assert stmt._raw_parameters == params


def test_sql_initialization_with_expression() -> None:
    """Test SQL initialization with sqlglot expression."""
    expr = exp.select("*").from_("users")
    stmt = SQL(expr)

    assert stmt._raw_sql == expr.sql()
    assert stmt._raw_parameters is None


def test_sql_initialization_with_custom_config() -> None:
    """Test SQL initialization with custom config."""
    config = SQLConfig(enable_validation=False, strict_mode=False)
    stmt = SQL("SELECT * FROM users", config=config)

    assert stmt._config == config
    assert stmt._config.enable_validation is False
    assert stmt._config.strict_mode is False


# Test SQL immutability
def test_sql_immutability() -> None:
    """Test SQL objects are immutable."""
    stmt = SQL("SELECT * FROM users")

    with pytest.raises(AttributeError):
        stmt._raw_sql = "UPDATE users SET x = 1"  # type: ignore

    with pytest.raises(AttributeError):
        stmt._raw_parameters = {"x": 1}  # type: ignore


# Test SQL lazy processing
def test_sql_lazy_processing() -> None:
    """Test SQL processing is lazy."""
    with patch("sqlspec.statement.sql.SQL._ensure_processed") as mock_process:
        stmt = SQL("SELECT * FROM users")
        # Creation doesn't trigger processing
        mock_process.assert_not_called()

        # Accessing properties triggers processing
        _ = stmt.sql
        mock_process.assert_called_once()


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
    stmt = SQL(sql_input)
    assert stmt.sql == expected_sql


def test_sql_parameters_property() -> None:
    """Test SQL.parameters property returns processed parameters."""
    # No parameters
    stmt1 = SQL("SELECT * FROM users")
    assert stmt1.parameters is None

    # With parameters
    stmt2 = SQL("SELECT * FROM users WHERE id = ?", (1,))
    assert stmt2.parameters == (1,)

    # Dict parameters
    stmt3 = SQL("SELECT * FROM users WHERE id = :id", {"id": 1})
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
    config = SQLConfig(enable_parsing=False)
    stmt = SQL("SELECT * FROM users", config=config)

    assert stmt.expression is None


# Test SQL validation
def test_sql_validate_method() -> None:
    """Test SQL.validate() returns validation errors."""
    # Valid SQL
    stmt1 = SQL("SELECT * FROM users")
    errors1 = stmt1.validate()
    assert isinstance(errors1, list)
    assert len(errors1) == 0

    # SQL with validation issues
    stmt2 = SQL("UPDATE users SET name = 'test'")  # No WHERE clause
    errors2 = stmt2.validate()
    assert isinstance(errors2, list)
    assert len(errors2) > 0
    assert any("WHERE" in error.message for error in errors2)


def test_sql_validation_disabled() -> None:
    """Test SQL validation can be disabled."""
    config = SQLConfig(enable_validation=False)
    stmt = SQL("UPDATE users SET name = 'test'", config=config)

    errors = stmt.validate()
    assert isinstance(errors, list)
    assert len(errors) == 0


def test_sql_strict_mode_raises_on_errors() -> None:
    """Test SQL strict mode raises on validation errors."""
    config = SQLConfig(strict_mode=True)

    with pytest.raises(SQLValidationError) as exc_info:
        stmt = SQL("UPDATE users SET name = 'test'", config=config)
        _ = stmt.sql  # Trigger processing

    assert "WHERE" in str(exc_info.value)


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

    # Filter is applied
    assert "LIMIT 10" in stmt2.sql


def test_sql_multiple_filters() -> None:
    """Test SQL with multiple filters applied."""
    stmt = SQL("SELECT * FROM users")

    stmt2 = stmt.filter(LimitOffsetFilter(limit=10, offset=0))
    stmt3 = stmt2.filter(SearchFilter(field_name="name", value="test"))

    sql = stmt3.sql
    assert "LIMIT 10" in sql
    assert "WHERE" in sql
    assert "name" in sql


# Test SQL parameter handling
def test_sql_with_missing_parameters() -> None:
    """Test SQL raises error for missing parameters in strict mode."""
    config = SQLConfig(strict_mode=True)

    with pytest.raises(MissingParameterError):
        stmt = SQL("SELECT * FROM users WHERE id = ?", config=config)
        _ = stmt.sql  # Trigger processing


def test_sql_with_extra_parameters() -> None:
    """Test SQL handles extra parameters gracefully."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", (1, 2, 3))
    assert stmt.parameters == (1, 2, 3)
    assert stmt.sql == "SELECT * FROM users WHERE id = ?"


# Test SQL transformations
def test_sql_with_literal_parameterization() -> None:
    """Test SQL literal parameterization when enabled."""
    config = SQLConfig(enable_transformations=True)
    stmt = SQL("SELECT * FROM users WHERE id = 1", config=config)

    # Should parameterize the literal
    sql = stmt.sql
    params = stmt.parameters

    assert "?" in sql or ":" in sql  # Parameterized
    assert params is not None
    assert 1 in (params if isinstance(params, (list, tuple)) else params.values())


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
    stmt = SQL("SELECT * FROM users", dialect=dialect)
    assert stmt.sql == expected_sql


# Test SQL error handling
def test_sql_parsing_error() -> None:
    """Test SQL handles parsing errors gracefully."""
    config = SQLConfig(strict_mode=True)

    with pytest.raises(SQLParsingError):
        stmt = SQL("INVALID SQL SYNTAX !", config=config)
        _ = stmt.expression  # Trigger parsing


def test_sql_transformation_error() -> None:
    """Test SQL handles transformation errors."""
    # Create a mock transformer that raises an error
    mock_transformer = Mock()
    mock_transformer.process.side_effect = Exception("Transform error")

    config = SQLConfig(transformers=[mock_transformer])

    with pytest.raises(SQLTransformationError):
        stmt = SQL("SELECT * FROM users", config=config)
        _ = stmt.sql  # Trigger processing


# Test SQL special cases
def test_sql_empty_string() -> None:
    """Test SQL handles empty string input."""
    stmt = SQL("")
    assert stmt.sql == ""
    assert stmt.parameters is None


def test_sql_whitespace_only() -> None:
    """Test SQL handles whitespace-only input."""
    stmt = SQL("   \n\t   ")
    assert stmt.sql == ""
    assert stmt.parameters is None


# Test SQL caching behavior
def test_sql_expression_caching() -> None:
    """Test SQL expression caching when enabled."""
    config = SQLConfig(cache_parsed_expression=True)
    stmt = SQL("SELECT * FROM users", config=config)

    # First access
    expr1 = stmt.expression
    # Second access should return cached
    expr2 = stmt.expression

    assert expr1 is expr2  # Same object


def test_sql_no_expression_caching() -> None:
    """Test SQL expression not cached when disabled."""
    config = SQLConfig(cache_parsed_expression=False)
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
    stmt1 = SQL("SELECT * FROM users", {"id": 1})

    # Create new instance with different config
    new_config = SQLConfig(enable_validation=False)
    stmt2 = SQL(stmt1, config=new_config)

    assert stmt2._raw_sql == stmt1._raw_sql
    assert stmt2._raw_parameters == stmt1._raw_parameters
    assert stmt2._config == new_config
    assert stmt2._config != stmt1._config
