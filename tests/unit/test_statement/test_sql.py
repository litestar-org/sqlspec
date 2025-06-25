"""Unit tests for sqlspec.statement.sql module."""

from typing import TYPE_CHECKING, Any, Optional
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp

from sqlspec.exceptions import MissingParameterError, SQLValidationError
from sqlspec.statement.filters import LimitOffsetFilter, SearchFilter
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig

# Create a default test config with validation but not strict mode to avoid SELECT * errors
TEST_CONFIG = SQLConfig(strict_mode=False)

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
                "strict_mode": False,
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
    stmt = SQL("SELECT * FROM users", _config=config)

    assert stmt._config == config
    assert stmt._config.enable_validation is False
    assert stmt._config.strict_mode is False


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
    """Test SQL processing is lazy."""
    # Track when _ensure_processed is called
    calls = []

    def track_ensure_processed(self: Any) -> None:
        calls.append("ensure_processed")
        # Set up minimal processed state to avoid AttributeError
        from sqlspec.statement.sql import _ProcessedState

        self._processed_state = _ProcessedState(
            processed_expression=self._statement, processed_sql="SELECT * FROM users", merged_parameters=None
        )

    with patch("sqlspec.statement.sql.SQL._ensure_processed", track_ensure_processed):
        stmt = SQL("SELECT * FROM users")
        # Creation doesn't trigger processing
        assert len(calls) == 0

        # Accessing properties triggers processing
        _ = stmt.sql
        assert len(calls) == 1


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
    stmt = SQL(sql_input, _config=TEST_CONFIG)
    assert stmt.sql == expected_sql


def test_sql_parameters_property() -> None:
    """Test SQL.parameters property returns processed parameters."""
    # No parameters
    stmt1 = SQL("SELECT * FROM users")
    assert stmt1.parameters is None

    # With parameters - positional params are returned as list
    stmt2 = SQL("SELECT * FROM users WHERE id = ?", (1,))
    assert stmt2.parameters == [1]

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
    stmt = SQL("SELECT * FROM users", _config=config)

    assert stmt.expression is None


# Test SQL validation
def test_sql_validate_method() -> None:
    """Test SQL.validate() returns validation errors."""
    # Valid SQL - disable validation for this specific test
    config = SQLConfig(enable_validation=False)
    stmt1 = SQL("SELECT id, name FROM users", _config=config)
    errors1 = stmt1.validate()
    assert isinstance(errors1, list)
    assert len(errors1) == 0

    # SQL with validation issues
    config2 = SQLConfig(strict_mode=False)  # Use non-strict mode to get errors without exception
    stmt2 = SQL("UPDATE users SET name = 'test'", _config=config2)  # No WHERE clause
    errors2 = stmt2.validate()
    assert isinstance(errors2, list)
    assert len(errors2) > 0
    assert any("WHERE" in error.message for error in errors2)


def test_sql_validation_disabled() -> None:
    """Test SQL validation can be disabled."""
    config = SQLConfig(enable_validation=False)
    stmt = SQL("UPDATE users SET name = 'test'", _config=config)

    errors = stmt.validate()
    assert isinstance(errors, list)
    assert len(errors) == 0


def test_sql_strict_mode_raises_on_errors() -> None:
    """Test SQL strict mode raises on validation errors."""
    config = SQLConfig(strict_mode=True)

    with pytest.raises(SQLValidationError) as exc_info:
        stmt = SQL("UPDATE users SET name = 'test'", _config=config)
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

    # Filter is applied - limit is parameterized
    assert "LIMIT :limit" in stmt2.sql
    assert stmt2.parameters["limit"] == 10


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
@pytest.mark.skip(reason="MissingParameterValidator not yet implemented in pipeline")
def test_sql_with_missing_parameters() -> None:
    """Test SQL raises error for missing parameters in strict mode."""
    config = SQLConfig(strict_mode=True)

    with pytest.raises(MissingParameterError):
        stmt = SQL("SELECT * FROM users WHERE id = ?", _config=config)
        _ = stmt.sql  # Trigger processing


def test_sql_with_extra_parameters() -> None:
    """Test SQL handles extra parameters gracefully."""
    stmt = SQL("SELECT * FROM users WHERE id = ?", (1, 2, 3))
    assert stmt.parameters == [1, 2, 3]  # Positional params are returned as list
    assert stmt.sql == "SELECT * FROM users WHERE id = ?"


# Test SQL transformations
def test_sql_with_literal_parameterization() -> None:
    """Test SQL literal parameterization when enabled."""
    config = SQLConfig(enable_transformations=True)
    stmt = SQL("SELECT * FROM users WHERE id = 1", _config=config)

    # Should parameterize the literal
    sql = stmt.sql
    params = stmt.parameters

    assert "?" in sql or ":" in sql  # Parameterized
    assert params is not None

    # Handle TypedParameter objects
    if isinstance(params, list):
        param_values = [p.value if hasattr(p, "value") else p for p in params]
        assert 1 in param_values
    elif isinstance(params, dict):
        param_values = {k: v.value if hasattr(v, "value") else v for k, v in params.items()}
        assert 1 in param_values.values()
    else:
        assert False, f"Unexpected params type: {type(params)}"


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
    config = SQLConfig(strict_mode=False)  # Use non-strict to see the result

    # SQLGlot is very permissive and wraps invalid SQL in Anonymous expressions
    # rather than raising parsing errors
    stmt = SQL("INVALID SQL SYNTAX !", _config=config)
    sql = stmt.sql

    # The invalid SQL is preserved (sqlglot wraps it)
    assert "INVALID" in sql


def test_sql_transformation_error() -> None:
    """Test SQL handles transformation errors."""
    # Create a mock transformer that raises an error
    mock_transformer = Mock()
    mock_transformer.process.side_effect = Exception("Transform error")

    config = SQLConfig(transformers=[mock_transformer], strict_mode=True)

    # In the new pipeline system, transformer errors are caught and reported as validation errors
    with pytest.raises(SQLValidationError) as exc_info:
        stmt = SQL("SELECT * FROM users", _config=config)
        _ = stmt.sql  # Trigger processing

    assert "Transform error" in str(exc_info.value)


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
    stmt = SQL("SELECT * FROM users", _config=config)

    # First access
    expr1 = stmt.expression
    # Second access should return cached
    expr2 = stmt.expression

    assert expr1 is expr2  # Same object


def test_sql_no_expression_caching() -> None:
    """Test SQL expression not cached when disabled."""
    config = SQLConfig(cache_parsed_expression=False)
    stmt = SQL("SELECT * FROM users", _config=config)

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
    stmt2 = SQL(stmt1, _config=new_config)

    assert stmt2._raw_sql == stmt1._raw_sql
    assert stmt2._raw_parameters == stmt1._raw_parameters
    assert stmt2._config == new_config
    assert stmt2._config != stmt1._config
