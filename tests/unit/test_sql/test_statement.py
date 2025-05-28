from typing import Any, Optional
from unittest.mock import Mock

import pytest
import sqlglot
from sqlglot import exp
from sqlglot import parse_one

from sqlspec.exceptions import (
    ParameterError,
    RiskLevel,
    SQLValidationError,
)
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.statement import (
    SQLSanitizer,
    SQLStatement,
    SQLValidator,
    StatementConfig,
    ValidationResult,
    validate_sql,
)

# Basic Initialization Tests


def test_simple_sql_string_no_parameters() -> None:
    """Test initialization with simple SQL string and no parameters."""
    sql = "SELECT * FROM users"
    stmt = SQLStatement(sql)
    assert stmt.sql == sql
    assert stmt.parameters is None or stmt.parameters == {}
    assert stmt.expression is not None
    assert stmt.is_safe is True  # Assuming default config enables parsing and validation


@pytest.mark.parametrize(
    ("sql", "params_key", "params_value", "expected_params"),
    [
        ("SELECT * FROM users WHERE id = ?", "args", [123], [123]),
        ("SELECT * FROM users WHERE name = :name", "kwargs", {"name": "John"}, {"name": "John"}),
        (
            "SELECT * FROM users WHERE name = :name AND age = :age",
            "parameters",
            {"name": "Alice", "age": 30},
            {"name": "Alice", "age": 30},
        ),
        ("SELECT * FROM users WHERE id = ? AND status = ?", "parameters", [123, "active"], [123, "active"]),
    ],
    ids=[
        "positional_args",
        "named_kwargs",
        "explicit_parameters_dict",
        "explicit_parameters_list",
    ],
)
def test_sql_with_parameters_initialization(sql: str, params_key: str, params_value: Any, expected_params: Any) -> None:
    """Test SQL initialization with various parameter types."""
    if params_key == "args":
        stmt = SQLStatement(sql, args=params_value)
    elif params_key == "kwargs":
        stmt = SQLStatement(sql, kwargs=params_value)
    elif params_key == "parameters":
        stmt = SQLStatement(sql, parameters=params_value)
    else:
        pytest.fail(f"Invalid params_key: {params_key}")  # Should not happen

    assert stmt.sql == sql
    assert stmt.parameters == expected_params


def test_wrapping_existing_sqlstatement() -> None:
    """Test wrapping an existing SQLStatement instance."""
    original_sql_qmark = "SELECT * FROM users WHERE id = ?"
    original_stmt_qmark = SQLStatement(original_sql_qmark, args=[123])

    # Wrap without changes
    wrapped_stmt = SQLStatement(original_stmt_qmark)
    assert wrapped_stmt.sql == original_sql_qmark
    assert wrapped_stmt.parameters == [123]
    assert wrapped_stmt.is_safe == original_stmt_qmark.is_safe

    # Wrap with new parameters (args)
    new_stmt_args = SQLStatement(original_stmt_qmark, args=[456])
    assert new_stmt_args.sql == original_sql_qmark
    assert new_stmt_args.parameters == [456]

    # Wrap with new parameters (explicit parameters)
    new_stmt_params = SQLStatement(original_stmt_qmark, parameters=[789])
    assert new_stmt_params.sql == original_sql_qmark
    assert new_stmt_params.parameters == [789]

    original_sql_named = "SELECT * FROM users WHERE name = :name"
    original_stmt_named = SQLStatement(original_sql_named, kwargs={"name": "Alice"})

    # Wrap with new parameters (kwargs)
    new_stmt_kwargs = SQLStatement(original_stmt_named, kwargs={"name": "Bob"})
    assert new_stmt_kwargs.sql == original_sql_named
    assert new_stmt_kwargs.parameters == {"name": "Bob"}


def test_sqlglot_expression_input() -> None:
    """Test initialization with sqlglot Expression object."""
    expression = exp.Select().select(exp.Star()).from_("users")
    stmt = SQLStatement(expression)

    assert stmt.expression == expression
    assert "SELECT * FROM USERS" in stmt.sql.upper()
    assert stmt.is_safe is True


def test_parameter_merging_priority() -> None:
    """Test parameter merging: `parameters` takes precedence over `args`/`kwargs`."""
    sql = "SELECT * FROM users WHERE id = :id"
    stmt = SQLStatement(sql, parameters={"id": 1}, args=[2], kwargs={"id": 3})
    assert stmt.parameters == {"id": 1}


def test_args_and_kwargs_merging_into_dict_if_sql_allows() -> None:
    """
    Test merging args and kwargs.
    If SQL is like "SELECT ... WHERE id = ? AND name = :name",
    SQLStatement is expected to merge args and kwargs into a dictionary,
    potentially by naming positional parameters (e.g., ? becomes :_p1 or similar).
    """
    sql_mixed = "SELECT * FROM users WHERE id = ? AND name = :name"
    stmt_mixed = SQLStatement(sql_mixed, args=[123], kwargs={"name": "John"})

    params = stmt_mixed.parameters
    assert isinstance(params, dict), "Parameters should be merged into a dict"
    assert params.get("name") == "John", "Named parameter 'name' should be present"

    # Check if the positional argument was also included (e.g., as '_1', 'p1', or similar)
    # This depends on sqlglot's automatic naming or SQLStatement's convention.
    # We expect at least two parameters if merging happened.
    assert len(params) >= 2, "Positional argument should also be in the merged dict"

    # Verify that one of the keys is not 'name' and its value is 123
    positional_param_found = False
    for key, value in params.items():
        if key != "name" and value == 123:
            positional_param_found = True
            break
    assert positional_param_found, "Positional argument (123) not found in merged parameters"


@pytest.mark.parametrize(
    ("config_settings", "should_raise", "expected_error", "error_match"),
    [
        (
            {"enable_parsing": False, "allow_mixed_parameters": False},
            True,
            ParameterError,
            "Cannot mix args and kwargs",
        ),
        ({"enable_parsing": False, "allow_mixed_parameters": True}, False, None, None),
    ],
    ids=["mixed_not_allowed_no_parsing", "mixed_allowed_no_parsing"],
)
def test_mixed_parameters_handling_without_parsing(
    config_settings: dict[str, Any],
    should_raise: bool,
    expected_error: Optional[type[Exception]],
    error_match: Optional[str],
) -> None:
    """Test handling of mixed args and kwargs when parsing is disabled."""
    config = StatementConfig(**config_settings)
    sql = "SELECT * FROM users"

    if should_raise:
        with pytest.raises(expected_error, match=error_match):  # type: ignore[arg-type]
            SQLStatement(sql, args=[1], kwargs={"name": "test"}, statement_config=config)
    else:
        stmt = SQLStatement(sql, args=[1], kwargs={"name": "test"}, statement_config=config)
        # If allowed, parameters should be a tuple: (args, kwargs)
        assert isinstance(stmt.parameters, tuple)
        assert stmt.parameters[0] == [1]
        assert stmt.parameters[1] == {"name": "test"}


def test_scalar_parameter() -> None:
    """Test single scalar parameter."""
    sql = "SELECT * FROM users WHERE id = ?"
    stmt = SQLStatement(sql, parameters=123)
    # SQLStatement wraps scalar parameters in a list if the SQL implies positional placeholders
    # or if it's ambiguous. If it's for a single '?', it should likely become [123].
    # However, the original test was `assert stmt.parameters == 123`.
    # Let's assume SQLStatement stores it as passed if `parameters` arg is used directly.
    assert stmt.parameters == 123


@pytest.mark.parametrize(
    ("params_input", "expected_params_stored"),
    [
        (None, None),
        ({}, {}),
        ([], []),
    ],
    ids=["none_params", "empty_dict_params", "empty_list_params"],
)
def test_empty_parameters(params_input: Any, expected_params_stored: Any) -> None:
    """Test with empty/None parameters."""
    sql = "SELECT * FROM users"
    stmt = SQLStatement(sql, parameters=params_input)
    assert stmt.parameters == expected_params_stored


# Placeholder Transformation Tests


@pytest.fixture
def stmt_qmark_fixture() -> SQLStatement:
    return SQLStatement("SELECT * FROM users WHERE id = ?", args=[123])


@pytest.fixture
def stmt_named_fixture() -> SQLStatement:
    return SQLStatement("SELECT * FROM users WHERE name = :name", kwargs={"name": "John"})


def test_get_sql_default_format(stmt_qmark_fixture: SQLStatement) -> None:
    """Test get_sql() returns original SQL if no transformation needed."""
    assert stmt_qmark_fixture.get_sql() == "SELECT * FROM users WHERE id = ?"


@pytest.mark.parametrize(
    ("stmt_fixture_name", "style_requested", "expected_sql_pattern"),
    [
        ("stmt_named_fixture", "qmark", "SELECT * FROM users WHERE name = ?"),
        # For qmark to named, sqlglot might generate names like :_1 or use column name if possible
        (
            "stmt_qmark_fixture",
            "named_colon",
            r"SELECT * FROM users WHERE id = :param_0",
        ),  # Example, actual name might vary
    ],
    ids=["named_to_qmark", "qmark_to_named"],
)
def test_get_sql_placeholder_style_conversion(
    stmt_fixture_name: str, style_requested: str, expected_sql_pattern: str, request: Any
) -> None:
    """Test get_sql() with qmark and named placeholder style conversions."""
    stmt = request.getfixturevalue(stmt_fixture_name)
    result_sql = stmt.get_sql(placeholder_style=ParameterStyle(style_requested))

    if style_requested == "named":
        # Check for the presence of a named parameter for 'id'
        assert "id = :" in result_sql
    else:  # qmark
        assert result_sql.upper() == expected_sql_pattern.upper()


def test_get_sql_static_style() -> None:
    """Test get_sql() with static style (parameters substituted)."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ? AND active = ? AND percentage = ?"
    stmt = SQLStatement(sql, args=[123, "John's Cafe", True, 0.75])
    result_sql = stmt.get_sql(placeholder_style=ParameterStyle.STATIC)

    assert "123" in result_sql
    assert "'John''s Cafe'" in result_sql  # Standard SQL escaping for single quote
    assert "TRUE" in result_sql.upper() or "1" in result_sql  # Boolean representation
    assert "0.75" in result_sql  # Float representation
    assert "?" not in result_sql
    assert ":" not in result_sql


def test_get_sql_with_statement_separator(stmt_qmark_fixture: SQLStatement) -> None:
    """Test get_sql() with statement separator."""
    result_sql_with_sep = stmt_qmark_fixture.get_sql(include_statement_separator=True)
    assert result_sql_with_sep.endswith(";")

    result_sql_no_sep = stmt_qmark_fixture.get_sql(include_statement_separator=False)
    assert not result_sql_no_sep.endswith(";")


# Validation Tests


@pytest.fixture
def validator_mock_fixture() -> Mock:
    mock = Mock(spec=SQLValidator)
    # Ensure the mock's validate method returns a ValidationResult instance
    mock.validate.return_value = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, issues=[])
    return mock


@pytest.fixture
def config_with_validator_fixture(validator_mock_fixture: Mock) -> StatementConfig:
    return StatementConfig(validator=validator_mock_fixture)


def test_validation_called_on_init(
    config_with_validator_fixture: StatementConfig, validator_mock_fixture: Mock
) -> None:
    """Test validators are called during initialization."""
    sql = "SELECT * FROM table"
    SQLStatement(sql, statement_config=config_with_validator_fixture)
    validator_mock_fixture.validate.assert_called_once()


def test_is_safe_property_reflects_validation(validator_mock_fixture: Mock) -> None:
    """Test is_safe property reflects validation outcome."""
    sql = "SELECT * FROM table"

    validator_mock_fixture.validate.return_value = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, issues=[])
    config_safe = StatementConfig(validator=validator_mock_fixture)
    stmt_safe = SQLStatement(sql, statement_config=config_safe)
    assert stmt_safe.is_safe is True
    assert stmt_safe._validation_result is not None
    assert stmt_safe._validation_result.is_safe is True

    validator_mock_fixture.reset_mock()  # Reset for the next scenario
    validator_mock_fixture.validate.return_value = ValidationResult(
        is_safe=False, risk_level=RiskLevel.HIGH, issues=["SQL Injection"]
    )
    config_unsafe = StatementConfig(validator=validator_mock_fixture, strict_mode=False)
    stmt_unsafe = SQLStatement(sql, statement_config=config_unsafe)
    assert stmt_unsafe.is_safe is False
    assert stmt_unsafe._validation_result is not None
    assert stmt_unsafe._validation_result.is_safe is False


def test_validation_error_on_high_risk_if_configured(validator_mock_fixture: Mock) -> None:
    """Test SQLValidationError is raised for high-risk findings if configured."""
    sql = "DROP TABLE users"
    validator_mock_fixture.validate.return_value = ValidationResult(
        is_safe=False, risk_level=RiskLevel.CRITICAL, issues=["Destructive command"]
    )
    config = StatementConfig(validator=validator_mock_fixture, strict_mode=True)

    with pytest.raises(SQLValidationError):
        SQLStatement(sql, statement_config=config)


def test_no_validation_error_if_risk_below_threshold(validator_mock_fixture: Mock) -> None:
    """Test no error if validation fails but risk is below configured threshold."""
    sql = "SELECT questionable_function()"
    validator_mock_fixture.validate.return_value = ValidationResult(
        is_safe=False, risk_level=RiskLevel.MEDIUM, issues=["Uses risky function"]
    )
    # Configure to raise on HIGH, MEDIUM is < HIGH.
    # We set strict_mode=False here to ensure that the SQLValidationError is not raised
    # due to the strict_mode setting itself, but rather due to the risk level.
    # SQLValidator.min_risk_to_raise defaults to HIGH.
    config = StatementConfig(strict_mode=False, validator=validator_mock_fixture)

    stmt = SQLStatement(sql, statement_config=config)
    assert stmt.is_safe is False  # Should still be marked unsafe


# Sanitization Tests


@pytest.fixture
def sanitizer_mock_fixture() -> Mock:
    mock = Mock(spec=SQLSanitizer)
    # Define a default return value for sanitize to avoid issues if not overridden
    mock.sanitize.side_effect = lambda sql_input, dialect=None: sql_input
    return mock


@pytest.fixture
def config_with_sanitizer_fixture(sanitizer_mock_fixture: Mock) -> StatementConfig:
    return StatementConfig(sanitizer=sanitizer_mock_fixture)


def test_sanitization_called_on_init(
    config_with_sanitizer_fixture: StatementConfig, sanitizer_mock_fixture: Mock
) -> None:
    """Test sanitizers are called during initialization."""
    original_sql = "SELECT * FROM users -- comment"
    # The sanitize method of SQLSanitizer is expected to return an exp.Expression
    sanitized_expression_output = sqlglot.parse_one("SELECT * FROM users")  # Sanitized version as expression
    sanitizer_mock_fixture.sanitize.return_value = sanitized_expression_output

    _ = SQLStatement(original_sql, statement_config=config_with_sanitizer_fixture)

    # SQLStatement calls sanitize with the parsed version of original_sql
    expected_input_to_sanitize = sqlglot.parse_one(original_sql)
    # Changed dialect=None to positional None
    sanitizer_mock_fixture.sanitize.assert_called_once_with(expected_input_to_sanitize, None)


def test_sanitization_modifies_sql_property(sanitizer_mock_fixture: Mock) -> None:
    """Test that the .sql property reflects sanitized SQL."""
    original_sql = "SELECT Evil(); /* Bad stuff */"
    # What the mock sanitize method should return (as an expression)
    # sqlglot normalizes function names to uppercase if unquoted.
    sanitized_sql_expr_returned_by_mock = sqlglot.parse_one("SELECT SAFE()")
    # What stmt.sql should be after sanitization (string form)
    expected_final_sql_string = "SELECT SAFE()"

    sanitizer_mock_fixture.sanitize.return_value = sanitized_sql_expr_returned_by_mock
    # Ensure relevant flags are true in config
    config = StatementConfig(
        sanitizer=sanitizer_mock_fixture,
        enable_parsing=True,  # Default
        enable_sanitization=True  # Default
    )

    stmt = SQLStatement(original_sql, statement_config=config)
    assert stmt.sql == expected_final_sql_string


# Dialect Handling Tests


def test_get_sql_target_dialect_conversion() -> None:
    """Test get_sql() with target dialect for SQL syntax conversion."""
    sql_mysql_specific = "SELECT `a` FROM `b` WHERE `c` <=> NULL"  # MySQL NULL-safe equal
    # Parse with MySQL dialect
    stmt_mysql_parsed = SQLStatement(sql_mysql_specific, dialect="mysql")

    # Transpile to PostgreSQL, where <=> is IS NOT DISTINCT FROM
    # sqlglot normalizes identifiers (removes backticks if not needed for postgres)
    # and changes the operator.
    result_postgres = stmt_mysql_parsed.get_sql(placeholder_style=ParameterStyle.NAMED_COLON, dialect="postgres")

    # Check for key elements of PostgreSQL syntax
    assert "IS NOT DISTINCT FROM" in result_postgres.upper()
    assert "<=>" not in result_postgres
    assert "`" not in result_postgres  # Backticks should be gone or replaced by standard quotes if needed


# Misc Tests


def test_statement_str_representation(stmt_qmark_fixture: SQLStatement) -> None:
    """Test __str__ representation of SQLStatement."""
    assert str(stmt_qmark_fixture) == stmt_qmark_fixture.get_sql()


def test_statement_repr_representation() -> None:
    """Test __repr__ representation of SQLStatement."""
    sql = "SELECT 1"
    stmt = SQLStatement(sql, args=[])
    representation = repr(stmt)
    assert "SQLStatement" in representation
    assert f"sql='{sql}'" in representation
    assert "parameters=[]" in representation
    # Changed from _statement_config to _config to match implementation
    assert f"_config={stmt._statement_config!r}" in representation


def test_statement_equality() -> None:
    """Test equality comparison between SQLStatement instances."""
    sql = "SELECT * FROM test"
    params1: list[Any] = [1, "foo"]
    params2: dict[str, Any] = {"id": 1, "name": "foo"}  # Different parameter structure
    config1 = StatementConfig()
    config1_dialect = "sqlite"
    config2 = StatementConfig()
    config2_dialect = "postgres"  # Different config

    stmt1_a = SQLStatement(sql, parameters=params1, statement_config=config1, dialect=config1_dialect)
    stmt1_b = SQLStatement(sql, parameters=params1, statement_config=config1, dialect=config1_dialect)  # Identical

    stmt_diff_params_val = SQLStatement(sql, parameters=[2, "bar"], statement_config=config1, dialect=config1_dialect)
    stmt_diff_params_struct = SQLStatement(sql, parameters=params2, statement_config=config1, dialect=config1_dialect)
    stmt_diff_sql = SQLStatement(
        "SELECT id FROM test", parameters=params1, statement_config=config1, dialect=config1_dialect
    )
    stmt_diff_config = SQLStatement(sql, parameters=params1, statement_config=config2, dialect=config2_dialect)

    # Test equality with args that resolve to same parameters
    stmt_with_args = SQLStatement(sql, args=params1, statement_config=config1, dialect=config1_dialect)

    assert stmt1_a == stmt1_b
    assert stmt1_a != stmt_diff_params_val
    assert stmt1_a != stmt_diff_params_struct
    assert stmt1_a != stmt_diff_sql
    assert stmt1_a != stmt_diff_config
    assert stmt1_a == stmt_with_args  # Assuming args are processed into parameters consistently

    assert stmt1_a != "SELECT * FROM test"  # type: ignore[comparison-overlap]


# Helper Functions Tests (is_sql_safe, validate_sql, sanitize_sql)


def test_validate_sql_helper_safe_query() -> None:
    """Test validate_sql helper for a clearly safe query."""
    sql = "SELECT name FROM products WHERE id = 123"
    result = validate_sql(sql)  # Uses default config + default validators
    assert result.is_safe
    assert result.risk_level == RiskLevel.SAFE


def test_validate_sql_helper_unsafe_query() -> None:
    """Test validate_sql helper for a query flagged by default validators (e.g., DML without WHERE)."""
    # DELETE without WHERE might be flagged as risky by default rules
    sql = "DELETE FROM products"
    result = validate_sql(sql)
    assert not result.is_safe
    assert result.risk_level > RiskLevel.SAFE  # type: ignore[operator]
