"""Tests for sqlspec.exceptions module."""

from __future__ import annotations

import pytest

from sqlspec.exceptions import (
    ExtraParameterError,
    ImproperConfigurationError,
    IntegrityError,
    MissingDependencyError,
    MissingParameterError,
    MultipleResultsFoundError,
    NotFoundError,
    ParameterError,
    ParameterStyleMismatchError,
    QueryError,
    RepositoryError,
    RiskLevel,
    SerializationError,
    SQLBuilderError,
    SQLConversionError,
    SQLInjectionError,
    SQLLoadingError,
    SQLParsingError,
    SQLSpecError,
    SQLTransformationError,
    SQLValidationError,
    UnknownParameterError,
    UnsafeSQLError,
    wrap_exceptions,
)

# SQLSpecError (Base Exception) Tests


def test_sqlspec_error_basic_initialization() -> None:
    """Test basic SQLSpecError initialization."""
    error = SQLSpecError("Test error message")
    assert str(error) == "Test error message"
    assert error.detail == "Test error message"


def test_sqlspec_error_with_detail() -> None:
    """Test SQLSpecError with explicit detail."""
    error = SQLSpecError("Main message", detail="Detailed information")
    assert str(error) == "Main message Detailed information"
    assert error.detail == "Detailed information"


def test_sqlspec_error_with_multiple_args() -> None:
    """Test SQLSpecError with multiple arguments."""
    error = SQLSpecError("Error 1", "Error 2", "Error 3", detail="Detail")
    assert "Error 1" in str(error)
    assert "Error 2" in str(error)
    assert "Error 3" in str(error)
    assert "Detail" in str(error)


def test_sqlspec_error_repr() -> None:
    """Test SQLSpecError repr."""
    error = SQLSpecError("Test error", detail="Test detail")
    assert repr(error) == "SQLSpecError - Test detail"


def test_sqlspec_error_repr_without_detail() -> None:
    """Test SQLSpecError repr without detail."""
    error = SQLSpecError()
    assert repr(error) == "SQLSpecError"


def test_sqlspec_error_no_args() -> None:
    """Test SQLSpecError with no arguments."""
    error = SQLSpecError()
    assert str(error) == ""
    assert error.detail == ""


def test_sqlspec_error_none_args() -> None:
    """Test SQLSpecError with None arguments."""
    error = SQLSpecError(None, "Valid arg", None, detail="Test detail")
    assert "Valid arg" in str(error)
    assert "Test detail" in str(error)


@pytest.mark.parametrize(
    ("args", "detail", "expected_detail"),
    [
        (("First error",), "", "First error"),
        (("First", "Second"), "", "First"),
        ((), "Explicit detail", "Explicit detail"),
        (("Main message",), "Override detail", "Override detail"),
        ((), "", ""),
    ],
    ids=["single_arg", "multiple_args", "explicit_detail", "detail_override", "empty"],
)
def test_sqlspec_error_detail_handling(args: tuple[str, ...], detail: str, expected_detail: str) -> None:
    """Test SQLSpecError detail handling with various combinations."""
    error = SQLSpecError(*args, detail=detail)
    assert error.detail == expected_detail


# RiskLevel Enum Tests


def test_risk_level_values() -> None:
    """Test RiskLevel enum values."""
    assert RiskLevel.SKIP.value == 1
    assert RiskLevel.SAFE.value == 2
    assert RiskLevel.LOW.value == 3
    assert RiskLevel.MEDIUM.value == 4
    assert RiskLevel.HIGH.value == 5
    assert RiskLevel.CRITICAL.value == 6


def test_risk_level_string_representation() -> None:
    """Test RiskLevel string representation."""
    assert str(RiskLevel.SKIP) == "skip"
    assert str(RiskLevel.SAFE) == "safe"
    assert str(RiskLevel.LOW) == "low"
    assert str(RiskLevel.MEDIUM) == "medium"
    assert str(RiskLevel.HIGH) == "high"
    assert str(RiskLevel.CRITICAL) == "critical"


def test_risk_level_ordering() -> None:
    """Test RiskLevel ordering."""
    assert RiskLevel.SKIP < RiskLevel.SAFE
    assert RiskLevel.SAFE < RiskLevel.LOW
    assert RiskLevel.LOW < RiskLevel.MEDIUM
    assert RiskLevel.MEDIUM < RiskLevel.HIGH
    assert RiskLevel.HIGH < RiskLevel.CRITICAL


@pytest.mark.parametrize(
    ("risk_level", "expected_str"),
    [
        (RiskLevel.SKIP, "skip"),
        (RiskLevel.SAFE, "safe"),
        (RiskLevel.LOW, "low"),
        (RiskLevel.MEDIUM, "medium"),
        (RiskLevel.HIGH, "high"),
        (RiskLevel.CRITICAL, "critical"),
    ],
    ids=["skip", "safe", "low", "medium", "high", "critical"],
)
def test_risk_level_parametrized_strings(risk_level: RiskLevel, expected_str: str) -> None:
    """Test RiskLevel string conversion."""
    assert str(risk_level) == expected_str


# MissingDependencyError Tests


def test_missing_dependency_error_basic() -> None:
    """Test basic MissingDependencyError."""
    error = MissingDependencyError("test_package")
    assert "test_package" in str(error)
    assert "not installed" in str(error)
    assert "pip install sqlspec[test_package]" in str(error)


def test_missing_dependency_error_with_install_package() -> None:
    """Test MissingDependencyError with custom install package."""
    error = MissingDependencyError("short_name", "long-package-name")
    assert "short_name" in str(error)
    assert "pip install sqlspec[long-package-name]" in str(error)
    assert "pip install long-package-name" in str(error)


def test_missing_dependency_error_inheritance() -> None:
    """Test MissingDependencyError inheritance."""
    error = MissingDependencyError("test")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, ImportError)


@pytest.mark.parametrize(
    ("package", "install_package", "expected_install"),
    [
        ("psycopg2", None, "psycopg2"),
        ("pg", "psycopg2", "psycopg2"),
        ("asyncpg", None, "asyncpg"),
        ("mysql", "pymysql", "pymysql"),
    ],
    ids=["psycopg2_direct", "psycopg2_alias", "asyncpg_direct", "mysql_custom"],
)
def test_missing_dependency_error_various_packages(
    package: str, install_package: str | None, expected_install: str
) -> None:
    """Test MissingDependencyError with various package configurations."""
    if install_package:
        error = MissingDependencyError(package, install_package)
    else:
        error = MissingDependencyError(package)

    assert package in str(error)
    assert expected_install in str(error)


# SQL Exception Tests


def test_sql_loading_error() -> None:
    """Test SQLLoadingError."""
    error = SQLLoadingError("Custom loading error")
    assert str(error) == "Custom loading error"
    assert isinstance(error, SQLSpecError)


def test_sql_loading_error_default_message() -> None:
    """Test SQLLoadingError with default message."""
    error = SQLLoadingError()
    assert "Issues loading referenced SQL file" in str(error)


def test_sql_parsing_error() -> None:
    """Test SQLParsingError."""
    error = SQLParsingError("Custom parsing error")
    assert str(error) == "Custom parsing error"
    assert isinstance(error, SQLSpecError)


def test_sql_parsing_error_default_message() -> None:
    """Test SQLParsingError with default message."""
    error = SQLParsingError()
    assert "Issues parsing SQL statement" in str(error)


def test_sql_builder_error() -> None:
    """Test SQLBuilderError."""
    error = SQLBuilderError("Custom builder error")
    assert str(error) == "Custom builder error"
    assert isinstance(error, SQLSpecError)


def test_sql_builder_error_default_message() -> None:
    """Test SQLBuilderError with default message."""
    error = SQLBuilderError()
    assert "Issues building SQL statement" in str(error)


def test_sql_conversion_error() -> None:
    """Test SQLConversionError."""
    error = SQLConversionError("Custom conversion error")
    assert str(error) == "Custom conversion error"
    assert isinstance(error, SQLSpecError)


def test_sql_conversion_error_default_message() -> None:
    """Test SQLConversionError with default message."""
    error = SQLConversionError()
    assert "Issues converting SQL statement" in str(error)


# SQLValidationError Tests


def test_sql_validation_error_basic() -> None:
    """Test basic SQLValidationError."""
    error = SQLValidationError("Validation failed")
    assert "Validation failed" in str(error)
    assert error.sql is None
    assert error.risk_level == RiskLevel.MEDIUM


def test_sql_validation_error_with_sql() -> None:
    """Test SQLValidationError with SQL context."""
    sql_query = "SELECT * FROM users WHERE id = 1 OR 1=1"
    error = SQLValidationError("SQL injection detected", sql=sql_query)
    assert "SQL injection detected" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query


def test_sql_validation_error_with_risk_level() -> None:
    """Test SQLValidationError with custom risk level."""
    error = SQLValidationError("High risk operation", risk_level=RiskLevel.HIGH)
    assert error.risk_level == RiskLevel.HIGH


def test_sql_validation_error_full_context() -> None:
    """Test SQLValidationError with full context."""
    sql_query = "DROP TABLE users"
    error = SQLValidationError("Dangerous DDL operation", sql=sql_query, risk_level=RiskLevel.CRITICAL)
    assert "Dangerous DDL operation" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query
    assert error.risk_level == RiskLevel.CRITICAL


@pytest.mark.parametrize(
    ("message", "sql", "risk_level"),
    [
        ("Basic error", None, RiskLevel.MEDIUM),
        ("Error with SQL", "SELECT 1", RiskLevel.MEDIUM),
        ("High risk", None, RiskLevel.HIGH),
        ("Critical with SQL", "DROP TABLE test", RiskLevel.CRITICAL),
        ("Low risk operation", "SELECT name FROM users", RiskLevel.LOW),
    ],
    ids=["basic", "with_sql", "high_risk", "critical_with_sql", "low_risk"],
)
def test_sql_validation_error_parametrized(message: str, sql: str | None, risk_level: RiskLevel) -> None:
    """Test SQLValidationError with various parameter combinations."""
    error = SQLValidationError(message, sql=sql, risk_level=risk_level)
    assert message in str(error)
    assert error.sql == sql
    assert error.risk_level == risk_level
    if sql:
        assert sql in str(error)


# SQLTransformationError Tests


def test_sql_transformation_error_basic() -> None:
    """Test basic SQLTransformationError."""
    error = SQLTransformationError("Transformation failed")
    assert "Transformation failed" in str(error)
    assert error.sql is None


def test_sql_transformation_error_with_sql() -> None:
    """Test SQLTransformationError with SQL context."""
    sql_query = "SELECT * FROM complex_view"
    error = SQLTransformationError("Failed to optimize query", sql=sql_query)
    assert "Failed to optimize query" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query


# SQLInjectionError Tests


def test_sql_injection_error_basic() -> None:
    """Test basic SQLInjectionError."""
    error = SQLInjectionError("Potential SQL injection detected")
    assert "Potential SQL injection detected" in str(error)
    assert error.risk_level == RiskLevel.CRITICAL
    assert error.pattern is None


def test_sql_injection_error_with_pattern() -> None:
    """Test SQLInjectionError with injection pattern."""
    error = SQLInjectionError("SQL injection found", pattern="1=1")
    assert "SQL injection found" in str(error)
    assert "Pattern: 1=1" in str(error)
    assert error.pattern == "1=1"


def test_sql_injection_error_with_sql_and_pattern() -> None:
    """Test SQLInjectionError with SQL and pattern."""
    sql_query = "SELECT * FROM users WHERE id = 1 OR 1=1"
    error = SQLInjectionError("Classic injection pattern", sql=sql_query, pattern="OR 1=1")
    assert "Classic injection pattern" in str(error)
    assert "Pattern: OR 1=1" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query
    assert error.pattern == "OR 1=1"
    assert error.risk_level == RiskLevel.CRITICAL


@pytest.mark.parametrize(
    ("message", "sql", "pattern"),
    [
        ("Basic injection", None, None),
        ("With pattern", None, "' OR '1'='1"),
        ("With SQL", "SELECT * FROM users WHERE name = 'admin' OR '1'='1'", None),
        ("Full context", "DROP TABLE users; --", "DROP TABLE"),
        ("Union injection", "SELECT * UNION SELECT password FROM admin", "UNION"),
    ],
    ids=["basic", "with_pattern", "with_sql", "full_context", "union_injection"],
)
def test_sql_injection_error_parametrized(message: str, sql: str | None, pattern: str | None) -> None:
    """Test SQLInjectionError with various parameter combinations."""
    error = SQLInjectionError(message, sql=sql, pattern=pattern)
    assert message in str(error)
    assert error.sql == sql
    assert error.pattern == pattern
    assert error.risk_level == RiskLevel.CRITICAL
    if pattern:
        assert f"Pattern: {pattern}" in str(error)


# UnsafeSQLError Tests


def test_unsafe_sql_error_basic() -> None:
    """Test basic UnsafeSQLError."""
    error = UnsafeSQLError("Unsafe SQL construct detected")
    assert "Unsafe SQL construct detected" in str(error)
    assert error.risk_level == RiskLevel.HIGH
    assert error.construct is None


def test_unsafe_sql_error_with_construct() -> None:
    """Test UnsafeSQLError with construct information."""
    error = UnsafeSQLError("Dynamic SQL generation", construct="EXEC")
    assert "Dynamic SQL generation" in str(error)
    assert "Construct: EXEC" in str(error)
    assert error.construct == "EXEC"


def test_unsafe_sql_error_with_sql_and_construct() -> None:
    """Test UnsafeSQLError with SQL and construct."""
    sql_query = "EXEC sp_executesql @sql"
    error = UnsafeSQLError("Dynamic execution detected", sql=sql_query, construct="EXEC sp_executesql")
    assert "Dynamic execution detected" in str(error)
    assert "Construct: EXEC sp_executesql" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query
    assert error.construct == "EXEC sp_executesql"
    assert error.risk_level == RiskLevel.HIGH


@pytest.mark.parametrize(
    ("message", "sql", "construct"),
    [
        ("Basic unsafe", None, None),
        ("With construct", None, "TRUNCATE"),
        ("With SQL", "TRUNCATE TABLE logs", None),
        ("Full context", "EXEC master..xp_cmdshell 'dir'", "xp_cmdshell"),
        ("Dangerous function", "SELECT * FROM openrowset('SQLOLEDB', '', '')", "openrowset"),
    ],
    ids=["basic", "with_construct", "with_sql", "full_context", "dangerous_function"],
)
def test_unsafe_sql_error_parametrized(message: str, sql: str | None, construct: str | None) -> None:
    """Test UnsafeSQLError with various parameter combinations."""
    error = UnsafeSQLError(message, sql=sql, construct=construct)
    assert message in str(error)
    assert error.sql == sql
    assert error.construct == construct
    assert error.risk_level == RiskLevel.HIGH
    if construct:
        assert f"Construct: {construct}" in str(error)


# QueryError Tests


def test_query_error() -> None:
    """Test QueryError."""
    error = QueryError("Query execution failed")
    assert str(error) == "Query execution failed"
    assert isinstance(error, SQLSpecError)


# Parameter Error Tests


def test_parameter_error_basic() -> None:
    """Test basic ParameterError."""
    error = ParameterError("Parameter validation failed")
    assert "Parameter validation failed" in str(error)
    assert error.sql is None


def test_parameter_error_with_sql() -> None:
    """Test ParameterError with SQL context."""
    sql_query = "SELECT * FROM users WHERE id = :user_id"
    error = ParameterError("Missing parameter", sql=sql_query)
    assert "Missing parameter" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query


def test_unknown_parameter_error() -> None:
    """Test UnknownParameterError."""
    error = UnknownParameterError("Unknown parameter syntax")
    assert isinstance(error, ParameterError)
    assert isinstance(error, SQLSpecError)


def test_missing_parameter_error() -> None:
    """Test MissingParameterError."""
    error = MissingParameterError("Required parameter missing")
    assert isinstance(error, ParameterError)
    assert isinstance(error, SQLSpecError)


def test_extra_parameter_error() -> None:
    """Test ExtraParameterError."""
    error = ExtraParameterError("Extra parameter provided")
    assert isinstance(error, ParameterError)
    assert isinstance(error, SQLSpecError)


# ParameterStyleMismatchError Tests


def test_parameter_style_mismatch_error_basic() -> None:
    """Test basic ParameterStyleMismatchError."""
    error = ParameterStyleMismatchError()
    assert "Parameter style mismatch" in str(error)
    assert "dictionary parameters provided" in str(error)
    assert error.sql is None


def test_parameter_style_mismatch_error_custom_message() -> None:
    """Test ParameterStyleMismatchError with custom message."""
    error = ParameterStyleMismatchError("Custom parameter mismatch")
    assert "Custom parameter mismatch" in str(error)


def test_parameter_style_mismatch_error_with_sql() -> None:
    """Test ParameterStyleMismatchError with SQL context."""
    sql_query = "SELECT * FROM users WHERE id = ?"
    error = ParameterStyleMismatchError("Positional vs named mismatch", sql=sql_query)
    assert "Positional vs named mismatch" in str(error)
    assert sql_query in str(error)
    assert error.sql == sql_query


@pytest.mark.parametrize(
    ("message", "sql"),
    [
        (None, None),
        ("Custom message", None),
        (None, "SELECT * FROM users WHERE id = ?"),
        ("Custom with SQL", "SELECT * FROM users WHERE name = :name"),
    ],
    ids=["default", "custom_message", "with_sql", "custom_with_sql"],
)
def test_parameter_style_mismatch_error_parametrized(message: str | None, sql: str | None) -> None:
    """Test ParameterStyleMismatchError with various parameter combinations."""
    if message and sql:
        error = ParameterStyleMismatchError(message, sql=sql)
    elif message:
        error = ParameterStyleMismatchError(message)
    elif sql:
        error = ParameterStyleMismatchError(sql=sql)
    else:
        error = ParameterStyleMismatchError()

    assert error.sql == sql
    if message:
        assert message in str(error)
    else:
        assert "Parameter style mismatch" in str(error)


# Repository and Database Error Tests


def test_improper_configuration_error() -> None:
    """Test ImproperConfigurationError."""
    error = ImproperConfigurationError("Invalid configuration")
    assert isinstance(error, SQLSpecError)


def test_serialization_error() -> None:
    """Test SerializationError."""
    error = SerializationError("JSON encoding failed")
    assert isinstance(error, SQLSpecError)


def test_repository_error() -> None:
    """Test RepositoryError."""
    error = RepositoryError("Repository operation failed")
    assert isinstance(error, SQLSpecError)


def test_integrity_error() -> None:
    """Test IntegrityError."""
    error = IntegrityError("Foreign key constraint violation")
    assert isinstance(error, RepositoryError)
    assert isinstance(error, SQLSpecError)


def test_not_found_error() -> None:
    """Test NotFoundError."""
    error = NotFoundError("User not found")
    assert isinstance(error, RepositoryError)
    assert isinstance(error, SQLSpecError)


def test_multiple_results_found_error() -> None:
    """Test MultipleResultsFoundError."""
    error = MultipleResultsFoundError("Expected single result, found multiple")
    assert isinstance(error, RepositoryError)
    assert isinstance(error, SQLSpecError)


# Exception Hierarchy Tests


def test_exception_hierarchy() -> None:
    """Test exception inheritance hierarchy."""
    # All custom exceptions should inherit from SQLSpecError
    exceptions_to_test: list[SQLSpecError] = [
        MissingDependencyError("test"),
        SQLLoadingError(),
        SQLParsingError(),
        SQLBuilderError(),
        SQLConversionError(),
        SQLValidationError("test"),
        SQLTransformationError("test"),
        SQLInjectionError("test"),
        UnsafeSQLError("test"),
        QueryError("test"),
        ParameterError("test"),
        UnknownParameterError("test"),
        MissingParameterError("test"),
        ExtraParameterError("test"),
        ParameterStyleMismatchError(),
        ImproperConfigurationError("test"),
        SerializationError("test"),
        RepositoryError("test"),
        IntegrityError("test"),
        NotFoundError("test"),
        MultipleResultsFoundError("test"),
    ]

    for exception in exceptions_to_test:
        assert isinstance(exception, SQLSpecError)
        assert isinstance(exception, Exception)


def test_specialized_inheritance() -> None:
    """Test specialized exception inheritance."""
    # MissingDependencyError should also be ImportError
    missing_dep = MissingDependencyError("test")
    assert isinstance(missing_dep, ImportError)

    # Repository exceptions should inherit from RepositoryError
    repository_exceptions = [IntegrityError("test"), NotFoundError("test"), MultipleResultsFoundError("test")]

    for repo_exception in repository_exceptions:
        assert isinstance(repo_exception, RepositoryError)

    # Parameter exceptions should inherit from ParameterError
    parameter_exceptions: list[ParameterError] = [
        UnknownParameterError("test"),
        MissingParameterError("test"),
        ExtraParameterError("test"),
    ]

    for param_exception in parameter_exceptions:
        assert isinstance(param_exception, ParameterError)

    # Validation exceptions should inherit from SQLValidationError
    validation_exceptions: list[SQLValidationError] = [SQLInjectionError("test"), UnsafeSQLError("test")]

    for validation_exception in validation_exceptions:
        assert isinstance(validation_exception, SQLValidationError)


# wrap_exceptions Context Manager Tests


def test_wrap_exceptions_context_manager_success() -> None:
    """Test wrap_exceptions context manager with successful execution."""
    with wrap_exceptions():
        result = "success"
    assert result == "success"


def test_wrap_exceptions_context_manager_with_exception() -> None:
    """Test wrap_exceptions context manager with exception."""
    with pytest.raises(RepositoryError) as exc_info:
        with wrap_exceptions():
            raise ValueError("Original error")

    assert isinstance(exc_info.value, RepositoryError)
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert str(exc_info.value.__cause__) == "Original error"


def test_wrap_exceptions_context_manager_disabled() -> None:
    """Test wrap_exceptions context manager with wrapping disabled."""
    with pytest.raises(ValueError) as exc_info:
        with wrap_exceptions(wrap_exceptions=False):
            raise ValueError("Original error")

    assert str(exc_info.value) == "Original error"
    assert not isinstance(exc_info.value, RepositoryError)


def test_wrap_exceptions_context_manager_already_repository_error() -> None:
    """Test wrap_exceptions with existing RepositoryError."""
    original_error = RepositoryError("Already a repository error")

    with pytest.raises(RepositoryError) as exc_info:
        with wrap_exceptions():
            raise original_error

    # Should NOT wrap existing SQLSpec exceptions - they pass through as-is
    assert exc_info.value is original_error
    assert exc_info.value.__cause__ is None


def test_wrap_exceptions_context_manager_sqlspec_exceptions_pass_through() -> None:
    """Test wrap_exceptions with various SQLSpec exceptions."""
    sqlspec_exceptions = [
        SQLValidationError("Validation error"),
        ParameterError("Parameter error"),
        MissingDependencyError("test"),
        SQLInjectionError("Injection detected"),
    ]

    for original_error in sqlspec_exceptions:
        with pytest.raises(type(original_error)) as exc_info:
            with wrap_exceptions():
                raise original_error

        # Should NOT wrap existing SQLSpec exceptions - they pass through as-is
        assert exc_info.value is original_error
        assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
    ("exception_type", "message"),
    [
        (ValueError, "Value error"),
        (TypeError, "Type error"),
        (KeyError, "Key error"),
        (AttributeError, "Attribute error"),
        (RuntimeError, "Runtime error"),
        (OSError, "OS error"),
    ],
    ids=["value_error", "type_error", "key_error", "attribute_error", "runtime_error", "os_error"],
)
def test_wrap_exceptions_various_exception_types(exception_type: type[Exception], message: str) -> None:
    """Test wrap_exceptions with various exception types."""
    with pytest.raises(RepositoryError) as exc_info:
        with wrap_exceptions():
            raise exception_type(message)

    assert isinstance(exc_info.value, RepositoryError)
    assert isinstance(exc_info.value.__cause__, exception_type)

    # KeyError automatically adds quotes around the message
    if exception_type is KeyError:
        assert str(exc_info.value.__cause__) == f"'{message}'"
    else:
        assert str(exc_info.value.__cause__) == message


# Edge Cases and Error Context Tests


def test_exception_with_empty_messages() -> None:
    """Test exceptions with empty messages."""
    exceptions = [SQLSpecError(""), SQLValidationError(""), ParameterError(""), RepositoryError("")]

    for exception in exceptions:
        assert str(exception) == ""


def test_exception_with_none_sql_context() -> None:
    """Test exceptions with None SQL context."""
    error = SQLValidationError("Test error", sql=None)
    assert error.sql is None
    assert "Test error" == str(error)


def test_exception_with_empty_sql_context() -> None:
    """Test exceptions with empty SQL context."""
    error = SQLValidationError("Test error", sql="")
    assert error.sql == ""
    assert "Test error\nSQL:" in str(error)


def test_exception_with_multiline_sql() -> None:
    """Test exceptions with multiline SQL."""
    multiline_sql = """SELECT *
FROM users
WHERE id = 1
OR 1=1"""

    error = SQLInjectionError("Injection detected", sql=multiline_sql)
    assert error.sql == multiline_sql
    assert multiline_sql in str(error)


def test_parameter_error_sql_context_isolation() -> None:
    """Test that SQL context in parameter errors doesn't affect original."""
    original_sql = "SELECT * FROM users WHERE id = :id"
    error = ParameterError("Test error", sql=original_sql)

    # Modifying error.sql shouldn't affect original_sql
    error.sql = "MODIFIED"
    assert original_sql == "SELECT * FROM users WHERE id = :id"


# Performance and Edge Case Tests


def test_exception_with_very_long_sql() -> None:
    """Test exception with very long SQL statement."""
    long_sql = "SELECT " + ", ".join([f"column_{i}" for i in range(1000)]) + " FROM big_table"
    error = SQLValidationError("Long query validation", sql=long_sql)

    assert error.sql == long_sql
    assert long_sql in str(error)


def test_exception_with_special_characters_in_sql() -> None:
    """Test exception with special characters in SQL."""
    special_sql = "SELECT 'test\n\t\"quote' FROM users WHERE data LIKE '%\\%'"
    error = SQLValidationError("Special chars", sql=special_sql)

    assert error.sql == special_sql
    assert special_sql in str(error)


def test_risk_level_enum_completeness() -> None:
    """Test that all RiskLevel values are covered."""
    all_risk_levels = [
        RiskLevel.SKIP,
        RiskLevel.SAFE,
        RiskLevel.LOW,
        RiskLevel.MEDIUM,
        RiskLevel.HIGH,
        RiskLevel.CRITICAL,
    ]

    # Ensure we have all expected values
    assert len(all_risk_levels) == 6

    # Ensure they have the expected ordering
    for i in range(len(all_risk_levels) - 1):
        assert all_risk_levels[i] < all_risk_levels[i + 1]


def test_exception_chaining() -> None:
    """Test exception chaining behavior."""
    original_error = ValueError("Original problem")

    try:
        raise original_error
    except ValueError as e:
        wrapped_error = RepositoryError("Wrapped problem")
        wrapped_error.__cause__ = e

    assert wrapped_error.__cause__ is original_error
    assert isinstance(wrapped_error.__cause__, ValueError)


def test_context_manager_nested_exceptions() -> None:
    """Test wrap_exceptions with nested context managers."""
    with pytest.raises(RepositoryError):
        with wrap_exceptions():
            with wrap_exceptions():
                raise ValueError("Nested error")


def test_missing_dependency_error_edge_cases() -> None:
    """Test MissingDependencyError edge cases."""
    # Empty package name
    error = MissingDependencyError("")
    assert "''" in str(error)

    # Very long package name
    long_package = "very_long_package_name_that_exceeds_normal_length"
    error = MissingDependencyError(long_package)
    assert long_package in str(error)

    # Package with special characters
    special_package = "package-with-hyphens_and_underscores.dots"
    error = MissingDependencyError(special_package)
    assert special_package in str(error)
