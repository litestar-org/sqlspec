"""Comprehensive tests for sqlspec.exceptions module.

Tests all custom exceptions, inheritance hierarchy, error handling,
wrapping functionality, and message formatting. Uses function-based pytest.
"""

from typing import Any

import pytest

from sqlspec.exceptions import (
    BackendNotRegisteredError,
    ExtraParameterError,
    FileNotFoundInStorageError,
    ImproperConfigurationError,
    IntegrityError,
    MissingDependencyError,
    MissingParameterError,
    MultipleResultsFoundError,
    NotFoundError,
    ParameterError,
    ParameterStyleMismatchError,
    PipelineExecutionError,
    QueryError,
    RepositoryError,
    RiskLevel,
    SerializationError,
    SQLBuilderError,
    SQLCompilationError,
    SQLConversionError,
    SQLFileNotFoundError,
    SQLFileParseError,
    SQLFileParsingError,
    SQLInjectionError,
    SQLLoadingError,
    SQLParsingError,
    SQLSpecError,
    SQLTransformationError,
    SQLValidationError,
    StorageOperationFailedError,
    UnknownParameterError,
    UnsafeSQLError,
    wrap_exceptions,
)

# Base Exception Tests

def test_sqlspec_error_basic_initialization() -> None:
    """Test SQLSpecError basic initialization."""
    error = SQLSpecError("Test message")
    assert str(error) == "Test message"
    assert error.detail == "Test message"


def test_sqlspec_error_with_detail() -> None:
    """Test SQLSpecError initialization with explicit detail."""
    error = SQLSpecError("Main message", detail="Detailed info")
    assert error.detail == "Detailed info"
    assert "Detailed info" in str(error)


def test_sqlspec_error_with_multiple_args() -> None:
    """Test SQLSpecError with multiple arguments."""
    error = SQLSpecError("First", "Second", "Third")
    assert "Second" in str(error)
    assert "Third" in str(error)


def test_sqlspec_error_repr() -> None:
    """Test SQLSpecError string representation."""
    error = SQLSpecError("Test message")
    assert repr(error) == "SQLSpecError - Test message"


def test_sqlspec_error_repr_without_detail() -> None:
    """Test SQLSpecError repr when no detail."""
    error = SQLSpecError()
    assert repr(error) == "SQLSpecError"


def test_sqlspec_error_str_combination() -> None:
    """Test SQLSpecError combines args and detail in str."""
    error = SQLSpecError("Arg1", "Arg2", detail="Detail")
    result = str(error)
    assert "Arg1" in result
    assert "Arg2" in result
    assert "Detail" in result


def test_sqlspec_error_empty_args_filtered() -> None:
    """Test SQLSpecError filters empty args."""
    error = SQLSpecError("Valid", "", None, "Also valid", detail="Detail")
    result = str(error)
    assert "Valid" in result
    assert "Also valid" in result
    assert "Detail" in result
    # Empty strings and None should be filtered out


def test_sqlspec_error_detail_from_args() -> None:
    """Test SQLSpecError uses first arg as detail if no explicit detail."""
    error = SQLSpecError("This becomes detail", "This is arg")
    assert error.detail == "This becomes detail"
    assert "This is arg" in str(error)


# Risk Level Tests

def test_risk_level_string_representation() -> None:
    """Test RiskLevel string conversion."""
    assert str(RiskLevel.SKIP) == "skip"
    assert str(RiskLevel.SAFE) == "safe"
    assert str(RiskLevel.LOW) == "low"
    assert str(RiskLevel.MEDIUM) == "medium"
    assert str(RiskLevel.HIGH) == "high"
    assert str(RiskLevel.CRITICAL) == "critical"


def test_risk_level_ordering() -> None:
    """Test RiskLevel comparison operations."""
    assert RiskLevel.SKIP < RiskLevel.SAFE
    assert RiskLevel.SAFE < RiskLevel.LOW
    assert RiskLevel.LOW < RiskLevel.MEDIUM
    assert RiskLevel.MEDIUM < RiskLevel.HIGH
    assert RiskLevel.HIGH < RiskLevel.CRITICAL

    assert RiskLevel.CRITICAL > RiskLevel.HIGH
    assert RiskLevel.HIGH >= RiskLevel.HIGH
    assert RiskLevel.LOW <= RiskLevel.MEDIUM


def test_risk_level_ordering_edge_cases() -> None:
    """Test RiskLevel ordering with equal values."""
    assert RiskLevel.MEDIUM == RiskLevel.MEDIUM
    assert RiskLevel.MEDIUM <= RiskLevel.MEDIUM
    assert RiskLevel.MEDIUM >= RiskLevel.MEDIUM
    assert not (RiskLevel.MEDIUM < RiskLevel.MEDIUM)
    assert not (RiskLevel.MEDIUM > RiskLevel.MEDIUM)


def test_risk_level_ordering_with_non_risk_level() -> None:
    """Test RiskLevel comparison with non-RiskLevel objects."""
    assert RiskLevel.MEDIUM.__lt__("not_risk_level") is NotImplemented
    assert RiskLevel.MEDIUM.__le__("not_risk_level") is NotImplemented
    assert RiskLevel.MEDIUM.__gt__("not_risk_level") is NotImplemented
    assert RiskLevel.MEDIUM.__ge__("not_risk_level") is NotImplemented


# Missing Dependency Error Tests

def test_missing_dependency_error_basic() -> None:
    """Test MissingDependencyError basic functionality."""
    error = MissingDependencyError("redis")
    message = str(error)
    assert "redis" in message
    assert "pip install" in message
    assert "sqlspec[redis]" in message


def test_missing_dependency_error_with_install_package() -> None:
    """Test MissingDependencyError with different install package."""
    error = MissingDependencyError("redis", "redis-py")
    message = str(error)
    assert "redis" in message
    assert "sqlspec[redis-py]" in message
    assert "pip install redis-py" in message


def test_missing_dependency_error_inheritance() -> None:
    """Test MissingDependencyError inherits from both SQLSpecError and ImportError."""
    error = MissingDependencyError("package")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, ImportError)
    assert isinstance(error, Exception)


# Backend Registration Error Tests

def test_backend_not_registered_error() -> None:
    """Test BackendNotRegisteredError functionality."""
    error = BackendNotRegisteredError("s3")
    message = str(error)
    assert "s3" in message
    assert "not registered" in message
    assert isinstance(error, SQLSpecError)


# SQL Loading Error Tests

def test_sql_loading_error_default_message() -> None:
    """Test SQLLoadingError with default message."""
    error = SQLLoadingError()
    assert "Issues loading referenced SQL file" in str(error)
    assert isinstance(error, SQLSpecError)


def test_sql_loading_error_custom_message() -> None:
    """Test SQLLoadingError with custom message."""
    error = SQLLoadingError("Custom loading error")
    assert str(error) == "Custom loading error"


# SQL Parsing Error Tests

def test_sql_parsing_error_default_message() -> None:
    """Test SQLParsingError with default message."""
    error = SQLParsingError()
    assert "Issues parsing SQL statement" in str(error)
    assert isinstance(error, SQLSpecError)


def test_sql_parsing_error_custom_message() -> None:
    """Test SQLParsingError with custom message."""
    error = SQLParsingError("Custom parsing error")
    assert str(error) == "Custom parsing error"


# SQL File Parsing Error Tests

def test_sql_file_parsing_error_default_message() -> None:
    """Test SQLFileParsingError with default message."""
    error = SQLFileParsingError()
    assert "Issues parsing SQL files" in str(error)
    assert isinstance(error, SQLSpecError)


def test_sql_file_parsing_error_custom_message() -> None:
    """Test SQLFileParsingError with custom message."""
    error = SQLFileParsingError("Custom file parsing error")
    assert str(error) == "Custom file parsing error"


# SQL Builder Error Tests

def test_sql_builder_error_default_message() -> None:
    """Test SQLBuilderError with default message."""
    error = SQLBuilderError()
    assert "Issues building SQL statement" in str(error)
    assert isinstance(error, SQLSpecError)


def test_sql_builder_error_custom_message() -> None:
    """Test SQLBuilderError with custom message."""
    error = SQLBuilderError("Custom builder error")
    assert str(error) == "Custom builder error"


# SQL Compilation Error Tests

def test_sql_compilation_error_default_message() -> None:
    """Test SQLCompilationError with default message."""
    error = SQLCompilationError()
    assert "Issues compiling SQL statement" in str(error)
    assert isinstance(error, SQLSpecError)


def test_sql_compilation_error_custom_message() -> None:
    """Test SQLCompilationError with custom message."""
    error = SQLCompilationError("Custom compilation error")
    assert str(error) == "Custom compilation error"


# SQL Conversion Error Tests

def test_sql_conversion_error_default_message() -> None:
    """Test SQLConversionError with default message."""
    error = SQLConversionError()
    assert "Issues converting SQL statement" in str(error)
    assert isinstance(error, SQLSpecError)


def test_sql_conversion_error_custom_message() -> None:
    """Test SQLConversionError with custom message."""
    error = SQLConversionError("Custom conversion error")
    assert str(error) == "Custom conversion error"


# SQL Validation Error Tests

def test_sql_validation_error_basic() -> None:
    """Test SQLValidationError basic functionality."""
    error = SQLValidationError("Validation failed")
    assert str(error) == "Validation failed"
    assert error.sql is None
    assert error.risk_level == RiskLevel.MEDIUM


def test_sql_validation_error_with_sql() -> None:
    """Test SQLValidationError with SQL context."""
    sql = "SELECT * FROM users WHERE id = ?"
    error = SQLValidationError("Validation failed", sql=sql)
    message = str(error)
    assert "Validation failed" in message
    assert sql in message
    assert error.sql == sql


def test_sql_validation_error_with_risk_level() -> None:
    """Test SQLValidationError with custom risk level."""
    error = SQLValidationError("Critical issue", risk_level=RiskLevel.CRITICAL)
    assert error.risk_level == RiskLevel.CRITICAL


def test_sql_validation_error_inheritance() -> None:
    """Test SQLValidationError inherits from SQLSpecError."""
    error = SQLValidationError("Test")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, SQLValidationError)


# SQL Transformation Error Tests

def test_sql_transformation_error_basic() -> None:
    """Test SQLTransformationError basic functionality."""
    error = SQLTransformationError("Transformation failed")
    assert str(error) == "Transformation failed"
    assert error.sql is None


def test_sql_transformation_error_with_sql() -> None:
    """Test SQLTransformationError with SQL context."""
    sql = "SELECT * FROM users"
    error = SQLTransformationError("Transform error", sql=sql)
    message = str(error)
    assert "Transform error" in message
    assert sql in message
    assert error.sql == sql


def test_sql_transformation_error_inheritance() -> None:
    """Test SQLTransformationError inherits from SQLSpecError."""
    error = SQLTransformationError("Test")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, SQLTransformationError)


# SQL Injection Error Tests

def test_sql_injection_error_basic() -> None:
    """Test SQLInjectionError basic functionality."""
    error = SQLInjectionError("SQL injection detected")
    assert str(error) == "SQL injection detected"
    assert error.risk_level == RiskLevel.CRITICAL
    assert error.pattern is None


def test_sql_injection_error_with_pattern() -> None:
    """Test SQLInjectionError with injection pattern."""
    error = SQLInjectionError("Injection detected", pattern="'; DROP TABLE")
    message = str(error)
    assert "Injection detected" in message
    assert "'; DROP TABLE" in message
    assert error.pattern == "'; DROP TABLE"


def test_sql_injection_error_with_sql_and_pattern() -> None:
    """Test SQLInjectionError with SQL and pattern."""
    sql = "SELECT * FROM users WHERE name = 'test'; DROP TABLE users;"
    error = SQLInjectionError("Dangerous SQL", sql=sql, pattern="DROP TABLE")
    message = str(error)
    assert "Dangerous SQL" in message
    assert "DROP TABLE" in message
    assert sql in message


def test_sql_injection_error_inheritance() -> None:
    """Test SQLInjectionError inherits from SQLValidationError."""
    error = SQLInjectionError("Test")
    assert isinstance(error, SQLValidationError)
    assert isinstance(error, SQLSpecError)


# Unsafe SQL Error Tests

def test_unsafe_sql_error_basic() -> None:
    """Test UnsafeSQLError basic functionality."""
    error = UnsafeSQLError("Unsafe construct detected")
    assert str(error) == "Unsafe construct detected"
    assert error.risk_level == RiskLevel.HIGH
    assert error.construct is None


def test_unsafe_sql_error_with_construct() -> None:
    """Test UnsafeSQLError with construct information."""
    error = UnsafeSQLError("Unsafe detected", construct="EXEC")
    message = str(error)
    assert "Unsafe detected" in message
    assert "EXEC" in message
    assert error.construct == "EXEC"


def test_unsafe_sql_error_inheritance() -> None:
    """Test UnsafeSQLError inherits from SQLValidationError."""
    error = UnsafeSQLError("Test")
    assert isinstance(error, SQLValidationError)
    assert isinstance(error, SQLSpecError)


# Parameter Error Tests

def test_parameter_error_basic() -> None:
    """Test ParameterError basic functionality."""
    error = ParameterError("Parameter issue")
    assert str(error) == "Parameter issue"
    assert error.sql is None
    assert isinstance(error, SQLSpecError)


def test_parameter_error_with_sql() -> None:
    """Test ParameterError with SQL context."""
    sql = "SELECT * FROM users WHERE id = ?"
    error = ParameterError("Missing parameter", sql=sql)
    message = str(error)
    assert "Missing parameter" in message
    assert sql in message
    assert error.sql == sql


def test_unknown_parameter_error() -> None:
    """Test UnknownParameterError inherits correctly."""
    error = UnknownParameterError("Unknown param")
    assert isinstance(error, ParameterError)
    assert isinstance(error, SQLSpecError)


def test_missing_parameter_error() -> None:
    """Test MissingParameterError inherits correctly."""
    error = MissingParameterError("Missing param")
    assert isinstance(error, ParameterError)
    assert isinstance(error, SQLSpecError)


def test_extra_parameter_error() -> None:
    """Test ExtraParameterError inherits correctly."""
    error = ExtraParameterError("Extra param")
    assert isinstance(error, ParameterError)
    assert isinstance(error, SQLSpecError)


# Parameter Style Mismatch Error Tests

def test_parameter_style_mismatch_error_default_message() -> None:
    """Test ParameterStyleMismatchError with default message."""
    error = ParameterStyleMismatchError()
    message = str(error)
    assert "Parameter style mismatch" in message
    assert "dictionary parameters" in message
    assert error.sql is None


def test_parameter_style_mismatch_error_custom_message() -> None:
    """Test ParameterStyleMismatchError with custom message."""
    error = ParameterStyleMismatchError("Custom mismatch error")
    assert "Custom mismatch error" in str(error)


def test_parameter_style_mismatch_error_with_sql() -> None:
    """Test ParameterStyleMismatchError with SQL context."""
    sql = "SELECT * FROM users WHERE id = %s"
    error = ParameterStyleMismatchError("Style mismatch", sql=sql)
    message = str(error)
    assert "Style mismatch" in message
    assert sql in message
    assert error.sql == sql


# Repository Error Hierarchy Tests

def test_query_error_inheritance() -> None:
    """Test QueryError inherits from SQLSpecError."""
    error = QueryError("Query failed")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, QueryError)


def test_repository_error_inheritance() -> None:
    """Test RepositoryError inherits from SQLSpecError."""
    error = RepositoryError("Repository failed")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, RepositoryError)


def test_integrity_error_inheritance() -> None:
    """Test IntegrityError inherits from RepositoryError."""
    error = IntegrityError("Integrity violated")
    assert isinstance(error, RepositoryError)
    assert isinstance(error, SQLSpecError)


def test_not_found_error_inheritance() -> None:
    """Test NotFoundError inherits from RepositoryError."""
    error = NotFoundError("Entity not found")
    assert isinstance(error, RepositoryError)
    assert isinstance(error, SQLSpecError)


def test_multiple_results_found_error_inheritance() -> None:
    """Test MultipleResultsFoundError inherits from RepositoryError."""
    error = MultipleResultsFoundError("Multiple results")
    assert isinstance(error, RepositoryError)
    assert isinstance(error, SQLSpecError)


# Configuration and Serialization Error Tests

def test_improper_configuration_error() -> None:
    """Test ImproperConfigurationError inheritance."""
    error = ImproperConfigurationError("Bad config")
    assert isinstance(error, SQLSpecError)


def test_serialization_error() -> None:
    """Test SerializationError inheritance."""
    error = SerializationError("Serialization failed")
    assert isinstance(error, SQLSpecError)


# Storage Error Tests

def test_storage_operation_failed_error() -> None:
    """Test StorageOperationFailedError inheritance."""
    error = StorageOperationFailedError("Storage op failed")
    assert isinstance(error, SQLSpecError)


def test_file_not_found_in_storage_error() -> None:
    """Test FileNotFoundInStorageError inherits from StorageOperationFailedError."""
    error = FileNotFoundInStorageError("File not found")
    assert isinstance(error, StorageOperationFailedError)
    assert isinstance(error, SQLSpecError)


# SQL File Error Tests

def test_sql_file_not_found_error_basic() -> None:
    """Test SQLFileNotFoundError basic functionality."""
    error = SQLFileNotFoundError("test.sql")
    message = str(error)
    assert "test.sql" in message
    assert "not found" in message
    assert error.name == "test.sql"
    assert error.path is None


def test_sql_file_not_found_error_with_path() -> None:
    """Test SQLFileNotFoundError with path."""
    error = SQLFileNotFoundError("test.sql", "/path/to/sql")
    message = str(error)
    assert "test.sql" in message
    assert "/path/to/sql" in message
    assert error.name == "test.sql"
    assert error.path == "/path/to/sql"


def test_sql_file_not_found_error_inheritance() -> None:
    """Test SQLFileNotFoundError inherits from SQLSpecError."""
    error = SQLFileNotFoundError("test.sql")
    assert isinstance(error, SQLSpecError)


def test_sql_file_parse_error() -> None:
    """Test SQLFileParseError functionality."""
    original_error = ValueError("Parse error")
    error = SQLFileParseError("test.sql", "/path/to/test.sql", original_error)

    message = str(error)
    assert "test.sql" in message
    assert "/path/to/test.sql" in message
    assert "Parse error" in message

    assert error.name == "test.sql"
    assert error.path == "/path/to/test.sql"
    assert error.original_error is original_error
    assert isinstance(error, SQLSpecError)


# Pipeline Execution Error Tests

def test_pipeline_execution_error_basic() -> None:
    """Test PipelineExecutionError basic functionality."""
    error = PipelineExecutionError("Pipeline failed")
    assert str(error) == "Pipeline failed"
    assert error.operation_index is None
    assert error.failed_operation is None
    assert error.partial_results == []
    assert error.driver_error is None


def test_pipeline_execution_error_with_all_params() -> None:
    """Test PipelineExecutionError with all parameters."""
    original_error = ValueError("Driver error")

    class MockOperation:
        def __init__(self) -> None:
            self.sql = MockSQL()
            self.original_parameters = {"id": 1}

    class MockSQL:
        def to_sql(self) -> str:
            return "SELECT * FROM users"

    failed_op = MockOperation()
    partial_results = ["result1", "result2"]

    error = PipelineExecutionError(
        "Pipeline failed at step 2",
        operation_index=2,
        failed_operation=failed_op,
        partial_results=partial_results,
        driver_error=original_error,
    )

    assert error.operation_index == 2
    assert error.failed_operation is failed_op
    assert error.partial_results == partial_results
    assert error.driver_error is original_error


def test_pipeline_execution_error_get_failed_sql() -> None:
    """Test PipelineExecutionError get_failed_sql method."""
    class MockOperation:
        def __init__(self) -> None:
            self.sql = MockSQL()

    class MockSQL:
        def to_sql(self) -> str:
            return "SELECT * FROM users WHERE id = ?"

    failed_op = MockOperation()
    error = PipelineExecutionError("Failed", failed_operation=failed_op)

    sql = error.get_failed_sql()
    assert sql == "SELECT * FROM users WHERE id = ?"


def test_pipeline_execution_error_get_failed_sql_no_operation() -> None:
    """Test PipelineExecutionError get_failed_sql with no operation."""
    error = PipelineExecutionError("Failed")
    assert error.get_failed_sql() is None


def test_pipeline_execution_error_get_failed_sql_no_sql_attr() -> None:
    """Test PipelineExecutionError get_failed_sql with operation without sql."""
    class MockOperation:
        def __init__(self) -> None:
            pass  # No sql attribute

    failed_op = MockOperation()
    error = PipelineExecutionError("Failed", failed_operation=failed_op)
    assert error.get_failed_sql() is None


def test_pipeline_execution_error_get_failed_parameters() -> None:
    """Test PipelineExecutionError get_failed_parameters method."""
    class MockOperation:
        def __init__(self) -> None:
            self.original_parameters = {"id": 1, "name": "test"}

    failed_op = MockOperation()
    error = PipelineExecutionError("Failed", failed_operation=failed_op)

    params = error.get_failed_parameters()
    assert params == {"id": 1, "name": "test"}


def test_pipeline_execution_error_get_failed_parameters_no_operation() -> None:
    """Test PipelineExecutionError get_failed_parameters with no operation."""
    error = PipelineExecutionError("Failed")
    assert error.get_failed_parameters() is None


def test_pipeline_execution_error_get_failed_parameters_no_params_attr() -> None:
    """Test PipelineExecutionError get_failed_parameters with operation without params."""
    class MockOperation:
        def __init__(self) -> None:
            pass  # No original_parameters attribute

    failed_op = MockOperation()
    error = PipelineExecutionError("Failed", failed_operation=failed_op)
    assert error.get_failed_parameters() is None


def test_pipeline_execution_error_inheritance() -> None:
    """Test PipelineExecutionError inherits from SQLSpecError."""
    error = PipelineExecutionError("Failed")
    assert isinstance(error, SQLSpecError)


# Exception Wrapping Context Manager Tests

def test_wrap_exceptions_no_exception() -> None:
    """Test wrap_exceptions context manager when no exception occurs."""
    with wrap_exceptions():
        result = "success"

    assert result == "success"


def test_wrap_exceptions_wrap_enabled() -> None:
    """Test wrap_exceptions wraps exceptions when wrap_exceptions=True."""
    with pytest.raises(RepositoryError, match="An error occurred during the operation"):
        with wrap_exceptions(wrap_exceptions=True):
            raise ValueError("Original error")


def test_wrap_exceptions_wrap_disabled() -> None:
    """Test wrap_exceptions doesn't wrap when wrap_exceptions=False."""
    with pytest.raises(ValueError, match="Original error"):
        with wrap_exceptions(wrap_exceptions=False):
            raise ValueError("Original error")


def test_wrap_exceptions_sqlspec_error_passthrough() -> None:
    """Test wrap_exceptions doesn't wrap SQLSpecError subclasses."""
    original_error = SQLBuilderError("Builder error")

    with pytest.raises(SQLBuilderError, match="Builder error"):
        with wrap_exceptions(wrap_exceptions=True):
            raise original_error


def test_wrap_exceptions_suppress_single_exception() -> None:
    """Test wrap_exceptions can suppress specific exception types."""
    with wrap_exceptions(suppress=ValueError):
        raise ValueError("This should be suppressed")

    # If we get here, the exception was suppressed successfully


def test_wrap_exceptions_suppress_multiple_exceptions() -> None:
    """Test wrap_exceptions can suppress multiple exception types."""
    with wrap_exceptions(suppress=(ValueError, TypeError)):
        raise ValueError("This should be suppressed")

    with wrap_exceptions(suppress=(ValueError, TypeError)):
        raise TypeError("This should also be suppressed")


def test_wrap_exceptions_suppress_not_matching() -> None:
    """Test wrap_exceptions doesn't suppress non-matching exceptions."""
    with pytest.raises(RepositoryError):
        with wrap_exceptions(suppress=ValueError):
            raise TypeError("This should be wrapped, not suppressed")


def test_wrap_exceptions_suppress_takes_precedence() -> None:
    """Test that suppression takes precedence over wrapping."""
    with wrap_exceptions(wrap_exceptions=True, suppress=ValueError):
        raise ValueError("This should be suppressed, not wrapped")


def test_wrap_exceptions_chaining() -> None:
    """Test wrap_exceptions preserves original exception as cause."""
    original_error = ValueError("Original error")

    try:
        with wrap_exceptions(wrap_exceptions=True):
            raise original_error
    except RepositoryError as e:
        assert e.__cause__ is original_error
    else:
        pytest.fail("Expected RepositoryError to be raised")


# Error Message Formatting Tests

@pytest.mark.parametrize(
    "error_class,args,expected_parts",
    [
        (SQLLoadingError, (), ["Issues loading referenced SQL file"]),
        (SQLParsingError, (), ["Issues parsing SQL statement"]),
        (SQLBuilderError, (), ["Issues building SQL statement"]),
        (SQLCompilationError, (), ["Issues compiling SQL statement"]),
        (SQLConversionError, (), ["Issues converting SQL statement"]),
        (SQLFileParsingError, (), ["Issues parsing SQL files"]),
        (MissingDependencyError, ("redis",), ["redis", "pip install", "sqlspec[redis]"]),
        (BackendNotRegisteredError, ("s3",), ["s3", "not registered"]),
        (SQLFileNotFoundError, ("test.sql", "/path"), ["test.sql", "/path", "not found"]),
    ],
    ids=[
        "loading_default",
        "parsing_default",
        "builder_default",
        "compilation_default",
        "conversion_default",
        "file_parsing_default",
        "missing_dependency",
        "backend_not_registered",
        "file_not_found",
    ],
)
def test_error_message_formatting(
    error_class: type[SQLSpecError], args: tuple[Any, ...], expected_parts: list[str]
) -> None:
    """Test error message formatting for various exception types."""
    error = error_class(*args)
    message = str(error)

    for expected_part in expected_parts:
        assert expected_part in message


# Exception Hierarchy Validation Tests

def test_exception_hierarchy_validation() -> None:
    """Test that all custom exceptions properly inherit from SQLSpecError."""
    sqlspec_exceptions = [
        BackendNotRegisteredError("test"),
        SQLLoadingError(),
        SQLParsingError(),
        SQLFileParsingError(),
        SQLBuilderError(),
        SQLCompilationError(),
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
        StorageOperationFailedError("test"),
        FileNotFoundInStorageError("test"),
        SQLFileNotFoundError("test.sql"),
        SQLFileParseError("test.sql", "/path", ValueError("error")),
        PipelineExecutionError("test"),
    ]

    for error in sqlspec_exceptions:
        assert isinstance(error, SQLSpecError)
        assert isinstance(error, Exception)


# Edge Cases and Error Conditions

def test_sql_validation_error_empty_message() -> None:
    """Test SQLValidationError with empty message."""
    error = SQLValidationError("")
    # Should still work, even with empty message
    assert isinstance(error, SQLValidationError)


def test_parameter_style_mismatch_none_sql() -> None:
    """Test ParameterStyleMismatchError handles None SQL gracefully."""
    error = ParameterStyleMismatchError("Test", sql=None)
    message = str(error)
    assert "Test" in message
    # Should not crash with None SQL


def test_pipeline_execution_error_empty_partial_results() -> None:
    """Test PipelineExecutionError with explicitly empty partial results."""
    error = PipelineExecutionError("Failed", partial_results=[])
    assert error.partial_results == []


def test_complex_exception_chaining() -> None:
    """Test complex exception chaining scenarios."""
    def raise_original() -> None:
        raise ValueError("Original error")

    def raise_intermediate() -> None:
        try:
            raise_original()
        except ValueError as e:
            raise SQLParsingError("Parsing failed") from e

    try:
        raise_intermediate()
    except SQLParsingError as e:
        assert isinstance(e.__cause__, ValueError)
        assert str(e.__cause__) == "Original error"
    else:
        pytest.fail("Expected SQLParsingError")


def test_exception_with_complex_details() -> None:
    """Test exception handling with complex detail structures."""
    complex_detail = {
        "error_code": "SQL001",
        "context": {"table": "users", "operation": "SELECT"},
        "suggestions": ["Check column names", "Verify table exists"],
    }

    error = SQLSpecError("Complex error", detail=str(complex_detail))
    message = str(error)
    assert "SQL001" in message
    assert "users" in message
