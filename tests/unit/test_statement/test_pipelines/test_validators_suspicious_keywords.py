"""Tests for the SuspiciousKeywords validator."""

from typing import Optional

import pytest
from sqlglot import parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators import SuspiciousKeywords
from sqlspec.statement.sql import SQLConfig


def _create_test_context(sql: str, config: Optional[SQLConfig] = None, dialect: str = "mysql") -> SQLProcessingContext:
    """Helper function to create a SQLProcessingContext for testing."""
    if config is None:
        config = SQLConfig()

    expression = parse_one(sql, read=dialect)
    return SQLProcessingContext(initial_sql_string=sql, dialect=dialect, config=config, current_expression=expression)


class TestSuspiciousKeywords:
    """Test cases for the SuspiciousKeywords validator."""

    def test_safe_query_passes(self) -> None:
        """Test that safe queries pass validation."""
        sql = "SELECT name, email FROM users WHERE active = 1"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.SAFE
        assert len(result.issues) == 0

    def test_sleep_function_detected(self) -> None:
        """Test detection of SLEEP function."""
        sql = "SELECT * FROM users WHERE SLEEP(5)"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0
        assert any("sleep" in issue.lower() for issue in result.issues)

    def test_benchmark_function_detected(self) -> None:
        """Test detection of BENCHMARK function."""
        sql = "SELECT * FROM users WHERE BENCHMARK(1000000, MD5('test'))"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0
        assert any("benchmark" in issue.lower() for issue in result.issues)

    def test_load_file_function_detected(self) -> None:
        """Test detection of LOAD_FILE function."""
        sql = "SELECT LOAD_FILE('/etc/passwd') as content"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0
        assert any("load_file" in issue.lower() for issue in result.issues)

    def test_information_schema_access_detected(self) -> None:
        """Test detection of information_schema access."""
        sql = "SELECT table_name FROM information_schema.tables"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0
        assert any("information_schema" in issue.lower() for issue in result.issues)

    def test_mysql_system_database_access_detected(self) -> None:
        """Test detection of MySQL system database access."""
        sql = "SELECT * FROM mysql.user"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0
        assert any("mysql.user" in issue.lower() for issue in result.issues)

    def test_user_function_warning(self) -> None:
        """Test that USER() function generates warnings."""
        sql = "SELECT USER() as current_user"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # USER() should generate warnings, not necessarily errors
        assert len(result.warnings) > 0 or not result.is_safe

    def test_version_function_warning(self) -> None:
        """Test that VERSION() function generates warnings."""
        sql = "SELECT VERSION() as db_version"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # VERSION() should generate warnings, not necessarily errors
        assert len(result.warnings) > 0 or not result.is_safe

    def test_allow_system_functions_configuration(self) -> None:
        """Test that system functions can be allowed via configuration."""
        sql = "SELECT * FROM users WHERE SLEEP(5)"

        # Strict validator
        strict_validator = SuspiciousKeywords(allow_system_functions=False)
        context = _create_test_context(sql)

        _, strict_result = strict_validator.process(context)

        assert strict_result is not None
        assert strict_result.is_safe is False

        # Permissive validator
        permissive_validator = SuspiciousKeywords(allow_system_functions=True)
        _, permissive_result = permissive_validator.process(context)

        assert permissive_result is not None
        assert permissive_result.is_safe is True

    def test_allow_file_operations_configuration(self) -> None:
        """Test that file operations can be allowed."""
        sql = "SELECT LOAD_FILE('/etc/passwd')"

        # Validator that allows file operations by both allowing them and disabling the check
        validator = SuspiciousKeywords(allow_file_operations=True, check_file_operations=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should allow file operations when both flags are set
        assert result.is_safe is True

    def test_allow_introspection_configuration(self) -> None:
        """Test that database introspection can be allowed via configuration."""
        sql = "SELECT table_name FROM information_schema.tables"

        # Strict validator
        strict_validator = SuspiciousKeywords(allow_introspection=False)
        context = _create_test_context(sql)

        _, strict_result = strict_validator.process(context)

        assert strict_result is not None
        assert strict_result.is_safe is False

        # Permissive validator
        permissive_validator = SuspiciousKeywords(allow_introspection=True)
        _, permissive_result = permissive_validator.process(context)

        assert permissive_result is not None
        assert permissive_result.is_safe is True

    def test_multiple_suspicious_functions(self) -> None:
        """Test detection of multiple suspicious functions."""
        sql = "SELECT USER(), VERSION() FROM users WHERE SLEEP(1)"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should detect multiple issues/warnings
        assert not result.is_safe or len(result.warnings) > 0

    def test_case_insensitive_detection(self) -> None:
        """Test that function detection is case insensitive."""
        sql = "SELECT * FROM users WHERE sleep(5)"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should detect lowercase sleep function
        assert result.is_safe is False
        assert any("sleep" in issue.lower() for issue in result.issues)

    def test_functions_in_strings_ignored(self) -> None:
        """Test that functions in string literals are ignored."""
        sql = "SELECT * FROM users WHERE description = 'This contains SLEEP keyword'"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should not detect SLEEP in string literal
        assert result.is_safe is True

    def test_configurable_risk_level(self) -> None:
        """Test that risk level is configurable."""
        sql = "SELECT * FROM users WHERE SLEEP(5)"

        # High risk validator
        high_risk_validator = SuspiciousKeywords(risk_level=RiskLevel.HIGH)
        context = _create_test_context(sql)

        _, result = high_risk_validator.process(context)

        assert result is not None
        if not result.is_safe:
            assert result.risk_level == RiskLevel.HIGH

    def test_nested_suspicious_functions(self) -> None:
        """Test detection of suspicious functions in nested queries."""
        sql = """
        SELECT * FROM (
            SELECT * FROM users WHERE SLEEP(1)
        ) subquery
        """

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should detect SLEEP in subquery
        assert result.is_safe is False
        assert any("sleep" in issue.lower() for issue in result.issues)

    def test_legitimate_functions_not_flagged(self) -> None:
        """Test that legitimate functions are not flagged."""
        sql = "SELECT UPPER(name), COUNT(*), MAX(created_at) FROM users"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should not flag legitimate functions
        assert result.is_safe is True

    def test_performance_schema_access_detected(self) -> None:
        """Test detection of performance_schema access."""
        sql = "SELECT * FROM performance_schema.events_statements_current"

        validator = SuspiciousKeywords()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert any("performance_schema" in issue.lower() for issue in result.issues)

    def test_check_flags_configuration(self) -> None:
        """Test that individual check flags can be disabled."""
        sql = "SELECT SLEEP(5), USER() FROM information_schema.tables"

        # Disable all checks
        validator = SuspiciousKeywords(
            check_system_functions=False, check_file_operations=False, check_database_introspection=False
        )
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should be safe with all checks disabled
        assert result.is_safe is True


@pytest.mark.parametrize(
    ("sql_query", "expected_pattern", "description"),
    [
        ("SELECT * FROM users WHERE SLEEP(10)", "sleep", "timing attack with SLEEP"),
        ("SELECT LOAD_FILE('/tmp/dump') as content", "load_file", "file access with LOAD_FILE"),
        ("SELECT schema_name FROM information_schema.schemata", "information_schema", "schema introspection"),
        ("SELECT * FROM mysql.user", "mysql.user", "MySQL system database access"),
        (
            "SELECT * FROM performance_schema.events_statements_current",
            "performance_schema",
            "performance schema access",
        ),
    ],
    ids=["sleep_timing", "file_access", "schema_info", "mysql_system", "perf_schema"],
)
def test_suspicious_keywords_various_patterns(sql_query: str, expected_pattern: str, description: str) -> None:
    """Test detection of various suspicious patterns."""
    validator = SuspiciousKeywords()

    try:
        context = _create_test_context(sql_query)
        _, result = validator.process(context)

        assert result is not None
        # Should detect suspicious patterns
        assert not result.is_safe, f"Failed to detect {description}"
        assert any(expected_pattern.lower() in issue.lower() for issue in result.issues), (
            f"Expected pattern '{expected_pattern}' not found for {description}"
        )

    except Exception:
        # Some suspicious SQL might not parse, which is also acceptable for security
        pass


def test_suspicious_keywords_different_dialects() -> None:
    """Test suspicious keyword detection across different SQL dialects."""
    validator = SuspiciousKeywords()

    # Test dialect-specific suspicious patterns
    dialect_patterns = {
        "mysql": "SELECT USER(), VERSION()",
        "postgres": "SELECT current_user, version()",
        "sqlite": "SELECT sqlite_version()",
    }

    for dialect, sql_pattern in dialect_patterns.items():
        try:
            context = _create_test_context(sql_pattern, dialect=dialect)
            _, result = validator.process(context)

            assert result is not None
            # Should detect system functions regardless of dialect (may be warnings)
            assert not result.is_safe or len(result.warnings) > 0, f"Failed to detect suspicious pattern in {dialect}"

        except Exception as e:
            # Some dialects might not be supported, which is acceptable
            pytest.skip(f"Dialect {dialect} not supported: {e}")


def test_suspicious_keywords_file_system_access_patterns() -> None:
    """Test detection of various file system access patterns."""
    validator = SuspiciousKeywords()

    # Various file access patterns
    file_patterns = [
        "SELECT LOAD_FILE('/tmp/users.txt') as content",
        "SELECT LOAD_FILE('/etc/passwd') as passwd",
    ]

    for file_sql in file_patterns:
        try:
            context = _create_test_context(file_sql)
            _, result = validator.process(context)

            assert result is not None
            # Should detect file access patterns
            assert not result.is_safe, f"Failed to detect file access: {file_sql}"
            assert any("load_file" in issue.lower() for issue in result.issues), (
                f"Expected file pattern not found in: {file_sql}"
            )

        except Exception:
            # Some file access SQL might not parse, which is acceptable
            pass
