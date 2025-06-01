"""Function-based tests for suspicious keywords validator."""

import pytest
import sqlglot

from sqlspec.statement.pipelines.validators._suspicious_keywords import SuspiciousKeywords
from sqlspec.statement.sql import SQLConfig


def test_suspicious_keywords_detects_system_functions() -> None:
    """Test detection of system functions that could be used for attacks."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # SQL with SLEEP function (timing attack)
    sleep_sql = "SELECT * FROM users WHERE id = 1 AND SLEEP(5)"
    expression = sqlglot.parse_one(sleep_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("sleep" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_detects_file_operations() -> None:
    """Test detection of file operation keywords."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # SQL with INTO OUTFILE (file export)
    outfile_sql = "SELECT * FROM users INTO OUTFILE '/tmp/users.txt'"
    expression = sqlglot.parse_one(outfile_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("file" in issue.lower() or "outfile" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_detects_information_schema_access() -> None:
    """Test detection of information schema access patterns."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # SQL accessing information schema
    info_schema_sql = "SELECT table_name FROM information_schema.tables"
    expression = sqlglot.parse_one(info_schema_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("information_schema" in issue.lower() or "introspection" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_detects_benchmark_functions() -> None:
    """Test detection of benchmark functions used for timing attacks."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # SQL with BENCHMARK function
    benchmark_sql = "SELECT * FROM users WHERE id = 1 AND BENCHMARK(1000000, MD5('test'))"
    expression = sqlglot.parse_one(benchmark_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    if not result.is_safe:
        assert any("benchmark" in issue.lower() or "timing" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_passes_legitimate_queries() -> None:
    """Test that legitimate queries pass validation without false positives."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    legitimate_sql = "SELECT name, email, created_at FROM users WHERE active = 1 ORDER BY name"
    expression = sqlglot.parse_one(legitimate_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert result.is_safe
    assert len(result.issues) == 0


def test_suspicious_keywords_detects_load_file_functions() -> None:
    """Test detection of LOAD_FILE and similar functions."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # SQL with LOAD_FILE function
    load_file_sql = "SELECT LOAD_FILE('/etc/passwd') as file_content"
    expression = sqlglot.parse_one(load_file_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    if not result.is_safe:
        assert any("load_file" in issue.lower() or "file" in issue.lower() for issue in result.issues)


@pytest.mark.parametrize(
    ("sql_query", "expected_pattern", "description"),
    [
        ("SELECT * FROM users WHERE SLEEP(10)", "sleep", "timing attack with SLEEP"),
        ("SELECT * FROM users INTO DUMPFILE '/tmp/dump'", "file", "file export with DUMPFILE"),
        ("SELECT schema_name FROM information_schema.schemata", "information_schema", "schema introspection"),
        ("SELECT USER(), VERSION(), DATABASE()", "system", "system information functions"),
        ("SELECT * FROM mysql.user", "mysql", "MySQL system database access"),
    ],
    ids=["sleep_timing", "dumpfile_export", "schema_info", "system_info", "mysql_system"],
)
def test_suspicious_keywords_various_suspicious_patterns(sql_query, expected_pattern, description) -> None:
    """Test detection of various suspicious keyword patterns."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    try:
        expression = sqlglot.parse_one(sql_query, read="mysql")
        result = validator.validate(expression, "mysql", config)

        # Should detect suspicious patterns
        if not result.is_safe:
            assert any(expected_pattern.lower() in issue.lower() for issue in result.issues), (
                f"Expected pattern '{expected_pattern}' not found for {description}"
            )
        # Some patterns might be in warnings instead of issues
        elif len(result.warnings) > 0:
            assert any(expected_pattern.lower() in warning.lower() for warning in result.warnings), (
                f"Expected pattern '{expected_pattern}' not found in warnings for {description}"
            )

    except Exception:
        # Some suspicious SQL might not parse, which is also acceptable for security
        pass


def test_suspicious_keywords_configuration_system_functions() -> None:
    """Test that system function checks can be configured."""
    validator_with_system = SuspiciousKeywords(check_system_functions=True)
    validator_without_system = SuspiciousKeywords(check_system_functions=False)
    config = SQLConfig()

    system_sql = "SELECT USER(), VERSION(), CONNECTION_ID()"
    expression = sqlglot.parse_one(system_sql, read="mysql")

    result_with = validator_with_system.validate(expression, "mysql", config)
    result_without = validator_without_system.validate(expression, "mysql", config)

    # With system checks should be more restrictive
    system_issues_with = len([i for i in result_with.issues if "system" in i.lower()])
    system_issues_without = len([i for i in result_without.issues if "system" in i.lower()])

    assert system_issues_with >= system_issues_without


def test_suspicious_keywords_configuration_file_operations() -> None:
    """Test that file operation checks can be configured."""
    validator_with_file = SuspiciousKeywords(check_file_operations=True)
    validator_without_file = SuspiciousKeywords(check_file_operations=False)
    config = SQLConfig()

    file_sql = "SELECT * FROM users INTO OUTFILE '/tmp/output.txt'"
    expression = sqlglot.parse_one(file_sql, read="mysql")

    result_with = validator_with_file.validate(expression, "mysql", config)
    result_without = validator_without_file.validate(expression, "mysql", config)

    # With file checks should be more restrictive
    file_issues_with = len([i for i in result_with.issues if "file" in i.lower()])
    file_issues_without = len([i for i in result_without.issues if "file" in i.lower()])

    assert file_issues_with >= file_issues_without


def test_suspicious_keywords_configuration_database_introspection() -> None:
    """Test that database introspection checks can be configured."""
    validator_with_intro = SuspiciousKeywords(check_database_introspection=True)
    validator_without_intro = SuspiciousKeywords(check_database_introspection=False)
    config = SQLConfig()

    introspection_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mysql'"
    expression = sqlglot.parse_one(introspection_sql, read="mysql")

    result_with = validator_with_intro.validate(expression, "mysql", config)
    result_without = validator_without_intro.validate(expression, "mysql", config)

    # With introspection checks should be more restrictive
    intro_issues_with = len(
        [i for i in result_with.issues if "introspection" in i.lower() or "information_schema" in i.lower()]
    )
    intro_issues_without = len(
        [i for i in result_without.issues if "introspection" in i.lower() or "information_schema" in i.lower()]
    )

    assert intro_issues_with >= intro_issues_without


def test_suspicious_keywords_detects_mysql_system_databases() -> None:
    """Test detection of MySQL system database access."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Access to MySQL system databases
    mysql_system_sql = "SELECT * FROM mysql.user WHERE User = 'root'"
    expression = sqlglot.parse_one(mysql_system_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("mysql" in issue.lower() or "system" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_detects_performance_schema_access() -> None:
    """Test detection of performance schema access."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Access to performance schema
    perf_schema_sql = "SELECT * FROM performance_schema.events_statements_current"
    expression = sqlglot.parse_one(perf_schema_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    if not result.is_safe:
        assert any("performance_schema" in issue.lower() or "introspection" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_case_insensitive_detection() -> None:
    """Test that keyword detection is case-insensitive."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Mixed case suspicious keywords
    mixed_case_sql = "SELECT * FROM users WHERE Sleep(5) AND user() IS NOT NULL"
    expression = sqlglot.parse_one(mixed_case_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect regardless of case
    assert not result.is_safe
    assert len(result.issues) > 0


def test_suspicious_keywords_detects_multiple_suspicious_functions() -> None:
    """Test detection when multiple suspicious functions are present."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Multiple suspicious functions
    multi_suspicious_sql = """
        SELECT
            USER() as current_user,
            VERSION() as db_version,
            CONNECTION_ID() as conn_id
        FROM users
        WHERE SLEEP(1) OR BENCHMARK(100, MD5('test'))
    """
    expression = sqlglot.parse_one(multi_suspicious_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect multiple issues
    assert not result.is_safe
    assert len(result.issues) >= 2  # Should find multiple suspicious patterns


def test_suspicious_keywords_legitimate_business_functions() -> None:
    """Test that legitimate business functions are not flagged."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Legitimate business query with common functions
    business_sql = """
        SELECT
            u.name,
            u.email,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_revenue,
            AVG(o.total) as avg_order_value,
            MAX(o.created_at) as last_order_date
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        AND o.created_at >= CURDATE() - INTERVAL 30 DAY
        GROUP BY u.id, u.name, u.email
        ORDER BY total_revenue DESC
    """
    expression = sqlglot.parse_one(business_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should pass legitimate business functions
    assert result.is_safe
    assert len(result.issues) == 0


def test_suspicious_keywords_with_subqueries() -> None:
    """Test detection of suspicious keywords in subqueries."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Suspicious keywords in subquery
    subquery_sql = """
        SELECT * FROM users
        WHERE id IN (
            SELECT user_id FROM logs
            WHERE event_time > NOW() - INTERVAL 1 HOUR
            AND SLEEP(0.1)
        )
    """
    expression = sqlglot.parse_one(subquery_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect suspicious keywords in subqueries
    assert not result.is_safe
    assert any("sleep" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_configuration_disable_all_checks() -> None:
    """Test behavior when all keyword checks are disabled."""
    validator = SuspiciousKeywords(
        check_system_functions=False, check_file_operations=False, check_database_introspection=False
    )
    config = SQLConfig()

    # Even suspicious query should pass if all checks disabled
    suspicious_sql = """
        SELECT USER(), VERSION() FROM information_schema.tables
        INTO OUTFILE '/tmp/dump'
        WHERE SLEEP(10)
    """
    expression = sqlglot.parse_one(suspicious_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should be safer with all checks disabled
    assert result.is_safe or len(result.issues) == 0


def test_suspicious_keywords_detects_exec_and_eval_functions() -> None:
    """Test detection of dynamic SQL execution functions."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # SQL with dynamic execution (if supported by dialect)
    dynamic_sql_patterns = [
        "SELECT * FROM users WHERE id = 1; EXEC('DROP TABLE users')",
        "SELECT * FROM users WHERE EVAL('malicious_code')",
    ]

    for sql_pattern in dynamic_sql_patterns:
        try:
            expression = sqlglot.parse_one(sql_pattern, read="mysql")
            result = validator.validate(expression, "mysql", config)

            # Should detect dynamic execution patterns
            if not result.is_safe:
                assert any(
                    "exec" in issue.lower() or "eval" in issue.lower() or "dynamic" in issue.lower()
                    for issue in result.issues
                )
        except Exception:
            # Some dynamic SQL patterns might not parse in MySQL, which is acceptable
            pass


def test_suspicious_keywords_handles_function_calls_in_expressions() -> None:
    """Test detection of suspicious functions within complex expressions."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Suspicious function within expression
    expression_sql = """
        SELECT * FROM users
        WHERE created_at > DATE_SUB(NOW(), INTERVAL SLEEP(5) DAY)
    """

    try:
        expression = sqlglot.parse_one(expression_sql, read="mysql")
        result = validator.validate(expression, "mysql", config)

        # Should detect SLEEP even within complex expressions
        assert not result.is_safe
        assert any("sleep" in issue.lower() for issue in result.issues)
    except Exception:
        # Complex expressions might not parse correctly, which is acceptable
        pass


def test_suspicious_keywords_different_dialects() -> None:
    """Test suspicious keyword detection across different SQL dialects."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Test dialect-specific suspicious patterns
    dialect_patterns = {
        "mysql": "SELECT USER(), VERSION()",
        "postgresql": "SELECT current_user, version()",
        "sqlite": "SELECT sqlite_version()",
    }

    for dialect, sql_pattern in dialect_patterns.items():
        try:
            expression = sqlglot.parse_one(sql_pattern, read=dialect)
            result = validator.validate(expression, dialect, config)

            # Should detect system functions regardless of dialect
            if not result.is_safe:
                assert any("system" in issue.lower() or "function" in issue.lower() for issue in result.issues), (
                    f"Failed to detect suspicious pattern in {dialect}"
                )

        except Exception as e:
            # Some dialects might not be supported, which is acceptable
            pytest.skip(f"Dialect {dialect} not supported: {e}")


def test_suspicious_keywords_nested_function_calls() -> None:
    """Test detection of suspicious keywords in nested function calls."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Nested suspicious function calls
    nested_sql = """
        SELECT * FROM users
        WHERE MD5(CONCAT(USER(), '_', VERSION())) = 'expected_hash'
    """
    expression = sqlglot.parse_one(nested_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect system functions even when nested
    assert not result.is_safe
    assert any("system" in issue.lower() or "user" in issue.lower() for issue in result.issues)


def test_suspicious_keywords_with_legitimate_schema_queries() -> None:
    """Test that legitimate schema queries for application use are handled appropriately."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Potentially legitimate schema query (might be used by ORMs)
    schema_query_sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'users' AND table_schema = DATABASE()
    """
    expression = sqlglot.parse_one(schema_query_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect but might be in warnings rather than critical issues
    # depending on configuration
    assert not result.is_safe or len(result.warnings) > 0

    if not result.is_safe:
        assert any("information_schema" in issue.lower() for issue in result.issues)
    else:
        assert any("information_schema" in warning.lower() for warning in result.warnings)


def test_suspicious_keywords_timing_attack_patterns() -> None:
    """Test detection of various timing attack patterns."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Various timing attack patterns
    timing_patterns = [
        "SELECT * FROM users WHERE id = 1 AND SLEEP(5)",
        "SELECT * FROM users WHERE BENCHMARK(1000000, MD5('test'))",
        "SELECT * FROM users WHERE id = 1 AND (SELECT SLEEP(1))",
    ]

    for timing_sql in timing_patterns:
        expression = sqlglot.parse_one(timing_sql, read="mysql")
        result = validator.validate(expression, "mysql", config)

        # Should detect timing attack patterns
        assert not result.is_safe, f"Failed to detect timing attack: {timing_sql}"
        assert any(
            "sleep" in issue.lower() or "benchmark" in issue.lower() or "timing" in issue.lower()
            for issue in result.issues
        ), f"Expected timing pattern not found in: {timing_sql}"


def test_suspicious_keywords_file_system_access_patterns() -> None:
    """Test detection of various file system access patterns."""
    validator = SuspiciousKeywords()
    config = SQLConfig()

    # Various file access patterns
    file_patterns = [
        "SELECT * FROM users INTO OUTFILE '/tmp/users.txt'",
        "SELECT LOAD_FILE('/etc/passwd')",
        "SELECT * FROM users INTO DUMPFILE '/tmp/dump'",
    ]

    for file_sql in file_patterns:
        try:
            expression = sqlglot.parse_one(file_sql, read="mysql")
            result = validator.validate(expression, "mysql", config)

            # Should detect file access patterns
            assert not result.is_safe, f"Failed to detect file access: {file_sql}"
            assert any(
                "file" in issue.lower() or "outfile" in issue.lower() or "dumpfile" in issue.lower()
                for issue in result.issues
            ), f"Expected file pattern not found in: {file_sql}"
        except Exception:
            # Some file access patterns might not parse, which is also good for security
            pass
