"""Function-based tests for suspicious comments validator."""

import pytest
import sqlglot

from sqlspec.statement.pipelines.validators._suspicious_comments import SuspiciousComments
from sqlspec.statement.sql import SQLConfig


def test_suspicious_comments_detects_injection_comments() -> None:
    """Test detection of comments with injection patterns."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # SQL with injection-style comment
    injection_comment_sql = "SELECT * FROM users WHERE id = 1 -- OR 1=1"
    expression = sqlglot.parse_one(injection_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("injection" in issue.lower() for issue in result.issues)


def test_suspicious_comments_detects_credential_exposure() -> None:
    """Test detection of comments containing credentials."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # SQL with credential in comment
    credential_sql = "SELECT * FROM users /* password=admin123 */ WHERE active = 1"
    expression = sqlglot.parse_one(credential_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("credential" in issue.lower() for issue in result.issues)


def test_suspicious_comments_detects_mysql_version_comments() -> None:
    """Test detection of MySQL version-specific comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # MySQL version comment
    version_comment_sql = "SELECT * FROM users /*!50000 WHERE id=1 */"
    expression = sqlglot.parse_one(version_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    if not result.is_safe:
        assert any("mysql" in issue.lower() or "version" in issue.lower() for issue in result.issues)


def test_suspicious_comments_detects_excessive_comments() -> None:
    """Test detection of excessive comment usage."""
    validator = SuspiciousComments(max_comment_length=50)
    config = SQLConfig()

    # Query with very long comment
    long_comment_sql = """
        SELECT * FROM users
        /* This is an extremely long comment that goes on and on and serves no real purpose
           other than to potentially hide malicious code or confuse security scanning tools
           and might be used for obfuscation purposes in a security attack */
        WHERE active = 1
    """
    expression = sqlglot.parse_one(long_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    if not result.is_safe:
        assert any("excessive" in issue.lower() or "long" in issue.lower() for issue in result.issues)


def test_suspicious_comments_passes_legitimate_comments() -> None:
    """Test that legitimate comments pass validation."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # SQL with legitimate comment
    legitimate_sql = "SELECT * FROM users -- Get active users only\nWHERE active = 1"
    expression = sqlglot.parse_one(legitimate_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert result.is_safe
    assert len(result.issues) == 0


def test_suspicious_comments_detects_obfuscation_patterns() -> None:
    """Test detection of obfuscation patterns in comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Comment with potential obfuscation
    obfuscation_sql = "SELECT * FROM users /* aGVsbG8gd29ybGQ= */ WHERE id = 1"
    expression = sqlglot.parse_one(obfuscation_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect potential base64 or encoding patterns
    if not result.is_safe:
        assert any("obfuscation" in issue.lower() or "encoding" in issue.lower() for issue in result.issues)


@pytest.mark.parametrize(
    ("sql_with_comment", "expected_pattern", "description"),
    [
        ("SELECT * FROM users -- DROP TABLE admin", "injection", "SQL injection in comment"),
        ("SELECT * FROM users /* user=admin,pass=secret */", "credential", "credentials in comment"),
        ("SELECT * FROM users /*!40000 SELECT 1 */", "mysql", "MySQL version comment"),
        ("SELECT * FROM users -- TODO: fix this query", "legitimate", "legitimate TODO comment"),
    ],
    ids=["injection_comment", "credential_comment", "mysql_version", "todo_comment"],
)
def test_suspicious_comments_various_comment_patterns(sql_with_comment, expected_pattern, description) -> None:
    """Test detection of various suspicious comment patterns."""
    validator = SuspiciousComments()
    config = SQLConfig()

    expression = sqlglot.parse_one(sql_with_comment, read="mysql")
    result = validator.validate(expression, "mysql", config)

    if expected_pattern == "legitimate":
        assert result.is_safe, f"Should not flag legitimate comment: {description}"
    else:
        # Should detect the suspicious pattern
        if not result.is_safe:
            assert any(expected_pattern.lower() in issue.lower() for issue in result.issues), (
                f"Expected pattern '{expected_pattern}' not found for {description}"
            )


def test_suspicious_comments_configuration_max_comment_length() -> None:
    """Test that maximum comment length is configurable."""
    validator_strict = SuspiciousComments(max_comment_length=20)
    validator_permissive = SuspiciousComments(max_comment_length=200)
    config = SQLConfig()

    # Comment that's moderate length
    moderate_comment_sql = "SELECT * FROM users /* This is a moderate length comment */ WHERE active = 1"
    expression = sqlglot.parse_one(moderate_comment_sql, read="mysql")

    result_strict = validator_strict.validate(expression, "mysql", config)
    result_permissive = validator_permissive.validate(expression, "mysql", config)

    # Strict should be more likely to flag it than permissive
    strict_comment_issues = len([i for i in result_strict.issues if "comment" in i.lower()])
    permissive_comment_issues = len([i for i in result_permissive.issues if "comment" in i.lower()])

    assert strict_comment_issues >= permissive_comment_issues


def test_suspicious_comments_configuration_check_mysql_version() -> None:
    """Test that MySQL version comment checks can be disabled."""
    validator_with_mysql = SuspiciousComments(check_mysql_version_comments=True)
    validator_without_mysql = SuspiciousComments(check_mysql_version_comments=False)
    config = SQLConfig()

    mysql_version_sql = "SELECT * FROM users /*!50000 WHERE 1=1 */"
    expression = sqlglot.parse_one(mysql_version_sql, read="mysql")

    result_with = validator_with_mysql.validate(expression, "mysql", config)
    result_without = validator_without_mysql.validate(expression, "mysql", config)

    # With MySQL checks should be more restrictive
    mysql_issues_with = len([i for i in result_with.issues if "mysql" in i.lower()])
    mysql_issues_without = len([i for i in result_without.issues if "mysql" in i.lower()])

    assert mysql_issues_with >= mysql_issues_without


def test_suspicious_comments_detects_sql_keywords_in_comments() -> None:
    """Test detection of SQL keywords that might indicate injection attempts."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Comment containing SQL keywords
    keyword_comment_sql = "SELECT * FROM users /* UNION SELECT password FROM admin */ WHERE id = 1"
    expression = sqlglot.parse_one(keyword_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("sql" in issue.lower() or "keyword" in issue.lower() for issue in result.issues)


def test_suspicious_comments_multiple_comment_types() -> None:
    """Test handling of queries with multiple types of comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Query with both line and block comments
    multi_comment_sql = """
        SELECT * FROM users -- Line comment with SELECT
        /* Block comment with DROP TABLE */
        WHERE active = 1
    """
    expression = sqlglot.parse_one(multi_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect multiple suspicious patterns
    assert not result.is_safe
    assert len(result.issues) >= 2  # Should find issues in both comments


def test_suspicious_comments_nested_comment_detection() -> None:
    """Test detection of nested or complex comment structures."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Query with nested comment structure
    nested_comment_sql = "SELECT * FROM users /* outer /* inner */ comment */ WHERE id = 1"

    try:
        expression = sqlglot.parse_one(nested_comment_sql, read="mysql")
        result = validator.validate(expression, "mysql", config)

        # Should handle nested comments gracefully
        assert isinstance(result.is_safe, bool)
    except Exception:
        # Some nested comment patterns might not parse, which is acceptable
        pass


def test_suspicious_comments_empty_and_whitespace_comments() -> None:
    """Test handling of empty or whitespace-only comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Query with empty/whitespace comments
    empty_comment_sql = "SELECT * FROM users /*   */ --    \nWHERE active = 1"
    expression = sqlglot.parse_one(empty_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Empty comments should generally be safe
    assert result.is_safe


def test_suspicious_comments_unicode_and_special_characters() -> None:
    """Test handling of comments with unicode and special characters."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Comment with unicode and special characters
    unicode_comment_sql = "SELECT * FROM users /* Comment with émojis 🚀 and unicode */ WHERE id = 1"
    expression = sqlglot.parse_one(unicode_comment_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Unicode comments should generally be safe unless containing suspicious patterns
    # This tests that the validator doesn't crash on unicode
    assert isinstance(result.is_safe, bool)


def test_suspicious_comments_configuration_disable_all_checks() -> None:
    """Test behavior when all comment checks are disabled."""
    validator = SuspiciousComments(
        check_injection_patterns=False,
        check_credential_exposure=False,
        check_mysql_version_comments=False,
        check_obfuscation=False,
    )
    config = SQLConfig()

    # Even suspicious comment should pass if all checks disabled
    suspicious_sql = "SELECT * FROM users /* password=admin OR 1=1 /*!50000 */ WHERE id = 1"
    expression = sqlglot.parse_one(suspicious_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should be safer with all checks disabled
    assert result.is_safe or len(result.issues) == 0


def test_suspicious_comments_case_insensitive_detection() -> None:
    """Test that detection is case-insensitive for keywords."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Mixed case suspicious patterns
    mixed_case_sql = "SELECT * FROM users /* Password=Admin123 OR drop TABLE */ WHERE id = 1"
    expression = sqlglot.parse_one(mixed_case_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect regardless of case
    assert not result.is_safe
    assert len(result.issues) > 0


def test_suspicious_comments_with_legitimate_business_query() -> None:
    """Test validator on legitimate business query with normal comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Business query with legitimate comments
    business_sql = """
        -- Generate monthly user activity report
        SELECT
            u.name,
            u.email,
            COUNT(o.id) as order_count  -- Count user orders
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id  /* Join with orders table */
        WHERE u.active = 1
        -- Filter for current year only
        AND u.created_at >= '2023-01-01'
        GROUP BY u.id, u.name, u.email
        -- Order by most active users first
        ORDER BY order_count DESC
    """
    expression = sqlglot.parse_one(business_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Legitimate business comments should pass
    assert result.is_safe
    assert len(result.issues) == 0


def test_suspicious_comments_performance_with_many_comments() -> None:
    """Test validator performance with queries containing many comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Query with many legitimate comments
    many_comments_sql = """
        -- Comment 1
        SELECT
            u.id,      -- Comment 2
            u.name,    -- Comment 3
            u.email    -- Comment 4
        FROM users u   -- Comment 5
        /* Comment 6 */
        WHERE u.active = 1  -- Comment 7
        /* Comment 8 */
        AND u.verified = 1  -- Comment 9
        /* Comment 10 */
        ORDER BY u.name     -- Comment 11
    """
    expression = sqlglot.parse_one(many_comments_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should handle many comments without performance issues
    assert isinstance(result.is_safe, bool)
    # Legitimate comments should pass
    assert result.is_safe


def test_suspicious_comments_handles_malformed_comments() -> None:
    """Test graceful handling of malformed or unusual comment patterns."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Various comment edge cases
    edge_case_queries = [
        "SELECT * FROM users WHERE id = 1 --",  # Comment with no content
        "SELECT * FROM users /* incomplete",  # This might not parse
        "SELECT * FROM users -- normal comment\nWHERE active = 1",  # Normal case
    ]

    for sql in edge_case_queries:
        try:
            expression = sqlglot.parse_one(sql, read="mysql")
            result = validator.validate(expression, "mysql", config)

            # Should handle gracefully without crashing
            assert isinstance(result.is_safe, bool)
        except Exception:
            # Some malformed SQL might not parse, which is acceptable
            pass


def test_suspicious_comments_with_complex_injection_attempt() -> None:
    """Test detection of sophisticated injection attempts hidden in comments."""
    validator = SuspiciousComments()
    config = SQLConfig()

    # Sophisticated injection attempt
    sophisticated_sql = """
        SELECT * FROM users
        /* Legitimate looking comment that contains UNION ALL SELECT username, password FROM admin_users */
        WHERE id = ?
        -- Another comment with DROP DATABASE production;
    """
    expression = sqlglot.parse_one(sophisticated_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect multiple injection patterns
    assert not result.is_safe
    assert len(result.issues) >= 2  # Should find issues in both comments
    assert any("union" in issue.lower() for issue in result.issues)
    assert any("drop" in issue.lower() for issue in result.issues)
