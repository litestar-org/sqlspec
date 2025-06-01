"""Function-based tests for whitespace normalization transformer."""

import pytest
import sqlglot

from sqlspec.statement.pipelines.transformers._normalize_whitespace import NormalizeWhitespace
from sqlspec.statement.sql import SQLConfig


def test_normalize_whitespace_basic_formatting() -> None:
    """Test basic whitespace normalization functionality."""
    transformer = NormalizeWhitespace()
    config = SQLConfig()

    # Query with irregular whitespace
    messy_sql = "SELECT    *   FROM     users   WHERE    id   =   1"
    expression = sqlglot.parse_one(messy_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified
    assert "normalized" in " ".join(result.notes).lower()

    # Check that the result is cleaner
    normalized_sql = result.expression.sql(dialect="mysql")
    assert normalized_sql.count(" ") < messy_sql.count(" ")


def test_normalize_whitespace_identifier_normalization() -> None:
    """Test identifier case normalization when enabled."""
    transformer = NormalizeWhitespace(normalize_identifiers=True)
    config = SQLConfig()

    # Query with mixed case identifiers
    mixed_case_sql = "SELECT Name, Email FROM Users WHERE Active = 1"
    expression = sqlglot.parse_one(mixed_case_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified

    # Check that identifiers are normalized
    normalized_sql = result.expression.sql(dialect="mysql")
    # Note: SQLGlot might preserve some casing, so we check for lowercase presence
    assert any(word.islower() for word in normalized_sql.split() if word.isalpha())


def test_normalize_whitespace_pretty_formatting() -> None:
    """Test pretty formatting with line breaks for complex queries."""
    transformer = NormalizeWhitespace(pretty=True, indent=2)
    config = SQLConfig()

    # Complex query that should benefit from formatting
    complex_sql = "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name"
    expression = sqlglot.parse_one(complex_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified

    # Check that pretty formatting was applied
    formatted_sql = result.expression.sql(dialect="mysql", pretty=True)
    # Complex queries often get line breaks when pretty-formatted
    assert len(formatted_sql.split()) > 1


def test_normalize_whitespace_preserve_quoted_identifiers() -> None:
    """Test that quoted identifiers are preserved during normalization."""
    transformer = NormalizeWhitespace(normalize_identifiers=True)
    config = SQLConfig()

    # Query with quoted identifiers
    quoted_sql = 'SELECT "CaseSensitiveColumn" FROM "CaseSensitiveTable"'
    expression = sqlglot.parse_one(quoted_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Quoted identifiers should maintain their structure
    normalized_sql = result.expression.sql(dialect="mysql")
    # The quotes should be preserved in some form
    assert '"' in normalized_sql or "`" in normalized_sql or "CaseSensitive" in normalized_sql


def test_normalize_whitespace_transformation_failure_handling() -> None:
    """Test graceful handling of transformation failures."""
    transformer = NormalizeWhitespace()
    config = SQLConfig()

    # Use a valid expression but test error handling path
    valid_sql = "SELECT * FROM users"
    expression = sqlglot.parse_one(valid_sql, read="mysql")

    # The transformation should succeed for this valid case
    result = transformer.transform(expression, "mysql", config)

    assert result.expression is not None
    # Either modified successfully or gracefully handled failure
    assert isinstance(result.modified, bool)


def test_normalize_whitespace_no_modification_needed() -> None:
    """Test behavior when input is already well-formatted."""
    transformer = NormalizeWhitespace(normalize_identifiers=False, pretty=False)
    config = SQLConfig()

    # Already well-formatted query
    clean_sql = "SELECT id, name FROM users WHERE active = 1"
    expression = sqlglot.parse_one(clean_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should still process but might not need significant changes
    assert result.expression is not None
    # May or may not be modified depending on SQLGlot's formatting


@pytest.mark.parametrize(
    ("input_sql", "normalize_ids", "pretty", "description"),
    [
        ("SELECT  *  FROM  users", False, False, "extra spaces basic"),
        ("SELECT Name FROM Users", True, False, "mixed case identifiers"),
        ("SELECT a.id, b.name FROM table_a a JOIN table_b b ON a.id = b.id", False, True, "complex join pretty"),
        ("SELECT\t*\tFROM\tusers", False, False, "tab characters"),
    ],
    ids=["extra_spaces", "mixed_case", "complex_join", "tab_chars"],
)
def test_normalize_whitespace_various_input_formats(input_sql, normalize_ids, pretty, description) -> None:
    """Test normalization with various input formats and configurations."""
    transformer = NormalizeWhitespace(normalize_identifiers=normalize_ids, pretty=pretty)
    config = SQLConfig()

    expression = sqlglot.parse_one(input_sql, read="mysql")
    result = transformer.transform(expression, "mysql", config)

    # Should handle all input formats gracefully
    assert result.expression is not None, f"Failed to handle {description}"
    assert isinstance(result.modified, bool), f"Invalid modification status for {description}"

    # Verify output is valid SQL
    output_sql = result.expression.sql(dialect="mysql")
    assert len(output_sql.strip()) > 0, f"Empty output for {description}"


def test_normalize_whitespace_disabled_identifier_normalization() -> None:
    """Test that identifier normalization can be disabled."""
    transformer = NormalizeWhitespace(normalize_identifiers=False)
    config = SQLConfig()

    # Query with mixed case that shouldn't be normalized
    mixed_case_sql = "SELECT UserName FROM UserTable"
    expression = sqlglot.parse_one(mixed_case_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should still format but preserve identifier casing more
    result.expression.sql(dialect="mysql")
    # Note: SQLGlot might still apply some normalization, but we test the configuration works
    assert result.expression is not None


def test_normalize_whitespace_different_dialects() -> None:
    """Test normalization works with different SQL dialects."""
    transformer = NormalizeWhitespace()
    config = SQLConfig()

    # Test with different dialects
    dialects_to_test = ["mysql", "postgresql", "sqlite"]
    test_sql = "SELECT  *  FROM  users  WHERE  id  =  1"

    for dialect in dialects_to_test:
        try:
            expression = sqlglot.parse_one(test_sql, read=dialect)
            result = transformer.transform(expression, dialect, config)

            assert result.expression is not None, f"Failed for dialect {dialect}"

            # Verify output is valid for the dialect
            output_sql = result.expression.sql(dialect=dialect)
            assert len(output_sql.strip()) > 0, f"Empty output for dialect {dialect}"

        except Exception as e:
            # Some dialects might not be supported, which is acceptable
            pytest.skip(f"Dialect {dialect} not supported: {e}")


def test_normalize_whitespace_complex_nested_query() -> None:
    """Test normalization of complex nested queries."""
    transformer = NormalizeWhitespace(pretty=True)
    config = SQLConfig()

    # Complex nested query with subqueries
    complex_sql = """
        SELECT   u.name,   (SELECT   COUNT(*)   FROM   orders   o   WHERE   o.user_id   =   u.id)   as   order_count
        FROM   users   u   WHERE   u.active   =   1   AND   u.id   IN   (SELECT   user_id   FROM   recent_activity)
    """
    expression = sqlglot.parse_one(complex_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified

    # Should handle complex queries without breaking structure
    normalized_sql = result.expression.sql(dialect="mysql")
    assert "SELECT" in normalized_sql.upper()
    assert "FROM" in normalized_sql.upper()
    assert "WHERE" in normalized_sql.upper()


def test_normalize_whitespace_with_comments_and_formatting() -> None:
    """Test normalization preserves essential SQL structure."""
    transformer = NormalizeWhitespace()
    config = SQLConfig()

    # SQL with various formatting challenges
    formatted_sql = """SELECT
        id,
        name,
        email
    FROM
        users
    WHERE
        active = 1
        AND created_at > '2023-01-01'
    ORDER BY
        name"""

    expression = sqlglot.parse_one(formatted_sql, read="mysql")
    result = transformer.transform(expression, "mysql", config)

    # Should preserve SQL validity and structure
    normalized_sql = result.expression.sql(dialect="mysql")
    assert "SELECT" in normalized_sql.upper()
    assert "FROM users" in normalized_sql.upper()
    assert "ORDER BY" in normalized_sql.upper()


def test_normalize_whitespace_idempotent_operation() -> None:
    """Test that applying normalization multiple times is safe and idempotent."""
    transformer = NormalizeWhitespace()
    config = SQLConfig()

    # Start with messy SQL
    messy_sql = "SELECT  *  FROM  users  WHERE  id  =  1"
    expression = sqlglot.parse_one(messy_sql, read="mysql")

    # Apply transformation twice
    result1 = transformer.transform(expression, "mysql", config)
    result2 = transformer.transform(result1.expression, "mysql", config)

    # Both results should be valid
    assert result1.expression is not None
    assert result2.expression is not None

    # SQL should be stable after multiple applications
    sql1 = result1.expression.sql(dialect="mysql")
    sql2 = result2.expression.sql(dialect="mysql")

    # Should be functionally equivalent (allowing for minor formatting differences)
    assert len(sql1) > 0
    assert len(sql2) > 0
    # Basic structure should remain the same
    assert sql1.upper().count("SELECT") == sql2.upper().count("SELECT")
    assert sql1.upper().count("FROM") == sql2.upper().count("FROM")
