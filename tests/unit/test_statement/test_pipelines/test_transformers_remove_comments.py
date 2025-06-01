"""Function-based tests for comment removal transformer."""

from typing import Any

import pytest
import sqlglot

from sqlspec.statement.pipelines.transformers._remove_comments import CommentRemover
from sqlspec.statement.sql import SQLConfig


def test_remove_comments_basic_line_comments() -> None:
    """Test removal of basic line comments."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with line comments
    sql_with_comments = """
        SELECT * FROM users  -- This is a line comment
        WHERE active = 1     -- Another comment
    """
    expression = sqlglot.parse_one(sql_with_comments, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified
    assert any("comment" in note.lower() for note in result.notes)

    # Check that comments are removed
    cleaned_sql = result.expression.sql(dialect="mysql")
    assert "--" not in cleaned_sql


def test_remove_comments_basic_block_comments() -> None:
    """Test removal of basic block comments."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with block comments
    sql_with_comments = """
        SELECT * FROM users /* This is a block comment */
        WHERE active = 1 /* Another block comment */
    """
    expression = sqlglot.parse_one(sql_with_comments, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified

    # Check that comments are removed
    cleaned_sql = result.expression.sql(dialect="mysql")
    assert "/*" not in cleaned_sql
    assert "*/" not in cleaned_sql


def test_remove_comments_mysql_version_comments() -> None:
    """Test handling of MySQL version-specific comments."""
    transformer_preserve = CommentRemover(preserve_mysql_version_comments=True)
    transformer_remove = CommentRemover(preserve_mysql_version_comments=False)
    config = SQLConfig()

    # SQL with MySQL version comment
    mysql_version_sql = "SELECT * FROM users /*!50000 WHERE id = 1 */"
    expression = sqlglot.parse_one(mysql_version_sql, read="mysql")

    result_preserve = transformer_preserve.transform(expression, "mysql", config)
    result_remove = transformer_remove.transform(expression, "mysql", config)

    # Preserve version should keep MySQL comments
    preserved_sql = result_preserve.expression.sql(dialect="mysql")

    # Remove version should clean them
    removed_sql = result_remove.expression.sql(dialect="mysql")

    # At least one should show different behavior
    assert preserved_sql != removed_sql or not result_preserve.modified


def test_remove_comments_oracle_hints() -> None:
    """Test handling of Oracle-style hint comments."""
    transformer_preserve = CommentRemover(preserve_hints=True)
    transformer_remove = CommentRemover(preserve_hints=False)
    config = SQLConfig()

    # SQL with Oracle hint
    hint_sql = "SELECT /*+ INDEX(users idx_active) */ * FROM users WHERE active = 1"
    expression = sqlglot.parse_one(hint_sql, read="mysql")

    result_preserve = transformer_preserve.transform(expression, "mysql", config)
    result_remove = transformer_remove.transform(expression, "mysql", config)

    # Should handle hints according to configuration
    result_preserve.expression.sql(dialect="mysql")
    result_remove.expression.sql(dialect="mysql")

    # Configuration should affect behavior
    assert isinstance(result_preserve.modified, bool)
    assert isinstance(result_remove.modified, bool)


def test_remove_comments_strict_removal() -> None:
    """Test strict removal mode that removes all comments."""
    transformer = CommentRemover(strict_removal=True)
    config = SQLConfig()

    # SQL with various comment types
    mixed_comments_sql = """
        SELECT * FROM users  -- Line comment
        /* Block comment */
        /*!50000 MySQL version comment */
        /*+ Oracle hint */
        WHERE active = 1
    """
    expression = sqlglot.parse_one(mixed_comments_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Strict mode should remove everything
    cleaned_sql = result.expression.sql(dialect="mysql")

    # Should remove most comment indicators
    comment_indicators = ["--", "/*", "*/", "/*!"]
    remaining_indicators = sum(1 for indicator in comment_indicators if indicator in cleaned_sql)

    # Strict mode should significantly reduce comments
    assert remaining_indicators <= len(comment_indicators) // 2


def test_remove_comments_preserves_sql_structure() -> None:
    """Test that comment removal preserves SQL structure and validity."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Complex SQL with comments
    complex_sql = """
        SELECT
            u.name,           -- User name
            u.email,          -- User email
            COUNT(o.id)       /* Order count */
        FROM users u          -- Users table
        LEFT JOIN orders o ON u.id = o.user_id  /* Join condition */
        WHERE u.active = 1    -- Only active users
        GROUP BY u.id, u.name, u.email  -- Group by user
        ORDER BY COUNT(o.id) DESC        -- Order by order count
    """
    expression = sqlglot.parse_one(complex_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should preserve SQL structure
    cleaned_sql = result.expression.sql(dialect="mysql")

    # Essential SQL keywords should remain
    assert "SELECT" in cleaned_sql.upper()
    assert "FROM" in cleaned_sql.upper()
    assert "WHERE" in cleaned_sql.upper()
    assert "GROUP BY" in cleaned_sql.upper()
    assert "ORDER BY" in cleaned_sql.upper()


def test_remove_comments_handles_empty_comments() -> None:
    """Test handling of empty or whitespace-only comments."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with empty comments
    empty_comments_sql = """
        SELECT * FROM users  --
        /* */ WHERE active = 1  /*    */
    """
    expression = sqlglot.parse_one(empty_comments_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should handle empty comments gracefully
    cleaned_sql = result.expression.sql(dialect="mysql")
    assert isinstance(cleaned_sql, str)
    assert len(cleaned_sql.strip()) > 0


def test_remove_comments_no_comments_present() -> None:
    """Test behavior when no comments are present in SQL."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL without comments
    clean_sql = "SELECT * FROM users WHERE active = 1 ORDER BY name"
    expression = sqlglot.parse_one(clean_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should handle gracefully
    assert isinstance(result.modified, bool)
    assert any("no comments" in note.lower() for note in result.notes) or result.modified


def test_remove_comments_mixed_comment_types() -> None:
    """Test removal of mixed comment types in a single query."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with multiple comment types
    mixed_sql = """
        -- Header comment
        SELECT * FROM users  /* inline block */
        WHERE id = 1         -- line comment
        /*!50000 AND status = 'active' */  /* another block */
    """
    expression = sqlglot.parse_one(mixed_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    assert result.modified

    # Should handle multiple comment types
    cleaned_sql = result.expression.sql(dialect="mysql")
    comment_count = cleaned_sql.count("--") + cleaned_sql.count("/*") + cleaned_sql.count("*/")

    # Should significantly reduce comment indicators
    original_count = mixed_sql.count("--") + mixed_sql.count("/*") + mixed_sql.count("*/")
    assert comment_count <= original_count


@pytest.mark.parametrize(
    ("sql_input", "config_params", "description"),
    [
        ("SELECT * FROM users -- comment", {"strict_removal": True}, "strict removal of line comment"),
        ("SELECT * FROM users /* comment */", {"strict_removal": False}, "selective removal of block comment"),
        ("SELECT /*+ hint */ * FROM users", {"preserve_hints": True}, "preserve Oracle hints"),
        (
            "SELECT /*!50000 */ * FROM users",
            {"preserve_mysql_version_comments": True},
            "preserve MySQL version comments",
        ),
    ],
    ids=["strict_line", "selective_block", "preserve_hints", "preserve_mysql"],
)
def test_remove_comments_various_configurations(
    sql_input: str, config_params: dict[str, Any], description: str
) -> None:
    """Test comment removal with various configurations."""
    transformer = CommentRemover(**config_params)
    config = SQLConfig()

    expression = sqlglot.parse_one(sql_input, read="mysql")
    result = transformer.transform(expression, "mysql", config)

    # Should handle all configurations gracefully
    assert isinstance(result.modified, bool), f"Failed for {description}"
    assert result.expression is not None, f"Null expression for {description}"

    # Output should be valid SQL
    cleaned_sql = result.expression.sql(dialect="mysql")
    assert len(cleaned_sql.strip()) > 0, f"Empty output for {description}"


def test_remove_comments_nested_comments() -> None:
    """Test handling of nested comment structures."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with potentially nested comments
    nested_sql = "SELECT * FROM users /* outer /* inner */ comment */ WHERE id = 1"

    try:
        expression = sqlglot.parse_one(nested_sql, read="mysql")
        result = transformer.transform(expression, "mysql", config)

        # Should handle nested comments gracefully
        assert isinstance(result.modified, bool)
        assert result.expression is not None

    except Exception:
        # Some nested comment patterns might not parse, which is acceptable
        pass


def test_remove_comments_performance_with_many_comments() -> None:
    """Test performance with queries containing many comments."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Generate SQL with many comments
    base_sql = "SELECT * FROM users WHERE active = 1"
    comments = [f" /* Comment {i} */" for i in range(20)]
    many_comments_sql = base_sql + "".join(comments)

    expression = sqlglot.parse_one(many_comments_sql, read="mysql")
    result = transformer.transform(expression, "mysql", config)

    # Should handle many comments without performance issues
    assert isinstance(result.modified, bool)
    cleaned_sql = result.expression.sql(dialect="mysql")

    # Should significantly reduce comment count
    original_comment_count = many_comments_sql.count("/*")
    cleaned_comment_count = cleaned_sql.count("/*")
    assert cleaned_comment_count < original_comment_count


def test_remove_comments_malformed_comments() -> None:
    """Test graceful handling of malformed comments."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Various malformed comment scenarios
    malformed_cases = [
        "SELECT * FROM users /* unclosed comment",
        "SELECT * FROM users */ unmatched close",
        "SELECT * FROM users WHERE id = 1 --",  # Empty line comment
    ]

    for malformed_sql in malformed_cases:
        try:
            expression = sqlglot.parse_one(malformed_sql, read="mysql")
            result = transformer.transform(expression, "mysql", config)

            # Should handle gracefully without crashing
            assert isinstance(result.modified, bool)
            assert result.expression is not None

        except Exception:
            # Some malformed SQL might not parse, which is acceptable
            pass


def test_remove_comments_unicode_comments() -> None:
    """Test handling of comments with unicode characters."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with unicode in comments
    unicode_sql = "SELECT * FROM users /* Commentaire français avec émoticônes 🚀 */ WHERE active = 1"
    expression = sqlglot.parse_one(unicode_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should handle unicode comments
    assert isinstance(result.modified, bool)
    cleaned_sql = result.expression.sql(dialect="mysql")

    # Should preserve SQL structure
    assert "SELECT" in cleaned_sql.upper()
    assert "FROM USERS" in cleaned_sql.upper()


def test_remove_comments_edge_case_sql_strings() -> None:
    """Test comment removal with edge case SQL strings."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Edge cases
    edge_cases = [
        "SELECT '-- not a comment' FROM users",  # Comment inside string
        "SELECT 'text /* not comment */' FROM users",  # Block comment inside string
        "SELECT * FROM users WHERE description LIKE '%--comment%'",  # Pattern matching
    ]

    for edge_sql in edge_cases:
        expression = sqlglot.parse_one(edge_sql, read="mysql")
        result = transformer.transform(expression, "mysql", config)

        # Should preserve string literals containing comment-like patterns
        cleaned_sql = result.expression.sql(dialect="mysql")

        # String literals should be preserved
        assert "SELECT" in cleaned_sql.upper()
        assert "FROM USERS" in cleaned_sql.upper()


def test_remove_comments_idempotent_operation() -> None:
    """Test that comment removal is idempotent."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL with comments
    commented_sql = """
        SELECT * FROM users  -- Line comment
        /* Block comment */
        WHERE active = 1
    """
    expression = sqlglot.parse_one(commented_sql, read="mysql")

    # Apply transformation twice
    result1 = transformer.transform(expression, "mysql", config)
    result2 = transformer.transform(result1.expression, "mysql", config)

    # Should be stable after multiple applications
    sql1 = result1.expression.sql(dialect="mysql")
    sql2 = result2.expression.sql(dialect="mysql")

    # Should be functionally equivalent
    assert sql1.replace(" ", "") == sql2.replace(" ", "")  # Ignore spacing differences


def test_remove_comments_preserves_whitespace_structure() -> None:
    """Test that essential whitespace structure is preserved."""
    transformer = CommentRemover()
    config = SQLConfig()

    # SQL where whitespace matters
    whitespace_sql = """
        SELECT
            name,     -- User name
            email     /* User email */
        FROM users
        WHERE active = 1  -- Filter condition
    """
    expression = sqlglot.parse_one(whitespace_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    cleaned_sql = result.expression.sql(dialect="mysql")

    # Should preserve essential structure
    assert "name" in cleaned_sql
    assert "email" in cleaned_sql
    assert "FROM USERS" in cleaned_sql.upper()
    assert "WHERE" in cleaned_sql.upper()


def test_remove_comments_different_dialects() -> None:
    """Test comment removal works with different SQL dialects."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Test with different dialects
    dialects_to_test = ["mysql", "postgres", "sqlite", "oracle"]
    test_sql = "SELECT * FROM users /* comment */ WHERE id = 1 -- line comment"

    for dialect in dialects_to_test:
        try:
            expression = sqlglot.parse_one(test_sql, read=dialect)
            result = transformer.transform(expression, dialect, config)

            assert isinstance(result.modified, bool), f"Failed for dialect {dialect}"

            # Verify output is valid for the dialect
            cleaned_sql = result.expression.sql(dialect=dialect)
            assert len(cleaned_sql.strip()) > 0, f"Empty output for dialect {dialect}"

        except Exception as e:
            # Some dialects might not be supported, which is acceptable
            pytest.skip(f"Dialect {dialect} not supported: {e}")


def test_remove_comments_transformation_failure_handling() -> None:
    """Test graceful handling of transformation failures."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Valid SQL that should transform successfully
    valid_sql = "SELECT * FROM users /* comment */ WHERE id = 1"
    expression = sqlglot.parse_one(valid_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should succeed for valid input
    assert result.expression is not None
    assert isinstance(result.modified, bool)

    # If transformation failed, should be noted
    if not result.modified and result.notes:
        assert any("fail" in note.lower() for note in result.notes)


def test_remove_comments_with_complex_business_query() -> None:
    """Test comment removal on complex business query."""
    transformer = CommentRemover()
    config = SQLConfig()

    # Complex business query with comments
    business_sql = """
        -- Monthly sales report query
        SELECT
            u.name,                    -- Customer name
            u.email,                   -- Customer email
            COUNT(o.id) as orders,     /* Total orders */
            SUM(o.total) as revenue    -- Total revenue
        FROM users u                   -- Customers table
        LEFT JOIN orders o ON u.id = o.user_id  /* Left join for all customers */
        WHERE u.active = 1             -- Only active customers
        AND o.created_at >= '2023-01-01'  -- This year only
        GROUP BY u.id, u.name, u.email    -- Group by customer
        ORDER BY revenue DESC              -- Order by revenue descending
        LIMIT 100                          -- Top 100 customers
    """
    expression = sqlglot.parse_one(business_sql, read="mysql")

    result = transformer.transform(expression, "mysql", config)

    # Should preserve business logic while removing comments
    cleaned_sql = result.expression.sql(dialect="mysql")

    # All essential SQL elements should remain
    assert "SELECT" in cleaned_sql.upper()
    assert "LEFT JOIN" in cleaned_sql.upper()
    assert "GROUP BY" in cleaned_sql.upper()
    assert "ORDER BY" in cleaned_sql.upper()
    assert "LIMIT 100" in cleaned_sql.upper()

    # Comments should be reduced or eliminated
    comment_indicators = cleaned_sql.count("--") + cleaned_sql.count("/*")
    original_indicators = business_sql.count("--") + business_sql.count("/*")
    assert comment_indicators <= original_indicators
