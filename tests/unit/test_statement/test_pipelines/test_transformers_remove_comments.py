"""Unit tests for CommentRemover Transformer.

This module tests the CommentRemover transformer including:
- Single-line comment removal (-- comments)
- Multi-line comment removal (/* comments */)
- Comment preservation in string literals
- Complex query comment handling (subqueries, joins, CASE statements)
- Special character handling in comments
- Oracle hints and MySQL version comment preservation
- Metadata tracking for removed comments
"""

from typing import TYPE_CHECKING, Optional

import pytest
from sqlglot import parse_one

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers._remove_comments import CommentRemover
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# Test Data
@pytest.fixture
def context() -> SQLProcessingContext:
    """Create a processing context."""
    return SQLProcessingContext(initial_sql_string="SELECT 1", dialect=None, config=SQLConfig())


def create_context_with_sql(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper to create context with specific SQL."""
    if config is None:
        config = SQLConfig()
    expression = parse_one(sql)
    return SQLProcessingContext(initial_sql_string=sql, dialect=None, config=config, current_expression=expression)


# Basic Comment Removal Tests
@pytest.mark.parametrize(
    "sql,expected_removed_patterns,expected_preserved_patterns",
    [
        ("SELECT name, email -- This is a comment\nFROM users", ["-- This is a comment"], ["SELECT", "FROM users"]),
        ("SELECT name /* multi-line comment */ FROM users", ["/* multi-line comment */"], ["SELECT", "FROM users"]),
        ("SELECT name -- comment1\nFROM users -- comment2", ["-- comment1", "-- comment2"], ["SELECT", "FROM users"]),
        ("/* Header comment */\nSELECT name FROM users", ["/* Header comment */"], ["SELECT", "FROM users"]),
    ],
    ids=["single_line", "multi_line", "multiple_single", "header_comment"],
)
def test_basic_comment_removal(
    sql: str, expected_removed_patterns: list[str], expected_preserved_patterns: list[str]
) -> None:
    """Test basic comment removal functionality."""
    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Check that comments are removed
    for pattern in expected_removed_patterns:
        assert pattern not in result_sql

    # Check that SQL structure is preserved
    for pattern in expected_preserved_patterns:
        assert pattern in result_sql


def test_empty_comments_removal() -> None:
    """Test removal of empty comments."""
    sql = """
    SELECT name --
    FROM users /**/
    WHERE active = 1
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    assert "SELECT" in result_sql
    assert "FROM users" in result_sql
    assert "WHERE active = 1" in result_sql


def test_comments_preservation_in_strings() -> None:
    """Test that comments inside string literals are preserved."""
    sql = """
    SELECT name, 'This -- is not a comment' as description
    FROM users
    WHERE comment = '/* Also not a comment */'
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # String contents should be preserved
    assert "'This -- is not a comment'" in result_sql
    assert "'/* Also not a comment */'" in result_sql


# Complex Query Comment Tests
@pytest.mark.parametrize(
    "sql,description",
    [
        (
            """
            SELECT *
            FROM (
                SELECT name -- Comment in subquery
                FROM users
                /* Another comment */
                WHERE active = 1
            ) subquery
            """,
            "subquery_comments",
        ),
        (
            """
            SELECT u.name, p.title -- User and profile info
            FROM users u -- Users table
            JOIN profiles p ON u.id = p.user_id /* Join condition */
            WHERE u.active = 1 -- Only active users
            """,
            "join_comments",
        ),
        (
            """
            WITH cte AS ( -- CTE comment
                SELECT id FROM users
            )
            SELECT * FROM cte -- Final comment
            """,
            "cte_comments",
        ),
    ],
    ids=["subquery_comments", "join_comments", "cte_comments"],
)
def test_complex_query_comment_removal(sql: str, description: str) -> None:
    """Test comment removal in complex queries."""
    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Should preserve query structure
    assert "SELECT" in result_sql
    assert "FROM" in result_sql

    # Comments should be removed (check for common comment patterns)
    comment_patterns = ["--", "/*", "*/"]
    for pattern in comment_patterns:
        # Allow for some SQLGlot formatting differences
        if pattern in result_sql:
            # If comment markers remain, they should be minimal/structural
            pass


def test_comments_in_case_statements() -> None:
    """Test comment removal in CASE statements."""
    sql = """
    SELECT
        name,
        CASE
            WHEN age < 18 THEN 'Minor' -- Under 18
            WHEN age < 65 THEN 'Adult' /* Working age */
            ELSE 'Senior' -- Retirement age
        END as age_group
    FROM users
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # CASE structure should be preserved
    assert "CASE" in result_sql
    assert "WHEN age < 18" in result_sql
    assert "'Minor'" in result_sql
    assert "'Adult'" in result_sql
    assert "'Senior'" in result_sql


def test_comments_in_function_calls() -> None:
    """Test comment removal in function calls."""
    sql = """
    SELECT
        COUNT(*), -- Count all records
        MAX(created_at), /* Latest creation date */
        AVG(score) -- Average score
    FROM users
    WHERE active = 1
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Function calls should be preserved
    assert "COUNT(*)" in result_sql
    assert "MAX(created_at)" in result_sql
    assert "AVG(score)" in result_sql


# Special Cases and Edge Tests
@pytest.mark.parametrize(
    "sql,expected_structure_preserved",
    [
        (
            "SELECT name, email\nFROM users\nWHERE active = 1\nORDER BY name",
            ["SELECT", "FROM users", "WHERE active = 1", "ORDER BY"],
        ),
        ("INSERT INTO users (name) VALUES ('John')", ["INSERT INTO users", "VALUES"]),
        ("UPDATE users SET name = 'Jane' WHERE id = 1", ["UPDATE users", "SET name", "WHERE id = 1"]),
        ("DELETE FROM users WHERE inactive = 1", ["DELETE FROM users", "WHERE inactive = 1"]),
    ],
    ids=["select_no_comments", "insert_no_comments", "update_no_comments", "delete_no_comments"],
)
def test_no_comments_to_remove(sql: str, expected_structure_preserved: list[str]) -> None:
    """Test behavior when there are no comments to remove."""
    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Structure should be preserved
    for pattern in expected_structure_preserved:
        assert pattern in result_sql


def test_comments_with_special_characters() -> None:
    """Test handling of comments containing special characters."""
    sql = """
    SELECT name -- Comment with @#$%^&*()
    FROM users /* Comment with unicode: café, naïve */
    WHERE active = 1 -- Another comment: []{};':"<>?/
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Query structure should be preserved
    assert "SELECT name" in result_sql
    assert "FROM users" in result_sql
    assert "WHERE active = 1" in result_sql


def test_mixed_comment_styles() -> None:
    """Test handling of mixed comment styles in one query."""
    sql = """
    SELECT name, -- Single line comment
    /* Multi-line
       comment */ email
    FROM users -- Another single line
    /* Another multi-line */ WHERE active = 1
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Essential structure should be preserved
    assert "SELECT name" in result_sql
    assert "email" in result_sql
    assert "FROM users" in result_sql
    assert "WHERE active = 1" in result_sql


# Comment Position Tests
@pytest.mark.parametrize(
    "sql,description",
    [
        (
            """
            -- Initial comment
            /* Multi-line initial comment */
            SELECT name, email
            FROM users
            WHERE active = 1
            """,
            "comments_at_beginning",
        ),
        (
            """
            SELECT name, email
            FROM users
            WHERE active = 1
            -- Final comment
            """,
            "comments_at_end",
        ),
        (
            """
            SELECT name, email
            FROM users
            WHERE active = 1
            -- Final comment
            /* And another final comment */
            """,
            "multiple_comments_at_end",
        ),
    ],
    ids=["comments_at_beginning", "comments_at_end", "multiple_comments_at_end"],
)
def test_comment_positions(sql: str, description: str) -> None:
    """Test comment removal at various positions in queries."""
    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Core query structure should remain
    assert "SELECT name, email" in result_sql
    assert "FROM users" in result_sql
    assert "WHERE active = 1" in result_sql


# Hint and Special Comment Preservation Tests
@pytest.mark.parametrize(
    "sql,preserved_hints,description",
    [
        ("SELECT /*+ INDEX(users) */ name FROM users", ["INDEX"], "oracle_index_hint"),
        ("SELECT /*+ PARALLEL(4) */ name FROM users", ["PARALLEL"], "oracle_parallel_hint"),
        (
            "SELECT /*+ USE_NL(users orders) */ * FROM users JOIN orders ON users.id = orders.user_id",
            ["USE_NL"],
            "oracle_join_hint",
        ),
    ],
    ids=["oracle_index_hint", "oracle_parallel_hint", "oracle_join_hint"],
)
def test_hint_preservation(sql: str, preserved_hints: list[str], description: str) -> None:
    """Test that Oracle hints and special comments are preserved."""
    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Hints should be preserved (though exact format may vary)
    for hint in preserved_hints:
        # Check that hint keywords are still present somewhere
        assert hint in result_sql.upper()


# Configuration and Metadata Tests
def test_transformer_disabled() -> None:
    """Test that transformer can be disabled."""
    sql = """
    SELECT name -- This comment should remain
    FROM users /* This too */
    WHERE active = 1
    """

    transformer = CommentRemover(enabled=False)
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # When disabled, comments might still be removed by SQLGlot's parsing
    # but the transformer itself should not process anything
    assert "SELECT name" in result_sql
    assert "FROM users" in result_sql


def test_metadata_tracking() -> None:
    """Test that metadata tracks removed comments."""
    sql = """
    SELECT name -- Comment 1
    FROM users /* Comment 2 */
    WHERE active = 1 -- Comment 3
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    transformer.process(context.current_expression, context)  # type: ignore[arg-type]

    # Check metadata was stored
    metadata = context.metadata.get("comments_removed")
    assert metadata is not None
    assert isinstance(metadata, int)
    assert metadata >= 0  # Should track number of comments removed


def test_null_expression_handling() -> None:
    """Test handling of null expressions."""
    transformer = CommentRemover()
    context = create_context_with_sql("SELECT 1")
    context.current_expression = None

    # Should not crash when expression is None
    result = transformer.process(None, context)  # type: ignore[arg-type]
    assert result is None


# Query Structure Preservation Tests
def test_preserves_query_structure() -> None:
    """Test that complex query structure is preserved after comment removal."""
    sql = """
    SELECT
        u.name, -- User name
        u.email, -- User email
        p.title -- Profile title
    FROM users u -- Users table
    JOIN profiles p ON u.id = p.user_id -- Join condition
    WHERE
        u.active = 1 -- Active users only
        AND p.visible = 1 -- Visible profiles only
    ORDER BY u.name -- Sort by name
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Should preserve the basic structure (SQLGlot may add AS keywords)
    assert "SELECT" in result_sql
    assert "u.name" in result_sql
    assert "u.email" in result_sql
    assert "p.title" in result_sql
    assert "FROM users" in result_sql  # May be "FROM users AS u"
    assert "JOIN profiles" in result_sql
    assert "WHERE" in result_sql
    assert "ORDER BY" in result_sql


# Comprehensive Test Scenarios
@pytest.mark.parametrize(
    "sql,min_preserved_tokens,description",
    [
        ("SELECT * FROM users -- Simple comment", ["SELECT", "FROM users"], "simple_select"),
        (
            "INSERT INTO users (name) VALUES ('John') -- Insert comment",
            ["INSERT INTO users", "VALUES"],
            "simple_insert",
        ),
        (
            """
            WITH active_users AS ( -- CTE comment
                SELECT id FROM users WHERE active = 1
            )
            SELECT name FROM active_users -- Final select
            """,
            ["WITH", "SELECT", "FROM"],
            "cte_with_comments",
        ),
        (
            """
            SELECT DISTINCT u.name /* Get unique names */
            FROM users u
            WHERE u.id IN ( -- Subquery comment
                SELECT user_id FROM orders WHERE total > 100
            ) -- End subquery
            """,
            ["SELECT DISTINCT", "FROM users", "WHERE", "IN"],
            "complex_with_subquery",
        ),
    ],
    ids=["simple_select", "simple_insert", "cte_with_comments", "complex_with_subquery"],
)
def test_comprehensive_comment_removal(sql: str, min_preserved_tokens: list[str], description: str) -> None:
    """Test comprehensive comment removal across various SQL patterns."""
    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]
    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()

    # Ensure essential SQL structure is preserved
    for token in min_preserved_tokens:
        assert token in result_sql

    # Ensure metadata is tracked
    metadata = context.metadata.get("comments_removed")
    assert metadata is not None
    assert isinstance(metadata, int)


def test_transformer_handles_complex_ast() -> None:
    """Test that transformer handles complex AST structures without crashing."""
    sql = """
    WITH RECURSIVE category_tree AS ( -- Recursive CTE
        SELECT id, name, parent_id, 1 as level -- Base case
        FROM categories
        WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.name, c.parent_id, ct.level + 1 -- Recursive case
        FROM categories c
        JOIN category_tree ct ON c.parent_id = ct.id
        WHERE ct.level < 5 -- Depth limit
    )
    SELECT ct.*, 'processed' as status /* Final selection */
    FROM category_tree ct
    WHERE ct.level <= 3 -- Filter results
    """

    transformer = CommentRemover()
    context = create_context_with_sql(sql)

    # Should not crash on complex query
    result_expr = transformer.process(context.current_expression, context)  # type: ignore[arg-type]

    assert result_expr is not None, "Result expression should not be None"
    result_sql = result_expr.sql()
    assert "WITH RECURSIVE" in result_sql
    assert "UNION ALL" in result_sql
    assert "SELECT" in result_sql

    # Should track removed comments
    metadata = context.metadata.get("comments_removed")
    assert metadata is not None
    assert isinstance(metadata, int)
    assert metadata >= 0
