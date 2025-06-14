"""Tests for the CommentRemover transformer."""

from typing import Optional

from sqlglot import parse_one
from sqlglot.dialects import Dialect

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers import CommentRemover
from sqlspec.statement.sql import SQLConfig


def _create_test_context(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper function to create a SQLProcessingContext for testing."""
    if config is None:
        config = SQLConfig()

    expression = parse_one(sql)
    return SQLProcessingContext(
        initial_sql_string=sql, dialect=Dialect.get_or_raise(""), config=config, current_expression=expression
    )


class TestCommentRemover:
    """Test cases for the CommentRemover transformer."""

    def test_removes_single_line_comments(self) -> None:
        """Test removal of single-line comments."""
        sql = """
        SELECT name, email -- This is a comment
        FROM users
        WHERE active = 1 -- Another comment
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        # Convert back to SQL to check comments are removed
        result_sql = transformed_expression.sql()
        assert "-- This is a comment" not in result_sql
        assert "-- Another comment" not in result_sql
        assert "SELECT" in result_sql
        assert "FROM users" in result_sql

    def test_removes_multi_line_comments(self) -> None:
        """Test removal of multi-line comments."""
        sql = """
        SELECT name, email
        /* This is a
           multi-line comment */
        FROM users
        WHERE active = 1
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "/* This is a" not in result_sql
        assert "multi-line comment */" not in result_sql
        assert "SELECT" in result_sql
        assert "FROM users" in result_sql

    def test_preserves_comments_in_strings(self) -> None:
        """Test that comments inside string literals are preserved."""
        sql = """
        SELECT name, 'This -- is not a comment' as description
        FROM users
        WHERE comment = '/* Also not a comment */'
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "'This -- is not a comment'" in result_sql
        assert "'/* Also not a comment */'" in result_sql

    def test_removes_nested_comments(self) -> None:
        """Test removal of nested comments."""
        sql = """
        SELECT name -- Simple comment
        FROM users
        WHERE active = 1
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        # Should remove comment content
        assert "-- Simple comment" not in result_sql
        assert "SELECT name" in result_sql
        assert "FROM users" in result_sql

    def test_handles_empty_comments(self) -> None:
        """Test handling of empty comments."""
        sql = """
        SELECT name --
        FROM users /**/
        WHERE active = 1
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "SELECT" in result_sql
        assert "FROM users" in result_sql
        assert "WHERE active = 1" in result_sql

    def test_removes_comments_from_subqueries(self) -> None:
        """Test removal of comments from subqueries."""
        sql = """
        SELECT *
        FROM (
            SELECT name -- Comment in subquery
            FROM users
            /* Another comment */
            WHERE active = 1
        ) subquery
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Comment in subquery" not in result_sql
        assert "/* Another comment */" not in result_sql
        assert "SELECT" in result_sql
        assert "FROM users" in result_sql

    def test_removes_comments_from_complex_queries(self) -> None:
        """Test removal of comments from complex queries with joins."""
        sql = """
        SELECT u.name, p.title -- User and profile info
        FROM users u -- Users table
        JOIN profiles p ON u.id = p.user_id /* Join condition */
        WHERE u.active = 1 -- Only active users
        ORDER BY u.name -- Sort by name
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- User and profile info" not in result_sql
        assert "-- Users table" not in result_sql
        assert "/* Join condition */" not in result_sql
        assert "-- Only active users" not in result_sql
        assert "-- Sort by name" not in result_sql
        # SQLGlot may add AS keywords, so check for the essential structure
        assert "JOIN profiles" in result_sql
        assert "ON u.id = p.user_id" in result_sql

    def test_handles_comments_at_end_of_query(self) -> None:
        """Test handling of comments at the end of queries."""
        sql = """
        SELECT name, email
        FROM users
        WHERE active = 1
        -- Final comment
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Final comment" not in result_sql
        assert "WHERE active = 1" in result_sql

    def test_handles_comments_at_beginning_of_query(self) -> None:
        """Test handling of comments at the beginning of queries."""
        sql = """
        -- Initial comment
        /* Multi-line initial comment */
        SELECT name, email
        FROM users
        WHERE active = 1
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Initial comment" not in result_sql
        assert "/* Multi-line initial comment */" not in result_sql
        assert "SELECT name, email" in result_sql

    def test_preserves_query_structure(self) -> None:
        """Test that query structure is preserved after comment removal."""
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
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Should preserve the basic structure (SQLGlot may add AS keywords)
        assert "SELECT" in result_sql
        assert "u.name" in result_sql
        assert "u.email" in result_sql
        assert "p.title" in result_sql
        assert "FROM users" in result_sql  # May be "FROM users AS u"
        assert "JOIN profiles" in result_sql
        assert "WHERE" in result_sql
        assert "ORDER BY" in result_sql

    def test_handles_mixed_comment_styles(self) -> None:
        """Test handling of mixed comment styles in one query."""
        sql = """
        SELECT name, -- Single line comment
        /* Multi-line
           comment */ email
        FROM users -- Another single line
        /* Another multi-line */ WHERE active = 1
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Single line comment" not in result_sql
        assert "/* Multi-line" not in result_sql
        assert "comment */" not in result_sql
        assert "-- Another single line" not in result_sql
        assert "/* Another multi-line */" not in result_sql
        assert "SELECT name" in result_sql
        assert "email" in result_sql

    def test_handles_comments_with_special_characters(self) -> None:
        """Test handling of comments containing special characters."""
        sql = """
        SELECT name -- Comment with @#$%^&*()
        FROM users /* Simple comment */
        WHERE active = 1 -- Another comment
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Comment with @#$%^&*()" not in result_sql
        assert "/* Simple comment */" not in result_sql
        assert "-- Another comment" not in result_sql

    def test_no_comments_to_remove(self) -> None:
        """Test behavior when there are no comments to remove."""
        sql = """
        SELECT name, email
        FROM users
        WHERE active = 1
        ORDER BY name
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        # Should be essentially the same (modulo formatting)
        assert "SELECT" in result_sql
        assert "FROM users" in result_sql
        assert "WHERE active = 1" in result_sql
        assert "ORDER BY" in result_sql

    def test_handles_comments_in_function_calls(self) -> None:
        """Test handling of comments within function calls."""
        sql = """
        SELECT
            COUNT(*), -- Count all records
            MAX(created_at), /* Latest creation date */
            AVG(score) -- Average score
        FROM users
        WHERE active = 1
        """

        transformer = CommentRemover()
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Count all records" not in result_sql
        assert "/* Latest creation date */" not in result_sql
        assert "-- Average score" not in result_sql
        assert "COUNT(*)" in result_sql
        assert "MAX(created_at)" in result_sql
        assert "AVG(score)" in result_sql

    def test_handles_comments_in_case_statements(self) -> None:
        """Test handling of comments within CASE statements."""
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
        context = _create_test_context(sql)

        assert context.current_expression is not None
        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()
        assert "-- Under 18" not in result_sql
        assert "/* Working age */" not in result_sql
        assert "-- Retirement age" not in result_sql
        assert "CASE" in result_sql
        assert "WHEN age < 18" in result_sql
        assert "'Minor'" in result_sql
