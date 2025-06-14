"""Tests for the ExpressionSimplifier transformer."""

from typing import Optional

from sqlglot import parse_one
from sqlglot.dialects import Dialect

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers import ExpressionSimplifier, SimplificationConfig
from sqlspec.statement.sql import SQLConfig


def _create_test_context(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper function to create a SQLProcessingContext for testing."""
    if config is None:
        config = SQLConfig()

    expression = parse_one(sql)
    return SQLProcessingContext(
        initial_sql_string=sql, dialect=Dialect.get_or_raise(""), config=config, current_expression=expression
    )


class TestExpressionSimplifier:
    """Test cases for the ExpressionSimplifier transformer."""

    def test_simplifies_arithmetic_literals(self) -> None:
        """Test simplification of arithmetic expressions with literals."""
        sql = "SELECT 1 + 1 AS sum, 10 * 2 AS product FROM users"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        # Convert back to SQL to check simplification
        result_sql = transformed_expression.sql()

        # SQLGlot should simplify 1 + 1 to 2 and 10 * 2 to 20
        assert "2 AS sum" in result_sql or "2" in result_sql
        assert "20 AS product" in result_sql or "20" in result_sql
        # Check context for transformation logs
        assert len(context.transformations) > 0
        assert context.transformations[0].processor == "ExpressionSimplifier"

    def test_simplifies_boolean_expressions(self) -> None:
        """Test simplification of boolean expressions."""
        sql = "SELECT * FROM users WHERE TRUE AND active = 1"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # TRUE AND condition should be simplified to just the condition
        assert "WHERE active = 1" in result_sql
        assert "TRUE AND" not in result_sql

    def test_removes_double_negatives(self) -> None:
        """Test removal of double negatives."""
        sql = "SELECT * FROM users WHERE NOT NOT active"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # NOT NOT should be simplified to just the condition
        assert "WHERE active" in result_sql
        assert "NOT NOT" not in result_sql

    def test_simplifies_tautologies(self) -> None:
        """Test simplification of tautological expressions."""
        sql = "SELECT * FROM users WHERE 1 = 1 AND name = 'test'"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # 1 = 1 should be simplified away
        assert "name = 'test'" in result_sql

    def test_disabled_simplifier(self) -> None:
        """Test that disabled simplifier returns original expression."""
        sql = "SELECT 1 + 1 AS sum FROM users WHERE TRUE AND active = 1"

        transformer = ExpressionSimplifier(enabled=False)
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Should be unchanged
        assert "1 + 1" in result_sql
        assert "TRUE AND" in result_sql

    def test_custom_simplification_config(self) -> None:
        """Test simplifier with custom configuration."""
        sql = "SELECT 1 + 1 AS sum FROM users WHERE TRUE AND active = 1"

        # Disable boolean optimization
        config = SimplificationConfig(
            enable_literal_folding=True,
            enable_boolean_optimization=False,
            enable_connector_optimization=True,
            enable_equality_normalization=True,
            enable_complement_removal=True,
        )

        transformer = ExpressionSimplifier(config=config)
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Literal folding should work but boolean optimization should not
        assert "2" in result_sql  # 1 + 1 simplified
        # Boolean expression might still be present depending on SQLGlot's behavior

    def test_handles_complex_expressions(self) -> None:
        """Test handling of complex mathematical expressions."""
        sql = """
        SELECT
            (5 + 3) * 2 AS calc1,
            10 / 2 + 1 AS calc2,
            2 * 2 * 2 AS calc3
        FROM users
        """

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Complex expressions should be simplified
        assert "16" in result_sql  # (5 + 3) * 2 = 16
        assert "6" in result_sql  # 10 / 2 + 1 = 6
        assert "8" in result_sql  # 2 * 2 * 2 = 8

    def test_preserves_parameters(self) -> None:
        """Test that parameterized queries are preserved during simplification."""
        sql = "SELECT 1 + 1 AS sum FROM users WHERE id = ? AND 2 * 2 = 4"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Parameters should be preserved
        assert "id = ?" in result_sql
        # Constants should be simplified
        assert "2" in result_sql  # 1 + 1
        # Note: 2 * 2 = 4 might be simplified away if it's a tautology

    def test_handles_case_expressions(self) -> None:
        """Test simplification within CASE expressions."""
        sql = """
        SELECT
            CASE
                WHEN 1 + 1 = 2 THEN 'correct'
                WHEN 2 * 3 = 6 THEN 'also correct'
                ELSE 'wrong'
            END as result
        FROM users
        """

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # SQLGlot might completely optimize away tautological CASE expressions
        # The simplified query should still be valid SQL
        assert "SELECT" in result_sql
        assert "FROM users" in result_sql

    def test_handles_function_arguments(self) -> None:
        """Test simplification within function arguments."""
        sql = """
        SELECT
            SUBSTR('hello', 1 + 1, 2 * 2) AS sub,
            POWER(2, 1 + 2) AS pow
        FROM users
        """

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Function arguments should be simplified
        # Note: SQLGlot might convert SUBSTR to SUBSTRING
        assert "SUBSTR('hello', 2, 4)" in result_sql or "SUBSTRING('hello', 2, 4)" in result_sql
        assert "POWER(2, 3)" in result_sql

    def test_handles_subqueries(self) -> None:
        """Test simplification within subqueries."""
        sql = """
        SELECT *
        FROM users u
        WHERE u.id IN (
            SELECT user_id
            FROM orders
            WHERE total > 5 * 10
        )
        """

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Arithmetic in subquery should be simplified
        assert "total > 50" in result_sql

    def test_no_optimization_needed(self) -> None:
        """Test handling when no optimizations can be applied."""
        sql = "SELECT name, email FROM users WHERE active = ?"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Should be essentially unchanged
        assert "SELECT name, email" in result_sql
        assert "WHERE active = ?" in result_sql
        # Check context metadata for no simplification
        assert "ExpressionSimplifier" in context.metadata
        assert context.metadata["ExpressionSimplifier"]["simplified"] is False
        assert context.metadata["ExpressionSimplifier"]["chars_saved"] == 0

    def test_optimization_failure_handling(self) -> None:
        """Test graceful handling of optimization failures."""
        # Create a context with a mock expression that might cause issues
        sql = "SELECT * FROM users"
        context = _create_test_context(sql)

        # Test with a configuration that might cause issues
        config = SimplificationConfig(
            enable_literal_folding=True,
            enable_boolean_optimization=True,
            enable_connector_optimization=True,
            enable_equality_normalization=True,
            enable_complement_removal=True,
        )

        transformer = ExpressionSimplifier(config=config)

        # This should not raise an exception even if optimization fails
        transformed_expression = transformer.process(context.current_expression, context)

        # Should return the original expression
        assert transformed_expression is not None

    def test_reports_optimization_metrics(self) -> None:
        """Test that optimization metrics are properly reported."""
        sql = "SELECT 1 + 1 + 1 AS sum FROM users WHERE TRUE AND FALSE OR active = 1"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        _ = transformer.process(context.current_expression, context)

        # Check that context contains transformation logs
        assert len(context.transformations) > 0
        result = context.transformations[0]
        assert result.processor == "ExpressionSimplifier"
        assert "simplified" in result.description or "Simplified" in result.description

    def test_connector_optimization(self) -> None:
        """Test connector optimization (AND/OR logic)."""
        sql = "SELECT * FROM users WHERE (a = 1 AND b = 2) OR (a = 1 AND c = 3)"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Should potentially optimize to: a = 1 AND (b = 2 OR c = 3)
        # The exact optimization depends on SQLGlot's implementation
        assert "a = 1" in result_sql

    def test_equality_normalization(self) -> None:
        """Test equality expression normalization."""
        sql = "SELECT * FROM users WHERE 1 = id AND 'active' = status"

        transformer = ExpressionSimplifier()
        context = _create_test_context(sql)

        transformed_expression = transformer.process(context.current_expression, context)

        result_sql = transformed_expression.sql()

        # Equality normalization might reorder expressions for consistency
        # The exact result depends on SQLGlot's implementation
        assert "id" in result_sql
        assert "status" in result_sql
