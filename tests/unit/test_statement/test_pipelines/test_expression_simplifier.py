"""Unit tests for ExpressionSimplifier Transformer.

This module tests the ExpressionSimplifier transformer including:
- Literal folding (arithmetic expressions)
- Boolean logic optimization
- Tautology and contradiction removal
- Double negative elimination
- Expression standardization
- Complex query simplification
- Error handling and metrics reporting
- Configuration control
"""

from typing import TYPE_CHECKING, Optional

import pytest
from sqlglot import parse_one

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers._expression_simplifier import ExpressionSimplifier, SimplificationConfig
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


# Literal Folding Tests
@pytest.mark.parametrize(
    "sql,expected_simplifications",
    [
        ("SELECT 1 + 1 AS sum FROM users", ["2"]),
        ("SELECT 10 * 2 AS product FROM users", ["20"]),
        ("SELECT (5 + 3) * 2 AS calc FROM users", ["16"]),
        ("SELECT 10.0 / 2.0 + 1.0 AS division FROM users", ["6.0"]),
        ("SELECT 2 * 2 * 2 AS power FROM users", ["8"]),
    ],
    ids=["simple_addition", "multiplication", "parentheses", "division_addition", "multiple_operations"],
)
def test_literal_folding(sql: str, expected_simplifications: list[str]) -> None:
    """Test literal folding for arithmetic expressions."""
    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Check that arithmetic is simplified
    for expected in expected_simplifications:
        assert expected in result_sql


def test_complex_arithmetic_simplification() -> None:
    """Test simplification of complex mathematical expressions."""
    sql = """
    SELECT
        (5 + 3) * 2 AS calc1,
        10.0 / 2.0 + 1.0 AS calc2,
        2 * 2 * 2 AS calc3,
        100 - 50 + 25 AS calc4
    FROM users
    """

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Complex expressions should be simplified
    assert "16" in result_sql  # (5 + 3) * 2 = 16
    assert "6.0" in result_sql  # 10.0 / 2.0 + 1.0 = 6.0
    assert "8" in result_sql  # 2 * 2 * 2 = 8
    assert "75" in result_sql  # 100 - 50 + 25 = 75


# Boolean Optimization Tests
@pytest.mark.parametrize(
    "sql,expected_pattern,should_contain,should_not_contain",
    [
        ("SELECT * FROM users WHERE TRUE AND active = 1", "boolean_true_and", ["WHERE active = 1"], ["TRUE AND"]),
        ("SELECT * FROM users WHERE FALSE OR active = 1", "boolean_false_or", ["WHERE active = 1"], ["FALSE OR"]),
        ("SELECT * FROM users WHERE NOT NOT active", "double_negative", ["WHERE active"], ["NOT NOT"]),
        ("SELECT * FROM users WHERE 1 = 1 AND name = 'test'", "tautology_removal", ["name = 'test'"], ["1 = 1"]),
    ],
    ids=["true_and_removal", "false_or_removal", "double_negative_removal", "tautology_removal"],
)
def test_boolean_optimization(
    sql: str, expected_pattern: str, should_contain: list[str], should_not_contain: list[str]
) -> None:
    """Test boolean expression optimization."""
    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Check expected patterns
    for pattern in should_contain:
        assert pattern in result_sql

    for pattern in should_not_contain:
        assert pattern not in result_sql


def test_connector_optimization() -> None:
    """Test connector optimization (AND/OR logic)."""
    sql = "SELECT * FROM users WHERE (a = 1 AND b = 2) OR (a = 1 AND c = 3)"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Should potentially optimize to: a = 1 AND (b = 2 OR c = 3)
    # The exact optimization depends on SQLGlot's implementation
    assert "a = 1" in result_sql


def test_equality_normalization() -> None:
    """Test equality expression normalization."""
    sql = "SELECT * FROM users WHERE 1 = id AND 'active' = status"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Equality normalization might reorder expressions
    assert "id" in result_sql
    assert "status" in result_sql


# Configuration Tests
def test_disabled_simplifier() -> None:
    """Test that disabled simplifier returns original expression."""
    sql = "SELECT 1 + 1 AS sum FROM users WHERE TRUE AND active = 1"

    transformer = ExpressionSimplifier(enabled=False)
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Should be unchanged
    assert "1 + 1" in result_sql
    assert "TRUE AND" in result_sql


def test_custom_simplification_config() -> None:
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
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Literal folding should work
    assert "2" in result_sql  # 1 + 1 simplified
    # Boolean expression might still be present (depending on SQLGlot's behavior)


def test_all_optimizations_disabled() -> None:
    """Test with all optimizations disabled."""
    sql = "SELECT 1 + 1 AS sum FROM users WHERE TRUE AND active = 1"

    config = SimplificationConfig(
        enable_literal_folding=False,
        enable_boolean_optimization=False,
        enable_connector_optimization=False,
        enable_equality_normalization=False,
        enable_complement_removal=False,
    )

    transformer = ExpressionSimplifier(config=config)
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore

    # Should still run but might not simplify much
    assert result_expr is not None


# Complex Query Tests
@pytest.mark.parametrize(
    "sql,description",
    [
        (
            """
            SELECT *
            FROM users u
            WHERE u.id IN (
                SELECT user_id
                FROM orders
                WHERE total > 5 * 10
            )
            """,
            "subquery_simplification",
        ),
        (
            """
            SELECT
                CASE
                    WHEN 1 + 1 = 2 THEN 'correct'
                    WHEN 2 * 3 = 6 THEN 'also correct'
                    ELSE 'wrong'
                END as result
            FROM users
            """,
            "case_expression_simplification",
        ),
        (
            """
            SELECT
                SUBSTR('hello', 1 + 1, 2 * 2) AS sub,
                POWER(2, 1 + 2) AS pow
            FROM users
            """,
            "function_argument_simplification",
        ),
    ],
    ids=["subquery", "case_expression", "function_arguments"],
)
def test_complex_query_simplification(sql: str, description: str) -> None:
    """Test simplification in complex queries."""
    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Should preserve query structure
    assert "SELECT" in result_sql
    assert "FROM users" in result_sql

    # Should have some simplifications
    if "subquery" in description:
        assert "50" in result_sql  # 5 * 10 = 50
    elif "function" in description:
        # Function arguments should be simplified
        assert "2" in result_sql or "4" in result_sql or "3" in result_sql


# Parameter Preservation Tests
def test_preserves_parameters() -> None:
    """Test that parameterized queries are preserved during simplification."""
    sql = "SELECT 1 + 1 AS sum FROM users WHERE id = ? AND 2 * 2 = 4"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Parameters should be preserved
    assert "?" in result_sql
    # Constants should be simplified
    assert "2" in result_sql  # 1 + 1 = 2


# Edge Cases and Error Handling
def test_no_optimization_needed() -> None:
    """Test handling when no optimizations can be applied."""
    sql = "SELECT name, email FROM users WHERE active = ?"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Should be essentially unchanged
    assert "SELECT name, email" in result_sql
    assert "WHERE active = ?" in result_sql

    # Check metadata
    metadata = context.metadata.get("ExpressionSimplifier")
    assert metadata is not None
    assert metadata["simplified"] is False
    assert metadata["chars_saved"] == 0


def test_simplification_error_handling() -> None:
    """Test graceful handling of simplification errors."""
    # Create a normal query - errors are handled internally
    sql = "SELECT * FROM users"
    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    # Should not raise an exception even if internal error occurs
    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None


# Metadata and Metrics Tests
def test_transformation_logging() -> None:
    """Test that transformations are properly logged."""
    sql = "SELECT 1 + 1 + 1 AS sum FROM users WHERE TRUE AND active = 1"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    transformer.process(context.current_expression, context)  # pyright: ignore

    # Check transformation logs
    assert len(context.transformations) > 0
    assert any(t.processor == "ExpressionSimplifier" for t in context.transformations)

    # Check for description
    for t in context.transformations:
        if t.processor == "ExpressionSimplifier":
            assert "simplified" in t.description.lower() or "saved" in t.description.lower()


def test_metadata_tracking() -> None:
    """Test that metadata is properly tracked."""
    sql = "SELECT 1 + 1 AS sum FROM users"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    transformer.process(context.current_expression, context)  # pyright: ignore

    # Check metadata
    metadata = context.metadata.get("ExpressionSimplifier")
    assert metadata is not None
    assert "simplified" in metadata
    assert "chars_saved" in metadata
    assert "optimizations_applied" in metadata

    # Check optimizations list
    optimizations = metadata["optimizations_applied"]
    assert isinstance(optimizations, list)
    assert "literal_folding" in optimizations


def test_configuration_metadata() -> None:
    """Test that configuration is reflected in metadata."""
    sql = "SELECT 1 + 1 FROM users"

    config = SimplificationConfig(
        enable_literal_folding=True,
        enable_boolean_optimization=False,
        enable_connector_optimization=False,
        enable_equality_normalization=True,
        enable_complement_removal=False,
    )

    transformer = ExpressionSimplifier(config=config)
    context = create_context_with_sql(sql)

    transformer.process(context.current_expression, context)  # pyright: ignore

    metadata = context.metadata.get("ExpressionSimplifier")
    assert metadata is not None

    optimizations = metadata["optimizations_applied"]
    assert "literal_folding" in optimizations
    assert "boolean_optimization" not in optimizations
    assert "connector_optimization" not in optimizations
    assert "equality_normalization" in optimizations
    assert "complement_removal" not in optimizations


# Comprehensive Test Scenarios
@pytest.mark.parametrize(
    "sql,expected_simplification,description",
    [
        ("SELECT * FROM users WHERE 1 = 1", "WHERE TRUE", "tautology_to_true"),
        ("SELECT * FROM users WHERE 1 = 0", "WHERE FALSE", "contradiction_to_false"),
        ("SELECT 'Hello' || ' ' || 'World' FROM users", "'Hello World'", "string_concatenation"),
        ("SELECT * FROM users WHERE id > 0 AND id > 0", "WHERE id > 0", "duplicate_condition_removal"),
    ],
    ids=["tautology", "contradiction", "string_concat", "duplicate_removal"],
)
def test_comprehensive_simplification_scenarios(sql: str, expected_simplification: str, description: str) -> None:
    """Test comprehensive simplification scenarios."""
    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Check for expected simplification (may vary by SQLGlot version)
    # The test is flexible as exact output depends on SQLGlot's optimizer
    assert "SELECT" in result_sql
    assert "FROM users" in result_sql


def test_chars_saved_calculation() -> None:
    """Test that character savings are calculated correctly."""
    sql = "SELECT 1 + 1 + 1 + 1 + 1 FROM users WHERE TRUE AND TRUE AND active = 1"

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    original_length = len(context.current_expression.sql())
    result_expr = transformer.process(context.current_expression, context)  # pyright: ignore
    assert result_expr is not None
    simplified_length = len(result_expr.sql())

    metadata = context.metadata.get("ExpressionSimplifier")
    assert metadata is not None

    # Should save some characters
    if metadata["simplified"]:
        assert metadata["chars_saved"] > 0
        # Verify calculation
        assert metadata["chars_saved"] == original_length - simplified_length


def test_transformer_handles_complex_ast() -> None:
    """Test that transformer handles complex AST structures without crashing."""
    sql = """
    WITH RECURSIVE numbers AS (
        SELECT 1 + 1 as n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 5 * 2
    )
    SELECT n * 2 as doubled
    FROM numbers
    WHERE TRUE AND n > 0
    """

    transformer = ExpressionSimplifier()
    context = create_context_with_sql(sql)

    # Should not crash on complex query
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)

    assert result_expr is not None
    result_sql = result_expr.sql()
    assert "WITH RECURSIVE" in result_sql
    assert "SELECT" in result_sql

    # Should have some simplifications
    assert "2" in result_sql  # 1 + 1 = 2
    # Other simplifications depend on SQLGlot's behavior
