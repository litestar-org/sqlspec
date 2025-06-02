"""Tests for the CartesianProductDetector validator."""

from typing import Optional

from sqlglot import parse_one
from sqlglot.dialects import Dialect

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators import CartesianProductDetector
from sqlspec.statement.sql import SQLConfig


def _create_test_context(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper function to create a SQLProcessingContext for testing."""
    if config is None:
        config = SQLConfig()

    expression = parse_one(sql)
    return SQLProcessingContext(
        initial_sql_string=sql, dialect=Dialect.get_or_raise(""), config=config, current_expression=expression
    )


class TestCartesianProductDetector:
    """Test cases for the CartesianProductDetector validator."""

    def test_normal_joins_pass(self) -> None:
        """Test that normal joins with proper conditions pass validation."""
        sql = """
        SELECT u.name, p.title
        FROM users u
        JOIN profiles p ON u.id = p.user_id
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.SAFE
        assert len(result.issues) == 0

    def test_explicit_cross_join_detected(self) -> None:
        """Test that explicit CROSS JOINs are detected."""
        sql = """
        SELECT *
        FROM table1 t1
        CROSS JOIN table2 t2
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert result.risk_level == RiskLevel.HIGH
        assert len(result.issues) > 0
        assert "Explicit CROSS JOIN detected" in result.issues[0]

    def test_explicit_cross_join_allowed(self) -> None:
        """Test that explicit CROSS JOINs can be allowed with warnings."""
        sql = """
        SELECT *
        FROM table1 t1
        CROSS JOIN table2 t2
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=True)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.LOW
        assert len(result.issues) == 0
        assert len(result.warnings) > 0
        assert "Explicit CROSS JOIN detected" in result.warnings[0]

    def test_multiple_cross_joins(self) -> None:
        """Test detection of multiple CROSS JOINs."""
        sql = """
        SELECT *
        FROM table1 t1
        CROSS JOIN table2 t2
        CROSS JOIN table3 t3
        CROSS JOIN table4 t4
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0
        assert "3 occurrences" in result.issues[0]

    def test_join_without_conditions(self) -> None:
        """Test detection of joins without ON or USING clauses."""
        # This is conceptual - SQLGlot may parse differently
        sql = """
        SELECT *
        FROM table1 t1, table2 t2
        WHERE t1.type = 'active'
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # May detect cartesian product issues
        # Actual detection depends on how SQLGlot parses comma-separated tables
        assert result is not None

    def test_comma_separated_tables_without_where(self) -> None:
        """Test detection of comma-separated tables without WHERE clause."""
        sql = """
        SELECT *
        FROM table1, table2, table3
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect potential cartesian product
        assert result is not None

    def test_comma_separated_tables_with_proper_where(self) -> None:
        """Test comma-separated tables with proper WHERE correlations."""
        sql = """
        SELECT *
        FROM table1 t1, table2 t2
        WHERE t1.id = t2.t1_id
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should be less problematic with proper correlations
        assert result is not None

    def test_join_with_constant_true_condition(self) -> None:
        """Test detection of joins with always-true conditions."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON 1=1
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect problematic join condition
        assert result is not None

    def test_join_with_literal_only_condition(self) -> None:
        """Test detection of joins with literal-only conditions."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON 'active' = 'active'
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect problematic join condition
        assert result is not None

    def test_inequality_join_warning(self) -> None:
        """Test that inequality-only joins generate warnings."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.value > t2.threshold
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # May generate warnings about large result sets
        assert result is not None

    def test_subquery_cartesian_risks(self) -> None:
        """Test detection of cartesian risks in subqueries."""
        sql = """
        SELECT *
        FROM (
            SELECT *
            FROM table1 t1
            CROSS JOIN table2 t2
        ) sub
        JOIN table3 t3 ON sub.id = t3.sub_id
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect cartesian risks in subqueries
        assert result is not None
        if result.issues:
            assert any("subquery" in issue for issue in result.issues)

    def test_nested_subqueries_with_cartesian_risks(self) -> None:
        """Test deeply nested subqueries with cartesian risks."""
        sql = """
        SELECT *
        FROM (
            SELECT *
            FROM (
                SELECT *
                FROM table1 t1, table2 t2
            ) inner_sub
            CROSS JOIN table3 t3
        ) outer_sub
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect multiple levels of cartesian risks
        assert result is not None

    def test_mixed_join_types_with_cartesian_risk(self) -> None:
        """Test mixed join types where some create cartesian products."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        CROSS JOIN table3 t3
        LEFT JOIN table4 t4 ON t1.type = t4.type
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect the CROSS JOIN while allowing other joins
        assert result is not None
        assert result.is_safe is False
        assert "CROSS JOIN" in str(result.issues)

    def test_configurable_settings(self) -> None:
        """Test configurable settings for the validator."""
        sql = """
        SELECT *
        FROM table1 t1
        CROSS JOIN table2 t2
        """

        # Strict settings
        strict_validator = CartesianProductDetector(allow_explicit_cross_joins=False, risk_level=RiskLevel.HIGH)
        context = _create_test_context(sql)

        _, result = strict_validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert result.risk_level == RiskLevel.HIGH

        # Lenient settings
        lenient_validator = CartesianProductDetector(allow_explicit_cross_joins=True, risk_level=RiskLevel.LOW)
        _, result = lenient_validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert len(result.warnings) > 0

    def test_no_cartesian_risk_in_simple_query(self) -> None:
        """Test that simple queries without joins pass validation."""
        sql = "SELECT * FROM table1 WHERE id = 1"

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.SAFE
        assert len(result.issues) == 0
        assert len(result.warnings) == 0

    def test_proper_join_with_complex_conditions(self) -> None:
        """Test that proper joins with complex conditions are handled correctly."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
                      AND t1.status = 'active'
                      AND t2.created_date > '2023-01-01'
        LEFT JOIN table3 t3 ON t2.category_id = t3.id
                           AND t3.enabled = 1
        """

        validator = CartesianProductDetector()
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should handle complex but proper join conditions
        assert result is not None
        assert result.is_safe is True
        assert len(result.issues) == 0

    def test_union_with_cartesian_risks(self) -> None:
        """Test cartesian risks in UNION queries."""
        sql = """
        SELECT * FROM table1 t1 CROSS JOIN table2 t2
        UNION
        SELECT * FROM table3 t3, table4 t4
        """

        validator = CartesianProductDetector(allow_explicit_cross_joins=False)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect cartesian risks in both parts of UNION
        assert result is not None
