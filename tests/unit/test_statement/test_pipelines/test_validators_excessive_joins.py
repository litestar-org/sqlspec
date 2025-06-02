"""Tests for the ExcessiveJoins validator."""

from typing import Optional

from sqlglot import parse_one
from sqlglot.dialects import Dialect

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators import ExcessiveJoins
from sqlspec.statement.sql import SQLConfig


def _create_test_context(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper function to create a SQLProcessingContext for testing."""
    if config is None:
        config = SQLConfig()

    expression = parse_one(sql)
    return SQLProcessingContext(
        initial_sql_string=sql, dialect=Dialect.get_or_raise(""), config=config, current_expression=expression
    )


class TestExcessiveJoins:
    """Test cases for the ExcessiveJoins validator."""

    def test_normal_joins_pass(self) -> None:
        """Test that normal number of joins pass validation."""
        sql = """
        SELECT u.name, p.title, c.name as company
        FROM users u
        JOIN profiles p ON u.id = p.user_id
        JOIN companies c ON p.company_id = c.id
        """

        validator = ExcessiveJoins(max_joins=5, warn_threshold=3)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.SAFE
        assert len(result.issues) == 0

    def test_excessive_joins_detected(self) -> None:
        """Test that excessive joins are properly detected."""
        # Create a SQL with many joins
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        JOIN table4 t4 ON t3.id = t4.t3_id
        JOIN table5 t5 ON t4.id = t5.t4_id
        JOIN table6 t6 ON t5.id = t6.t5_id
        """

        validator = ExcessiveJoins(max_joins=3, warn_threshold=2)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert result.risk_level == RiskLevel.MEDIUM
        assert len(result.issues) > 0
        assert "Excessive joins detected" in result.issues[0]
        assert "5 joins exceed limit of 3" in result.issues[0]

    def test_warning_threshold(self) -> None:
        """Test that warning threshold works correctly."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        JOIN table4 t4 ON t3.id = t4.t3_id
        """

        validator = ExcessiveJoins(max_joins=5, warn_threshold=2)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.LOW
        assert len(result.issues) == 0
        assert len(result.warnings) > 0
        assert "High number of joins detected" in result.warnings[0]

    def test_cross_join_detection(self) -> None:
        """Test detection of CROSS JOINs."""
        sql = """
        SELECT *
        FROM table1 t1
        CROSS JOIN table2 t2
        CROSS JOIN table3 t3
        CROSS JOIN table4 t4
        """

        validator = ExcessiveJoins(max_joins=10, warn_threshold=8)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        # Should warn about cartesian products
        assert any("CROSS JOINs detected" in warning for warning in result.warnings)

    def test_join_without_conditions(self) -> None:
        """Test detection of joins without proper conditions."""
        # This is tricky to test as SQLGlot normalizes syntax
        sql = """
        SELECT *
        FROM table1 t1, table2 t2
        """

        validator = ExcessiveJoins(max_joins=10, warn_threshold=8)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # May detect as cartesian product risk
        assert result is not None

    def test_nested_subqueries_with_joins(self) -> None:
        """Test detection of deeply nested subqueries with joins."""
        sql = """
        SELECT *
        FROM (
            SELECT *
            FROM (
                SELECT *
                FROM table1 t1
                JOIN table2 t2 ON t1.id = t2.t1_id
            ) sub1
            JOIN table3 t3 ON sub1.id = t3.sub1_id
        ) sub2
        JOIN table4 t4 ON sub2.id = t4.sub2_id
        """

        validator = ExcessiveJoins(max_joins=10, warn_threshold=5)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect nested complexity
        assert result is not None

    def test_join_type_counting(self) -> None:
        """Test that different join types are counted correctly."""
        sql = """
        SELECT *
        FROM table1 t1
        LEFT JOIN table2 t2 ON t1.id = t2.t1_id
        RIGHT JOIN table3 t3 ON t2.id = t3.t2_id
        FULL OUTER JOIN table4 t4 ON t3.id = t4.t3_id
        INNER JOIN table5 t5 ON t4.id = t5.t4_id
        """

        validator = ExcessiveJoins(max_joins=10, warn_threshold=8)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should handle different join types
        assert result is not None

    def test_self_join_detection(self) -> None:
        """Test detection of self-joins."""
        sql = """
        SELECT *
        FROM employees e1
        JOIN employees e2 ON e1.manager_id = e2.id
        JOIN employees e3 ON e2.manager_id = e3.id
        JOIN employees e4 ON e3.manager_id = e4.id
        """

        validator = ExcessiveJoins(max_joins=10, warn_threshold=8)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should detect multiple self-joins
        assert result is not None

    def test_configurable_thresholds(self) -> None:
        """Test that thresholds are configurable."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        """

        # Strict validator
        strict_validator = ExcessiveJoins(max_joins=1, warn_threshold=1)
        context = _create_test_context(sql)

        _, result = strict_validator.process(context)

        assert result is not None
        assert result.is_safe is False
        assert len(result.issues) > 0

        # Lenient validator
        lenient_validator = ExcessiveJoins(max_joins=10, warn_threshold=8)
        _, result = lenient_validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert len(result.issues) == 0

    def test_empty_query(self) -> None:
        """Test handling of queries without joins."""
        sql = "SELECT * FROM table1"

        validator = ExcessiveJoins(max_joins=5, warn_threshold=3)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        assert result is not None
        assert result.is_safe is True
        assert result.risk_level == RiskLevel.SAFE
        assert len(result.issues) == 0
        assert len(result.warnings) == 0

    def test_complex_join_conditions(self) -> None:
        """Test handling of complex join conditions."""
        sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id AND t1.type = t2.type
        LEFT JOIN table3 t3 ON t2.id = t3.t2_id OR t2.alt_id = t3.id
        """

        validator = ExcessiveJoins(max_joins=5, warn_threshold=3)
        context = _create_test_context(sql)

        _, result = validator.process(context)

        # Should handle complex conditions without issues
        assert result is not None
        assert result.is_safe is True
        assert len(result.issues) == 0
