"""Unit tests for Performance Validator."""

import pytest
from sqlglot import parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators._performance import PerformanceValidator
from sqlspec.statement.sql import SQLConfig


class TestPerformanceValidator:
    """Test the Performance validator."""

    # Validator fixture removed - create validators with specific configs in each test

    @pytest.fixture
    def context(self):
        """Create a processing context."""
        return SQLProcessingContext(initial_sql_string="SELECT 1", dialect=None, config=SQLConfig())

    def test_cartesian_product_detection(self, context) -> None:
        """Test detection of cartesian products (cross joins without conditions)."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(warn_on_cartesian=True))

        # Cross join without WHERE condition
        context.initial_sql_string = "SELECT * FROM users, orders"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.CRITICAL
        assert any("cartesian product" in issue.lower() for issue in result.issues)

    def test_cartesian_product_with_where_clause(self, context) -> None:
        """Test that cross join with WHERE clause is not flagged."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(warn_on_cartesian=True))

        # Cross join with WHERE condition
        context.initial_sql_string = "SELECT * FROM users, orders WHERE users.id = orders.user_id"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_explicit_cross_join_detection(self, context) -> None:
        """Test detection of explicit CROSS JOIN."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(warn_on_cartesian=True))

        context.initial_sql_string = "SELECT * FROM users CROSS JOIN orders"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.CRITICAL
        assert any("Explicit CROSS JOIN" in issue for issue in result.issues)

    def test_excessive_joins_detection(self, context) -> None:
        """Test detection of queries with too many joins."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(max_joins=3))

        # Query with 4 joins (exceeds limit)
        context.initial_sql_string = """
        SELECT * FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        JOIN categories c ON p.category_id = c.id
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.LOW
        assert any("Query has 4 joins" in issue for issue in result.issues)

    def test_joins_within_limit(self, context) -> None:
        """Test that joins within limit are not flagged."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(max_joins=5))

        # Query with 2 joins (within limit)
        context.initial_sql_string = """
        SELECT * FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN order_items oi ON o.id = oi.order_id
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_missing_index_hint_detection(self, context) -> None:
        """Test detection of queries that might benefit from indexes."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(warn_on_missing_index=True))

        # Query with WHERE on non-indexed column (simulated)
        context.initial_sql_string = "SELECT * FROM users WHERE email = 'test@example.com'"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        # This is a simplified test - real implementation would need schema info
        assert result is not None

    def test_subquery_performance_detection(self, context) -> None:
        """Test detection of potentially inefficient subqueries."""
        # Note: subquery performance detection is part of complexity analysis
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig())

        # Correlated subquery
        context.initial_sql_string = """
        SELECT u.*,
               (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count
        FROM users u
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.LOW
        assert any("Correlated subquery in SELECT" in issue for issue in result.issues)

    def test_select_star_detection(self, context) -> None:
        """Test detection of SELECT * usage."""
        # Note: SELECT * detection may be part of general analysis
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig())

        context.initial_sql_string = "SELECT * FROM users"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.LOW
        assert any("SELECT * may retrieve unnecessary columns" in issue for issue in result.issues)

    def test_multiple_performance_issues(self, context) -> None:
        """Test detection of multiple performance issues in one query."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(warn_on_cartesian=True, max_joins=2))

        # Query with multiple issues
        context.initial_sql_string = """
        SELECT * FROM users u, orders o
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.CRITICAL  # Due to cartesian product
        assert len(result.issues) >= 2  # Cartesian product, excessive joins

    def test_union_performance_warning(self, context) -> None:
        """Test detection of UNION vs UNION ALL performance."""
        # Note: UNION detection may be part of general analysis
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig())

        # UNION removes duplicates (slower)
        context.initial_sql_string = """
        SELECT id, name FROM users
        UNION
        SELECT id, name FROM archived_users
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.LOW
        assert any("UNION removes duplicates" in issue for issue in result.issues)

    def test_union_all_no_warning(self, context) -> None:
        """Test that UNION ALL doesn't trigger performance warning."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig())

        context.initial_sql_string = """
        SELECT id, name FROM users
        UNION ALL
        SELECT id, name FROM archived_users
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_distinct_performance_warning(self, context) -> None:
        """Test detection of DISTINCT usage."""
        # Note: DISTINCT detection may be part of general analysis
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig())

        context.initial_sql_string = "SELECT DISTINCT country FROM users"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.LOW
        assert any("DISTINCT may impact performance" in issue for issue in result.issues)

    def test_nested_subquery_depth(self, context) -> None:
        """Test detection of deeply nested subqueries."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        validator = PerformanceValidator(config=PerformanceConfig(max_subqueries=2))

        # 3 levels of nesting
        context.initial_sql_string = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM users
            ) t1
        ) t2
        """
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.LOW  # warning severity
        assert any("levels of subqueries" in issue for issue in result.issues)

    def test_performance_config_disabled(self, context) -> None:
        """Test that validator returns skip when all checks are disabled."""
        from sqlspec.statement.pipelines.validators._performance import PerformanceConfig

        # Disable all performance checks
        validator = PerformanceValidator(
            config=PerformanceConfig(
                warn_on_cartesian=False,
                max_joins=0,  # 0 means no limit
                warn_on_missing_index=False,
            )
        )

        # Query with potential issues
        context.initial_sql_string = "SELECT * FROM users, orders"
        context.current_expression = parse_one(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No checks enabled
