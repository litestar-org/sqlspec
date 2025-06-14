"""Unit tests for sqlspec.statement.pipelines.analyzers._analyzer module.

Tests the StatementAnalyzer class that extracts metadata and insights from SQL statements.
"""

import pytest
import sqlglot

from sqlspec.statement.pipelines.analyzers._analyzer import StatementAnalyzer
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def analyzer() -> StatementAnalyzer:
    """Create a StatementAnalyzer instance for testing."""
    return StatementAnalyzer()


def test_analyzer_simple_select_query(analyzer: StatementAnalyzer) -> None:
    """Test analysis of a simple SELECT query."""
    sql = "SELECT id, name FROM users WHERE active = 1"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.table_name == "users"
    assert "id" in analysis.columns
    assert "name" in analysis.columns
    assert "users" in analysis.tables
    assert not analysis.has_returning
    assert not analysis.is_from_select
    assert not analysis.uses_subqueries
    assert analysis.join_count == 0
    assert analysis.complexity_score >= 0


def test_analyzer_insert_query(analyzer: StatementAnalyzer) -> None:
    """Test analysis of an INSERT query."""
    sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Insert"
    assert "users" in analysis.tables
    assert not analysis.has_returning
    assert not analysis.is_from_select
    assert not analysis.uses_subqueries


def test_analyzer_insert_from_select(analyzer: StatementAnalyzer) -> None:
    """Test analysis of INSERT FROM SELECT pattern."""
    sql = "INSERT INTO backup_users SELECT * FROM users WHERE active = 0"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Insert"
    assert analysis.table_name == "backup_users"
    assert analysis.is_from_select
    assert "backup_users" in analysis.tables
    assert "users" in analysis.tables


def test_analyzer_update_query(analyzer: StatementAnalyzer) -> None:
    """Test analysis of an UPDATE query."""
    sql = "UPDATE users SET active = 0 WHERE last_login < '2023-01-01'"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Update"
    assert analysis.table_name == "users"
    assert "users" in analysis.tables
    assert not analysis.uses_subqueries


def test_analyzer_delete_query(analyzer: StatementAnalyzer) -> None:
    """Test analysis of a DELETE query."""
    sql = "DELETE FROM users WHERE active = 0"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Delete"
    assert analysis.table_name == "users"
    assert "users" in analysis.tables


def test_analyzer_query_with_joins(analyzer: StatementAnalyzer) -> None:
    """Test analysis of a query with joins."""
    sql = """
        SELECT u.name, p.title
        FROM users u
        JOIN profiles p ON u.id = p.user_id
        LEFT JOIN orders o ON u.id = o.user_id
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.join_count == 2
    assert "users" in analysis.tables
    assert "profiles" in analysis.tables
    assert "orders" in analysis.tables
    assert analysis.complexity_score > 0


def test_analyzer_query_with_subqueries(analyzer: StatementAnalyzer) -> None:
    """Test analysis of a query with subqueries."""
    sql = """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.uses_subqueries
    assert "users" in analysis.tables
    assert "orders" in analysis.tables


def test_analyzer_complex_business_query(analyzer: StatementAnalyzer) -> None:
    """Test analysis of a complex business query."""
    sql = """
        SELECT
            u.name,
            u.email,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_revenue,
            AVG(o.total) as avg_order_value
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        AND u.created_at >= '2023-01-01'
        GROUP BY u.id, u.name, u.email
        HAVING COUNT(o.id) > 0
        ORDER BY total_revenue DESC
        LIMIT 100
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.join_count == 1
    assert "users" in analysis.tables
    assert "orders" in analysis.tables
    assert analysis.function_count > 0  # COUNT, SUM, AVG
    assert "count" in analysis.aggregate_functions
    assert "sum" in analysis.aggregate_functions
    assert "avg" in analysis.aggregate_functions
    assert analysis.complexity_score >= 5  # Should be moderately complex


def test_analyzer_query_with_multiple_unions(analyzer: StatementAnalyzer) -> None:
    """Test analysis of a query with multiple UNION operations."""
    sql = """
        SELECT id, name FROM active_users
        UNION
        SELECT id, name FROM inactive_users
        UNION
        SELECT id, name FROM pending_users
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Union"
    assert len(analysis.tables) == 3
    assert "active_users" in analysis.tables
    assert "inactive_users" in analysis.tables
    assert "pending_users" in analysis.tables


def test_analyzer_query_complexity_thresholds(analyzer: StatementAnalyzer) -> None:
    """Test that complexity analysis generates appropriate warnings and issues."""
    # Create a query that should trigger complexity warnings
    sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        JOIN table4 t4 ON t3.id = t4.t3_id
        JOIN table5 t5 ON t4.id = t5.t4_id
        JOIN table6 t6 ON t5.id = t6.t5_id
        WHERE t1.active = 1
        AND t2.status = 'active'
        AND t3.type = 'premium'
        AND t4.category = 'special'
        AND t5.level > 5
        AND t6.enabled = 1
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.join_count >= 5
    assert analysis.complexity_score > 20
    assert len(analysis.tables) == 6


def test_analyzer_query_with_nested_subqueries(analyzer: StatementAnalyzer) -> None:
    """Test analysis of deeply nested subqueries."""
    sql = """
        SELECT * FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE order_id IN (
                SELECT id FROM order_items
                WHERE product_id IN (
                    SELECT id FROM products WHERE category = 'electronics'
                )
            )
        )
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.uses_subqueries
    assert len(analysis.tables) == 4


def test_analyzer_query_with_functions(analyzer: StatementAnalyzer) -> None:
    """Test analysis of queries with various functions."""
    sql = r"""
        SELECT
            UPPER(name),
            LOWER(email),
            CONCAT(first_name, ' ', last_name) as full_name,
            DATE_FORMAT(created_at, '%Y-%m-%d') as created_date,
            COALESCE(phone, 'N/A') as phone_number
        FROM users
        WHERE LENGTH(name) > 3
        AND REGEXP_LIKE(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.function_count >= 5
    assert analysis.complexity_score > 5


def test_analyzer_malformed_sql(analyzer: StatementAnalyzer) -> None:
    """Test analyzer behavior with malformed SQL."""
    malformed_sql = "SELECT * FROM WHERE invalid syntax"
    analysis = analyzer.analyze_statement(malformed_sql, "mysql")

    # Should handle gracefully and return minimal analysis
    assert analysis.statement_type == "Unknown"


def test_analyzer_empty_sql(analyzer: StatementAnalyzer) -> None:
    """Test analyzer behavior with empty SQL."""
    empty_sql = ""
    analysis = analyzer.analyze_statement(empty_sql, "mysql")

    # Should handle gracefully
    assert analysis.statement_type == "Unknown"


def test_analyzer_caching(analyzer: StatementAnalyzer) -> None:
    """Test that analyzer caches results for performance."""
    sql = "SELECT * FROM users WHERE id = 1"

    # First analysis
    analysis1 = analyzer.analyze_statement(sql, "mysql")

    # Second analysis of same SQL should use cache
    analysis2 = analyzer.analyze_statement(sql, "mysql")

    # Should be the same object (cached)
    assert analysis1 is analysis2


def test_analyzer_cache_clearing(analyzer: StatementAnalyzer) -> None:
    """Test cache clearing functionality."""
    sql = "SELECT * FROM users WHERE id = 1"

    # Analyze and cache
    analyzer.analyze_statement(sql, "mysql")

    # Clear cache
    analyzer.clear_cache()

    # Analyze again - should create new analysis
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis is not None


def test_analyzer_expression_analysis(analyzer: StatementAnalyzer) -> None:
    """Test direct expression analysis."""
    sql = "SELECT name, email FROM users WHERE active = 1"
    expression = sqlglot.parse_one(sql, read="mysql")

    analysis = analyzer.analyze_expression(expression)

    assert analysis.statement_type == "Select"
    assert "users" in analysis.tables
    assert analysis.complexity_score >= 0


def test_analyzer_process_method(analyzer: StatementAnalyzer) -> None:
    """Test the process method that implements ProcessorProtocol."""
    sql = "SELECT * FROM users"
    expression = sqlglot.parse_one(sql, read="mysql")
    config = SQLConfig()

    context = SQLProcessingContext(
        initial_sql_string=sql, dialect="mysql", config=config, current_expression=expression
    )

    result_expression = analyzer.process(expression, context)

    # Should return unchanged expression
    assert result_expression is expression


@pytest.mark.parametrize(
    ("sql", "expected_type", "expected_tables"),
    [
        ("SELECT * FROM users", "Select", ["users"]),
        ("INSERT INTO users (name) VALUES ('test')", "Insert", ["users"]),
        ("UPDATE users SET name = 'test'", "Update", ["users"]),
        ("DELETE FROM users WHERE id = 1", "Delete", ["users"]),
        ("CREATE TABLE test (id INT)", "Create", ["test"]),
    ],
    ids=["select", "insert", "update", "delete", "create"],
)
def test_analyzer_statement_types(
    analyzer: StatementAnalyzer, sql: str, expected_type: str, expected_tables: list[str]
) -> None:
    """Test analysis of different statement types."""
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == expected_type
    for table in expected_tables:
        assert table in analysis.tables


def test_analyzer_configurable_thresholds() -> None:
    """Test analyzer with custom complexity thresholds."""
    # Create analyzer with strict thresholds
    strict_analyzer = StatementAnalyzer(
        max_join_count=2, max_subquery_depth=1, max_function_calls=5, max_where_conditions=3
    )

    # Query that should trigger warnings with strict settings
    sql = """
        SELECT * FROM t1
        JOIN t2 ON t1.id = t2.t1_id
        JOIN t3 ON t2.id = t3.t2_id
        JOIN t4 ON t3.id = t4.t3_id
    """
    analysis = strict_analyzer.analyze_statement(sql, "mysql")

    assert analysis.join_count > 2
    assert len(analysis.complexity_issues) > 0 or len(analysis.complexity_warnings) > 0


def test_analyzer_comprehensive_metrics() -> None:
    """Test that analyzer captures comprehensive metrics."""
    analyzer = StatementAnalyzer()

    sql = """
        SELECT
            u.name,
            COUNT(*) as order_count,
            SUM(o.total) as revenue
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        AND EXISTS (SELECT 1 FROM profiles p WHERE p.user_id = u.id)
        GROUP BY u.id, u.name
        HAVING COUNT(*) > 5
        ORDER BY revenue DESC
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    # Check that all expected metrics are captured
    assert isinstance(analysis.complexity_score, int)
    assert isinstance(analysis.join_count, int)
    assert isinstance(analysis.function_count, int)
    assert isinstance(analysis.where_condition_count, int)
    assert isinstance(analysis.max_subquery_depth, int)
    assert isinstance(analysis.uses_subqueries, bool)
    assert isinstance(analysis.tables, list)
    assert isinstance(analysis.aggregate_functions, list)
    assert isinstance(analysis.complexity_warnings, list)
    assert isinstance(analysis.complexity_issues, list)
