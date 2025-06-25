"""Unit tests for StatementAnalyzer.

This module tests the StatementAnalyzer including:
- Statement type detection (SELECT, INSERT, UPDATE, DELETE, etc.)
- Table and column extraction
- Complexity analysis and scoring
- Join and subquery detection
- Function usage analysis
- Caching functionality
- ProcessorProtocol implementation
- Edge case handling
"""

from typing import TYPE_CHECKING

import pytest
import sqlglot
from sqlglot import exp

from sqlspec.statement.pipelines.analyzers._analyzer import StatementAnalysis, StatementAnalyzer
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# Test Data and Fixtures
@pytest.fixture
def analyzer() -> StatementAnalyzer:
    """Create a StatementAnalyzer instance for testing."""
    return StatementAnalyzer()


@pytest.fixture
def strict_analyzer() -> StatementAnalyzer:
    """Create an analyzer with strict complexity thresholds."""
    return StatementAnalyzer(max_join_count=2, max_subquery_depth=1, max_function_calls=5, max_where_conditions=3)


@pytest.fixture
def context() -> SQLProcessingContext:
    """Create a processing context."""
    config = SQLConfig(enable_analysis=True)
    return SQLProcessingContext(initial_sql_string="SELECT 1", dialect="mysql", config=config)


# Basic Statement Type Tests
@pytest.mark.parametrize(
    "sql,expected_type,expected_table",
    [
        ("SELECT id, name FROM users WHERE active = 1", "Select", "users"),
        ("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')", "Insert", "users"),
        ("UPDATE users SET active = 0 WHERE last_login < '2023-01-01'", "Update", "users"),
        ("DELETE FROM users WHERE active = 0", "Delete", "users"),
        ("CREATE TABLE test (id INT)", "Create", "test"),
    ],
    ids=["select", "insert", "update", "delete", "create"],
)
def test_statement_type_detection(
    analyzer: StatementAnalyzer, sql: str, expected_type: str, expected_table: str
) -> None:
    """Test detection of different statement types."""
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == expected_type
    if expected_table:
        assert expected_table in analysis.tables


def test_simple_select_analysis(analyzer: StatementAnalyzer) -> None:
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


# Insert Pattern Tests
def test_insert_from_select_detection(analyzer: StatementAnalyzer) -> None:
    """Test detection of INSERT FROM SELECT pattern."""
    sql = "INSERT INTO backup_users SELECT * FROM users WHERE active = 0"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Insert"
    assert analysis.table_name == "backup_users"
    assert analysis.is_from_select
    assert "backup_users" in analysis.tables
    assert "users" in analysis.tables


# Join Analysis Tests
@pytest.mark.parametrize(
    "sql,expected_join_count,expected_tables",
    [
        (
            """
            SELECT u.name, p.title
            FROM users u
            JOIN profiles p ON u.id = p.user_id
            """,
            1,
            ["users", "profiles"],
        ),
        (
            """
            SELECT u.name, p.title, o.total
            FROM users u
            JOIN profiles p ON u.id = p.user_id
            LEFT JOIN orders o ON u.id = o.user_id
            """,
            2,
            ["users", "profiles", "orders"],
        ),
        (
            """
            SELECT * FROM t1
            JOIN t2 ON t1.id = t2.t1_id
            JOIN t3 ON t2.id = t3.t2_id
            JOIN t4 ON t3.id = t4.t3_id
            """,
            3,
            ["t1", "t2", "t3", "t4"],
        ),
    ],
    ids=["single_join", "multiple_joins", "chain_joins"],
)
def test_join_analysis(
    analyzer: StatementAnalyzer, sql: str, expected_join_count: int, expected_tables: list[str]
) -> None:
    """Test analysis of queries with joins."""
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.join_count == expected_join_count
    for table in expected_tables:
        assert table in analysis.tables
    assert analysis.complexity_score > 0


# Subquery Analysis Tests
@pytest.mark.parametrize(
    "sql,should_have_subqueries,expected_tables",
    [
        (
            """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
            """,
            True,
            ["users", "orders"],
        ),
        (
            """
            SELECT * FROM users
            WHERE EXISTS (SELECT 1 FROM profiles WHERE profiles.user_id = users.id)
            """,
            True,
            ["users", "profiles"],
        ),
        (
            """
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
            """,
            True,
            ["users", "orders", "order_items", "products"],
        ),
    ],
    ids=["in_subquery", "exists_subquery", "nested_subqueries"],
)
def test_subquery_detection(
    analyzer: StatementAnalyzer, sql: str, should_have_subqueries: bool, expected_tables: list[str]
) -> None:
    """Test detection and analysis of subqueries."""
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.uses_subqueries == should_have_subqueries
    for table in expected_tables:
        assert table in analysis.tables


# Complex Business Query Tests
def test_complex_business_query_analysis(analyzer: StatementAnalyzer) -> None:
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
    assert analysis.has_aggregation
    assert "GROUP BY" in analysis.operations


# Function Analysis Tests
def test_function_usage_analysis(analyzer: StatementAnalyzer) -> None:
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


# Complexity Threshold Tests
def test_complexity_threshold_warnings(strict_analyzer: StatementAnalyzer) -> None:
    """Test that complexity analysis generates appropriate warnings and issues."""
    # Query with many joins (exceeds strict threshold of 2)
    sql = """
        SELECT *
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        JOIN table4 t4 ON t3.id = t4.t3_id
    """
    analysis = strict_analyzer.analyze_statement(sql, "mysql")

    assert analysis.join_count > 2
    assert len(analysis.complexity_issues) > 0
    assert any("Excessive number of joins" in issue for issue in analysis.complexity_issues)


def test_cartesian_product_detection(analyzer: StatementAnalyzer) -> None:
    """Test detection of potential Cartesian products."""
    sql = """
        SELECT * FROM users, orders
        WHERE users.id > 0
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.potential_cartesian_products > 0
    assert len(analysis.complexity_issues) > 0
    assert any("Cartesian product" in issue for issue in analysis.complexity_issues)


# Union Query Tests
def test_union_query_analysis(analyzer: StatementAnalyzer) -> None:
    """Test analysis of UNION queries."""
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
    assert "UNION" in analysis.operations


# Edge Case Tests
@pytest.mark.parametrize(
    "sql,expected_type",
    [("", "Unknown"), ("SELECT * FROM WHERE invalid syntax", "Unknown"), ("Not valid SQL at all", "Unknown")],
    ids=["empty_sql", "malformed_sql", "invalid_sql"],
)
def test_error_handling(analyzer: StatementAnalyzer, sql: str, expected_type: str) -> None:
    """Test analyzer behavior with invalid SQL."""
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis.statement_type == expected_type


# Caching Tests
def test_statement_caching(analyzer: StatementAnalyzer) -> None:
    """Test that analyzer caches results for performance."""
    sql = "SELECT * FROM users WHERE id = 1"

    # First analysis
    analysis1 = analyzer.analyze_statement(sql, "mysql")

    # Second analysis of same SQL should use cache
    analysis2 = analyzer.analyze_statement(sql, "mysql")

    # Should be the same object (cached)
    assert analysis1 is analysis2


def test_cache_clearing(analyzer: StatementAnalyzer) -> None:
    """Test cache clearing functionality."""
    sql = "SELECT * FROM users WHERE id = 1"

    # Analyze and cache
    analysis1 = analyzer.analyze_statement(sql, "mysql")

    # Clear cache
    analyzer.clear_cache()

    # Analyze again - should create new analysis
    analysis2 = analyzer.analyze_statement(sql, "mysql")

    # Should be different objects (cache was cleared)
    assert analysis1 is not analysis2


# Direct Expression Analysis Tests
def test_expression_analysis(analyzer: StatementAnalyzer) -> None:
    """Test direct expression analysis."""
    sql = "SELECT name, email FROM users WHERE active = 1"
    expression = sqlglot.parse_one(sql, read="mysql")

    analysis = analyzer.analyze_expression(expression)

    assert analysis.statement_type == "Select"
    assert "users" in analysis.tables
    assert analysis.complexity_score >= 0


# ProcessorProtocol Implementation Tests
def test_processor_protocol_implementation(analyzer: StatementAnalyzer, context: SQLProcessingContext) -> None:
    """Test the process method that implements ProcessorProtocol."""
    sql = "SELECT * FROM users"
    expression = sqlglot.parse_one(sql, read="mysql")

    result_expression = analyzer.process(expression, context)

    # Should return unchanged expression
    assert result_expression is expression

    # Should add metadata to context
    metadata = context.metadata.get("StatementAnalyzer")
    assert metadata is not None
    assert "statement_type" in metadata
    assert "table_count" in metadata
    assert "complexity_score" in metadata


def test_processor_with_disabled_analysis(analyzer: StatementAnalyzer) -> None:
    """Test processor when analysis is disabled in config."""
    sql = "SELECT * FROM users"
    expression = sqlglot.parse_one(sql, read="mysql")

    # Create context with analysis disabled
    config = SQLConfig(enable_analysis=False)
    context = SQLProcessingContext(
        initial_sql_string=sql, dialect="mysql", config=config, current_expression=expression
    )

    result_expression = analyzer.process(expression, context)

    # Should return unchanged expression
    assert result_expression is expression

    # Should not add metadata when disabled
    assert "StatementAnalyzer" not in context.metadata


# Comprehensive Metrics Tests
def test_comprehensive_metrics_capture(analyzer: StatementAnalyzer) -> None:
    """Test that analyzer captures comprehensive metrics."""
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


# Window Function Tests
def test_window_function_detection(analyzer: StatementAnalyzer) -> None:
    """Test detection of window functions."""
    sql = """
        SELECT
            name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
        FROM employees
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.has_window_functions


# CTE Tests
def test_cte_detection(analyzer: StatementAnalyzer) -> None:
    """Test detection of Common Table Expressions."""
    sql = """
        WITH ranked_users AS (
            SELECT id, name, score FROM users WHERE active = 1
        )
        SELECT * FROM ranked_users WHERE score > 100
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.cte_count == 1


# Nested Function Tests
def test_nested_function_warnings(analyzer: StatementAnalyzer) -> None:
    """Test warnings for nested function usage."""
    sql = """
        SELECT
            CONCAT(UPPER(TRIM(first_name)), ' ', LOWER(TRIM(last_name))) as full_name
        FROM users
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    # Should detect nested functions
    assert analysis.function_count > 0


# Correlated Subquery Tests
def test_correlated_subquery_detection(analyzer: StatementAnalyzer) -> None:
    """Test detection of correlated subqueries."""
    sql = """
        SELECT u.*
        FROM users u
        WHERE EXISTS (
            SELECT 1
            FROM orders o
            WHERE o.user_id = u.id
            AND o.total > 1000
        )
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.uses_subqueries
    assert analysis.correlated_subquery_count > 0


# Complex WHERE Clause Tests
def test_complex_where_clause_analysis(strict_analyzer: StatementAnalyzer) -> None:
    """Test analysis of complex WHERE clauses."""
    sql = """
        SELECT * FROM users
        WHERE (active = 1 OR suspended = 0)
        AND (created_at > '2023-01-01' OR updated_at > '2023-01-01')
        AND email LIKE '%@example.com'
        AND country IN ('US', 'CA', 'MX')
        AND age BETWEEN 18 AND 65
    """
    analysis = strict_analyzer.analyze_statement(sql, "mysql")

    assert analysis.where_condition_count > 3
    assert len(analysis.complexity_warnings) > 0 or len(analysis.complexity_issues) > 0


# Statement Analysis Object Tests
def test_statement_analysis_attributes() -> None:
    """Test StatementAnalysis dataclass attributes."""
    analysis = StatementAnalysis(
        statement_type="Select",
        expression=exp.Select(),
        table_name="users",
        columns=["id", "name"],
        has_returning=False,
        is_from_select=False,
    )

    assert analysis.statement_type == "Select"
    assert analysis.table_name == "users"
    assert analysis.columns == ["id", "name"]
    assert not analysis.has_returning
    assert not analysis.is_from_select
    assert analysis.tables == []  # Default factory
    assert analysis.complexity_score == 0  # Default value


# Comprehensive Test Scenarios
@pytest.mark.parametrize(
    "sql,min_complexity_score,description",
    [
        ("SELECT id FROM users", 0, "simple_select"),
        ("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", 5, "subquery"),
        (
            """
            SELECT * FROM t1
            JOIN t2 ON t1.id = t2.id
            JOIN t3 ON t2.id = t3.id
            WHERE t1.active = 1
            """,
            6,
            "multiple_joins",
        ),
        (
            """
            WITH RECURSIVE cte AS (
                SELECT id, parent_id FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.parent_id FROM categories c
                JOIN cte ON c.parent_id = cte.id
            )
            SELECT * FROM cte
            """,
            10,
            "recursive_cte",
        ),
    ],
    ids=["simple_select", "subquery", "multiple_joins", "recursive_cte"],
)
def test_complexity_scoring(analyzer: StatementAnalyzer, sql: str, min_complexity_score: int, description: str) -> None:
    """Test complexity scoring for various query types."""
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis.complexity_score >= min_complexity_score
