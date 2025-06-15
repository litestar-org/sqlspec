"""Unit tests for StatementAnalyzer subquery detection.

This module contains tests that verify subquery detection works correctly
with our workarounds for sqlglot parser limitations, and includes expected
future behavior tests marked with xfail.

Tests include:
- Subquery detection in IN clauses
- Subquery detection in EXISTS clauses
- Multiple SELECT statement detection
- Complex nested subqueries
- Various subquery patterns (SELECT, FROM, WHERE clauses)
- SQLGlot parser workaround verification
"""

from typing import TYPE_CHECKING

import pytest
import sqlglot
from sqlglot import exp

from sqlspec.statement.pipelines.analyzers._analyzer import StatementAnalyzer

if TYPE_CHECKING:
    pass


# Test Fixtures
@pytest.fixture
def analyzer() -> StatementAnalyzer:
    """Create a StatementAnalyzer for testing."""
    return StatementAnalyzer()


# Basic Subquery Detection Tests
def test_subquery_detection_in_in_clause(analyzer: StatementAnalyzer) -> None:
    """Test that subqueries in IN clauses are detected (using workaround)."""
    sql = """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.uses_subqueries is True
    assert analysis.table_name == "users"


def test_subquery_detection_with_exists(analyzer: StatementAnalyzer) -> None:
    """Test that subqueries in EXISTS clauses are detected."""
    sql = """
        SELECT * FROM users u
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.uses_subqueries is True


def test_multiple_select_statements_detected_as_subqueries(analyzer: StatementAnalyzer) -> None:
    """Test that multiple SELECT statements indicate subquery presence."""
    sql = """
        SELECT u.name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count
        FROM users u
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.uses_subqueries is True


def test_no_subqueries_detected_for_simple_query(analyzer: StatementAnalyzer) -> None:
    """Test that simple queries without subqueries return False."""
    sql = "SELECT id, name FROM users WHERE active = 1"
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.uses_subqueries is False


# Multiple Subquery Tests
def test_subquery_count_includes_in_clause_subqueries(analyzer: StatementAnalyzer) -> None:
    """Test that subquery analysis counts IN clause subqueries correctly."""
    sql = """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
        AND department_id IN (SELECT id FROM departments WHERE active = 1)
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    # The _analyze_subqueries method should detect both IN clause subqueries
    assert analysis.uses_subqueries is True
    assert analysis.subquery_count >= 2


# SQLGlot Parser Limitation Tests
@pytest.mark.xfail(reason="sqlglot parser limitation: subqueries in IN clauses not wrapped in Subquery nodes")
def test_standard_subquery_detection_in_in_clause(analyzer: StatementAnalyzer) -> None:
    """Test that subqueries in IN clauses are properly wrapped in Subquery nodes.

    This test is expected to fail with current sqlglot versions due to parser
    limitations. It should pass when sqlglot fixes the parser to wrap subqueries
    in IN clauses with proper Subquery expression nodes.
    """
    sql = """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
    """
    parsed = sqlglot.parse_one(sql, dialect="mysql")

    # This should find 1 Subquery node when the parser is fixed
    subqueries = list(parsed.find_all(exp.Subquery))
    assert len(subqueries) == 1


@pytest.mark.xfail(reason="sqlglot parser limitation: subqueries in EXISTS clauses may not be wrapped properly")
def test_standard_subquery_detection_in_exists_clause(analyzer: StatementAnalyzer) -> None:
    """Test that subqueries in EXISTS clauses are properly wrapped in Subquery nodes.

    This test verifies the expected future behavior when sqlglot parser
    properly wraps all subqueries in Subquery expression nodes.
    """
    sql = """
        SELECT * FROM users u
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
    """
    parsed = sqlglot.parse_one(sql, dialect="mysql")

    # This should find 1 Subquery node when the parser is fixed
    subqueries = list(parsed.find_all(exp.Subquery))
    assert len(subqueries) == 1


# Current Workaround Behavior Documentation
def test_current_workaround_behavior_documentation(analyzer: StatementAnalyzer) -> None:
    """Document current workaround behavior for future reference.

    This test documents how our workaround currently detects subqueries
    and serves as a regression test to ensure the workaround continues
    to work until sqlglot fixes the parser.
    """
    sql = """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
    """
    parsed = sqlglot.parse_one(sql, dialect="mysql")

    # Updated behavior: sqlglot now properly wraps subqueries in IN clauses
    subqueries = list(parsed.find_all(exp.Subquery))
    assert len(subqueries) == 1, "Updated sqlglot behavior: IN clause subqueries are now wrapped"

    # Updated behavior: IN clause contains Subquery node that wraps Select
    in_clauses = list(parsed.find_all(exp.In))
    assert len(in_clauses) == 1

    in_clause = in_clauses[0]
    assert "query" in in_clause.args, "IN clause should have query in args"
    query_node = in_clause.args.get("query")
    assert isinstance(query_node, exp.Subquery), "Query should be Subquery node"

    # Our workaround successfully detects this
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis.uses_subqueries is True, "Workaround should detect subquery"


# Complex Nested Subquery Tests
def test_complex_nested_subqueries(analyzer: StatementAnalyzer) -> None:
    """Test detection of complex nested subqueries using our workaround."""
    sql = """
        SELECT u.name,
               (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count
        FROM users u
        WHERE u.id IN (
            SELECT DISTINCT user_id
            FROM orders
            WHERE total > (SELECT AVG(total) FROM orders)
        )
        AND EXISTS (
            SELECT 1 FROM user_permissions up
            WHERE up.user_id = u.id AND up.permission = 'admin'
        )
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Select"
    assert analysis.uses_subqueries is True
    assert analysis.max_subquery_depth >= 2  # Has nested subqueries
    # This query has multiple levels of nesting and various subquery contexts


# Parameterized Pattern Tests
@pytest.mark.parametrize(
    "sql_template,description",
    [
        ("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", "IN clause subquery"),
        (
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
            "EXISTS clause subquery",
        ),
        ("SELECT id, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users", "SELECT clause subquery"),
        ("SELECT * FROM (SELECT user_id FROM orders GROUP BY user_id) subq", "FROM clause subquery (derived table)"),
    ],
    ids=["in_clause", "exists_clause", "select_clause", "from_clause"],
)
def test_subquery_detection_patterns(analyzer: StatementAnalyzer, sql_template: str, description: str) -> None:
    """Test various subquery patterns are detected by our workaround."""
    analysis = analyzer.analyze_statement(sql_template, "mysql")
    assert analysis.uses_subqueries is True, f"Failed to detect {description}"


# Correlated Subquery Tests
@pytest.mark.parametrize(
    "sql,expected_correlated",
    [
        (
            """
            SELECT * FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id
            )
            """,
            True,
        ),
        (
            """
            SELECT * FROM users
            WHERE id IN (
                SELECT user_id FROM orders
                WHERE total > 100
            )
            """,
            False,
        ),
    ],
    ids=["correlated_exists", "non_correlated_in"],
)
def test_correlated_subquery_detection(analyzer: StatementAnalyzer, sql: str, expected_correlated: bool) -> None:
    """Test detection of correlated vs non-correlated subqueries."""
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis.uses_subqueries is True
    if expected_correlated:
        assert analysis.correlated_subquery_count > 0


# Subquery Depth Tests
@pytest.mark.parametrize(
    "sql,min_depth",
    [
        ("SELECT * FROM users", 0),
        ("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", 1),
        (
            """
            SELECT * FROM users
            WHERE id IN (
                SELECT user_id FROM orders
                WHERE total > (SELECT AVG(total) FROM orders)
            )
            """,
            2,
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
            3,
        ),
    ],
    ids=["no_subquery", "single_level", "double_nested", "triple_nested"],
)
def test_subquery_depth_calculation(analyzer: StatementAnalyzer, sql: str, min_depth: int) -> None:
    """Test correct calculation of subquery nesting depth."""
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis.max_subquery_depth >= min_depth


# Edge Cases
def test_cte_not_counted_as_subquery(analyzer: StatementAnalyzer) -> None:
    """Test that CTEs are not counted as subqueries."""
    sql = """
        WITH user_orders AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT u.*, uo.order_count
        FROM users u
        JOIN user_orders uo ON u.id = uo.user_id
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    # CTE should be detected separately
    assert analysis.cte_count == 1
    # The SELECT inside CTE is not a subquery in the traditional sense
    assert analysis.uses_subqueries is False


def test_union_queries_subquery_detection(analyzer: StatementAnalyzer) -> None:
    """Test subquery detection in UNION queries."""
    sql = """
        SELECT id, name FROM users WHERE id IN (SELECT user_id FROM active_sessions)
        UNION
        SELECT id, name FROM users WHERE id IN (SELECT user_id FROM inactive_sessions)
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    assert analysis.statement_type == "Union"
    assert analysis.uses_subqueries is True
    assert analysis.subquery_count >= 2


# Performance Warning Tests
def test_excessive_subquery_warnings(analyzer: StatementAnalyzer) -> None:
    """Test that excessive subqueries generate warnings."""
    # Create analyzer with low thresholds for testing
    strict_analyzer = StatementAnalyzer(max_subquery_depth=2)

    sql = """
        SELECT * FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE order_id IN (
                SELECT id FROM order_items
                WHERE product_id IN (
                    SELECT id FROM products
                )
            )
        )
    """

    analysis = strict_analyzer.analyze_statement(sql, "mysql")

    assert analysis.uses_subqueries is True
    assert analysis.max_subquery_depth > 2
    assert len(analysis.complexity_issues) > 0
    assert any("subquery nesting depth" in issue for issue in analysis.complexity_issues)


# Empty and Invalid SQL Tests
@pytest.mark.parametrize("sql", ["", "INVALID SQL", "SELECT FROM WHERE"], ids=["empty", "invalid", "malformed"])
def test_invalid_sql_subquery_detection(analyzer: StatementAnalyzer, sql: str) -> None:
    """Test subquery detection with invalid SQL."""
    analysis = analyzer.analyze_statement(sql, "mysql")
    assert analysis.statement_type == "Unknown"
    assert analysis.uses_subqueries is False


# Subquery Table Extraction Tests
def test_subquery_table_extraction(analyzer: StatementAnalyzer) -> None:
    """Test that tables from subqueries are correctly extracted."""
    sql = """
        SELECT * FROM users u
        WHERE u.id IN (
            SELECT user_id
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            WHERE oi.product_id IN (
                SELECT id FROM products WHERE category = 'electronics'
            )
        )
    """
    analysis = analyzer.analyze_statement(sql, "mysql")

    # Should extract all tables including those in subqueries
    expected_tables = ["users", "orders", "order_items", "products"]
    for table in expected_tables:
        assert table in analysis.tables
