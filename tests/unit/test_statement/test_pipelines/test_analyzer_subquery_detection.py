"""Tests for subquery detection in StatementAnalyzer.

This module contains tests that verify subquery detection works correctly
with our workarounds for sqlglot parser limitations, and includes expected
future behavior tests marked with xfail.
"""

import pytest
from sqlglot import exp

from sqlspec.statement.pipelines.analyzers._analyzer import StatementAnalyzer


class TestSubqueryDetection:
    """Test subquery detection in various SQL contexts."""

    @pytest.fixture
    def analyzer(self) -> StatementAnalyzer:
        """Create a StatementAnalyzer for testing."""
        return StatementAnalyzer()

    def test_subquery_detection_in_in_clause(self, analyzer: StatementAnalyzer) -> None:
        """Test that subqueries in IN clauses are detected (using workaround)."""
        sql = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
        """
        analysis = analyzer.analyze_statement(sql, "mysql")

        assert analysis.statement_type == "Select"
        assert analysis.uses_subqueries is True
        assert analysis.table_name == "users"

    def test_subquery_detection_with_exists(self, analyzer: StatementAnalyzer) -> None:
        """Test that subqueries in EXISTS clauses are detected."""
        sql = """
            SELECT * FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        """
        analysis = analyzer.analyze_statement(sql, "mysql")

        assert analysis.statement_type == "Select"
        assert analysis.uses_subqueries is True

    def test_multiple_select_statements_detected_as_subqueries(self, analyzer: StatementAnalyzer) -> None:
        """Test that multiple SELECT statements indicate subquery presence."""
        sql = """
            SELECT u.name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count
            FROM users u
        """
        analysis = analyzer.analyze_statement(sql, "mysql")

        assert analysis.statement_type == "Select"
        assert analysis.uses_subqueries is True

    def test_no_subqueries_detected_for_simple_query(self, analyzer: StatementAnalyzer) -> None:
        """Test that simple queries without subqueries return False."""
        sql = "SELECT id, name FROM users WHERE active = 1"
        analysis = analyzer.analyze_statement(sql, "mysql")

        assert analysis.statement_type == "Select"
        assert analysis.uses_subqueries is False

    def test_subquery_count_includes_in_clause_subqueries(self, analyzer: StatementAnalyzer) -> None:
        """Test that subquery analysis counts IN clause subqueries correctly."""
        sql = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
            AND department_id IN (SELECT id FROM departments WHERE active = 1)
        """
        analysis = analyzer.analyze_statement(sql, "mysql")

        # The _analyze_subqueries method should detect both IN clause subqueries
        assert analysis.uses_subqueries is True

    @pytest.mark.xfail(reason="sqlglot parser limitation: subqueries in IN clauses not wrapped in Subquery nodes")
    def test_standard_subquery_detection_in_in_clause(self, analyzer: StatementAnalyzer) -> None:
        """Test that subqueries in IN clauses are properly wrapped in Subquery nodes.

        This test is expected to fail with current sqlglot versions due to parser
        limitations. It should pass when sqlglot fixes the parser to wrap subqueries
        in IN clauses with proper Subquery expression nodes.
        """
        import sqlglot

        sql = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
        """
        parsed = sqlglot.parse_one(sql, dialect="mysql")

        # This should find 1 Subquery node when the parser is fixed
        subqueries = list(parsed.find_all(exp.Subquery))
        assert len(subqueries) == 1

    @pytest.mark.xfail(reason="sqlglot parser limitation: subqueries in EXISTS clauses may not be wrapped properly")
    def test_standard_subquery_detection_in_exists_clause(self, analyzer: StatementAnalyzer) -> None:
        """Test that subqueries in EXISTS clauses are properly wrapped in Subquery nodes.

        This test verifies the expected future behavior when sqlglot parser
        properly wraps all subqueries in Subquery expression nodes.
        """
        import sqlglot

        sql = """
            SELECT * FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        """
        parsed = sqlglot.parse_one(sql, dialect="mysql")

        # This should find 1 Subquery node when the parser is fixed
        subqueries = list(parsed.find_all(exp.Subquery))
        assert len(subqueries) == 1

    def test_current_workaround_behavior_documentation(self, analyzer: StatementAnalyzer) -> None:
        """Document current workaround behavior for future reference.

        This test documents how our workaround currently detects subqueries
        and serves as a regression test to ensure the workaround continues
        to work until sqlglot fixes the parser.
        """
        import sqlglot

        sql = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
        """
        parsed = sqlglot.parse_one(sql, dialect="mysql")

        # Current behavior: no Subquery nodes found
        subqueries = list(parsed.find_all(exp.Subquery))
        assert len(subqueries) == 0, "Current sqlglot behavior: IN clause subqueries not wrapped"

        # Current behavior: IN clause contains Select node directly
        in_clauses = list(parsed.find_all(exp.In))
        assert len(in_clauses) == 1

        in_clause = in_clauses[0]
        assert "query" in in_clause.args, "IN clause should have query in args"
        query_node = in_clause.args.get("query")
        assert isinstance(query_node, exp.Select), "Query should be Select node"

        # Our workaround successfully detects this
        analysis = analyzer.analyze_statement(sql, "mysql")
        assert analysis.uses_subqueries is True, "Workaround should detect subquery"

    def test_complex_nested_subqueries(self, analyzer: StatementAnalyzer) -> None:
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
        # This query has multiple levels of nesting and various subquery contexts

    @pytest.mark.parametrize(
        "sql_template,description",
        [
            ("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", "IN clause subquery"),
            (
                "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
                "EXISTS clause subquery",
            ),
            ("SELECT id, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users", "SELECT clause subquery"),
            (
                "SELECT * FROM (SELECT user_id FROM orders GROUP BY user_id) subq",
                "FROM clause subquery (derived table)",
            ),
        ],
    )
    def test_subquery_detection_patterns(
        self, analyzer: StatementAnalyzer, sql_template: str, description: str
    ) -> None:
        """Test various subquery patterns are detected by our workaround."""
        analysis = analyzer.analyze_statement(sql_template, "mysql")
        assert analysis.uses_subqueries is True, f"Failed to detect {description}"
