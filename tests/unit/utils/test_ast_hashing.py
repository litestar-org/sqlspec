"""Tests for AST hashing utilities."""

from sqlglot import exp, parse_one

from sqlspec.utils import hash_expression


class TestASTHashing:
    """Test AST hashing functionality."""

    def test_hash_none(self) -> None:
        """Test hashing None returns consistent value."""
        assert hash_expression(None) == hash(None)
        # Multiple calls should return same value
        assert hash_expression(None) == hash_expression(None)

    def test_hash_simple_expression(self) -> None:
        """Test hashing simple expressions."""
        expr1 = parse_one("SELECT 1")
        expr2 = parse_one("SELECT 1")

        # Same query should produce same hash
        assert hash_expression(expr1) == hash_expression(expr2)

        # Different query should produce different hash
        expr3 = parse_one("SELECT 2")
        assert hash_expression(expr1) != hash_expression(expr3)

    def test_hash_complex_query(self) -> None:
        """Test hashing complex queries with joins and where clauses."""
        query1 = parse_one("""
            SELECT a.id, b.name
            FROM users a
            JOIN departments b ON a.dept_id = b.id
            WHERE a.active = true
        """)
        query2 = parse_one("""
            SELECT a.id, b.name
            FROM users a
            JOIN departments b ON a.dept_id = b.id
            WHERE a.active = true
        """)

        # Identical complex queries should have same hash
        assert hash_expression(query1) == hash_expression(query2)

    def test_hash_different_where_clauses(self) -> None:
        """Test that different WHERE clauses produce different hashes."""
        query1 = parse_one("SELECT * FROM users WHERE active = true")
        query2 = parse_one("SELECT * FROM users WHERE active = false")

        assert hash_expression(query1) != hash_expression(query2)

    def test_hash_column_order_matters(self) -> None:
        """Test that column order affects hash."""
        query1 = parse_one("SELECT a, b FROM table1")
        query2 = parse_one("SELECT b, a FROM table1")

        # Different column order should produce different hash
        assert hash_expression(query1) != hash_expression(query2)

    def test_hash_with_subqueries(self) -> None:
        """Test hashing queries with subqueries."""
        query1 = parse_one("""
            SELECT * FROM (
                SELECT id, name FROM users WHERE active = true
            ) AS active_users
        """)
        query2 = parse_one("""
            SELECT * FROM (
                SELECT id, name FROM users WHERE active = true
            ) AS active_users
        """)

        assert hash_expression(query1) == hash_expression(query2)

    def test_hash_with_cte(self) -> None:
        """Test hashing queries with CTEs."""
        query1 = parse_one("""
            WITH active_users AS (
                SELECT * FROM users WHERE active = true
            )
            SELECT * FROM active_users
        """)
        query2 = parse_one("""
            WITH active_users AS (
                SELECT * FROM users WHERE active = true
            )
            SELECT * FROM active_users
        """)

        assert hash_expression(query1) == hash_expression(query2)

    def test_circular_reference_handling(self) -> None:
        """Test that circular references are handled properly."""
        # Create a simple expression
        expr = parse_one("SELECT 1")

        # Manually create a circular reference (this is artificial but tests the safety)
        if hasattr(expr, "parent"):
            expr.parent = expr

        # Should not raise an exception
        hash1 = hash_expression(expr)
        assert isinstance(hash1, int)

    def test_hash_stability_across_runs(self) -> None:
        """Test that hashes are stable across multiple runs."""
        query = "SELECT a, b, c FROM table1 WHERE x > 10 AND y < 20"

        hashes = []
        for _ in range(5):
            expr = parse_one(query)
            hashes.append(hash_expression(expr))

        # All hashes should be identical
        assert len(set(hashes)) == 1

    def test_hash_with_different_aliases(self) -> None:
        """Test that different aliases produce different hashes."""
        query1 = parse_one("SELECT a AS col1 FROM table1")
        query2 = parse_one("SELECT a AS col2 FROM table1")

        assert hash_expression(query1) != hash_expression(query2)

    def test_hash_expression_types(self) -> None:
        """Test hashing different expression types."""
        # Column expression
        col = exp.Column(this=exp.Identifier(this="col1"))
        hash1 = hash_expression(col)

        # Table expression
        table = exp.Table(this=exp.Identifier(this="table1"))
        hash2 = hash_expression(table)

        # Different types should have different hashes
        assert hash1 != hash2

    def test_hash_with_parameters(self) -> None:
        """Test that parameter placeholders are hashed correctly."""
        query1 = parse_one("SELECT * FROM users WHERE id = :id")
        query2 = parse_one("SELECT * FROM users WHERE id = :user_id")

        # Different parameter names should produce different hashes
        assert hash_expression(query1) != hash_expression(query2)
