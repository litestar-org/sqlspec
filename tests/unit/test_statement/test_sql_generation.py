"""Test SQL generation performance optimizations."""

import time

import pytest

from sqlspec.statement.sql import SQL, StatementConfig


class TestSQLGenerationPerformance:
    """Test performance optimizations in SQL generation."""

    def test_copy_false_optimization_functional(self) -> None:
        """Verify copy=False optimization doesn't break functionality."""
        sql = SQL("SELECT * FROM users WHERE id = ?", 123)

        # Should generate identical SQL regardless of copy parameter
        original_sql, original_parameters = sql.compile()

        # This should work identically after optimization
        assert original_sql == "SELECT * FROM users WHERE id = ?"
        assert original_parameters == (123,)

    def test_expression_reuse_safety(self) -> None:
        """Verify expressions can be safely reused after copy=False."""
        base_sql = "SELECT * FROM users WHERE active = ?"
        sql1 = SQL(base_sql, True)
        sql2 = SQL(base_sql, False)

        # Both should compile independently
        sql1_compiled, parameters1 = sql1.compile()
        sql2_compiled, parameters2 = sql2.compile()

        assert sql1_compiled == sql2_compiled
        assert parameters1 != parameters2
        assert parameters1 == (True,)
        assert parameters2 == (False,)

    def test_complex_query_functionality(self):
        """Test that complex queries still work correctly after optimization."""
        complex_query = """
        SELECT u.id, u.name, p.title, c.name as category
        FROM users u
        JOIN posts p ON u.id = p.user_id
        JOIN categories c ON p.category_id = c.id
        WHERE u.active = ? AND p.published = ?
        ORDER BY p.created_at DESC
        LIMIT ?
        """

        sql = SQL(complex_query, True, True, 10)
        compiled_sql, parameters = sql.compile()

        # Should compile without errors
        assert "SELECT" in compiled_sql
        assert "JOIN" in compiled_sql
        assert parameters == (True, True, 10)

    def test_batch_operation_functionality(self):
        """Test that batch operations work correctly after optimization."""
        base_query = "INSERT INTO users (name, email) VALUES (?, ?)"
        batch_parameters = [
            ("user1", "user1@example.com"),
            ("user2", "user2@example.com"),
            ("user3", "user3@example.com"),
        ]

        sql = SQL(base_query).as_many(batch_parameters)
        compiled_sql, parameters = sql.compile()

        # Should handle batch operations correctly
        assert "INSERT INTO users" in compiled_sql
        assert parameters == batch_parameters

    def test_parameter_styles_maintained(self):
        """Test that parameter style handling is preserved."""
        # Test different parameter styles
        queries_and_parameters = [
            ("SELECT * FROM users WHERE id = ?", (1,)),
            ("SELECT * FROM users WHERE id = :id", {"id": 1}),
            ("SELECT * FROM users WHERE id = %(id)s", {"id": 1}),
        ]

        for query, parameters in queries_and_parameters:
            if isinstance(parameters, tuple):
                sql = SQL(query, *parameters)
            else:
                sql = SQL(query, **parameters)

            compiled_sql, compiled_parameters = sql.compile()
            # Should compile without errors
            assert "SELECT" in compiled_sql

    def test_script_execution_safety(self):
        """Test that script execution works correctly after optimization."""
        script = """
        CREATE TABLE test_table (id INTEGER PRIMARY KEY);
        INSERT INTO test_table (id) VALUES (1);
        SELECT * FROM test_table;
        """

        sql = SQL(script).as_script()
        compiled_sql, parameters = sql.compile()

        # Scripts should compile correctly
        assert "CREATE TABLE" in compiled_sql
        assert "INSERT INTO" in compiled_sql
        assert "SELECT" in compiled_sql

    @pytest.mark.benchmark
    def test_copy_optimization_performance_basic(self):
        """Basic performance test for copy=False optimization."""
        test_queries = [
            ("SELECT * FROM users WHERE id = ?", (1,)),
            ("INSERT INTO users (name, email) VALUES (?, ?)", ("test", "test@example.com")),
            ("UPDATE users SET name = ? WHERE id = ?", ("updated", 1)),
            ("DELETE FROM users WHERE id = ?", (1,)),
        ]

        iterations = 50

        start_time = time.perf_counter()
        for _ in range(iterations):
            for query, parameters in test_queries:
                sql = SQL(query, *parameters)
                sql.compile()
        end_time = time.perf_counter()

        execution_time = end_time - start_time

        # Performance should be reasonable (not a strict assertion, just monitoring)
        # With copy=False optimization, this should be significantly faster

        # Basic sanity check - should complete in reasonable time
        assert execution_time < 5.0  # Should complete within 5 seconds

    def test_expression_mutation_safety(self):
        """Test that expressions aren't mutated unsafely after copy=False."""
        base_query = "SELECT * FROM users WHERE id = ?"

        # Create multiple SQL objects from the same base
        sql_objects = [SQL(base_query, i) for i in range(1, 4)]

        # Compile all of them
        compiled_results = [sql.compile() for sql in sql_objects]

        # Each should have maintained its own parameters
        for i, (compiled_sql, parameters) in enumerate(compiled_results):
            assert compiled_sql == "SELECT * FROM users WHERE id = ?"
            assert parameters == (i + 1,)  # Parameters should be 1, 2, 3

    def test_caching_compatibility(self):
        """Test that copy=False optimization works with caching."""
        config = StatementConfig(enable_caching=True)
        query = "SELECT * FROM users WHERE active = ?"

        # First execution
        sql1 = SQL(query, True, statement_config=config)
        result1 = sql1.compile()

        # Second execution with same query (should potentially use cache)
        sql2 = SQL(query, True, statement_config=config)
        result2 = sql2.compile()

        # Results should be identical
        assert result1 == result2

        # Different parameters should give different results
        sql3 = SQL(query, False, statement_config=config)
        result3 = sql3.compile()

        assert result3[0] == result1[0]  # Same SQL
        assert result3[1] != result1[1]  # Different parameters
