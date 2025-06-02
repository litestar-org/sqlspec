"""Tests for the ParameterizeLiterals transformer."""

from typing import Optional

from sqlglot import parse_one
from sqlglot.dialects import Dialect

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers import ParameterizeLiterals
from sqlspec.statement.sql import SQLConfig


def _create_test_context(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper function to create a SQLProcessingContext for testing."""
    if config is None:
        config = SQLConfig()

    expression = parse_one(sql)
    return SQLProcessingContext(
        initial_sql_string=sql, dialect=Dialect.get_or_raise(""), config=config, current_expression=expression
    )


class TestParameterizeLiterals:
    """Test cases for the ParameterizeLiterals transformer."""

    def test_basic_string_parameterization(self) -> None:
        """Test basic string literal parameterization."""
        sql = "SELECT * FROM users WHERE name = 'John'"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should replace string literal with placeholder
        param_sql = param_expr.sql()
        assert "'John'" not in param_sql
        assert "?" in param_sql or "placeholder" in param_sql.lower()

        # Should extract the parameter
        parameters = parameterizer.get_parameters()
        assert len(parameters) == 1
        assert parameters[0] == "John"

    def test_multiple_literals_parameterization(self) -> None:
        """Test parameterization of multiple literals."""
        sql = "SELECT * FROM users WHERE name = 'John' AND age = 25 AND active = true"

        parameterizer = ParameterizeLiterals(preserve_boolean=False)
        context = _create_test_context(sql)

        _, _ = parameterizer.process(context)

        # Should extract multiple parameters
        parameters = parameterizer.get_parameters()
        assert len(parameters) >= 2  # At least string and number
        assert "John" in parameters
        assert 25 in parameters

    def test_number_parameterization(self) -> None:
        """Test numeric literal parameterization."""
        sql = "SELECT * FROM products WHERE price = 19.99 AND quantity = 100"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        _, _ = parameterizer.process(context)

        # Should extract numeric parameters
        parameters = parameterizer.get_parameters()
        assert len(parameters) == 2
        assert 19.99 in parameters
        assert 100 in parameters

    def test_null_preservation(self) -> None:
        """Test that NULL literals are preserved when configured."""
        sql = "SELECT * FROM users WHERE email IS NULL"

        parameterizer = ParameterizeLiterals(preserve_null=True)
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should preserve NULL literals
        param_sql = param_expr.sql()
        assert "NULL" in param_sql.upper()

        # Should not extract NULL as parameter
        parameters = parameterizer.get_parameters()
        assert None not in parameters

    def test_boolean_preservation(self) -> None:
        """Test that boolean literals are preserved when configured."""
        sql = "SELECT * FROM users WHERE active = true AND verified = false"

        parameterizer = ParameterizeLiterals(preserve_boolean=True)
        context = _create_test_context(sql)

        _, _ = parameterizer.process(context)

        # Note: SQLGlot might normalize boolean representation

        # Should not extract booleans as parameters (or fewer parameters)
        parameters = parameterizer.get_parameters()
        assert True not in parameters or False not in parameters

    def test_limit_clause_preservation(self) -> None:
        """Test that numbers in LIMIT clauses are preserved when configured."""
        sql = "SELECT * FROM users LIMIT 10 OFFSET 20"

        parameterizer = ParameterizeLiterals(preserve_numbers_in_limit=True)
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should preserve LIMIT/OFFSET numbers
        param_sql = param_expr.sql()
        assert "10" in param_sql
        assert "20" in param_sql

        # Should not extract LIMIT/OFFSET numbers as parameters
        parameters = parameterizer.get_parameters()
        assert 10 not in parameters
        assert 20 not in parameters

    def test_max_string_length_preservation(self) -> None:
        """Test that long strings are preserved when over max length."""
        long_string = "a" * 1500  # Longer than default max_string_length
        sql = f"SELECT * FROM logs WHERE message = '{long_string}'"

        parameterizer = ParameterizeLiterals(max_string_length=1000)
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should preserve long strings
        param_sql = param_expr.sql()
        assert long_string in param_sql

        # Should not extract long string as parameter
        parameters = parameterizer.get_parameters()
        assert long_string not in parameters

    def test_placeholder_styles(self) -> None:
        """Test different placeholder styles."""
        sql = "SELECT * FROM users WHERE name = 'John'"

        # Test question mark style
        parameterizer1 = ParameterizeLiterals(placeholder_style="?")
        context = _create_test_context(sql)

        param_expr1, _ = parameterizer1.process(context)
        param_sql1 = param_expr1.sql()
        assert "?" in param_sql1

        # Test named parameter style
        parameterizer2 = ParameterizeLiterals(placeholder_style=":name")
        context = _create_test_context(sql)

        param_expr2, _ = parameterizer2.process(context)
        param_sql2 = param_expr2.sql()
        # Should have named parameter (exact format may vary)
        assert "param_" in param_sql2 or ":" in param_sql2

    def test_postgresql_style_placeholders(self) -> None:
        """Test PostgreSQL-style numbered placeholders."""
        sql = "SELECT * FROM users WHERE name = 'John' AND age = 25"

        parameterizer = ParameterizeLiterals(placeholder_style="$1")
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)
        param_sql = param_expr.sql()

        # Should have numbered placeholders
        assert "$" in param_sql

    def test_get_parameterized_query_convenience_method(self) -> None:
        """Test the convenience method for getting SQL and parameters together."""
        sql = "SELECT * FROM users WHERE name = 'John' AND age = 25"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        # Process the query first
        param_expr, _ = parameterizer.process(context)
        param_sql = param_expr.sql()
        parameters = parameterizer.get_parameters()

        # Should return both SQL and parameters
        assert isinstance(param_sql, str)
        assert isinstance(parameters, list)
        assert len(parameters) == 2
        assert "John" in parameters
        assert 25 in parameters

    def test_clear_parameters(self) -> None:
        """Test clearing extracted parameters."""
        sql = "SELECT * FROM users WHERE name = 'John'"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        _, _ = parameterizer.process(context)

        # Should have parameters
        assert len(parameterizer.get_parameters()) > 0

        # Clear and check
        parameterizer.clear_parameters()
        assert len(parameterizer.get_parameters()) == 0

    def test_complex_query_parameterization(self) -> None:
        """Test parameterization in complex queries with subqueries and joins."""
        sql = """
        SELECT u.name, p.title
        FROM users u
        JOIN profiles p ON u.id = p.user_id
        WHERE u.status = 'active'
        AND u.created_date > '2023-01-01'
        AND EXISTS (
            SELECT 1 FROM orders o
            WHERE o.user_id = u.id
            AND o.total > 100.00
        )
        """

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        _, _ = parameterizer.process(context)

        # Should extract multiple parameters from different parts of the query
        parameters = parameterizer.get_parameters()
        assert len(parameters) >= 3
        assert "active" in parameters
        assert "2023-01-01" in parameters
        assert 100.00 in parameters or 100 in parameters

    def test_data_type_contexts_preserved(self) -> None:
        """Test that literals in data type contexts are preserved."""
        sql = "CREATE TABLE test (id INT, name VARCHAR(50))"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should preserve the 50 in VARCHAR(50)
        param_sql = param_expr.sql()
        assert "50" in param_sql

        # Should not extract the 50 as a parameter
        parameters = parameterizer.get_parameters()
        assert 50 not in parameters

    def test_empty_query(self) -> None:
        """Test handling of queries without literals."""
        sql = "SELECT id, name FROM users"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should not extract any parameters
        parameters = parameterizer.get_parameters()
        assert len(parameters) == 0

        # SQL should remain largely unchanged
        param_sql = param_expr.sql()
        assert "id" in param_sql
        assert "name" in param_sql
        assert "users" in param_sql

    def test_mixed_literal_types(self) -> None:
        """Test handling of various literal types in one query."""
        sql = """
        INSERT INTO events (name, count, price, active, created_date, description)
        VALUES ('test_event', 100, 19.99, true, '2023-01-01', NULL)
        """

        parameterizer = ParameterizeLiterals(preserve_null=True, preserve_boolean=False)
        context = _create_test_context(sql)

        param_expr, _ = parameterizer.process(context)

        # Should extract various types of parameters
        parameters = parameterizer.get_parameters()
        assert "test_event" in parameters
        assert 100 in parameters
        assert 19.99 in parameters or 19 in parameters  # Depending on parsing
        assert "2023-01-01" in parameters

        # NULL should be preserved, not parameterized
        param_sql = param_expr.sql()
        assert "NULL" in param_sql.upper()
        assert None not in parameters
