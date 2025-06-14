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

        param_expr = parameterizer.process(context.current_expression, context)

        # Should replace string literal with placeholder
        param_sql = param_expr.sql()
        assert "'John'" not in param_sql
        assert "?" in param_sql or "placeholder" in param_sql.lower()

        # Should extract the parameter
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) == 1
        assert parameters[0] == "John"

    def test_multiple_literals_parameterization(self) -> None:
        """Test parameterization of multiple literals."""
        sql = "SELECT * FROM users WHERE name = 'John' AND age = 25 AND active = true"

        parameterizer = ParameterizeLiterals(preserve_boolean=False)
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Should extract multiple parameters
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) >= 2  # At least string and number
        assert "John" in parameters
        assert 25 in parameters

    def test_number_parameterization(self) -> None:
        """Test numeric literal parameterization."""
        sql = "SELECT * FROM products WHERE price = 19.99 AND quantity = 100"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Should extract numeric parameters
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) == 2
        assert 19.99 in parameters
        assert 100 in parameters

    def test_null_preservation(self) -> None:
        """Test that NULL literals are preserved when configured."""
        sql = "SELECT * FROM users WHERE email IS NULL"

        parameterizer = ParameterizeLiterals(preserve_null=True)
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)

        # Should preserve NULL literals
        param_sql = param_expr.sql()
        assert "NULL" in param_sql.upper()

        # Should not extract NULL as parameter
        parameters = context.extracted_parameters_from_pipeline or []
        assert None not in parameters

    def test_boolean_preservation(self) -> None:
        """Test that boolean literals are preserved when configured."""
        sql = "SELECT * FROM users WHERE active = true AND verified = false"

        parameterizer = ParameterizeLiterals(preserve_boolean=True)
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Note: SQLGlot might normalize boolean representation

        # Should not extract booleans as parameters (or fewer parameters)
        parameters = context.extracted_parameters_from_pipeline or []
        assert True not in parameters or False not in parameters

    def test_limit_clause_preservation(self) -> None:
        """Test that numbers in LIMIT clauses are preserved when configured."""
        sql = "SELECT * FROM users LIMIT 10 OFFSET 20"

        parameterizer = ParameterizeLiterals(preserve_numbers_in_limit=True)
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)

        # Should preserve LIMIT/OFFSET numbers
        param_sql = param_expr.sql()
        assert "10" in param_sql
        assert "20" in param_sql

        # Should not extract LIMIT/OFFSET numbers as parameters
        parameters = context.extracted_parameters_from_pipeline or []
        assert 10 not in parameters
        assert 20 not in parameters

    def test_max_string_length_preservation(self) -> None:
        """Test that long strings are preserved when over max length."""
        long_string = "a" * 1500  # Longer than default max_string_length
        sql = f"SELECT * FROM logs WHERE message = '{long_string}'"

        parameterizer = ParameterizeLiterals(max_string_length=1000)
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)

        # Should preserve long strings
        param_sql = param_expr.sql()
        assert long_string in param_sql

        # Should not extract long string as parameter
        parameters = context.extracted_parameters_from_pipeline or []
        assert long_string not in parameters

    def test_placeholder_styles(self) -> None:
        """Test different placeholder styles."""
        sql = "SELECT * FROM users WHERE name = 'John'"

        # Test question mark style
        parameterizer1 = ParameterizeLiterals(placeholder_style="?")
        context = _create_test_context(sql)

        param_expr1 = parameterizer1.process(context.current_expression, context)
        param_sql1 = param_expr1.sql()
        assert "?" in param_sql1

        # Test named parameter style
        parameterizer2 = ParameterizeLiterals(placeholder_style=":name")
        context = _create_test_context(sql)

        param_expr2 = parameterizer2.process(context.current_expression, context)
        param_sql2 = param_expr2.sql()
        # Should have named parameter (exact format may vary)
        assert "param_" in param_sql2 or ":" in param_sql2

    def test_postgresql_style_placeholders(self) -> None:
        """Test PostgreSQL-style numbered placeholders."""
        sql = "SELECT * FROM users WHERE name = 'John' AND age = 25"

        parameterizer = ParameterizeLiterals(placeholder_style="$1")
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)
        param_sql = param_expr.sql()

        # Should have numbered placeholders
        assert "$" in param_sql

    def test_get_parameterized_query_convenience_method(self) -> None:
        """Test the convenience method for getting SQL and parameters together."""
        sql = "SELECT * FROM users WHERE name = 'John' AND age = 25"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        # Process the query first
        param_expr = parameterizer.process(context.current_expression, context)
        param_sql = param_expr.sql()
        parameters = context.extracted_parameters_from_pipeline

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

        parameterizer.process(context.current_expression, context)

        # Should have parameters
        assert len(context.extracted_parameters_from_pipeline) > 0

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

        parameterizer.process(context.current_expression, context)

        # Should extract multiple parameters from different parts of the query
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) >= 3
        assert "active" in parameters
        assert "2023-01-01" in parameters
        assert 100.00 in parameters or 100 in parameters

    def test_data_type_contexts_preserved(self) -> None:
        """Test that literals in data type contexts are preserved."""
        sql = "CREATE TABLE test (id INT, name VARCHAR(50))"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)

        # Should preserve the 50 in VARCHAR(50)
        param_sql = param_expr.sql()
        assert "50" in param_sql

        # Should not extract the 50 as a parameter
        parameters = context.extracted_parameters_from_pipeline or []
        assert 50 not in parameters

    def test_empty_query(self) -> None:
        """Test handling of queries without literals."""
        sql = "SELECT id, name FROM users"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)

        # Should not extract any parameters
        parameters = context.extracted_parameters_from_pipeline or []
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

        param_expr = parameterizer.process(context.current_expression, context)

        # Should extract various types of parameters
        parameters = context.extracted_parameters_from_pipeline
        assert "test_event" in parameters
        assert 100 in parameters
        assert 19.99 in parameters or 19 in parameters  # Depending on parsing
        assert "2023-01-01" in parameters

        # NULL should be preserved, not parameterized
        param_sql = param_expr.sql()
        assert "NULL" in param_sql.upper()
        assert None not in parameters

    # New tests for enhanced features
    def test_context_aware_parameterization(self) -> None:
        """Test context-aware parameterization based on AST position."""
        sql = """
        SELECT
            id,
            name,
            CASE
                WHEN age > 18 THEN 'Adult'
                ELSE 'Minor'
            END as category
        FROM users
        WHERE created_at > '2023-01-01'
        """

        parameterizer = ParameterizeLiterals(placeholder_style=":name")
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Check that parameters were extracted
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) > 0

        # Check parameter metadata
        metadata = context.metadata.get("parameter_metadata")
        assert metadata is not None
        assert len(metadata) == len(parameters)

        # Verify context information is captured
        for meta in metadata:
            assert "context" in meta
            assert meta["context"] in ["case_when", "where", "general", "select"]

    def test_array_parameterization(self) -> None:
        """Test parameterization of array literals."""
        sql = "SELECT * FROM products WHERE category IN (1, 2, 3, 4, 5)"

        parameterizer = ParameterizeLiterals(parameterize_arrays=True, parameterize_in_lists=True)
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Should parameterize the IN list values
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) == 5
        assert parameters == [1, 2, 3, 4, 5]

    def test_in_clause_size_limit(self) -> None:
        """Test IN clause parameterization with size limits."""
        # Small IN list (should be parameterized)
        small_sql = "SELECT * FROM users WHERE id IN (1, 2, 3)"

        parameterizer = ParameterizeLiterals(max_in_list_size=5)
        context = _create_test_context(small_sql)

        parameterizer.process(context.current_expression, context)
        parameters = context.extracted_parameters_from_pipeline
        assert len(parameters) == 3

        # Large IN list (should not be parameterized)
        large_values = ", ".join(str(i) for i in range(100))
        large_sql = f"SELECT * FROM users WHERE id IN ({large_values})"

        parameterizer = ParameterizeLiterals(max_in_list_size=50)
        context = _create_test_context(large_sql)

        parameterizer.process(context.current_expression, context)
        parameters = context.extracted_parameters_from_pipeline or []
        # Should not parameterize due to size limit
        assert len(parameters) == 0

    def test_preserve_in_functions(self) -> None:
        """Test preserving literals in specific functions."""
        sql = """
        SELECT
            COALESCE(name, 'Unknown') as display_name,
            ROUND(price, 2) as rounded_price
        FROM products
        """

        parameterizer = ParameterizeLiterals(preserve_in_functions=["COALESCE", "IFNULL"])
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)
        param_sql = param_expr.sql()

        # 'Unknown' should be preserved in COALESCE
        assert "'Unknown'" in param_sql

        # 2 in ROUND should be parameterized
        parameters = context.extracted_parameters_from_pipeline
        assert 2 in parameters

    def test_type_preservation(self) -> None:
        """Test preservation of exact literal types."""
        sql = """
        SELECT * FROM accounts
        WHERE balance = 123.456789012345
        AND count = 42
        AND name = 'Test'
        """

        parameterizer = ParameterizeLiterals(type_preservation=True)
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Check parameter metadata for type information
        metadata = context.metadata.get("parameter_metadata")
        assert metadata is not None

        # Find metadata for each parameter
        types_found = {meta["type"] for meta in metadata}
        assert "decimal" in types_found or "float" in types_found
        assert "integer" in types_found
        assert "string" in types_found

    def test_named_parameter_generation(self) -> None:
        """Test generation of named parameters with context hints."""
        sql = """
        SELECT u.name, o.total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.status = 'active'
        AND o.created_at > '2023-01-01'
        """

        parameterizer = ParameterizeLiterals(placeholder_style=":name")
        context = _create_test_context(sql)

        param_expr = parameterizer.process(context.current_expression, context)
        param_sql = param_expr.sql()

        # Should have context-aware parameter names
        assert ":param_" in param_sql or ":where_param_" in param_sql

        # Check metadata
        metadata = context.metadata.get("parameter_metadata")
        assert metadata is not None

        # Should have captured context
        contexts = [meta["context"] for meta in metadata]
        assert any("where" in ctx or "join" in ctx for ctx in contexts)

    def test_get_parameter_metadata(self) -> None:
        """Test retrieving parameter metadata."""
        sql = "SELECT * FROM users WHERE age = 25 AND name = 'John'"

        parameterizer = ParameterizeLiterals()
        context = _create_test_context(sql)

        parameterizer.process(context.current_expression, context)

        # Get metadata from context (not from parameterizer)
        metadata = context.metadata.get("parameter_metadata")
        assert metadata is not None
        assert len(metadata) == 2

        # Check metadata structure
        for meta in metadata:
            assert "index" in meta
            assert "type" in meta
            assert "original_sql" in meta
            assert "context" in meta
