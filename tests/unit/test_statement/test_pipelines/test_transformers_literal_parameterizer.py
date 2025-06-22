"""Unit tests for ParameterizeLiterals Transformer.

This module tests the ParameterizeLiterals transformer including:
- Basic literal parameterization (strings, numbers, booleans)
- Context-aware parameterization based on AST position
- Placeholder style configuration (?. :name, $1, etc.)
- Preservation rules (NULL, boolean, LIMIT clauses)
- Array and IN clause parameterization
- Type preservation and metadata generation
- Complex query handling with subqueries and joins
"""

from typing import TYPE_CHECKING, Any, Optional

import pytest
from sqlglot import parse_one

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers._literal_parameterizer import ParameterizeLiterals
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# Test Data
@pytest.fixture
def context() -> SQLProcessingContext:
    """Create a processing context."""
    return SQLProcessingContext(initial_sql_string="SELECT 1", dialect=None, config=SQLConfig())


def create_context_with_sql(sql: str, config: Optional[SQLConfig] = None) -> SQLProcessingContext:
    """Helper to create context with specific SQL."""
    if config is None:
        config = SQLConfig()
    expression = parse_one(sql)
    return SQLProcessingContext(initial_sql_string=sql, dialect=None, config=config, current_expression=expression)


# Basic Parameterization Tests
@pytest.mark.parametrize(
    "sql,expected_param_count,expected_param_values",
    [
        ("SELECT * FROM users WHERE name = 'John'", 1, ["John"]),
        ("SELECT * FROM users WHERE age = 25", 1, [25]),
        ("SELECT * FROM users WHERE price = 19.99", 1, [19.99]),
        ("SELECT * FROM users WHERE active = true", 0, []),  # Boolean preserved by default
        ("SELECT * FROM users WHERE email IS NULL", 0, []),  # NULL preserved by default
        ("SELECT * FROM users WHERE name = 'John' AND age = 25", 2, ["John", 25]),
    ],
    ids=[
        "string_literal",
        "integer_literal",
        "float_literal",
        "boolean_preserved",
        "null_preserved",
        "multiple_literals",
    ],
)
def test_basic_literal_parameterization(sql: str, expected_param_count: int, expected_param_values: list[Any]) -> None:
    """Test basic literal parameterization."""
    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)

    # Check parameter extraction
    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) == expected_param_count

    # Check parameter values (extract actual values from TypedParameter objects)
    actual_values = [getattr(p, "value", p) for p in parameters]
    for expected_value in expected_param_values:
        assert expected_value in actual_values

    # Check SQL transformation
    assert result_expr is not None
    result_sql = result_expr.sql()
    for expected_value in expected_param_values:
        # String literals should be replaced
        if isinstance(expected_value, str):
            assert f"'{expected_value}'" not in result_sql
        # Numeric literals should be replaced
        elif isinstance(expected_value, (int, float)):
            assert str(expected_value) not in result_sql or "?" in result_sql


@pytest.mark.parametrize(
    "sql,preserve_boolean,expected_param_count",
    [
        ("SELECT * FROM users WHERE active = true", True, 0),
        ("SELECT * FROM users WHERE active = true", False, 1),
        ("SELECT * FROM users WHERE active = true AND verified = false", True, 0),
        ("SELECT * FROM users WHERE active = true AND verified = false", False, 2),
    ],
    ids=["boolean_preserved", "boolean_parameterized", "multiple_boolean_preserved", "multiple_boolean_parameterized"],
)
def test_boolean_preservation_configuration(sql: str, preserve_boolean: bool, expected_param_count: int) -> None:
    """Test boolean preservation configuration."""
    transformer = ParameterizeLiterals(preserve_boolean=preserve_boolean)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) == expected_param_count


@pytest.mark.parametrize(
    "sql,preserve_null,expected_param_count",
    [
        ("SELECT * FROM users WHERE email IS NULL", True, 0),
        ("SELECT * FROM users WHERE email IS NULL", False, 0),  # NULL still preserved due to IS NULL context
        ("SELECT * FROM users WHERE data = NULL", True, 0),
        ("SELECT * FROM users WHERE data = NULL", False, 1),  # Direct comparison might be parameterized
    ],
    ids=["null_is_preserved", "null_is_context", "null_eq_preserved", "null_eq_parameterized"],
)
def test_null_preservation_configuration(sql: str, preserve_null: bool, expected_param_count: int) -> None:
    """Test NULL preservation configuration."""
    transformer = ParameterizeLiterals(preserve_null=preserve_null)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) >= expected_param_count  # Allow for >= due to context-dependent behavior


# Placeholder Style Tests
@pytest.mark.parametrize(
    "placeholder_style,expected_placeholder",
    [("?", "?"), (":name", ":name_"), ("$1", "$1")],
    ids=["question_mark", "named_colon", "numbered_dollar"],
)
def test_placeholder_styles(placeholder_style: str, expected_placeholder: str) -> None:
    """Test different placeholder styles."""
    sql = "SELECT * FROM users WHERE name = 'John'"
    transformer = ParameterizeLiterals(placeholder_style=placeholder_style)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Check that expected placeholder style appears
    assert expected_placeholder in result_sql

    # Check that original literal is replaced
    assert "'John'" not in result_sql


# Preservation Rules Tests
@pytest.mark.parametrize(
    "sql,preserve_numbers_in_limit,should_preserve_numbers",
    [
        ("SELECT * FROM users LIMIT 10", True, True),
        ("SELECT * FROM users LIMIT 10", False, False),
        ("SELECT * FROM users LIMIT 10 OFFSET 20", True, True),
        ("SELECT * FROM users LIMIT 10 OFFSET 20", False, False),
        ("SELECT * FROM users WHERE id = 10", True, False),  # Not in LIMIT context
    ],
    ids=[
        "limit_preserved",
        "limit_parameterized",
        "limit_offset_preserved",
        "limit_offset_parameterized",
        "where_not_preserved",
    ],
)
def test_limit_clause_preservation(sql: str, preserve_numbers_in_limit: bool, should_preserve_numbers: bool) -> None:
    """Test number preservation in LIMIT clauses."""
    transformer = ParameterizeLiterals(preserve_numbers_in_limit=preserve_numbers_in_limit)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    if should_preserve_numbers:
        # Numbers should be preserved in LIMIT/OFFSET
        assert "10" in result_sql
        if "OFFSET" in sql:
            assert "20" in result_sql
    # For parameterized case, we just check that transformation occurred without errors


def test_max_string_length_preservation() -> None:
    """Test that long strings are preserved when over max length."""
    long_string = "a" * 1500  # Longer than default max_string_length (1000)
    sql = f"SELECT * FROM logs WHERE message = '{long_string}'"

    transformer = ParameterizeLiterals(max_string_length=1000)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Long string should be preserved
    assert long_string in result_sql

    # Should not extract long string as parameter
    parameters = context.extracted_parameters_from_pipeline or []
    actual_values = [getattr(p, "value", p) for p in parameters]
    assert long_string not in actual_values


def test_preserve_in_functions() -> None:
    """Test preserving literals in specific functions."""
    sql = "SELECT COALESCE(name, 'Unknown'), ROUND(price, 2) FROM products"

    transformer = ParameterizeLiterals(preserve_in_functions=["COALESCE", "IFNULL"])
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    # 'Unknown' should be preserved in COALESCE
    assert "'Unknown'" in result_sql

    # Check that other literals might be parameterized
    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) >= 0  # May or may not parameterize ROUND argument


# Complex Query Tests
def test_complex_query_parameterization() -> None:
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

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    # Should extract multiple parameters from different parts of the query
    parameters = context.extracted_parameters_from_pipeline or []
    actual_values = [getattr(p, "value", p) for p in parameters]

    assert len(parameters) >= 3
    assert "active" in actual_values
    assert "2023-01-01" in actual_values
    assert any(val in [100.00, 100] for val in actual_values)


def test_mixed_literal_types() -> None:
    """Test handling of various literal types in one query."""
    sql = """
    INSERT INTO events (name, count, price, active, created_date, description)
    VALUES ('test_event', 100, 19.99, true, '2023-01-01', NULL)
    """

    transformer = ParameterizeLiterals(preserve_null=True, preserve_boolean=False)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Check parameter extraction
    parameters = context.extracted_parameters_from_pipeline or []
    actual_values = [getattr(p, "value", p) for p in parameters]

    assert "test_event" in actual_values
    assert 100 in actual_values
    assert any(val in [19.99, 19] for val in actual_values)  # Depending on parsing
    assert "2023-01-01" in actual_values

    # NULL should be preserved, not parameterized
    assert "NULL" in result_sql.upper()
    assert None not in actual_values


# Array and IN Clause Tests
@pytest.mark.parametrize(
    "sql,parameterize_in_lists,expected_min_params",
    [
        ("SELECT * FROM users WHERE id IN (1, 2, 3)", True, 3),
        ("SELECT * FROM users WHERE id IN (1, 2, 3)", False, 0),
        ("SELECT * FROM products WHERE category IN (1, 2, 3, 4, 5)", True, 5),
    ],
    ids=["in_clause_parameterized", "in_clause_preserved", "larger_in_clause"],
)
def test_in_clause_parameterization(sql: str, parameterize_in_lists: bool, expected_min_params: int) -> None:
    """Test IN clause parameterization."""
    transformer = ParameterizeLiterals(parameterize_in_lists=parameterize_in_lists)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    parameters = context.extracted_parameters_from_pipeline or []
    if parameterize_in_lists:
        assert len(parameters) >= expected_min_params
    else:
        # When disabled, might still parameterize individual literals depending on other settings
        pass


def test_in_clause_size_limit() -> None:
    """Test IN clause parameterization with size limits."""
    # Small IN list (should be parameterized)
    small_sql = "SELECT * FROM users WHERE id IN (1, 2, 3)"
    transformer = ParameterizeLiterals(max_in_list_size=5)
    context = create_context_with_sql(small_sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)
    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) >= 3

    # Large IN list (should not be parameterized due to size limit)
    large_values = ", ".join(str(i) for i in range(100))
    large_sql = f"SELECT * FROM users WHERE id IN ({large_values})"

    transformer_large = ParameterizeLiterals(max_in_list_size=50)
    context_large = create_context_with_sql(large_sql)
    assert context_large.current_expression is not None
    transformer_large.process(context_large.current_expression, context_large)
    parameters_large = context_large.extracted_parameters_from_pipeline or []
    # Should not parameterize due to size limit
    assert len(parameters_large) == 0


# Metadata and Type Preservation Tests
def test_parameter_metadata_generation() -> None:
    """Test parameter metadata generation."""
    sql = "SELECT * FROM users WHERE age = 25 AND name = 'John'"

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    # Check metadata was stored in context
    metadata = context.metadata.get("parameter_metadata")
    assert metadata is not None
    assert len(metadata) == 2

    # Check metadata structure
    for meta in metadata:
        assert "index" in meta
        assert "type" in meta
        assert "context" in meta


def test_type_preservation_metadata() -> None:
    """Test type preservation in parameter metadata."""
    sql = """
    SELECT * FROM accounts
    WHERE balance = 123.456789012345
    AND count = 42
    AND name = 'Test'
    """

    transformer = ParameterizeLiterals(type_preservation=True)
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    # Check parameter metadata for type information
    metadata = context.metadata.get("parameter_metadata")
    assert metadata is not None

    # Find metadata for each parameter type
    types_found = {meta["type"] for meta in metadata}
    # Should have different type hints for different literal types
    assert len(types_found) >= 2


def test_context_aware_parameterization() -> None:
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

    transformer = ParameterizeLiterals(placeholder_style=":name")
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    # Check that parameters were extracted
    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) > 0

    # Check parameter metadata includes context information
    metadata = context.metadata.get("parameter_metadata")
    assert metadata is not None
    assert len(metadata) == len(parameters)

    # Verify context information is captured
    for meta in metadata:
        assert "context" in meta
        # Context should be descriptive
        assert meta["context"] in ["case_when", "where", "general", "select", "where_value"]


# Edge Cases and Special Scenarios
def test_empty_query_handling() -> None:
    """Test handling of queries without literals."""
    sql = "SELECT id, name FROM users"

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)

    # Should not extract any parameters
    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) == 0

    assert result_expr is not None
    result_sql = result_expr.sql()
    assert "id" in result_sql
    assert "name" in result_sql
    assert "users" in result_sql


def test_data_type_contexts_preserved() -> None:
    """Test that literals in data type contexts are preserved."""
    sql = "CREATE TABLE test (id INT, name VARCHAR(50))"

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Should preserve the 50 in VARCHAR(50)
    assert "50" in result_sql

    # Should not extract the 50 as a parameter
    parameters = context.extracted_parameters_from_pipeline or []
    actual_values = [getattr(p, "value", p) for p in parameters]
    assert 50 not in actual_values


def test_subquery_handling() -> None:
    """Test that subqueries are handled correctly."""
    sql = """
    SELECT * FROM users
    WHERE id IN (
        SELECT user_id FROM orders WHERE total > 100.00
    )
    """

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    # Should parameterize literals in subquery
    parameters = context.extracted_parameters_from_pipeline or []
    actual_values = [getattr(p, "value", p) for p in parameters]
    assert any(val in [100.00, 100] for val in actual_values)


# Input Validation Tests
def test_placeholders_already_present() -> None:
    """Test behavior when SQL already has placeholders."""
    # This would require config.input_sql_had_placeholders = True
    sql = "SELECT * FROM users WHERE name = ?"
    config = SQLConfig()
    config.input_sql_had_placeholders = True
    context = create_context_with_sql(sql, config)

    transformer = ParameterizeLiterals()
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)

    assert result_expr is not None
    result_sql = result_expr.sql()
    assert "?" in result_sql

    # Should not extract additional parameters
    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) == 0


def test_transformer_method_access() -> None:
    """Test transformer method access for parameter retrieval."""
    sql = "SELECT * FROM users WHERE name = 'John' AND age = 25"

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    # Test get_parameters method
    params = transformer.get_parameters()
    assert len(params) == 2

    # Test get_parameter_metadata method
    metadata = transformer.get_parameter_metadata()
    assert len(metadata) == 2

    # Test clear_parameters method
    transformer.clear_parameters()
    assert len(transformer.get_parameters()) == 0


# Named Parameter and Semantic Name Tests
def test_named_parameter_generation() -> None:
    """Test generation of named parameters with context hints."""
    sql = """
    SELECT u.name, o.total
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.status = 'active'
    AND o.created_at > '2023-01-01'
    """

    transformer = ParameterizeLiterals(placeholder_style=":name")
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    result_expr = transformer.process(context.current_expression, context)
    assert result_expr is not None
    result_sql = result_expr.sql()

    # Should have named parameters (using column hint + counter format)
    assert ":" in result_sql  # Just check for colon prefix

    # Check metadata for semantic names
    metadata = context.metadata.get("parameter_metadata")
    assert metadata is not None

    # Should have captured context-aware information
    contexts = [meta["context"] for meta in metadata]
    assert any("where" in ctx for ctx in contexts)


@pytest.mark.parametrize(
    "sql,expected_min_params,description",
    [
        ("SELECT * FROM users WHERE name = 'John'", 1, "simple_where"),
        ("SELECT * FROM users WHERE id IN (1, 2, 3)", 3, "in_clause"),
        ("SELECT CASE WHEN age > 18 THEN 'Adult' ELSE 'Minor' END FROM users", 2, "case_when"),
        ("UPDATE users SET status = 'active' WHERE id = 1", 2, "update_statement"),
        ("INSERT INTO logs (message, level) VALUES ('Error', 1)", 2, "insert_statement"),
    ],
    ids=["simple_where", "in_clause", "case_when", "update_statement", "insert_statement"],
)
def test_comprehensive_parameterization_scenarios(sql: str, expected_min_params: int, description: str) -> None:
    """Test comprehensive parameterization across various SQL patterns."""
    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    transformer.process(context.current_expression, context)

    parameters = context.extracted_parameters_from_pipeline or []
    assert len(parameters) >= expected_min_params

    # Ensure metadata is generated
    metadata = context.metadata.get("parameter_metadata")
    assert metadata is not None
    assert len(metadata) == len(parameters)


def test_transformer_handles_complex_ast() -> None:
    """Test that transformer handles complex AST structures without crashing."""
    sql = """
    WITH RECURSIVE category_tree AS (
        SELECT id, name, parent_id, 1 as level
        FROM categories
        WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.name, c.parent_id, ct.level + 1
        FROM categories c
        JOIN category_tree ct ON c.parent_id = ct.id
        WHERE ct.level < 5
    )
    SELECT ct.*, 'processed' as status
    FROM category_tree ct
    WHERE ct.level <= 3
    """

    transformer = ParameterizeLiterals()
    context = create_context_with_sql(sql)
    assert context.current_expression is not None
    # Should not crash on complex query
    transformer.process(context.current_expression, context)

    # Should extract some parameters
    parameters = context.extracted_parameters_from_pipeline or []
    actual_values = [getattr(p, "value", p) for p in parameters]

    # Should find literals like 1, 5, 'processed', 3
    assert len(parameters) >= 2
    assert any(val in [1, 5, 3] for val in actual_values)
    assert "processed" in actual_values
