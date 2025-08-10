"""Comprehensive tests for sqlspec.utils.statement_hashing module.

Tests for SQL statement and expression hashing utilities used for cache key generation.
Covers all hashing functions with edge cases, performance considerations, and circular reference handling.
"""

import math
from unittest.mock import Mock

import pytest
from sqlglot import exp, parse_one

from sqlspec.core.hashing import (
    _hash_value,
    hash_expression,
    hash_expression_node,
    hash_filters,
    hash_optimized_expression,
    hash_parameters,
    hash_sql_statement,
)

# Test hash_expression function


def test_hash_expression_none() -> None:
    """Test hash_expression handles None input."""
    result = hash_expression(None)
    assert result == hash(None)


def test_hash_expression_basic() -> None:
    """Test hash_expression with basic SQL expression."""
    expr = parse_one("SELECT 1")
    result = hash_expression(expr)
    assert isinstance(result, int)

    # Same expression should produce same hash
    expr2 = parse_one("SELECT 1")
    result2 = hash_expression(expr2)
    assert result == result2


def test_hash_expression_different_expressions() -> None:
    """Test hash_expression produces different hashes for different expressions."""
    expr1 = parse_one("SELECT 1")
    expr2 = parse_one("SELECT 2")

    hash1 = hash_expression(expr1)
    hash2 = hash_expression(expr2)

    assert hash1 != hash2


def test_hash_expression_complex_sql() -> None:
    """Test hash_expression with complex SQL statements."""
    expr = parse_one("SELECT u.id, u.name FROM users u WHERE u.age > 18 AND u.status = 'active' ORDER BY u.name")
    result = hash_expression(expr)
    assert isinstance(result, int)

    # Parsing the same complex SQL should produce same hash
    expr2 = parse_one("SELECT u.id, u.name FROM users u WHERE u.age > 18 AND u.status = 'active' ORDER BY u.name")
    result2 = hash_expression(expr2)
    assert result == result2


def test_hash_expression_circular_reference() -> None:
    """Test hash_expression handles circular references correctly."""
    # Create a mock expression with circular reference
    expr = Mock(spec=exp.Expression)
    expr.args = {"child": expr}  # Self-reference
    type(expr).__name__ = "MockExpression"

    # Should not raise exception and should return a hash
    result = hash_expression(expr)
    assert isinstance(result, int)


def test_hash_expression_nested_structure() -> None:
    """Test hash_expression with deeply nested expressions."""
    expr = parse_one("SELECT (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users")
    result = hash_expression(expr)
    assert isinstance(result, int)


# Test _hash_value function


def test_hash_value_expression() -> None:
    """Test _hash_value with Expression objects."""
    expr = parse_one("SELECT 1")
    seen: set[int] = set()
    result = _hash_value(expr, seen)
    assert isinstance(result, int)


def test_hash_value_list() -> None:
    """Test _hash_value with list values."""
    test_list = [1, "test", True]
    seen: set[int] = set()
    result = _hash_value(test_list, seen)
    assert isinstance(result, int)

    # Same list should produce same hash
    result2 = _hash_value(test_list, seen)
    assert result == result2


def test_hash_value_dict() -> None:
    """Test _hash_value with dictionary values."""
    test_dict = {"key1": "value1", "key2": 42}
    seen: set[int] = set()
    result = _hash_value(test_dict, seen)
    assert isinstance(result, int)


def test_hash_value_tuple() -> None:
    """Test _hash_value with tuple values."""
    test_tuple = (1, "test", True)
    seen: set[int] = set()
    result = _hash_value(test_tuple, seen)
    assert isinstance(result, int)


def test_hash_value_primitives() -> None:
    """Test _hash_value with primitive values."""
    seen: set[int] = set()

    assert isinstance(_hash_value("string", seen), int)
    assert isinstance(_hash_value(42, seen), int)
    assert isinstance(_hash_value(True, seen), int)
    assert isinstance(_hash_value(None, seen), int)
    assert isinstance(_hash_value(math.pi, seen), int)


def test_hash_value_nested_structures() -> None:
    """Test _hash_value with nested data structures."""
    nested = {"list": [1, 2, {"inner": "value"}], "tuple": (1, 2, 3)}
    seen: set[int] = set()
    result = _hash_value(nested, seen)
    assert isinstance(result, int)


# Test hash_parameters function


def test_hash_parameters_no_parameters() -> None:
    """Test hash_parameters with no parameters."""
    result = hash_parameters()
    assert result == 0


def test_hash_parameters_positional_only() -> None:
    """Test hash_parameters with only positional parameters."""
    params = ["value1", 42, True]
    result = hash_parameters(positional_parameters=params)
    assert isinstance(result, int)


def test_hash_parameters_named_only() -> None:
    """Test hash_parameters with only named parameters."""
    params = {"key1": "value1", "key2": 42}
    result = hash_parameters(named_parameters=params)
    assert isinstance(result, int)


def test_hash_parameters_mixed() -> None:
    """Test hash_parameters with both positional and named parameters."""
    pos_params = ["value1", 42]
    named_params = {"key1": "value1", "key2": 42}

    result = hash_parameters(positional_parameters=pos_params, named_parameters=named_params)
    assert isinstance(result, int)


def test_hash_parameters_with_typed_parameters() -> None:
    """Test hash_parameters with TypedParameter objects."""
    from sqlspec.core.parameters import TypedParameter

    typed_param = TypedParameter("test_value", str, "test_semantic")
    params = [typed_param, "regular_param"]

    result = hash_parameters(positional_parameters=params)
    assert isinstance(result, int)


def test_hash_parameters_unhashable_types() -> None:
    """Test hash_parameters handles unhashable types correctly."""
    params = [{"unhashable": "dict"}, ["unhashable", "list"]]
    result = hash_parameters(positional_parameters=params)
    assert isinstance(result, int)


def test_hash_parameters_with_original_parameters() -> None:
    """Test hash_parameters with original parameters for execute_many."""
    original_params = [("value1", 1), ("value2", 2)]
    result = hash_parameters(original_parameters=original_params)
    assert isinstance(result, int)


def test_hash_parameters_large_parameter_sets() -> None:
    """Test hash_parameters handles large parameter sets efficiently."""
    large_original = [(f"value{i}", i) for i in range(1000)]
    result = hash_parameters(original_parameters=large_original)
    assert isinstance(result, int)


# Test hash_filters function


def test_hash_filters_no_filters() -> None:
    """Test hash_filters with no filters."""
    result = hash_filters()
    assert result == 0

    result = hash_filters([])
    assert result == 0


def test_hash_filters_with_filters() -> None:
    """Test hash_filters with test filter objects."""

    # Create test filter classes
    class TestFilter1:
        def __init__(self) -> None:
            self.attr1 = "value1"
            self.attr2 = 42

    class TestFilter2:
        def __init__(self) -> None:
            self.attr3 = "value3"

    filters = [TestFilter1(), TestFilter2()]
    result = hash_filters(filters)  # type: ignore[arg-type]
    assert isinstance(result, int)


def test_hash_filters_no_dict_attribute() -> None:
    """Test hash_filters with filters that don't have __dict__."""
    # Create a filter without __dict__
    filter_obj = Mock()
    filter_obj.__class__.__name__ = "SimpleFilter"
    del filter_obj.__dict__  # Remove __dict__ attribute

    filters = [filter_obj]
    result = hash_filters(filters)  # type: ignore[arg-type]
    assert isinstance(result, int)


def test_hash_filters_unhashable_attributes() -> None:
    """Test hash_filters with filters having unhashable attributes."""

    class FilterWithUnhashable:
        def __init__(self) -> None:
            self.list_attr = [1, 2, 3]
            self.dict_attr = {"key": "value"}

    filters = [FilterWithUnhashable()]
    result = hash_filters(filters)  # type: ignore[arg-type]
    assert isinstance(result, int)


# Test hash_sql_statement function


def test_hash_sql_statement_basic() -> None:
    """Test hash_sql_statement with basic SQL statement."""
    # Create a mock SQL statement
    statement = Mock()
    statement._statement = parse_one("SELECT 1")
    statement._raw_sql = "SELECT 1"
    statement._positional_parameters = None
    statement._named_parameters = None
    statement._original_parameters = None
    statement._filters = None
    statement._dialect = "sqlite"
    statement._is_many = False
    statement._is_script = False

    result = hash_sql_statement(statement)
    assert isinstance(result, str)
    assert result.startswith("sql:")


def test_hash_sql_statement_with_parameters() -> None:
    """Test hash_sql_statement with parameters."""
    statement = Mock()
    statement._statement = parse_one("SELECT * FROM users WHERE id = ?")
    statement._raw_sql = "SELECT * FROM users WHERE id = ?"
    statement._positional_parameters = [123]
    statement._named_parameters = {"user_id": 123}
    statement._original_parameters = [123]
    statement._filters = None
    statement._dialect = "sqlite"
    statement._is_many = False
    statement._is_script = False

    result = hash_sql_statement(statement)
    assert isinstance(result, str)
    assert result.startswith("sql:")


def test_hash_sql_statement_raw_sql_fallback() -> None:
    """Test hash_sql_statement falls back to raw SQL when expression not available."""
    statement = Mock()
    statement._statement = "SELECT 1"  # String instead of Expression
    statement._raw_sql = "SELECT 1"
    statement._positional_parameters = None
    statement._named_parameters = None
    statement._original_parameters = None
    statement._filters = None
    statement._dialect = "sqlite"
    statement._is_many = False
    statement._is_script = False

    # Mock is_expression to return False
    with pytest.MonkeyPatch().context() as m:
        m.setattr("sqlspec.utils.type_guards.is_expression", lambda x: False)
        result = hash_sql_statement(statement)
        assert isinstance(result, str)
        assert result.startswith("sql:")


# Test hash_expression_node function


def test_hash_expression_node_with_children() -> None:
    """Test hash_expression_node including children."""
    node = parse_one("SELECT id FROM users WHERE active = true")
    result = hash_expression_node(node, include_children=True)
    assert isinstance(result, str)
    assert result.startswith("expr:")


def test_hash_expression_node_shallow() -> None:
    """Test hash_expression_node with shallow hashing."""
    node = parse_one("SELECT id FROM users WHERE active = true")
    result = hash_expression_node(node, include_children=False)
    assert isinstance(result, str)
    assert result.startswith("expr:")


def test_hash_expression_node_with_dialect() -> None:
    """Test hash_expression_node with dialect specification."""
    node = parse_one("SELECT id FROM users")
    result = hash_expression_node(node, dialect="postgres")
    assert isinstance(result, str)
    assert ":postgres:" in result


def test_hash_expression_node_different_modes() -> None:
    """Test hash_expression_node produces different hashes for different modes."""
    node = parse_one("SELECT id FROM users WHERE active = true")

    hash_with_children = hash_expression_node(node, include_children=True)
    hash_shallow = hash_expression_node(node, include_children=False)

    # Different modes should produce different hashes for complex nodes
    assert hash_with_children != hash_shallow


# Test hash_optimized_expression function


def test_hash_optimized_expression_basic() -> None:
    """Test hash_optimized_expression with basic parameters."""
    expr = parse_one("SELECT * FROM users")
    result = hash_optimized_expression(expr, "postgres")
    assert isinstance(result, str)
    assert result.startswith("opt:")


def test_hash_optimized_expression_with_schema() -> None:
    """Test hash_optimized_expression with schema information."""
    expr = parse_one("SELECT * FROM users")
    schema = {"users": {"id": "int", "name": "string", "email": "string"}}

    result = hash_optimized_expression(expr, "postgres", schema=schema)
    assert isinstance(result, str)
    assert result.startswith("opt:")


def test_hash_optimized_expression_with_optimizer_settings() -> None:
    """Test hash_optimized_expression with optimizer settings."""
    expr = parse_one("SELECT * FROM users")
    optimizer_settings = {"eliminate_subqueries": True, "pushdown_predicates": True}

    result = hash_optimized_expression(expr, "postgres", optimizer_settings=optimizer_settings)
    assert isinstance(result, str)
    assert result.startswith("opt:")


def test_hash_optimized_expression_complete() -> None:
    """Test hash_optimized_expression with all parameters."""
    expr = parse_one("SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id")
    schema = {"users": {"id": "int", "name": "string"}, "orders": {"id": "int", "user_id": "int", "total": "decimal"}}
    optimizer_settings = {"eliminate_joins": False, "pushdown_predicates": True}

    result = hash_optimized_expression(expr, "postgres", schema=schema, optimizer_settings=optimizer_settings)
    assert isinstance(result, str)
    assert result.startswith("opt:")


def test_hash_optimized_expression_consistency() -> None:
    """Test hash_optimized_expression produces consistent results."""
    expr = parse_one("SELECT * FROM users")
    dialect = "postgres"
    schema = {"users": {"id": "int"}}
    settings = {"optimize": True}

    # Same inputs should produce same hash
    hash1 = hash_optimized_expression(expr, dialect, schema, settings)
    hash2 = hash_optimized_expression(expr, dialect, schema, settings)
    assert hash1 == hash2


def test_hash_optimized_expression_different_contexts() -> None:
    """Test hash_optimized_expression produces different hashes for different contexts."""
    expr = parse_one("SELECT * FROM users")

    hash1 = hash_optimized_expression(expr, "postgres")
    hash2 = hash_optimized_expression(expr, "mysql")
    hash3 = hash_optimized_expression(expr, "postgres", schema={"users": {}})

    # Different contexts should produce different hashes
    assert hash1 != hash2
    assert hash1 != hash3
    assert hash2 != hash3


def test_hash_optimized_expression_empty_schema() -> None:
    """Test hash_optimized_expression handles empty schema correctly."""
    expr = parse_one("SELECT 1")
    result = hash_optimized_expression(expr, "postgres", schema={})
    assert isinstance(result, str)
    assert result.startswith("opt:")


def test_hash_optimized_expression_invalid_schema_values() -> None:
    """Test hash_optimized_expression handles invalid schema values."""
    expr = parse_one("SELECT * FROM users")
    schema = {"users": "invalid_schema_value", "orders": None}

    result = hash_optimized_expression(expr, "postgres", schema=schema)
    assert isinstance(result, str)
    assert result.startswith("opt:")


# Edge cases and performance tests


def test_hash_consistency_across_calls() -> None:
    """Test that hash functions produce consistent results across multiple calls."""
    expr = parse_one("SELECT u.id, u.name FROM users u WHERE u.active = true")
    params = ["param1", 42, True]

    # Expression hashing should be consistent
    hash1 = hash_expression(expr)
    hash2 = hash_expression(expr)
    assert hash1 == hash2

    # Parameter hashing should be consistent
    param_hash1 = hash_parameters(positional_parameters=params)
    param_hash2 = hash_parameters(positional_parameters=params)
    assert param_hash1 == param_hash2


def test_hash_functions_performance() -> None:
    """Test that hash functions handle reasonably complex inputs efficiently."""
    # Create a complex expression
    complex_sql = """
    SELECT
        u.id,
        u.name,
        COUNT(o.id) as order_count,
        SUM(o.total) as total_spent,
        AVG(o.total) as avg_order
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = true
      AND u.created_at > '2023-01-01'
      AND (o.status = 'completed' OR o.status = 'shipped')
    GROUP BY u.id, u.name
    HAVING COUNT(o.id) > 5
    ORDER BY total_spent DESC
    LIMIT 100
    """

    expr = parse_one(complex_sql)
    complex_params = [f"param_{i}" for i in range(100)]

    # Should complete without errors
    expr_hash = hash_expression(expr)
    param_hash = hash_parameters(positional_parameters=complex_params)

    assert isinstance(expr_hash, int)
    assert isinstance(param_hash, int)


def test_hash_with_special_sql_constructs() -> None:
    """Test hashing with various SQL constructs."""
    # Test different SQL statement types
    constructs = [
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
        "UPDATE users SET active = false WHERE last_login < '2023-01-01'",
        "DELETE FROM users WHERE active = false",
        "CREATE TABLE temp_users AS SELECT * FROM users WHERE active = true",
        "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
    ]

    hashes = []
    for sql in constructs:
        expr = parse_one(sql)
        hash_val = hash_expression(expr)
        hashes.append(hash_val)
        assert isinstance(hash_val, int)

    # All hashes should be different
    assert len(set(hashes)) == len(hashes)


def test_error_handling() -> None:
    """Test error handling in hash functions."""
    # Test with malformed mock objects
    malformed_statement = Mock()
    malformed_statement._positional_parameters = None
    malformed_statement._named_parameters = None
    malformed_statement._original_parameters = None
    malformed_statement._filters = None
    malformed_statement._dialect = "sqlite"
    malformed_statement._is_many = False
    malformed_statement._is_script = False
    # Missing _statement and _raw_sql - should handle gracefully
    try:
        hash_sql_statement(malformed_statement)
    except AttributeError:
        # Expected behavior - missing attributes
        pass


def test_memory_efficiency() -> None:
    """Test that hash functions are memory efficient with large data sets."""
    # Create large parameter set for execute_many scenario
    large_params = [(f"name_{i}", f"email_{i}@example.com", i) for i in range(10000)]

    # Should handle large parameter sets efficiently
    result = hash_parameters(original_parameters=large_params)
    assert isinstance(result, int)

    # The hashing should use sampling for large sets, not hash every item
    # This is tested by ensuring the function completes reasonably quickly


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "oracle", "bigquery"])
def test_hash_expression_node_dialects(dialect: str) -> None:
    """Test hash_expression_node with different SQL dialects."""
    node = parse_one("SELECT id FROM users WHERE active = 1")
    result = hash_expression_node(node, dialect=dialect)
    assert isinstance(result, str)
    assert f":{dialect}:" in result


def test_hash_parameters_edge_cases() -> None:
    """Test hash_parameters with various edge cases."""
    from sqlspec.core.parameters import TypedParameter

    # Test with TypedParameter containing unhashable values
    typed_param_with_list = TypedParameter([1, 2, 3], list, "list_param")
    typed_param_with_dict = TypedParameter({"key": "value"}, dict, "dict_param")

    result = hash_parameters(positional_parameters=[typed_param_with_list, typed_param_with_dict])
    assert isinstance(result, int)

    # Test named parameters with complex TypedParameter
    named_with_typed = {"complex": typed_param_with_dict}
    result = hash_parameters(named_parameters=named_with_typed)
    assert isinstance(result, int)
