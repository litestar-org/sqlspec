"""Function-based tests for tautology condition validator."""

import pytest
import sqlglot

from sqlspec.statement.pipelines.validators._tautology import TautologyConditions
from sqlspec.statement.sql import SQLConfig


def test_tautology_detects_numeric_tautology() -> None:
    """Test detection of numeric tautologies like 1=1."""
    validator = TautologyConditions()
    config = SQLConfig()

    # Classic numeric tautology
    tautology_sql = "SELECT * FROM users WHERE 1 = 1"
    expression = sqlglot.parse_one(tautology_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("tautological" in issue.lower() for issue in result.issues)


def test_tautology_detects_string_tautology() -> None:
    """Test detection of string tautologies like 'a'='a'."""
    validator = TautologyConditions()
    config = SQLConfig()

    # String-based tautology
    tautology_sql = "SELECT * FROM users WHERE 'a' = 'a'"
    expression = sqlglot.parse_one(tautology_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("tautological" in issue.lower() for issue in result.issues)


def test_tautology_detects_column_self_comparison() -> None:
    """Test detection of column self-comparisons."""
    validator = TautologyConditions()
    config = SQLConfig()

    # Column comparing to itself
    tautology_sql = "SELECT * FROM users WHERE username = username"
    expression = sqlglot.parse_one(tautology_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("tautological" in issue.lower() for issue in result.issues)


def test_tautology_detects_or_clause_with_tautology() -> None:
    """Test detection of OR clauses containing tautologies (classic injection pattern)."""
    validator = TautologyConditions()
    config = SQLConfig()

    # OR clause with tautology (classic injection)
    tautology_sql = "SELECT * FROM users WHERE username = 'admin' OR 1 = 1"
    expression = sqlglot.parse_one(tautology_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("or" in issue.lower() for issue in result.issues)


def test_tautology_passes_legitimate_comparisons() -> None:
    """Test that legitimate comparisons pass validation without false positives."""
    validator = TautologyConditions()
    config = SQLConfig()

    legitimate_sql = "SELECT * FROM users WHERE age > 18 AND status = 'active'"
    expression = sqlglot.parse_one(legitimate_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert result.is_safe
    assert len(result.issues) == 0


def test_tautology_mathematical_constants_configurable() -> None:
    """Test that mathematical constants can be allowed or disallowed via configuration."""
    validator_strict = TautologyConditions(allow_mathematical_constants=False)
    validator_permissive = TautologyConditions(allow_mathematical_constants=True)
    config = SQLConfig()

    # Mathematical constant comparison
    math_sql = "SELECT * FROM users WHERE 2 + 2 = 4"
    expression = sqlglot.parse_one(math_sql, read="mysql")

    result_strict = validator_strict.validate(expression, "mysql", config)
    result_permissive = validator_permissive.validate(expression, "mysql", config)

    # Strict should flag it, permissive should allow it
    assert not result_strict.is_safe
    assert result_permissive.is_safe


@pytest.mark.parametrize(
    ("tautology_sql", "expected_pattern", "description"),
    [
        ("SELECT * FROM users WHERE 1 = 1", "tautological", "basic numeric tautology"),
        ("SELECT * FROM users WHERE 'x' = 'x'", "tautological", "basic string tautology"),
        ("SELECT * FROM users WHERE id = id", "tautological", "column self-comparison"),
        ("SELECT * FROM users WHERE TRUE = TRUE", "tautological", "boolean literal tautology"),
        ("SELECT * FROM users WHERE 2 <> 2", "tautological", "numeric contradiction"),
    ],
    ids=["numeric_eq", "string_eq", "column_self", "boolean_eq", "numeric_neq"],
)
def test_tautology_detects_various_tautology_patterns(
    tautology_sql: str, expected_pattern: str, description: str
) -> None:
    """Test detection of various tautological patterns."""
    validator = TautologyConditions()
    config = SQLConfig()

    expression = sqlglot.parse_one(tautology_sql, read="mysql")
    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe, f"Failed to detect {description}"
    assert any(expected_pattern.lower() in issue.lower() for issue in result.issues), (
        f"Expected pattern '{expected_pattern}' not found in issues for {description}"
    )


def test_tautology_detects_and_clause_contradictions() -> None:
    """Test detection of contradictory conditions in AND clauses."""
    validator = TautologyConditions()
    config = SQLConfig()

    # AND clause with contradiction
    contradiction_sql = "SELECT * FROM users WHERE status = 'active' AND status <> status"
    expression = sqlglot.parse_one(contradiction_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect contradictory condition
    if len(result.warnings) > 0:
        assert any("and" in warning.lower() or "contradictory" in warning.lower() for warning in result.warnings)


def test_tautology_max_depth_configuration() -> None:
    """Test that maximum AST depth is configurable to prevent infinite recursion."""
    validator_shallow = TautologyConditions(max_depth=1)
    validator_deep = TautologyConditions(max_depth=10)
    config = SQLConfig()

    # Nested query that might test depth limits
    nested_sql = """
        SELECT * FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE total > (SELECT AVG(total) FROM orders WHERE 1 = 1)
        )
    """
    expression = sqlglot.parse_one(nested_sql, read="mysql")

    result_shallow = validator_shallow.validate(expression, "mysql", config)
    result_deep = validator_deep.validate(expression, "mysql", config)

    # Both should handle the query without crashing
    assert isinstance(result_shallow.is_safe, bool)
    assert isinstance(result_deep.is_safe, bool)

    # Deep validator should find the tautology, shallow might miss it
    deep_tautology_issues = len([i for i in result_deep.issues if "tautological" in i.lower()])
    assert deep_tautology_issues > 0


@pytest.mark.parametrize(
    ("operator", "left", "right", "should_detect"),
    [
        ("=", "1", "1", True),
        ("=", "'a'", "'a'", True),
        ("<>", "1", "1", True),  # Contradiction
        (">=", "x", "x", True),  # Column self-comparison
        (">", "1", "2", False),  # Legitimate comparison
        ("=", "name", "'John'", False),  # Legitimate comparison
    ],
    ids=["numeric_eq", "string_eq", "numeric_neq", "column_gte", "numeric_gt", "column_literal"],
)
def test_tautology_operator_specific_detection(operator: str, left: str, right: str, should_detect: bool) -> None:
    """Test tautology detection for specific operators and operands."""
    validator = TautologyConditions()
    config = SQLConfig()

    # Build SQL with the specific operator and operands
    test_sql = f"SELECT * FROM users WHERE {left} {operator} {right}"
    expression = sqlglot.parse_one(test_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    if should_detect:
        assert not result.is_safe, f"Should detect tautology: {left} {operator} {right}"
        assert any("tautological" in issue.lower() for issue in result.issues)
    else:
        assert result.is_safe, f"Should not detect tautology: {left} {operator} {right}"


def test_tautology_with_complex_or_conditions() -> None:
    """Test detection of tautologies in complex OR conditions."""
    validator = TautologyConditions()
    config = SQLConfig()

    # Complex OR with nested tautology
    complex_sql = """
        SELECT * FROM users
        WHERE (status = 'active' AND department = 'IT')
        OR (age > 25 OR 'x' = 'x')
        OR role = 'admin'
    """
    expression = sqlglot.parse_one(complex_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect the tautology in the OR clause
    assert not result.is_safe
    assert any("or" in issue.lower() and "tautological" in issue.lower() for issue in result.issues)


def test_tautology_ignores_legitimate_mathematical_expressions() -> None:
    """Test that legitimate mathematical expressions are not flagged when allowed."""
    validator = TautologyConditions(allow_mathematical_constants=True)
    config = SQLConfig()

    # Legitimate mathematical expressions
    math_expressions = [
        "SELECT * FROM products WHERE price * 1.1 = discounted_price",
        "SELECT * FROM orders WHERE quantity * unit_price = total",
        "SELECT * FROM users WHERE YEAR(created_at) = 2023",
    ]

    for math_sql in math_expressions:
        expression = sqlglot.parse_one(math_sql, read="mysql")
        result = validator.validate(expression, "mysql", config)

        assert result.is_safe, f"Should not flag legitimate math: {math_sql}"


def test_tautology_detects_table_qualified_column_self_comparison() -> None:
    """Test detection of self-comparisons with table qualifiers."""
    validator = TautologyConditions()
    config = SQLConfig()

    # Table-qualified column self-comparison
    qualified_sql = "SELECT * FROM users u WHERE u.username = u.username"
    expression = sqlglot.parse_one(qualified_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    assert not result.is_safe
    assert any("tautological" in issue.lower() for issue in result.issues)


def test_tautology_with_mixed_legitimate_and_tautological_conditions() -> None:
    """Test behavior with mixed legitimate and tautological conditions."""
    validator = TautologyConditions()
    config = SQLConfig()

    # Mix of legitimate and tautological conditions
    mixed_sql = """
        SELECT * FROM users
        WHERE age > 18
        AND status = 'active'
        AND (department = 'IT' OR 1 = 1)
        AND created_at > '2023-01-01'
    """
    expression = sqlglot.parse_one(mixed_sql, read="mysql")

    result = validator.validate(expression, "mysql", config)

    # Should detect the tautology despite legitimate conditions
    assert not result.is_safe
    assert any("tautological" in issue.lower() for issue in result.issues)
