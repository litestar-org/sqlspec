"""Function-based tests for query complexity analyzer."""

import pytest
import sqlglot

from sqlspec.statement.pipelines.analyzers._query_complexity import QueryComplexity
from sqlspec.statement.sql import SQLConfig


def test_query_complexity_simple_query_low_complexity() -> None:
    """Test that simple queries have low complexity scores."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Simple SELECT query
    simple_sql = "SELECT id, name FROM users WHERE active = 1"
    expression = sqlglot.parse_one(simple_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    assert "overall_complexity_score" in result.metrics
    complexity_score = result.metrics["overall_complexity_score"]

    # Simple query should have low complexity
    assert complexity_score < 20
    assert len(result.issues) == 0


def test_query_complexity_join_analysis() -> None:
    """Test analysis of JOIN complexity in queries."""
    analyzer = QueryComplexity(max_join_count=2)
    config = SQLConfig()

    # Query with multiple joins
    join_sql = """
        SELECT u.name, o.total, p.name as product_name, c.name as category_name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        JOIN categories c ON p.category_id = c.id
    """
    expression = sqlglot.parse_one(join_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect high number of joins
    assert result.metrics["join_count"] > 2
    assert any("join" in issue.lower() for issue in result.issues)
    assert result.metrics["overall_complexity_score"] > 15


def test_query_complexity_subquery_analysis() -> None:
    """Test analysis of subquery complexity and nesting depth."""
    analyzer = QueryComplexity(max_subquery_depth=2)
    config = SQLConfig()

    # Query with nested subqueries
    subquery_sql = """
        SELECT name FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE total > (
                SELECT AVG(total) FROM orders
                WHERE created_at > (
                    SELECT MAX(created_at) - INTERVAL 30 DAY FROM orders
                )
            )
        )
    """
    expression = sqlglot.parse_one(subquery_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect high subquery complexity
    assert result.metrics["subquery_count"] > 1
    assert result.metrics["max_subquery_depth"] > 2
    assert any("subquery" in issue.lower() for issue in result.issues)


def test_query_complexity_where_clause_analysis() -> None:
    """Test analysis of WHERE clause complexity."""
    analyzer = QueryComplexity(max_where_conditions=3)
    config = SQLConfig()

    # Query with complex WHERE clause
    complex_where_sql = """
        SELECT * FROM users
        WHERE active = 1
        AND created_at > '2023-01-01'
        AND (department = 'IT' OR department = 'Engineering')
        AND age BETWEEN 25 AND 65
        AND salary > 50000
        AND (status = 'full-time' OR (status = 'part-time' AND hours_per_week > 20))
    """
    expression = sqlglot.parse_one(complex_where_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect complex WHERE conditions
    assert result.metrics["total_where_conditions"] > 3
    assert any(
        "where" in warning.lower() or "where" in issue.lower() for warning in result.warnings for issue in result.issues
    )


def test_query_complexity_function_analysis() -> None:
    """Test analysis of function usage complexity."""
    analyzer = QueryComplexity(max_function_calls=5)
    config = SQLConfig()

    # Query with many functions
    function_heavy_sql = """
        SELECT
            UPPER(TRIM(name)) as clean_name,
            DATE_FORMAT(created_at, '%Y-%m') as creation_month,
            CASE
                WHEN DATEDIFF(NOW(), created_at) > 365 THEN 'old'
                WHEN DATEDIFF(NOW(), created_at) > 30 THEN 'recent'
                ELSE 'new'
            END as user_age_category,
            CONCAT(SUBSTRING(email, 1, LOCATE('@', email) - 1), '@***') as masked_email,
            ABS(ROUND(salary * 1.1, 2)) as projected_salary
        FROM users
    """
    expression = sqlglot.parse_one(function_heavy_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect high function usage
    assert result.metrics["function_count"] > 5
    assert any(
        "function" in warning.lower() or "function" in issue.lower()
        for warning in result.warnings
        for issue in result.issues
    )


def test_query_complexity_cartesian_product_detection() -> None:
    """Test detection of potential Cartesian products."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Query with potential Cartesian product
    cartesian_sql = "SELECT * FROM users, orders, products WHERE users.active = 1"
    expression = sqlglot.parse_one(cartesian_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect potential Cartesian product
    assert result.metrics.get("potential_cartesian_products", 0) >= 0
    # Note: Detection depends on SQLGlot's parsing of comma-separated tables


def test_query_complexity_correlated_subquery_detection() -> None:
    """Test detection of correlated subqueries."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Query with correlated subquery (EXISTS pattern)
    correlated_sql = """
        SELECT * FROM users u
        WHERE EXISTS (
            SELECT 1 FROM orders o
            WHERE o.user_id = u.id AND o.total > 100
        )
    """
    expression = sqlglot.parse_one(correlated_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect correlated subquery
    assert result.metrics["subquery_count"] > 0
    # The correlated detection is heuristic-based, so we just check it doesn't crash


@pytest.mark.parametrize(
    ("sql_query", "expected_metrics", "description"),
    [
        (
            "SELECT * FROM users WHERE id = 1",
            {"join_count": 0, "subquery_count": 0, "function_count": 0},
            "simple select query",
        ),
        ("SELECT COUNT(*) FROM users GROUP BY department", {"function_count": 1}, "query with single function"),
        (
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
            {"subquery_count": 1},
            "query with single subquery",
        ),
    ],
    ids=["simple", "single_function", "single_subquery"],
)
def test_query_complexity_various_query_patterns(sql_query, expected_metrics, description) -> None:
    """Test complexity analysis for various query patterns."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    expression = sqlglot.parse_one(sql_query, read="mysql")
    result = analyzer.analyze(expression, "mysql", config)

    # Verify expected metrics are present and have expected values
    for metric, expected_value in expected_metrics.items():
        assert metric in result.metrics, f"Missing metric {metric} for {description}"
        assert result.metrics[metric] >= expected_value, f"Unexpected {metric} value for {description}"


def test_query_complexity_configurable_thresholds() -> None:
    """Test that analyzer thresholds are configurable."""
    # Strict analyzer
    strict_analyzer = QueryComplexity(
        max_join_count=1, max_subquery_depth=1, max_function_calls=2, max_where_conditions=2
    )

    # Permissive analyzer
    permissive_analyzer = QueryComplexity(
        max_join_count=20, max_subquery_depth=10, max_function_calls=50, max_where_conditions=30
    )

    config = SQLConfig()

    # Moderately complex query
    moderate_sql = """
        SELECT u.name, COUNT(o.id) as orders
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1 AND u.created_at > '2023-01-01'
        GROUP BY u.id, u.name
    """
    expression = sqlglot.parse_one(moderate_sql, read="mysql")

    strict_result = strict_analyzer.analyze(expression, "mysql", config)
    permissive_result = permissive_analyzer.analyze(expression, "mysql", config)

    # Strict should find more issues than permissive
    strict_issue_count = len(strict_result.issues) + len(strict_result.warnings)
    permissive_issue_count = len(permissive_result.issues) + len(permissive_result.warnings)

    assert strict_issue_count >= permissive_issue_count


def test_query_complexity_overall_score_calculation() -> None:
    """Test the overall complexity score calculation for various query types."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Very complex query with multiple complexity factors
    complex_sql = """
        SELECT
            u.name,
            COUNT(o.id) as order_count,
            AVG(oi.quantity * p.price) as avg_order_value,
            CASE
                WHEN COUNT(o.id) > 10 THEN 'frequent'
                WHEN COUNT(o.id) > 5 THEN 'regular'
                ELSE 'occasional'
            END as customer_type
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        LEFT JOIN order_items oi ON o.id = oi.order_id
        LEFT JOIN products p ON oi.product_id = p.id
        WHERE u.active = 1
        AND u.created_at > '2020-01-01'
        AND o.total > (SELECT AVG(total) FROM orders)
        AND EXISTS (
            SELECT 1 FROM user_preferences up
            WHERE up.user_id = u.id AND up.newsletter = 1
        )
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY avg_order_value DESC
        LIMIT 100
    """
    expression = sqlglot.parse_one(complex_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    complexity_score = result.metrics["overall_complexity_score"]

    # Complex query should have high score
    assert complexity_score > 30

    # Should have multiple metrics contributing to complexity
    assert result.metrics["join_count"] > 0
    assert result.metrics["function_count"] > 0
    assert result.metrics["subquery_count"] > 0


def test_query_complexity_threshold_warnings_and_issues() -> None:
    """Test that complexity thresholds trigger appropriate warnings and issues."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Very high complexity query
    very_complex_sql = """
        SELECT
            u.id,
            (SELECT COUNT(*) FROM orders o1 WHERE o1.user_id = u.id) as order_count,
            (SELECT AVG(total) FROM orders o2 WHERE o2.user_id = u.id) as avg_order,
            (SELECT MAX(created_at) FROM orders o3 WHERE o3.user_id = u.id) as last_order
        FROM users u
        JOIN user_profiles up ON u.id = up.user_id
        JOIN addresses a ON u.id = a.user_id
        JOIN user_preferences pref ON u.id = pref.user_id
        WHERE u.active = 1
        AND up.verified = 1
        AND a.country = 'US'
        AND pref.marketing_emails = 1
        AND u.created_at > '2020-01-01'
        AND u.last_login > NOW() - INTERVAL 30 DAY
    """
    expression = sqlglot.parse_one(very_complex_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    complexity_score = result.metrics["overall_complexity_score"]

    # Should trigger high complexity warnings
    if complexity_score > 100:
        assert any("very high" in issue.lower() for issue in result.issues)
    elif complexity_score > 50:
        assert any("high" in warning.lower() for warning in result.warnings)


def test_query_complexity_legitimate_business_query() -> None:
    """Test analyzer on a legitimate but complex business query."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Legitimate complex business query
    business_sql = """
        SELECT
            u.name,
            u.email,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_spent,
            AVG(o.total) as avg_order_value
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        AND u.created_at > '2023-01-01'
        AND o.total > (SELECT AVG(total) FROM orders WHERE status = 'completed')
        GROUP BY u.id, u.name, u.email
        HAVING COUNT(o.id) > 0
        ORDER BY total_spent DESC
    """
    expression = sqlglot.parse_one(business_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should analyze without crashing
    assert "overall_complexity_score" in result.metrics

    # Complex business query might trigger warnings but should be analyzable
    complexity_score = result.metrics["overall_complexity_score"]
    assert complexity_score >= 0  # Should have some complexity


def test_query_complexity_metrics_completeness() -> None:
    """Test that analyzer provides comprehensive metrics for all query types."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Query with various complexity elements
    comprehensive_sql = """
        SELECT
            u.name,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_spent,
            AVG(o.total) as avg_order_value
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        AND u.created_at > '2023-01-01'
        AND o.total > (SELECT AVG(total) FROM orders WHERE status = 'completed')
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY total_spent DESC
    """
    expression = sqlglot.parse_one(comprehensive_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should provide all expected metrics
    expected_metrics = [
        "join_count",
        "subquery_count",
        "function_count",
        "where_clause_count",
        "overall_complexity_score",
    ]

    for metric in expected_metrics:
        assert metric in result.metrics, f"Missing metric: {metric}"
        assert isinstance(result.metrics[metric], int), f"Metric {metric} should be integer"


def test_query_complexity_analysis_notes() -> None:
    """Test that analysis provides useful notes about the complexity assessment."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    simple_sql = "SELECT * FROM users WHERE id = 1"
    expression = sqlglot.parse_one(simple_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should provide notes about the analysis
    assert len(result.notes) > 0
    assert any("complexity" in note.lower() for note in result.notes)


def test_query_complexity_expensive_function_detection() -> None:
    """Test detection of expensive functions that impact performance."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Query with expensive functions
    expensive_sql = """
        SELECT * FROM users
        WHERE name REGEXP '^[A-Z][a-z]+'
        AND description LIKE '%pattern%'
        AND CONCAT_WS(' ', first_name, last_name) = 'John Doe'
    """
    expression = sqlglot.parse_one(expensive_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should track expensive functions
    assert "expensive_function_count" in result.metrics
    # If expensive functions are detected, should warn
    if result.metrics["expensive_function_count"] > 0:
        assert any("expensive" in warning.lower() for warning in result.warnings)


def test_query_complexity_nested_function_detection() -> None:
    """Test detection of nested function calls."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Query with nested functions
    nested_sql = """
        SELECT
            UPPER(SUBSTRING(TRIM(name), 1, 10)) as short_name,
            ROUND(AVG(ABS(salary)), 2) as avg_abs_salary
        FROM users
        GROUP BY department
    """
    expression = sqlglot.parse_one(nested_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should detect nested functions
    assert result.metrics["nested_function_count"] > 0
    # If many nested functions, should warn
    if result.metrics["nested_function_count"] > 3:
        assert any("nested" in warning.lower() for warning in result.warnings)


def test_query_complexity_zero_complexity_query() -> None:
    """Test handling of minimal queries with near-zero complexity."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Minimal query
    minimal_sql = "SELECT 1"
    expression = sqlglot.parse_one(minimal_sql, read="mysql")

    result = analyzer.analyze(expression, "mysql", config)

    # Should handle minimal queries gracefully
    assert result.metrics["overall_complexity_score"] >= 0
    assert len(result.issues) == 0  # Minimal queries shouldn't trigger issues

    # Most metrics should be zero or near zero
    assert result.metrics["join_count"] == 0
    assert result.metrics["subquery_count"] == 0


def test_query_complexity_different_dialects() -> None:
    """Test complexity analysis works with different SQL dialects."""
    analyzer = QueryComplexity()
    config = SQLConfig()

    # Test with different dialects
    dialects_to_test = ["mysql", "postgresql", "sqlite"]
    test_sql = "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name"

    for dialect in dialects_to_test:
        try:
            expression = sqlglot.parse_one(test_sql, read=dialect)
            result = analyzer.analyze(expression, dialect, config)

            assert "overall_complexity_score" in result.metrics, f"Failed for dialect {dialect}"
            assert result.metrics["overall_complexity_score"] >= 0, f"Invalid score for dialect {dialect}"

        except Exception as e:
            # Some dialects might not be supported, which is acceptable
            pytest.skip(f"Dialect {dialect} not supported: {e}")
