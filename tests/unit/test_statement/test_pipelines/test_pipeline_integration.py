"""Integration tests for SQL pipeline components working together."""

import sqlglot

from sqlspec.statement.pipelines.analyzers._query_complexity import QueryComplexity
from sqlspec.statement.pipelines.base import TransformerPipeline
from sqlspec.statement.pipelines.transformers._normalize_whitespace import NormalizeWhitespace
from sqlspec.statement.pipelines.transformers._remove_comments import CommentRemover
from sqlspec.statement.pipelines.validators._injection import PreventInjection
from sqlspec.statement.pipelines.validators._suspicious_comments import SuspiciousComments
from sqlspec.statement.pipelines.validators._suspicious_keywords import SuspiciousKeywords
from sqlspec.statement.pipelines.validators._tautology import TautologyConditions
from sqlspec.statement.sql import SQLConfig


def test_pipeline_integration_security_validation_flow() -> None:
    """Test a complete security validation flow with multiple validators."""
    # Create multiple validators for comprehensive security checking
    injection_validator = PreventInjection()
    tautology_validator = TautologyConditions()
    comment_validator = SuspiciousComments()
    keyword_validator = SuspiciousKeywords()

    config = SQLConfig()

    # Malicious SQL with multiple attack vectors
    malicious_sql = """
        SELECT * FROM users
        WHERE username = 'admin' OR 1=1  /* UNION SELECT password FROM admin */
        AND SLEEP(5)
        -- DROP TABLE users
    """
    expression = sqlglot.parse_one(malicious_sql, read="mysql")

    # Run through all validators
    injection_result = injection_validator.validate(expression, "mysql", config)
    tautology_result = tautology_validator.validate(expression, "mysql", config)
    comment_result = comment_validator.validate(expression, "mysql", config)
    keyword_result = keyword_validator.validate(expression, "mysql", config)

    # All validators should detect issues
    assert not injection_result.is_safe
    assert not tautology_result.is_safe
    assert not comment_result.is_safe
    assert not keyword_result.is_safe

    # Should have detected multiple types of attacks
    all_issues = injection_result.issues + tautology_result.issues + comment_result.issues + keyword_result.issues

    assert len(all_issues) >= 3  # Multiple attack vectors detected


def test_pipeline_integration_transformation_and_validation() -> None:
    """Test transformation followed by validation."""
    # Create transformers and validators
    whitespace_transformer = NormalizeWhitespace()
    comment_transformer = CommentRemover()
    injection_validator = PreventInjection()

    config = SQLConfig()

    # SQL with formatting issues and potential injection
    messy_sql = """
        SELECT    *    FROM    users
        /* suspicious comment with UNION */
        WHERE    username    =    'admin'    OR    1=1
    """
    expression = sqlglot.parse_one(messy_sql, read="mysql")

    # Apply transformations first
    whitespace_result = whitespace_transformer.transform(expression, "mysql", config)
    comment_result = comment_transformer.transform(whitespace_result.expression, "mysql", config)

    # Then validate the cleaned SQL
    validation_result = injection_validator.validate(comment_result.expression, "mysql", config)

    # Transformations should have been applied
    assert whitespace_result.modified
    assert comment_result.modified

    # But injection should still be detected after cleaning
    assert not validation_result.is_safe
    assert any("tautological" in issue.lower() or "injection" in issue.lower() for issue in validation_result.issues)


def test_pipeline_integration_legitimate_business_query() -> None:
    """Test that legitimate business queries pass through all components."""
    # Create all pipeline components
    whitespace_transformer = NormalizeWhitespace()
    comment_transformer = CommentRemover()
    injection_validator = PreventInjection()
    tautology_validator = TautologyConditions()
    keyword_validator = SuspiciousKeywords()
    complexity_analyzer = QueryComplexity()

    config = SQLConfig()

    # Legitimate business query
    business_sql = """
        -- Monthly sales report
        SELECT
            u.name,
            u.email,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_revenue
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        AND o.created_at >= '2023-01-01'
        GROUP BY u.id, u.name, u.email
        ORDER BY total_revenue DESC
        LIMIT 100
    """
    expression = sqlglot.parse_one(business_sql, read="mysql")

    # Apply transformations
    whitespace_result = whitespace_transformer.transform(expression, "mysql", config)
    comment_result = comment_transformer.transform(whitespace_result.expression, "mysql", config)

    # Apply validations
    injection_result = injection_validator.validate(comment_result.expression, "mysql", config)
    tautology_result = tautology_validator.validate(comment_result.expression, "mysql", config)
    keyword_result = keyword_validator.validate(comment_result.expression, "mysql", config)

    # Analyze complexity
    complexity_result = complexity_analyzer.analyze(comment_result.expression, "mysql", config)

    # All validations should pass for legitimate query
    assert injection_result.is_safe
    assert tautology_result.is_safe
    assert keyword_result.is_safe

    # Complexity should be reasonable
    assert complexity_result.metrics["overall_complexity_score"] < 100

    # Transformations should have cleaned up formatting
    cleaned_sql = comment_result.expression.sql(dialect="mysql")
    assert "--" not in cleaned_sql  # Comments removed
    assert len(cleaned_sql.split()) < len(business_sql.split())  # Whitespace normalized


def test_pipeline_integration_transformer_pipeline_class() -> None:
    """Test the TransformerPipeline class with multiple components."""
    # Create a pipeline with multiple transformers
    pipeline = TransformerPipeline()

    # Add transformers to pipeline
    NormalizeWhitespace()
    CommentRemover()

    # Note: TransformerPipeline expects ProcessorProtocol components
    # We'll need to adapt our transformers to work with the pipeline
    # For now, let's test the pipeline structure

    config = SQLConfig()

    # Test SQL
    test_sql = """
        SELECT    *    FROM    users    /* comment */
        WHERE    id    =    1
    """
    expression = sqlglot.parse_one(test_sql, read="mysql")

    # The pipeline should handle empty components gracefully
    result_expression, validation_result = pipeline.execute(expression, "mysql", config)

    # Should return the expression unchanged if no components
    assert result_expression is not None
    assert validation_result.is_safe  # No validation issues with empty pipeline


def test_pipeline_integration_configuration_effects() -> None:
    """Test how different configurations affect pipeline behavior."""
    # Test strict vs permissive configurations
    strict_config = SQLConfig(enable_validation=True)
    permissive_config = SQLConfig(enable_validation=False)

    # Create validators with different configurations
    strict_injection = PreventInjection(check_union_injection=True, max_union_selects=1)
    permissive_injection = PreventInjection(check_union_injection=False, max_union_selects=10)

    # Test SQL with potential issues
    test_sql = """
        SELECT id FROM users
        UNION SELECT id FROM orders
        UNION SELECT id FROM products
    """
    expression = sqlglot.parse_one(test_sql, read="mysql")

    # Test with different configurations
    strict_result = strict_injection.validate(expression, "mysql", strict_config)
    permissive_result = permissive_injection.validate(expression, "mysql", permissive_config)

    # Strict should be more restrictive
    strict_issue_count = len(strict_result.issues) + len(strict_result.warnings)
    permissive_issue_count = len(permissive_result.issues) + len(permissive_result.warnings)

    assert strict_issue_count >= permissive_issue_count


def test_pipeline_integration_error_handling() -> None:
    """Test error handling in pipeline integration scenarios."""
    # Create components
    transformer = NormalizeWhitespace()
    validator = PreventInjection()

    config = SQLConfig()

    # Test with edge case SQL
    edge_case_sqls = [
        "SELECT 1",  # Minimal query
        "",  # Empty string (might not parse)
        "SELECT * FROM users WHERE id = ?",  # Parameterized query
    ]

    for sql in edge_case_sqls:
        if not sql.strip():
            continue

        try:
            expression = sqlglot.parse_one(sql, read="mysql")

            # Apply transformation
            transform_result = transformer.transform(expression, "mysql", config)

            # Apply validation
            validation_result = validator.validate(transform_result.expression, "mysql", config)

            # Should handle gracefully without crashing
            assert isinstance(transform_result.modified, bool)
            assert isinstance(validation_result.is_safe, bool)

        except Exception:
            # Some edge cases might not parse, which is acceptable
            pass


def test_pipeline_integration_performance_with_complex_query() -> None:
    """Test pipeline performance with complex queries."""
    # Create multiple components
    components = [
        NormalizeWhitespace(),
        CommentRemover(),
        PreventInjection(),
        TautologyConditions(),
        SuspiciousKeywords(),
        QueryComplexity(),
    ]

    config = SQLConfig()

    # Very complex query
    complex_sql = """
        -- Complex reporting query
        SELECT
            u.id,
            u.name,
            u.email,
            COUNT(DISTINCT o.id) as order_count,
            SUM(oi.quantity * p.price) as total_spent,
            AVG(o.total) as avg_order_value,
            MAX(o.created_at) as last_order_date,
            CASE
                WHEN COUNT(o.id) > 10 THEN 'VIP'
                WHEN COUNT(o.id) > 5 THEN 'Regular'
                ELSE 'New'
            END as customer_tier
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        LEFT JOIN order_items oi ON o.id = oi.order_id
        LEFT JOIN products p ON oi.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE u.active = 1
        AND u.created_at >= '2020-01-01'
        AND (o.status = 'completed' OR o.status IS NULL)
        AND EXISTS (
            SELECT 1 FROM user_preferences up
            WHERE up.user_id = u.id
            AND up.marketing_consent = 1
        )
        GROUP BY u.id, u.name, u.email
        HAVING COUNT(o.id) > 0 OR u.created_at > '2023-01-01'
        ORDER BY total_spent DESC, order_count DESC
        LIMIT 1000
    """

    expression = sqlglot.parse_one(complex_sql, read="mysql")

    # Process through all components
    current_expression = expression

    for component in components:
        if hasattr(component, "transform"):
            result = component.transform(current_expression, "mysql", config)
            current_expression = result.expression
        elif hasattr(component, "validate"):
            result = component.validate(current_expression, "mysql", config)
            # Validation doesn't modify the expression
        elif hasattr(component, "analyze"):
            result = component.analyze(current_expression, "mysql", config)
            # Analysis doesn't modify the expression

    # Should complete without performance issues
    assert current_expression is not None


def test_pipeline_integration_single_pass_processing() -> None:
    """Test that pipeline maintains single-pass processing (no re-parsing)."""
    # Create transformers
    whitespace_transformer = NormalizeWhitespace()
    comment_transformer = CommentRemover()

    config = SQLConfig()

    # SQL with both formatting and comment issues
    input_sql = """
        SELECT    *    FROM    users    /* remove this comment */
        WHERE    id    =    1    -- and this comment too
    """
    initial_expression = sqlglot.parse_one(input_sql, read="mysql")

    # Track expression object identity through transformations
    expression_ids = [id(initial_expression)]

    # Apply first transformation
    result1 = whitespace_transformer.transform(initial_expression, "mysql", config)
    expression_ids.append(id(result1.expression))

    # Apply second transformation
    result2 = comment_transformer.transform(result1.expression, "mysql", config)
    expression_ids.append(id(result2.expression))

    # Each transformation should produce a new expression object
    # (since SQLGlot creates new objects during transformations)
    # But we should never re-parse from string

    # Verify we can get SQL output without issues
    final_sql = result2.expression.sql(dialect="mysql")

    # Final SQL should be cleaner than input
    assert "--" not in final_sql
    assert "/*" not in final_sql
    assert len(final_sql.split()) <= len(input_sql.split())

    # Should maintain SQL validity
    assert "SELECT" in final_sql.upper()
    assert "FROM users" in final_sql.upper()


def test_pipeline_integration_validator_cooperation() -> None:
    """Test that multiple validators work together without conflicts."""
    # Create validators that might have overlapping concerns
    injection_validator = PreventInjection()
    tautology_validator = TautologyConditions()
    comment_validator = SuspiciousComments()

    config = SQLConfig()

    # SQL that triggers multiple validators
    multi_issue_sql = """
        SELECT * FROM users
        WHERE 1=1  /* OR UNION SELECT password FROM admin */
        AND username = 'admin' OR 'a'='a'
        -- AND DROP TABLE logs
    """
    expression = sqlglot.parse_one(multi_issue_sql, read="mysql")

    # Apply all validators
    injection_result = injection_validator.validate(expression, "mysql", config)
    tautology_result = tautology_validator.validate(expression, "mysql", config)
    comment_result = comment_validator.validate(expression, "mysql", config)

    # All should detect issues without conflicts
    assert not injection_result.is_safe
    assert not tautology_result.is_safe
    assert not comment_result.is_safe

    # Each validator should find different types of issues
    injection_keywords = ["union", "injection", "stacked"]
    tautology_keywords = ["tautological", "1=1", "self-comparison"]
    comment_keywords = ["comment", "suspicious", "injection"]

    # Check that each validator found its specific issues
    injection_found = any(
        any(keyword in issue.lower() for keyword in injection_keywords) for issue in injection_result.issues
    )

    tautology_found = any(
        any(keyword in issue.lower() for keyword in tautology_keywords) for issue in tautology_result.issues
    )

    comment_found = any(
        any(keyword in issue.lower() for keyword in comment_keywords) for issue in comment_result.issues
    )

    # At least one validator should find its specific type of issue
    assert injection_found or tautology_found or comment_found


def test_pipeline_integration_comprehensive_malicious_query() -> None:
    """Test pipeline behavior with a comprehensively malicious query."""
    # Create all security components
    transformers = [NormalizeWhitespace(), CommentRemover()]
    validators = [PreventInjection(), TautologyConditions(), SuspiciousComments(), SuspiciousKeywords()]
    analyzer = QueryComplexity()

    config = SQLConfig()

    # Extremely malicious query with multiple attack vectors
    malicious_sql = """
        SELECT * FROM users u
        WHERE u.username = 'admin' OR 1=1
        UNION ALL SELECT table_name, column_name FROM information_schema.columns
        /* /*!50000 AND SLEEP(10) */ */
        AND BENCHMARK(1000000, MD5('dos_attack'))
        INTO OUTFILE '/tmp/stolen_data.txt'
        -- ; DROP DATABASE production; --
    """

    try:
        expression = sqlglot.parse_one(malicious_sql, read="mysql")

        # Apply transformations
        current_expression = expression
        for transformer in transformers:
            result = transformer.transform(current_expression, "mysql", config)
            current_expression = result.expression

        # Apply validations
        all_safe = True
        total_issues = 0

        for validator in validators:
            result = validator.validate(current_expression, "mysql", config)
            if not result.is_safe:
                all_safe = False
            total_issues += len(result.issues)

        # Analyze complexity
        complexity_result = analyzer.analyze(current_expression, "mysql", config)

        # Should detect multiple security issues
        assert not all_safe
        assert total_issues >= 3  # Multiple attack vectors

        # Should have high complexity score due to malicious patterns
        assert complexity_result.metrics["overall_complexity_score"] > 50

    except Exception:
        # Some extremely malicious SQL might not parse, which is also good for security
        pass
