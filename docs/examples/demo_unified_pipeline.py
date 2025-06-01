#!/usr/bin/env python3
"""Demonstration of the new UnifiedProcessor for efficient SQL analysis.

This script shows how the UnifiedProcessor combines analysis, transformation,
and validation in a single pass to avoid redundant parsing and processing.
"""

import time
from typing import Any

import sqlglot as sg

from sqlspec.statement.pipelines import (
    StatementAnalyzer,
    TransformerPipeline,
    UnifiedProcessor,
)
from sqlspec.statement.pipelines.transformers import (
    CommentRemover,
)
from sqlspec.statement.pipelines.validators import (
    CartesianProductDetector,
    ExcessiveJoins,
    PreventDDL,
    SuspiciousKeywords,
)
from sqlspec.statement.sql import SQLConfig

__all__ = ("compare_approaches", "demo_complex_query", "demo_new_approach", "demo_old_approach")


def demo_complex_query() -> str:
    """Return a complex SQL query for testing."""
    return """
    /* Complex query with multiple joins and subqueries */
    SELECT
        u.user_id,
        u.username,
        p.profile_data,
        o.order_count,
        r.avg_rating,
        -- Calculate total spent
        COALESCE(SUM(oi.quantity * oi.price), 0) as total_spent
    FROM users u
    LEFT JOIN user_profiles p ON u.user_id = p.user_id
    LEFT JOIN (
        SELECT
            user_id,
            COUNT(*) as order_count
        FROM orders
        WHERE status = 'completed'
        GROUP BY user_id
    ) o ON u.user_id = o.user_id
    LEFT JOIN (
        SELECT
            user_id,
            AVG(rating) as avg_rating
        FROM reviews r1
        WHERE EXISTS (
            SELECT 1 FROM orders o1
            WHERE o1.user_id = r1.user_id
            AND o1.order_date > '2023-01-01'
        )
        GROUP BY user_id
    ) r ON u.user_id = r.user_id
    LEFT JOIN order_items oi ON EXISTS (
        SELECT 1 FROM orders o2
        WHERE o2.user_id = u.user_id
        AND o2.order_id = oi.order_id
    )
    WHERE u.created_date > '2022-01-01'
    AND u.status = 'active'
    GROUP BY u.user_id, u.username, p.profile_data, o.order_count, r.avg_rating
    HAVING COUNT(DISTINCT oi.order_id) > 0
    ORDER BY total_spent DESC
    LIMIT 100;
    """


def demo_old_approach(sql: str, config: SQLConfig) -> dict[str, Any]:
    """Demonstrate the old approach with separate components."""

    start_time = time.time()
    results = {}

    # Parse the SQL multiple times (inefficient)
    expression = sg.parse_one(sql)

    # Step 1: Analysis
    analyzer = StatementAnalyzer()
    analysis_result = analyzer.analyze_expression(expression)
    results["analysis"] = analysis_result

    # Step 2: Transformations (each may re-parse)
    transformer_pipeline = TransformerPipeline(
        [
            CommentRemover(),
            # Note: HintRemover implements ProcessorProtocol directly, not SQLTransformer
        ]
    )
    transformed_expr, _ = transformer_pipeline.execute(expression, config=config)
    results["transformed_sql"] = transformed_expr.sql()

    # Step 3: Validations (each may re-analyze)
    validators = [
        ExcessiveJoins(max_joins=8, warn_threshold=5),
        CartesianProductDetector(),
        PreventDDL(),
        SuspiciousKeywords(),
    ]

    validation_issues = []
    validation_warnings = []

    for validator in validators:
        validation_result = validator.validate(transformed_expr, None, config)
        validation_issues.extend(validation_result.issues)
        validation_warnings.extend(validation_result.warnings)

    results["validation_issues"] = validation_issues
    results["validation_warnings"] = validation_warnings

    end_time = time.time()
    results["processing_time"] = end_time - start_time

    return results


def demo_new_approach(sql: str, config: SQLConfig) -> dict[str, Any]:
    """Demonstrate the new unified approach."""

    start_time = time.time()

    # Parse once, process once
    expression = sg.parse_one(sql)

    # Create unified processor with all components
    unified_processor = UnifiedProcessor(
        analyzers=[],  # Built-in analysis
        transformers=[
            CommentRemover(),
            # Note: HintRemover implements ProcessorProtocol directly, not SQLTransformer
        ],
        validators=[
            ExcessiveJoins(max_joins=8, warn_threshold=5),
            CartesianProductDetector(),
            PreventDDL(),
            SuspiciousKeywords(),
        ],
        cache_analysis=True,
    )

    # Single processing pass
    transformed_expr, validation_result = unified_processor.process(expression, dialect=None, config=config)

    end_time = time.time()
    processing_time = end_time - start_time

    # Access the internal analysis results
    analysis_cache = unified_processor._analysis_cache
    analysis_key = expression.sql()
    analysis_result = analysis_cache.get(analysis_key)

    validation_issues = validation_result.issues if validation_result else []
    validation_warnings = validation_result.warnings if validation_result else []

    results = {
        "transformed_sql": transformed_expr.sql(),
        "validation_issues": validation_issues,
        "validation_warnings": validation_warnings,
        "processing_time": processing_time,
        "analysis_metrics": analysis_result.metrics if analysis_result else {},
    }

    if analysis_result:
        analysis_result.metrics.get("complexity_score", 0)

    return results


def compare_approaches() -> None:
    """Compare the old and new approaches."""

    sql = demo_complex_query()
    config = SQLConfig(enable_validation=True, enable_transformations=True)

    # Run old approach
    old_results = demo_old_approach(sql, config)

    # Run new approach
    new_results = demo_new_approach(sql, config)

    # Compare results

    old_time = old_results["processing_time"]
    new_time = new_results["processing_time"]
    old_time / new_time if new_time > 0 else float("inf")

    # Verify results are equivalent

    # Show some analysis metrics from the new approach
    if new_results["analysis_metrics"]:
        metrics = new_results["analysis_metrics"]
        for value in metrics.values():
            if isinstance(value, (int, float, str, bool)):
                pass


if __name__ == "__main__":
    compare_approaches()
