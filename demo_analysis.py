#!/usr/bin/env python3
"""Demo of the new SQL Analysis Pipeline System.

This demonstrates how the StatementAnalyzer has been moved from the factory
to a proper pipeline component, making it configurable and extensible.
"""

from sqlspec.statement.pipelines.analyzers import StatementAnalyzer
from sqlspec.statement.sql import SQL, SQLConfig


def demo_basic_analysis() -> None:
    """Demo basic analysis functionality."""

    # Create a standalone analyzer
    analyzer = StatementAnalyzer()

    # Analyze some SQL statements
    sqls = [
        "SELECT * FROM users WHERE age > 18",
        "SELECT u.name, COUNT(*) as order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
        "UPDATE users SET status = 'active' WHERE created_at > '2023-01-01'",
        """
        WITH top_customers AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders
            WHERE created_at >= '2023-01-01'
            GROUP BY customer_id
            ORDER BY total DESC
            LIMIT 10
        )
        SELECT c.name, tc.total
        FROM customers c
        JOIN top_customers tc ON c.id = tc.customer_id
        """,
    ]

    for _i, sql in enumerate(sqls, 1):

        analyzer.analyze_statement(sql)



def demo_pipeline_integration() -> None:
    """Demo analysis integration with SQL statement pipeline."""

    # Configure SQL with analysis enabled
    config = SQLConfig(
        enable_analysis=True,
        enable_validation=False,  # Focus on analysis for this demo
        analysis_cache_size=500,
    )

    # Create SQL statements with analysis enabled
    sqls = [
        "SELECT u.id, u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name",
        "INSERT INTO audit_log (table_name, action, user_id) SELECT 'users', 'update', user_id FROM user_updates",
    ]

    for _i, sql_text in enumerate(sqls, 1):

        # Create SQL statement with analysis config
        stmt = SQL(sql_text, config=config)

        # Get analysis results
        stmt.analyze()


        # The analysis result is cached on the statement


def demo_factory_integration() -> None:
    """Demo how analysis works with the factory methods."""

    from sqlspec import sql

    # The factory methods now use the new StatementAnalyzer internally
    sql.insert("INSERT INTO users (name, email) VALUES ('Jane', 'jane@example.com')")
    sql.select("SELECT name, email FROM users WHERE active = true")



def demo_custom_analysis_pipeline() -> None:
    """Demo creating a custom analysis pipeline."""

    from sqlspec.statement.pipelines import TransformerPipeline

    # Create a custom pipeline with just analysis
    analyzer = StatementAnalyzer(cache_size=100)
    TransformerPipeline(components=[analyzer])

    # Configure SQL to use our custom pipeline
    config = SQLConfig(enable_parsing=True, processing_pipeline_components=[analyzer])

    sql_text = "SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name"
    stmt = SQL(sql_text, config=config)


    # Analysis happens automatically during SQL initialization when analyzer is in pipeline
    stmt.analyze()


if __name__ == "__main__":
    demo_basic_analysis()
    demo_pipeline_integration()
    demo_factory_integration()
    demo_custom_analysis_pipeline()

