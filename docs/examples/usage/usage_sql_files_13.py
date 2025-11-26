from pathlib import Path

__all__ = ("test_analytics_queries_example", )


def test_analytics_queries_example(tmp_path: Path) -> None:
    from docs.examples.usage.usage_sql_files_1 import create_loader
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    loader, _queries = create_loader(tmp_path)
    sql_analytics_path = tmp_path / "sql"
    sql_analytics_path.mkdir(parents=True, exist_ok=True)
    sql_analytics_file = sql_analytics_path / "analytics.sql"
    sql_analytics_file.write_text(
        """-- name: daily_sales
        SELECT order_date, SUM(total_amount) AS total_sales
        FROM orders
        WHERE order_date BETWEEN :start_date AND :end_date
        GROUP BY order_date;
        -- name: top_products
        SELECT product_id, SUM(quantity) AS total_sold
        FROM order_items
        WHERE order_date >= :start_date
        GROUP BY product_id
        ORDER BY total_sold DESC
        LIMIT :limit;
        """
    )
    # start-example
    import datetime

    # Load analytics queries
    loader.load_sql(tmp_path / "sql/analytics.sql")

    # Run daily sales report
    sales_query = loader.get_sql("daily_sales")
    config = SqliteConfig()
    spec = SQLSpec()
    spec.add_config(config)
    with spec.provide_session(config) as session:
        session.execute("""CREATE TABLE orders ( order_id INTEGER PRIMARY KEY, order_date DATE, total_amount REAL);""")
        session.execute("""
               CREATE TABLE order_items ( order_item_id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, order_date DATE);""")

        # Insert sample data
        session.execute("""
            INSERT INTO orders (order_id, order_date, total_amount) VALUES
            (1, '2025-01-05', 150.00),
            (2, '2025-01-15', 200.00),
            (3, '2025-01-20', 250.00);
        """)
        session.execute("""
            INSERT INTO order_items (order_item_id, order_id, product_id, quantity, order_date) VALUES
            (1, 1, 101, 2, '2025-01-05'),
            (2, 2, 102, 3, '2025-01-15'),
            (3, 3, 101, 1, '2025-01-20');
        """)
        session.execute(sales_query, start_date=datetime.date(2025, 1, 1), end_date=datetime.date(2025, 2, 1)).data

        # Top products
        products_query = loader.get_sql("top_products")
        session.execute(products_query, start_date=datetime.date(2025, 1, 1), limit=10).data
        # end-example
