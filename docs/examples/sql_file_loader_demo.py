"""SQL File Loader Example.

This example demonstrates how to use the SQL file loader to manage
SQL statements from files with aiosql-style named queries.
"""

import tempfile
from pathlib import Path

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.loader import SQLFileLoader
from sqlspec.statement.sql import SQL

__all__ = (
    "basic_loader_example",
    "caching_example",
    "database_integration_example",
    "main",
    "mixed_source_example",
    "setup_sql_files",
    "storage_backend_example",
)


def setup_sql_files(base_dir: Path) -> None:
    """Create example SQL files for demonstration."""
    sql_dir = base_dir / "sql"
    sql_dir.mkdir(exist_ok=True)

    # User queries file
    (sql_dir / "users.sql").write_text(
        """
-- name: get_user_by_id
SELECT
    id,
    username,
    email,
    created_at
FROM users
WHERE id = :user_id;

-- name: list_active_users
SELECT
    id,
    username,
    email,
    last_login
FROM users
WHERE is_active = true
ORDER BY username
LIMIT :limit OFFSET :offset;

-- name: create_user
INSERT INTO users (username, email, password_hash)
VALUES (:username, :email, :password_hash)
RETURNING id, username, email, created_at;
""".strip()
    )

    # Product queries file
    (sql_dir / "products.sql").write_text(
        """
-- name: search_products
SELECT
    p.id,
    p.name,
    p.description,
    p.price,
    c.name as category
FROM products p
JOIN categories c ON p.category_id = c.id
WHERE p.name ILIKE :search_term
ORDER BY p.name;

-- name: get_product
SELECT * FROM products WHERE id = :product_id;
""".strip()
    )

    # Analytics queries file
    (sql_dir / "analytics.sql").write_text(
        """
-- name: daily_sales
SELECT
    DATE(created_at) as sale_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sales
FROM orders
WHERE created_at >= :start_date
    AND created_at < :end_date
GROUP BY DATE(created_at)
ORDER BY sale_date;

-- name: top_products
SELECT
    p.name,
    COUNT(oi.id) as order_count,
    SUM(oi.quantity) as total_quantity
FROM order_items oi
JOIN products p ON oi.product_id = p.id
GROUP BY p.name
ORDER BY total_quantity DESC
LIMIT 10;
""".strip()
    )


def basic_loader_example() -> None:
    """Demonstrate basic SQL file loader usage."""
    print("=== Basic SQL File Loader Example ===\n")

    # Create SQL files in a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        # Initialize loader
        loader = SQLFileLoader()

        # Load SQL files
        sql_dir = base_dir / "sql"
        loader.load_sql(sql_dir / "users.sql", sql_dir / "products.sql", sql_dir / "analytics.sql")

        # List available queries
        queries = loader.list_queries()
        print(f"Available queries: {', '.join(queries)}\n")

        # Get SQL by query name
        user_sql = loader.get_sql("get_user_by_id", user_id=123)
        print(f"SQL object created with parameters: {user_sql.parameters}")
        print(f"SQL content: {str(user_sql)[:50]}...\n")

        # Add a query directly
        loader.add_named_sql("custom_health_check", "SELECT 'OK' as status, NOW() as timestamp")

        # Get the custom query
        health_sql = loader.get_sql("custom_health_check")
        print(f"Custom query added: {str(health_sql)}\n")

        # Get file info for a query
        file_info = loader.get_file_for_query("get_user_by_id")
        if file_info:
            print(f"Query 'get_user_by_id' is from file: {file_info.path}")
            print(f"File checksum: {file_info.checksum}\n")


def caching_example() -> None:
    """Demonstrate caching behavior."""
    print("=== Caching Example ===\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        # Create loader
        loader = SQLFileLoader()
        sql_file = base_dir / "sql" / "users.sql"

        # First load - reads from disk
        print("First load (from disk)...")
        loader.load_sql(sql_file)
        file1 = loader.get_file(str(sql_file))

        # Second load - uses cache (file already loaded)
        print("Second load (from cache)...")
        loader.load_sql(sql_file)
        file2 = loader.get_file(str(sql_file))

        print(f"Same file object from cache: {file1 is file2}")

        # Clear cache and reload
        print("\nClearing cache...")
        loader.clear_cache()
        print("Cache cleared")

        # After clearing, queries are gone
        print(f"Queries after clear: {loader.list_queries()}")

        # Reload the file
        loader.load_sql(sql_file)
        print(f"Queries after reload: {len(loader.list_queries())} queries loaded\n")


def database_integration_example() -> None:
    """Demonstrate using loaded SQL files with SQLSpec database connections."""
    print("=== Database Integration Example ===\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        # Initialize SQLSpec and register database
        sqlspec = SQLSpec()
        config = SqliteConfig(database=":memory:")
        sqlspec.add_config(config)

        # Initialize loader and load SQL files
        loader = SQLFileLoader()
        loader.load_sql(base_dir / "sql" / "users.sql")

        # Create tables
        with sqlspec.provide_session(type(config)) as session:
            # Create users table
            session.execute(
                SQL("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    email TEXT NOT NULL,
                    password_hash TEXT,
                    is_active BOOLEAN DEFAULT true,
                    last_login TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            )

            # Insert test data
            session.execute(
                SQL("""
                INSERT INTO users (username, email, is_active)
                VALUES
                    ('alice', 'alice@example.com', true),
                    ('bob', 'bob@example.com', true),
                    ('charlie', 'charlie@example.com', false)
            """)
            )

            # Get and execute a query
            get_user_sql = loader.get_sql("get_user_by_id", user_id=1)

            result = session.execute(get_user_sql)
            print("Get user by ID result:")
            for row in result.data:
                print(f"  - {row['username']} ({row['email']})")

            # Execute another query
            list_users_sql = loader.get_sql("list_active_users", limit=10, offset=0)

            result = session.execute(list_users_sql)
            print("\nActive users:")
            for row in result.data:
                print(f"  - {row['username']} (last login: {row['last_login'] or 'Never'})")


def mixed_source_example() -> None:
    """Demonstrate mixing file-loaded and directly-added queries."""
    print("=== Mixed Source Example ===\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        # Initialize loader
        loader = SQLFileLoader()

        # Load from files
        loader.load_sql(base_dir / "sql" / "users.sql")
        print(f"Loaded queries from file: {', '.join(loader.list_queries())}")

        # Add runtime queries
        loader.add_named_sql("health_check", "SELECT 'OK' as status")
        loader.add_named_sql("version_check", "SELECT version()")
        loader.add_named_sql(
            "table_count",
            """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """,
        )

        print(f"\nAll queries after adding runtime SQL: {', '.join(loader.list_queries())}")

        # Show source of queries
        print("\nQuery sources:")
        for query in ["get_user_by_id", "health_check", "version_check"]:
            source_file = loader.get_file_for_query(query)
            if source_file:
                print(f"  - {query}: from file {source_file.path}")
            else:
                print(f"  - {query}: directly added")


def storage_backend_example() -> None:
    """Demonstrate loading from different storage backends."""
    print("=== Storage Backend Example ===\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)

        # Create a SQL file with queries
        sql_file = base_dir / "queries.sql"
        sql_file.write_text(
            """
-- name: count_records
SELECT COUNT(*) as total FROM :table_name;

-- name: find_by_status
SELECT * FROM records WHERE status = :status;

-- name: update_timestamp
UPDATE records SET updated_at = NOW() WHERE id = :record_id;
""".strip()
        )

        # Initialize loader
        loader = SQLFileLoader()

        # Load from local file path
        print("Loading from local file path:")
        loader.load_sql(sql_file)
        print(f"Loaded queries: {', '.join(loader.list_queries())}")

        # You can also load from URIs (if storage backend is configured)
        # Example with file:// URI
        file_uri = f"file://{sql_file}"
        loader2 = SQLFileLoader()
        loader2.load_sql(file_uri)
        print(f"\nLoaded from file URI: {', '.join(loader2.list_queries())}")

        # Get SQL with parameters
        count_sql = loader.get_sql("count_records", table_name="users")
        print(f"\nGenerated SQL: {str(count_sql)}")
        print(f"Parameters: {count_sql.parameters}")


def main() -> None:
    """Run all examples."""
    basic_loader_example()
    print("\n" + "=" * 50 + "\n")

    caching_example()
    print("\n" + "=" * 50 + "\n")

    mixed_source_example()
    print("\n" + "=" * 50 + "\n")

    storage_backend_example()
    print("\n" + "=" * 50 + "\n")

    # Run database integration example
    database_integration_example()

    print("\nExamples completed!")


if __name__ == "__main__":
    main()
