# /// script
# dependencies = [
#   "sqlspec[sqlite]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///

"""SQL File Loader Example.

This example demonstrates how to use SQLSpec's integrated SQL file loader
to manage SQL statements from files with aiosql-style named queries.
"""

import tempfile
from pathlib import Path

from rich import print

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core.statement import SQL

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
    print("[bold cyan]=== Basic SQL File Loader Example ===[/bold cyan]\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        spec = SQLSpec()

        sql_dir = base_dir / "sql"
        spec.load_sql_files(sql_dir / "users.sql", sql_dir / "products.sql", sql_dir / "analytics.sql")

        queries = spec.list_sql_queries()
        print(f"[green]Available queries:[/green] {', '.join(queries)}\n")

        user_sql = spec.get_sql("get_user_by_id")
        print(f"[yellow]SQL object created:[/yellow] {user_sql.sql[:50]}...\n")

        spec.add_named_sql("custom_health_check", "SELECT 'OK' as status")

        health_sql = spec.get_sql("custom_health_check")
        print(f"[green]Custom query added:[/green] {health_sql!s}\n")

        files = spec.get_sql_files()
        if files:
            print(f"[magenta]Loaded files:[/magenta] {len(files)} SQL files")


def caching_example() -> None:
    """Demonstrate caching behavior."""
    print("[bold cyan]=== Caching Example ===[/bold cyan]\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        spec = SQLSpec()
        sql_file = base_dir / "sql" / "users.sql"

        print("[yellow]First load (from disk)...[/yellow]")
        spec.load_sql_files(sql_file)
        queries_before = len(spec.list_sql_queries())
        print(f"[green]Loaded {queries_before} queries[/green]")

        print("\n[yellow]Second load (from cache)...[/yellow]")
        spec.load_sql_files(sql_file)
        print("[green]Using cached data[/green]")

        print("\n[yellow]Clearing cache...[/yellow]")
        spec.clear_sql_cache()
        print("[green]Cache cleared[/green]")

        print(f"[magenta]Queries after clear:[/magenta] {len(spec.list_sql_queries())}")

        spec.load_sql_files(sql_file)
        print(f"[green]Queries after reload:[/green] {len(spec.list_sql_queries())} queries loaded\n")


def database_integration_example() -> None:
    """Demonstrate using loaded SQL files with SQLSpec database connections."""
    print("[bold cyan]=== Database Integration Example ===[/bold cyan]\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        spec = SQLSpec()
        db = spec.add_config(SqliteConfig())

        spec.load_sql_files(base_dir / "sql" / "users.sql")

        with spec.provide_session(db) as session:
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

            session.execute(
                SQL("""
                INSERT INTO users (username, email, is_active)
                VALUES
                    ('alice', 'alice@example.com', true),
                    ('bob', 'bob@example.com', true),
                    ('charlie', 'charlie@example.com', false)
            """)
            )

            result = session.execute(spec.get_sql("get_user_by_id"), user_id=1)
            print("[green]Get user by ID result:[/green]")
            for row in result.data:
                print(f"  [yellow]-[/yellow] {row['username']} ({row['email']})")

            result = session.execute(spec.get_sql("list_active_users"), limit=10, offset=0)
            print("\n[green]Active users:[/green]")
            for row in result.data:
                print(f"  [yellow]-[/yellow] {row['username']} (last login: {row['last_login'] or 'Never'})")


def mixed_source_example() -> None:
    """Demonstrate mixing file-loaded and directly-added queries."""
    print("[bold cyan]=== Mixed Source Example ===[/bold cyan]\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        setup_sql_files(base_dir)

        spec = SQLSpec()

        spec.load_sql_files(base_dir / "sql" / "users.sql")
        print(f"[green]Loaded queries from file:[/green] {', '.join(spec.list_sql_queries())}")

        spec.add_named_sql("health_check", "SELECT 'OK' as status")
        spec.add_named_sql("version_check", "SELECT sqlite_version()")
        spec.add_named_sql(
            "table_count",
            """
            SELECT COUNT(*) as count
            FROM sqlite_master
            WHERE type = 'table'
        """,
        )

        print(f"\n[green]All queries after adding runtime SQL:[/green] {', '.join(spec.list_sql_queries())}")

        print("\n[magenta]Query check:[/magenta]")
        for query in ["get_user_by_id", "health_check", "version_check"]:
            exists = spec.has_sql_query(query)
            status = "[green]exists[/green]" if exists else "[red]not found[/red]"
            print(f"  [yellow]-[/yellow] {query}: {status}")


def storage_backend_example() -> None:
    """Demonstrate loading from different storage backends."""
    print("[bold cyan]=== Storage Backend Example ===[/bold cyan]\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)

        sql_file = base_dir / "queries.sql"
        sql_file.write_text(
            """
-- name: count_records
SELECT COUNT(*) as total FROM sqlite_master;

-- name: find_by_status
SELECT * FROM records WHERE status = :status;

-- name: update_timestamp
UPDATE records SET updated_at = datetime('now') WHERE id = :record_id;
""".strip()
        )

        spec = SQLSpec()

        print("[yellow]Loading from local file path:[/yellow]")
        spec.load_sql_files(sql_file)
        print(f"[green]Loaded queries:[/green] {', '.join(spec.list_sql_queries())}")

        file_uri = f"file://{sql_file}"
        spec2 = SQLSpec()
        spec2.load_sql_files(file_uri)
        print(f"\n[green]Loaded from file URI:[/green] {', '.join(spec2.list_sql_queries())}")

        count_sql = spec.get_sql("count_records")
        print(f"\n[yellow]Generated SQL:[/yellow] {count_sql!s}")
        print(f"[magenta]Dialect:[/magenta] {count_sql.dialect or 'default'}")


def main() -> None:
    """Run all examples."""
    print("[bold blue]SQLSpec SQL File Loader Demo[/bold blue]\n")

    basic_loader_example()
    print("\n" + "[dim]" + "=" * 50 + "[/dim]\n")

    caching_example()
    print("\n" + "[dim]" + "=" * 50 + "[/dim]\n")

    mixed_source_example()
    print("\n" + "[dim]" + "=" * 50 + "[/dim]\n")

    storage_backend_example()
    print("\n" + "[dim]" + "=" * 50 + "[/dim]\n")

    database_integration_example()

    print("\n[bold green]✅ Examples completed![/bold green]")


if __name__ == "__main__":
    main()
