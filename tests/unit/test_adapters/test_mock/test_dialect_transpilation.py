"""Unit tests for dialect transpilation in mock driver."""

import pytest

from sqlspec.adapters.mock import MockAsyncConfig, MockSyncConfig


def test_postgres_serial_transpilation() -> None:
    """Test that Postgres SERIAL type is handled correctly."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE serial_test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """
        )
        session.execute("INSERT INTO serial_test (id, name) VALUES ($1, $2)", 1, "Test")

        result = session.select_one("SELECT * FROM serial_test WHERE id = $1", 1)
        assert result is not None
        assert result["name"] == "Test"


def test_postgres_dollar_params() -> None:
    """Test Postgres $1, $2, etc. parameter style."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute("CREATE TABLE dollar_params (a TEXT, b TEXT, c TEXT)")
        session.execute("INSERT INTO dollar_params VALUES ($1, $2, $3)", "x", "y", "z")

        result = session.select_one("SELECT * FROM dollar_params WHERE a = $1 AND b = $2", "x", "y")
        assert result is not None
        assert result["c"] == "z"


def test_mysql_percent_params() -> None:
    """Test MySQL %s parameter style."""
    config = MockSyncConfig(target_dialect="mysql")

    with config.provide_session() as session:
        session.execute("CREATE TABLE percent_params (a TEXT, b TEXT)")
        session.execute("INSERT INTO percent_params VALUES (%s, %s)", "hello", "world")

        result = session.select_one("SELECT * FROM percent_params WHERE a = %s", "hello")
        assert result is not None
        assert result["b"] == "world"


def test_sqlite_qmark_params() -> None:
    """Test SQLite ? parameter style (native, no transpilation)."""
    config = MockSyncConfig(target_dialect="sqlite")

    with config.provide_session() as session:
        session.execute("CREATE TABLE qmark_params (a TEXT, b INTEGER)")
        session.execute("INSERT INTO qmark_params VALUES (?, ?)", "test", 123)

        result = session.select_one("SELECT * FROM qmark_params WHERE a = ?", "test")
        assert result is not None
        assert result["b"] == 123


def test_postgres_boolean_type() -> None:
    """Test that Postgres BOOLEAN type works."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE bool_test (
                id INTEGER PRIMARY KEY,
                active INTEGER
            )
        """
        )
        session.execute("INSERT INTO bool_test VALUES ($1, $2)", 1, 1)
        session.execute("INSERT INTO bool_test VALUES ($1, $2)", 2, 0)

        result = session.select("SELECT * FROM bool_test WHERE active = $1", 1)
        assert len(result) == 1
        assert result[0]["id"] == 1


def test_varchar_type_handling() -> None:
    """Test VARCHAR type is handled across dialects."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE varchar_test (
                short_text VARCHAR(50),
                long_text VARCHAR(255)
            )
        """
        )
        session.execute("INSERT INTO varchar_test VALUES ($1, $2)", "short", "a" * 200)

        result = session.select_one("SELECT * FROM varchar_test")
        assert result is not None
        assert len(result["long_text"]) == 200


def test_numeric_decimal_handling() -> None:
    """Test NUMERIC/DECIMAL type handling."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE numeric_test (
                id INTEGER,
                price NUMERIC(10, 2)
            )
        """
        )
        session.execute("INSERT INTO numeric_test VALUES ($1, $2)", 1, "19.99")

        result = session.select_one("SELECT * FROM numeric_test WHERE id = $1", 1)
        assert result is not None


def test_initial_sql_with_postgres_dialect() -> None:
    """Test initial SQL executed with Postgres dialect transpilation."""
    config = MockSyncConfig(
        target_dialect="postgres",
        initial_sql=[
            "CREATE TABLE init_test (id INTEGER PRIMARY KEY, name VARCHAR(100))",
            "INSERT INTO init_test VALUES (1, 'InitValue')",
        ],
    )

    with config.provide_session() as session:
        result = session.select_one("SELECT * FROM init_test WHERE id = $1", 1)
        assert result is not None
        assert result["name"] == "InitValue"


def test_initial_sql_with_mysql_dialect() -> None:
    """Test initial SQL executed with MySQL dialect transpilation."""
    config = MockSyncConfig(
        target_dialect="mysql",
        initial_sql=["CREATE TABLE mysql_init (id INT, value TEXT)", "INSERT INTO mysql_init VALUES (1, 'MySQLValue')"],
    )

    with config.provide_session() as session:
        result = session.select_one("SELECT * FROM mysql_init WHERE id = %s", 1)
        assert result is not None
        assert result["value"] == "MySQLValue"


@pytest.mark.anyio
async def test_async_postgres_transpilation() -> None:
    """Test async driver with Postgres dialect transpilation."""
    config = MockAsyncConfig(target_dialect="postgres")

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_pg_test (id INTEGER, name TEXT)")
        await session.execute("INSERT INTO async_pg_test VALUES ($1, $2)", 1, "AsyncPG")

        result = await session.select_one("SELECT * FROM async_pg_test WHERE id = $1", 1)
        assert result is not None
        assert result["name"] == "AsyncPG"


@pytest.mark.anyio
async def test_async_initial_sql_transpilation() -> None:
    """Test async driver with initial SQL transpilation."""
    config = MockAsyncConfig(
        target_dialect="postgres",
        initial_sql=[
            "CREATE TABLE async_init (id INTEGER, data TEXT)",
            "INSERT INTO async_init VALUES (1, 'AsyncInit')",
        ],
    )

    async with config.provide_session() as session:
        result = await session.select_one("SELECT * FROM async_init WHERE id = $1", 1)
        assert result is not None
        assert result["data"] == "AsyncInit"


def test_multiple_statements_in_session() -> None:
    """Test multiple SQL statements in the same session."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute("CREATE TABLE multi1 (id INTEGER)")
        session.execute("CREATE TABLE multi2 (id INTEGER)")
        session.execute("INSERT INTO multi1 VALUES ($1)", 1)
        session.execute("INSERT INTO multi2 VALUES ($1)", 2)

        r1 = session.select_value("SELECT id FROM multi1")
        r2 = session.select_value("SELECT id FROM multi2")

        assert r1 == 1
        assert r2 == 2


def test_join_query_transpilation() -> None:
    """Test JOIN queries work with transpilation."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER)")
        session.execute("CREATE TABLE customers (id INTEGER, name TEXT)")
        session.execute("INSERT INTO customers VALUES ($1, $2)", 1, "Alice")
        session.execute("INSERT INTO orders VALUES ($1, $2)", 100, 1)

        result = session.select_one(
            """
            SELECT o.id as order_id, c.name as customer_name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            WHERE o.id = $1
            """,
            100,
        )
        assert result is not None
        assert result["customer_name"] == "Alice"
