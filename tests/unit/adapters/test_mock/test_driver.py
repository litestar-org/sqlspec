"""Unit tests for mock driver."""

import pytest

from sqlspec.adapters.mock import MockAsyncConfig, MockSyncConfig


def test_mock_sync_driver_basic_operations() -> None:
    """Test basic sync driver operations."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice")

        result = session.select("SELECT * FROM users")
        assert len(result) == 1
        assert result[0]["name"] == "Alice"


def test_mock_sync_driver_with_initial_sql() -> None:
    """Test sync driver with initial SQL setup."""
    config = MockSyncConfig(
        initial_sql=[
            "CREATE TABLE items (id INTEGER, name TEXT)",
            "INSERT INTO items VALUES (1, 'Widget')",
            "INSERT INTO items VALUES (2, 'Gadget')",
        ]
    )

    with config.provide_session() as session:
        result = session.select("SELECT * FROM items ORDER BY id")
        assert len(result) == 2
        assert result[0]["name"] == "Widget"
        assert result[1]["name"] == "Gadget"


def test_mock_sync_driver_postgres_dialect() -> None:
    """Test sync driver with Postgres dialect transpilation."""
    config = MockSyncConfig(target_dialect="postgres")

    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                price NUMERIC(10, 2)
            )
        """
        )
        session.execute("INSERT INTO products (id, name, price) VALUES ($1, $2, $3)", 1, "Widget", 19.99)

        result = session.select_one("SELECT * FROM products WHERE id = $1", 1)
        assert result is not None
        assert result["name"] == "Widget"


def test_mock_sync_driver_mysql_dialect() -> None:
    """Test sync driver with MySQL dialect transpilation."""
    config = MockSyncConfig(target_dialect="mysql")

    with config.provide_session() as session:
        session.execute(
            """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer TEXT,
                total DECIMAL(10, 2)
            )
        """
        )
        session.execute("INSERT INTO orders (id, customer, total) VALUES (%s, %s, %s)", 1, "Bob", 99.99)

        result = session.select_one("SELECT * FROM orders WHERE customer = %s", "Bob")
        assert result is not None
        assert result["id"] == 1


def test_mock_sync_driver_select_value() -> None:
    """Test select_value and select_value_or_none."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE counts (n INTEGER)")
        session.execute("INSERT INTO counts VALUES (42)")

        value = session.select_value("SELECT n FROM counts")
        assert value == 42

        none_value = session.select_value_or_none("SELECT n FROM counts WHERE n = ?", 999)
        assert none_value is None


def test_mock_sync_driver_execute_many() -> None:
    """Test execute_many for batch inserts."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE batch (id INTEGER, value TEXT)")
        session.execute_many("INSERT INTO batch VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])

        result = session.select("SELECT * FROM batch ORDER BY id")
        assert len(result) == 3


def test_mock_sync_driver_transaction_commit() -> None:
    """Test transaction commit."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE tx_test (id INTEGER)")
        session.begin()
        session.execute("INSERT INTO tx_test VALUES (1)")
        session.commit()

        result = session.select("SELECT * FROM tx_test")
        assert len(result) == 1


def test_mock_sync_driver_transaction_rollback() -> None:
    """Test transaction rollback."""
    config = MockSyncConfig()

    with config.provide_session() as session:
        session.execute("CREATE TABLE rb_test (id INTEGER)")
        session.execute("INSERT INTO rb_test VALUES (1)")
        session.commit()

        session.begin()
        session.execute("INSERT INTO rb_test VALUES (2)")
        session.rollback()

        result = session.select("SELECT * FROM rb_test")
        assert len(result) == 1


@pytest.mark.anyio
async def test_mock_async_driver_basic_operations() -> None:
    """Test basic async driver operations."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_users (id INTEGER PRIMARY KEY, name TEXT)")
        await session.execute("INSERT INTO async_users (id, name) VALUES (?, ?)", 1, "Charlie")

        result = await session.select("SELECT * FROM async_users")
        assert len(result) == 1
        assert result[0]["name"] == "Charlie"


@pytest.mark.anyio
async def test_mock_async_driver_with_initial_sql() -> None:
    """Test async driver with initial SQL setup."""
    config = MockAsyncConfig(
        initial_sql=[
            "CREATE TABLE async_items (id INTEGER, name TEXT)",
            "INSERT INTO async_items VALUES (1, 'AsyncWidget')",
        ]
    )

    async with config.provide_session() as session:
        result = await session.select("SELECT * FROM async_items")
        assert len(result) == 1
        assert result[0]["name"] == "AsyncWidget"


@pytest.mark.anyio
async def test_mock_async_driver_postgres_dialect() -> None:
    """Test async driver with Postgres dialect transpilation."""
    config = MockAsyncConfig(target_dialect="postgres")

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_products (id INTEGER PRIMARY KEY, name TEXT)")
        await session.execute("INSERT INTO async_products (id, name) VALUES ($1, $2)", 1, "AsyncProduct")

        result = await session.select_one("SELECT * FROM async_products WHERE id = $1", 1)
        assert result is not None
        assert result["name"] == "AsyncProduct"


@pytest.mark.anyio
async def test_mock_async_driver_transaction_operations() -> None:
    """Test async transaction operations."""
    config = MockAsyncConfig()

    async with config.provide_session() as session:
        await session.execute("CREATE TABLE async_tx (id INTEGER)")
        await session.begin()
        await session.execute("INSERT INTO async_tx VALUES (1)")
        await session.commit()

        result = await session.select("SELECT * FROM async_tx")
        assert len(result) == 1


def test_mock_config_target_dialect_property() -> None:
    """Test target_dialect property."""
    config = MockSyncConfig(target_dialect="postgres")
    assert config.target_dialect == "postgres"

    config2 = MockSyncConfig()
    assert config2.target_dialect == "sqlite"


def test_mock_config_initial_sql_property() -> None:
    """Test initial_sql property."""
    sql = ["CREATE TABLE test (id INTEGER)"]
    config = MockSyncConfig(initial_sql=sql)
    assert config.initial_sql == sql

    config2 = MockSyncConfig()
    assert config2.initial_sql is None


def test_mock_config_signature_namespace() -> None:
    """Test that signature namespace contains expected types."""
    config = MockSyncConfig()
    namespace = config.get_signature_namespace()

    assert "MockSyncDriver" in namespace
    assert "MockConnection" in namespace
    assert "MockSyncConfig" in namespace


def test_mock_async_config_signature_namespace() -> None:
    """Test that async signature namespace contains expected types."""
    config = MockAsyncConfig()
    namespace = config.get_signature_namespace()

    assert "MockAsyncDriver" in namespace
    assert "MockConnection" in namespace
    assert "MockAsyncConfig" in namespace
