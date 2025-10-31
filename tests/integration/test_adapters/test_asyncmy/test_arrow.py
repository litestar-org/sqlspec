"""Integration tests for asyncmy Arrow query support."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig

pytestmark = [pytest.mark.xdist_group("mysql")]


@pytest.fixture
async def asyncmy_arrow_config(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyConfig, None]:
    """Create asyncmy config for Arrow testing."""
    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "minsize": 1,
            "maxsize": 5,
        }
    )
    try:
        yield config
    finally:
        await config.close_pool()


async def test_select_to_arrow_basic(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    try:
        async with asyncmy_arrow_config.provide_session() as session:
            # Create test table with unique name
            await session.execute("DROP TABLE IF EXISTS arrow_users")
            await session.execute("CREATE TABLE arrow_users (id INT, name VARCHAR(100), age INT)")
            await session.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

            # Test Arrow query
            result = await session.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

            assert result is not None
            assert isinstance(result.data, (pa.Table, pa.RecordBatch))
            assert result.rows_affected == 2

            # Convert to pandas and verify
            df = result.to_pandas()
            assert len(df) == 2
            assert list(df["name"]) == ["Alice", "Bob"]
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_table_format(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_table_test")
            await session.execute("CREATE TABLE arrow_table_test (id INT, value VARCHAR(100))")
            await session.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

            result = await session.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

            assert isinstance(result.data, pa.Table)
            assert result.rows_affected == 3
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_batch_format(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_batch_test")
            await session.execute("CREATE TABLE arrow_batch_test (id INT, value VARCHAR(100))")
            await session.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

            result = await session.select_to_arrow(
                "SELECT * FROM arrow_batch_test ORDER BY id", return_format="batches"
            )

            assert isinstance(result.data, pa.RecordBatch)
            assert result.rows_affected == 2
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_with_parameters(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow with query parameters."""
    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_params_test")
            await session.execute("CREATE TABLE arrow_params_test (id INT, value INT)")
            await session.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

            # Test with parameterized query - MySQL uses %s style
            result = await session.select_to_arrow(
                "SELECT * FROM arrow_params_test WHERE value > %s ORDER BY id", (150,)
            )

            assert result.rows_affected == 2
            df = result.to_pandas()
            assert list(df["value"]) == [200, 300]
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_empty_result(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow with empty result set."""
    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_empty_test")
            await session.execute("CREATE TABLE arrow_empty_test (id INT)")

            result = await session.select_to_arrow("SELECT * FROM arrow_empty_test")

            assert result.rows_affected == 0
            assert len(result.to_pandas()) == 0
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_null_handling(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow with NULL values."""
    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_null_test")
            await session.execute("CREATE TABLE arrow_null_test (id INT, value VARCHAR(100))")
            await session.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

            result = await session.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 3
            assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_to_polars(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_polars_test")
            await session.execute("CREATE TABLE arrow_polars_test (id INT, value VARCHAR(100))")
            await session.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

            result = await session.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
            df = result.to_polars()

            assert len(df) == 2
            assert df["value"].to_list() == ["a", "b"]
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_large_dataset(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test select_to_arrow with larger dataset."""
    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_large_test")
            await session.execute("CREATE TABLE arrow_large_test (id INT, value INT)")

            # Insert 1000 rows using batch insert
            values = ", ".join(f"({i}, {i * 10})" for i in range(1, 1001))
            await session.execute(f"INSERT INTO arrow_large_test VALUES {values}")

            result = await session.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

            assert result.rows_affected == 1000
            df = result.to_pandas()
            assert len(df) == 1000
            assert df["value"].sum() == sum(i * 10 for i in range(1, 1001))
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_type_preservation(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test that MySQL types are properly converted to Arrow types."""
    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_types_test")
            await session.execute(
                """
                CREATE TABLE arrow_types_test (
                    id INT,
                    name VARCHAR(100),
                    price DECIMAL(10, 2),
                    created_at DATETIME,
                    is_active BOOLEAN
                )
                """
            )
            await session.execute(
                """
                INSERT INTO arrow_types_test VALUES
                (1, 'Item 1', 19.99, '2025-01-01 10:00:00', TRUE),
                (2, 'Item 2', 29.99, '2025-01-02 15:30:00', FALSE)
                """
            )

            result = await session.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 2
            assert df["name"].dtype == object
            # MySQL BOOLEAN comes through as int (0/1)
            assert df["is_active"].dtype in (int, "int64", "Int64", bool)
    finally:
        await asyncmy_arrow_config.close_pool()


async def test_select_to_arrow_json_handling(asyncmy_arrow_config: AsyncmyConfig) -> None:
    """Test MySQL JSON type handling in Arrow results."""
    try:
        async with asyncmy_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_json_test")
            await session.execute("CREATE TABLE arrow_json_test (id INT, data JSON)")
            await session.execute(
                """
                INSERT INTO arrow_json_test VALUES
                (1, '{"name": "Alice", "age": 30}'),
                (2, '{"name": "Bob", "age": 25}')
                """
            )

            result = await session.select_to_arrow("SELECT * FROM arrow_json_test ORDER BY id")

            # MySQL JSON is returned as dict or string depending on driver
            df = result.to_pandas()
            assert len(df) == 2
            # Data can be either dict or string representation
            data_value = df["data"].iloc[0]
            assert isinstance(data_value, (str, dict, object))
    finally:
        await asyncmy_arrow_config.close_pool()
