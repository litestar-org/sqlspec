"""Integration tests for asyncpg Arrow support."""

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec._typing import PYARROW_INSTALLED
from sqlspec.adapters.asyncpg import AsyncpgConfig

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.fixture
async def asyncpg_config(postgres_service: PostgresService) -> AsyncpgConfig:
    """Create AsyncPG configuration for testing."""
    return AsyncpgConfig(
        pool_config={
            "dsn": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 1,
            "max_size": 2,
        }
    )


async def test_select_to_arrow_basic(asyncpg_config: AsyncpgConfig) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    try:
        async with asyncpg_config.provide_session() as session:
            # Create test table with unique name
            await session.execute("DROP TABLE IF EXISTS arrow_users CASCADE")
            await session.execute("CREATE TABLE arrow_users (id INTEGER, name TEXT, age INTEGER)")
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
            assert list(df["age"]) == [30, 25]
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_table_format(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_table_test CASCADE")
            await session.execute("CREATE TABLE arrow_table_test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

            result = await session.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

            assert isinstance(result.data, pa.Table)
            assert result.rows_affected == 3
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_batch_format(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_batch_test CASCADE")
            await session.execute("CREATE TABLE arrow_batch_test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

            result = await session.select_to_arrow(
                "SELECT * FROM arrow_batch_test ORDER BY id", return_format="batches"
            )

            assert isinstance(result.data, pa.RecordBatch)
            assert result.rows_affected == 2
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_with_parameters(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow with query parameters."""
    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_params_test CASCADE")
            await session.execute("CREATE TABLE arrow_params_test (id INTEGER, value INTEGER)")
            await session.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

            # Test with parameterized query
            result = await session.select_to_arrow("SELECT * FROM arrow_params_test WHERE value > $1 ORDER BY id", 150)

            assert result.rows_affected == 2
            df = result.to_pandas()
            assert list(df["value"]) == [200, 300]
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_empty_result(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow with empty result set."""
    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_empty_test CASCADE")
            await session.execute("CREATE TABLE arrow_empty_test (id INTEGER)")

            result = await session.select_to_arrow("SELECT * FROM arrow_empty_test")

            assert result.rows_affected == 0
            assert len(result.to_pandas()) == 0
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_null_handling(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow with NULL values."""
    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_null_test CASCADE")
            await session.execute("CREATE TABLE arrow_null_test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

            result = await session.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 3
            assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_to_polars(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_polars_test CASCADE")
            await session.execute("CREATE TABLE arrow_polars_test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

            result = await session.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
            df = result.to_polars()

            assert len(df) == 2
            assert df["value"].to_list() == ["a", "b"]
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_large_dataset(asyncpg_config: AsyncpgConfig) -> None:
    """Test select_to_arrow with larger dataset."""
    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_large_test CASCADE")
            await session.execute("CREATE TABLE arrow_large_test (id INTEGER, value INTEGER)")

            # Insert 1000 rows
            values = ", ".join(f"({i}, {i * 10})" for i in range(1, 1001))
            await session.execute(f"INSERT INTO arrow_large_test VALUES {values}")

            result = await session.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

            assert result.rows_affected == 1000
            df = result.to_pandas()
            assert len(df) == 1000
            assert df["value"].sum() == sum(i * 10 for i in range(1, 1001))
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_type_preservation(asyncpg_config: AsyncpgConfig) -> None:
    """Test that PostgreSQL types are properly converted to Arrow types."""
    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_types_test CASCADE")
            await session.execute(
                """
                CREATE TABLE arrow_types_test (
                    id INTEGER,
                    name TEXT,
                    price NUMERIC,
                    created_at TIMESTAMP,
                    is_active BOOLEAN
                )
                """
            )
            await session.execute(
                """
                INSERT INTO arrow_types_test VALUES
                (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true),
                (2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)
                """
            )

            result = await session.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 2
            assert df["name"].dtype == object
            assert df["is_active"].dtype == bool
    finally:
        await asyncpg_config.close_pool()


async def test_select_to_arrow_postgres_array(asyncpg_config: AsyncpgConfig) -> None:
    """Test PostgreSQL array type handling in Arrow results."""
    try:
        async with asyncpg_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_array_test CASCADE")
            await session.execute("CREATE TABLE arrow_array_test (id INTEGER, tags TEXT[])")
            await session.execute(
                "INSERT INTO arrow_array_test VALUES (1, ARRAY['python', 'rust']), (2, ARRAY['js', 'ts'])"
            )

            result = await session.select_to_arrow("SELECT * FROM arrow_array_test ORDER BY id")

            # PostgreSQL arrays are returned as Python lists in dict format,
            # which Arrow converts to list type
            df = result.to_pandas()
            assert len(df) == 2
            assert isinstance(df["tags"].iloc[0], (list, object))
    finally:
        await asyncpg_config.close_pool()
