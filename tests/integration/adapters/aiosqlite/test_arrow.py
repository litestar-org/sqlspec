"""Integration tests for aiosqlite Arrow query support."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture
async def aiosqlite_arrow_config() -> AsyncGenerator[AiosqliteConfig, None]:
    """Create aiosqlite config for Arrow testing."""
    config = AiosqliteConfig()
    try:
        yield config
    finally:
        await config.close_pool()


async def test_select_to_arrow_null_handling(aiosqlite_arrow_config: AiosqliteConfig) -> None:
    """Test select_to_arrow with NULL values."""
    try:
        async with aiosqlite_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_null_test")
            await session.execute("CREATE TABLE arrow_null_test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

            result = await session.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 3
            assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        await aiosqlite_arrow_config.close_pool()


async def test_select_to_arrow_to_polars(aiosqlite_arrow_config: AiosqliteConfig) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    try:
        async with aiosqlite_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_polars_test")
            await session.execute("CREATE TABLE arrow_polars_test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

            result = await session.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
            df = result.to_polars()

            assert len(df) == 2
            assert df["value"].to_list() == ["a", "b"]
    finally:
        await aiosqlite_arrow_config.close_pool()


async def test_select_to_arrow_large_dataset(aiosqlite_arrow_config: AiosqliteConfig) -> None:
    """Test select_to_arrow with larger dataset."""
    try:
        async with aiosqlite_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_large_test")
            await session.execute("CREATE TABLE arrow_large_test (id INTEGER, value INTEGER)")

            # Insert 1000 rows
            for i in range(1, 1001):
                await session.execute("INSERT INTO arrow_large_test VALUES (?, ?)", (i, i * 10))

            result = await session.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

            assert result.rows_affected == 1000
            df = result.to_pandas()
            assert len(df) == 1000
            assert df["value"].sum() == sum(i * 10 for i in range(1, 1001))
    finally:
        await aiosqlite_arrow_config.close_pool()


async def test_select_to_arrow_type_preservation(aiosqlite_arrow_config: AiosqliteConfig) -> None:
    """Test that SQLite types are properly converted to Arrow types."""
    try:
        async with aiosqlite_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_types_test")
            await session.execute(
                """
                CREATE TABLE arrow_types_test (
                    id INTEGER,
                    name TEXT,
                    price REAL,
                    created_at TEXT,
                    is_active INTEGER
                )
                """
            )
            await session.execute(
                """
                INSERT INTO arrow_types_test VALUES
                (1, 'Item 1', 19.99, '2025-01-01 10:00:00', 1),
                (2, 'Item 2', 29.99, '2025-01-02 15:30:00', 0)
                """
            )

            result = await session.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

            df = result.to_pandas()
            from pandas.api.types import is_string_dtype

            assert len(df) == 2
            assert is_string_dtype(df["name"])
            # SQLite INTEGER (for booleans) comes through as int64
            assert df["is_active"].dtype in (int, "int64", "Int64")
    finally:
        await aiosqlite_arrow_config.close_pool()


async def test_select_to_arrow_json_handling(aiosqlite_arrow_config: AiosqliteConfig) -> None:
    """Test SQLite JSON type handling in Arrow results."""
    try:
        async with aiosqlite_arrow_config.provide_session() as session:
            await session.execute("DROP TABLE IF EXISTS arrow_json_test")
            await session.execute("CREATE TABLE arrow_json_test (id INTEGER, data TEXT)")
            await session.execute(
                """
                INSERT INTO arrow_json_test VALUES
                (1, '{"name": "Alice", "age": 30}'),
                (2, '{"name": "Bob", "age": 25}')
                """
            )

            result = await session.select_to_arrow("SELECT * FROM arrow_json_test ORDER BY id")

            # SQLite JSON is stored as TEXT, Arrow converts to string
            df = result.to_pandas()
            assert len(df) == 2
            assert isinstance(df["data"].iloc[0], str)
            assert "Alice" in df["data"].iloc[0]
    finally:
        await aiosqlite_arrow_config.close_pool()
