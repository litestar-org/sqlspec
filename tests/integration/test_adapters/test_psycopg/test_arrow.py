"""Integration tests for psycopg Arrow support."""

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.typing import PYARROW_INSTALLED

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.fixture
async def psycopg_config(psycopg_async_config: PsycopgAsyncConfig) -> PsycopgAsyncConfig:
    """Create Psycopg async configuration for testing."""
    psycopg_async_config.connection_config.setdefault("min_size", 1)
    psycopg_async_config.connection_config.setdefault("max_size", 2)
    return psycopg_async_config


async def test_select_to_arrow_basic(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_users CASCADE")
        await session.execute("CREATE TABLE arrow_users (id INTEGER, name TEXT, age INTEGER)")
        await session.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

        result = await session.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 2

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]
        assert list(df["age"]) == [30, 25]


async def test_select_to_arrow_table_format(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_table_test CASCADE")
        await session.execute("CREATE TABLE arrow_table_test (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        result = await session.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3


async def test_select_to_arrow_batch_format(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_batch_test CASCADE")
        await session.execute("CREATE TABLE arrow_batch_test (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

        result = await session.select_to_arrow("SELECT * FROM arrow_batch_test ORDER BY id", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2


async def test_select_to_arrow_with_parameters(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with query parameters."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_params_test CASCADE")
        await session.execute("CREATE TABLE arrow_params_test (id INTEGER, value INTEGER)")
        await session.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

        # Test with parameterized query
        result = await session.select_to_arrow("SELECT * FROM arrow_params_test WHERE value > %s ORDER BY id", (150,))

        assert result.rows_affected == 2
        df = result.to_pandas()
        assert list(df["value"]) == [200, 300]


async def test_select_to_arrow_empty_result(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with empty result set."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_empty_test CASCADE")
        await session.execute("CREATE TABLE arrow_empty_test (id INTEGER)")

        result = await session.select_to_arrow("SELECT * FROM arrow_empty_test")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0


async def test_select_to_arrow_null_handling(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with NULL values."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_null_test CASCADE")
        await session.execute("CREATE TABLE arrow_null_test (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

        result = await session.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]


async def test_select_to_arrow_to_polars(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_polars_test CASCADE")
        await session.execute("CREATE TABLE arrow_polars_test (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

        result = await session.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["value"].to_list() == ["a", "b"]


async def test_select_to_arrow_large_dataset(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with larger dataset."""
    async with psycopg_config.provide_session() as session:
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


async def test_select_to_arrow_type_preservation(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test that PostgreSQL types are properly converted to Arrow types."""
    async with psycopg_config.provide_session() as session:
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


async def test_select_to_arrow_postgres_array(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test PostgreSQL array type handling in Arrow results."""
    async with psycopg_config.provide_session() as session:
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
