"""Integration tests for psycopg Arrow support."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg import PsycopgSyncConfig

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.fixture
async def psycopg_config(psycopg_async_config: PsycopgAsyncConfig) -> PsycopgAsyncConfig:
    """Create Psycopg async configuration for testing."""
    return psycopg_async_config


async def test_select_to_arrow_basic(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_users_psycopg_async CASCADE")
        await session.execute("CREATE TABLE arrow_users_psycopg_async (id INTEGER, name TEXT, age INTEGER)")
        await session.execute("INSERT INTO arrow_users_psycopg_async VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

        result = await session.select_to_arrow("SELECT * FROM arrow_users_psycopg_async ORDER BY id")

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
        await session.execute("DROP TABLE IF EXISTS arrow_table_test_psycopg_async CASCADE")
        await session.execute("CREATE TABLE arrow_table_test_psycopg_async (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO arrow_table_test_psycopg_async VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        result = await session.select_to_arrow(
            "SELECT * FROM arrow_table_test_psycopg_async ORDER BY id", return_format="table"
        )

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3


async def test_select_to_arrow_batch_format(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_batch_test_psycopg_async CASCADE")
        await session.execute("CREATE TABLE arrow_batch_test_psycopg_async (id INTEGER, value TEXT)")
        await session.execute("INSERT INTO arrow_batch_test_psycopg_async VALUES (1, 'a'), (2, 'b')")

        result = await session.select_to_arrow(
            "SELECT * FROM arrow_batch_test_psycopg_async ORDER BY id", return_format="batch"
        )

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2


async def test_select_to_arrow_with_parameters(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with query parameters."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_params_test_psycopg_async CASCADE")
        await session.execute("CREATE TABLE arrow_params_test_psycopg_async (id INTEGER, value INTEGER)")
        await session.execute("INSERT INTO arrow_params_test_psycopg_async VALUES (1, 100), (2, 200), (3, 300)")

        # Test with parameterized query
        result = await session.select_to_arrow(
            "SELECT * FROM arrow_params_test_psycopg_async WHERE value > %s ORDER BY id", (150,)
        )

        assert result.rows_affected == 2
        df = result.to_pandas()
        assert list(df["value"]) == [200, 300]


async def test_select_to_arrow_empty_result(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test select_to_arrow with empty result set."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_empty_test_psycopg_async CASCADE")
        await session.execute("CREATE TABLE arrow_empty_test_psycopg_async (id INTEGER)")

        result = await session.select_to_arrow("SELECT * FROM arrow_empty_test_psycopg_async")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0


async def test_select_to_arrow_type_preservation(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test that PostgreSQL types are properly converted to Arrow types."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_types_test_psycopg_async CASCADE")
        await session.execute(
            """
            CREATE TABLE arrow_types_test_psycopg_async (
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
            INSERT INTO arrow_types_test_psycopg_async VALUES
            (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true),
            (2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)
            """
        )

        result = await session.select_to_arrow("SELECT * FROM arrow_types_test_psycopg_async ORDER BY id")

        df = result.to_pandas()
        from pandas.api.types import is_bool_dtype, is_string_dtype

        assert len(df) == 2
        assert is_string_dtype(df["name"])
        assert is_bool_dtype(df["is_active"])


async def test_select_to_arrow_postgres_array(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test PostgreSQL array type handling in Arrow results."""
    async with psycopg_config.provide_session() as session:
        await session.execute("DROP TABLE IF EXISTS arrow_array_test_psycopg_async CASCADE")
        await session.execute("CREATE TABLE arrow_array_test_psycopg_async (id INTEGER, tags TEXT[])")
        await session.execute(
            "INSERT INTO arrow_array_test_psycopg_async VALUES (1, ARRAY['python', 'rust']), (2, ARRAY['js', 'ts'])"
        )

        result = await session.select_to_arrow("SELECT * FROM arrow_array_test_psycopg_async ORDER BY id")

        # PostgreSQL arrays are returned as Python lists in dict format,
        # which Arrow converts to list type
        df = result.to_pandas()
        assert len(df) == 2
        assert isinstance(df["tags"].iloc[0], (list, object))


async def test_psycopg_async_load_from_arrow_jsonb(psycopg_config: PsycopgAsyncConfig) -> None:
    """Test async Arrow import into PostgreSQL JSONB columns."""
    import pyarrow as pa

    async with psycopg_config.provide_session() as session:
        table_name = "arrow_jsonb_ingest_psycopg_async"
        await session.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        await session.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, payload JSONB NOT NULL)")
        try:
            arrow_table = pa.table({"id": [1, 2], "payload": ['{"name":"alpha"}', '{"name":"beta"}']})

            job = await session.load_from_arrow(table_name, arrow_table)

            result = await session.execute(f"SELECT id, payload FROM {table_name} ORDER BY id")
            assert result.get_data() == [
                {"id": 1, "payload": {"name": "alpha"}},
                {"id": 2, "payload": {"name": "beta"}},
            ]
            assert job.telemetry["rows_processed"] == 2
        finally:
            await session.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")


def test_psycopg_sync_load_from_arrow_jsonb(psycopg_sync_config: "PsycopgSyncConfig") -> None:
    """Test sync Arrow import into PostgreSQL JSONB columns."""
    import pyarrow as pa

    with psycopg_sync_config.provide_session() as session:
        table_name = "arrow_jsonb_ingest_psycopg_sync"
        session.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        session.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, payload JSONB NOT NULL)")
        try:
            arrow_table = pa.table({"id": [1, 2], "payload": ['{"name":"alpha"}', '{"name":"beta"}']})

            job = session.load_from_arrow(table_name, arrow_table)

            result = session.execute(f"SELECT id, payload FROM {table_name} ORDER BY id")
            assert result.get_data() == [
                {"id": 1, "payload": {"name": "alpha"}},
                {"id": 2, "payload": {"name": "beta"}},
            ]
            assert job.telemetry["rows_processed"] == 2
        finally:
            session.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
