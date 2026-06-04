"""Integration tests for asyncpg Arrow support."""

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.typing import PYARROW_INSTALLED

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


async def test_select_to_arrow_basic(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_users CASCADE")
    await asyncpg_async_driver.execute("CREATE TABLE arrow_users (id INTEGER, name TEXT, age INTEGER)")
    await asyncpg_async_driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    result = await asyncpg_async_driver.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

    assert result is not None
    assert isinstance(result.data, (pa.Table, pa.RecordBatch))
    assert result.rows_affected == 2

    df = result.to_pandas()
    assert len(df) == 2
    assert list(df["name"]) == ["Alice", "Bob"]
    assert list(df["age"]) == [30, 25]

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_users CASCADE")


async def test_select_to_arrow_table_format(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_table_test CASCADE")
    await asyncpg_async_driver.execute("CREATE TABLE arrow_table_test (id INTEGER, value TEXT)")
    await asyncpg_async_driver.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    result = await asyncpg_async_driver.select_to_arrow(
        "SELECT * FROM arrow_table_test ORDER BY id", return_format="table"
    )

    assert isinstance(result.data, pa.Table)
    assert result.rows_affected == 3

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_table_test CASCADE")


async def test_select_to_arrow_batch_format(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_batch_test CASCADE")
    await asyncpg_async_driver.execute("CREATE TABLE arrow_batch_test (id INTEGER, value TEXT)")
    await asyncpg_async_driver.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

    result = await asyncpg_async_driver.select_to_arrow(
        "SELECT * FROM arrow_batch_test ORDER BY id", return_format="batch"
    )

    assert isinstance(result.data, pa.RecordBatch)
    assert result.rows_affected == 2

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_batch_test CASCADE")


async def test_select_to_arrow_with_parameters(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test select_to_arrow with query parameters."""

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_params_test CASCADE")
    await asyncpg_async_driver.execute("CREATE TABLE arrow_params_test (id INTEGER, value INTEGER)")
    await asyncpg_async_driver.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

    result = await asyncpg_async_driver.select_to_arrow(
        "SELECT * FROM arrow_params_test WHERE value > $1 ORDER BY id", (150,)
    )

    assert result.rows_affected == 2
    df = result.to_pandas()
    assert list(df["value"]) == [200, 300]

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_params_test CASCADE")


async def test_select_to_arrow_empty_result(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test select_to_arrow with empty result set."""

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_empty_test CASCADE")
    await asyncpg_async_driver.execute("CREATE TABLE arrow_empty_test (id INTEGER)")

    result = await asyncpg_async_driver.select_to_arrow("SELECT * FROM arrow_empty_test")

    assert result.rows_affected == 0
    assert len(result.to_pandas()) == 0

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_empty_test CASCADE")


async def test_select_to_arrow_type_preservation(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test that PostgreSQL types are properly converted to Arrow types."""

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_types_test CASCADE")
    await asyncpg_async_driver.execute(
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
    await asyncpg_async_driver.execute(
        """
        INSERT INTO arrow_types_test VALUES
        (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true),
        (2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)
        """
    )

    result = await asyncpg_async_driver.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

    df = result.to_pandas()
    from pandas.api.types import is_bool_dtype, is_string_dtype

    assert len(df) == 2
    assert is_string_dtype(df["name"])
    assert is_bool_dtype(df["is_active"])

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_types_test CASCADE")


async def test_select_to_arrow_postgres_array(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test PostgreSQL array type handling in Arrow results."""

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_array_test CASCADE")
    await asyncpg_async_driver.execute("CREATE TABLE arrow_array_test (id INTEGER, tags TEXT[])")
    await asyncpg_async_driver.execute(
        "INSERT INTO arrow_array_test VALUES (1, ARRAY['python', 'rust']), (2, ARRAY['js', 'ts'])"
    )

    result = await asyncpg_async_driver.select_to_arrow("SELECT * FROM arrow_array_test ORDER BY id")

    df = result.to_pandas()
    assert len(df) == 2
    assert isinstance(df["tags"].iloc[0], (list, object))

    await asyncpg_async_driver.execute("DROP TABLE IF EXISTS arrow_array_test CASCADE")


async def test_load_from_arrow_json_and_jsonb(asyncpg_async_driver: AsyncpgDriver) -> None:
    """Test Arrow import into PostgreSQL JSON and JSONB columns."""
    import pyarrow as pa

    table_name = "arrow_json_ingest_asyncpg"
    await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    await asyncpg_async_driver.execute(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY,
            payload_json JSON NOT NULL,
            payload_jsonb JSONB NOT NULL
        )
        """
    )

    try:
        arrow_table = pa.table({
            "id": [1, 2],
            "payload_json": ['{"name":"alpha","items":[1,2]}', '{"name":"beta","items":[3]}'],
            "payload_jsonb": ['{"status":"ready","count":1}', '{"status":"done","count":2}'],
        })

        job = await asyncpg_async_driver.load_from_arrow(table_name, arrow_table)

        result = await asyncpg_async_driver.execute(f"SELECT * FROM {table_name} ORDER BY id")
        rows = result.get_data()
        assert rows == [
            {
                "id": 1,
                "payload_json": {"name": "alpha", "items": [1, 2]},
                "payload_jsonb": {"count": 1, "status": "ready"},
            },
            {"id": 2, "payload_json": {"name": "beta", "items": [3]}, "payload_jsonb": {"count": 2, "status": "done"}},
        ]
        assert job.telemetry["rows_processed"] == 2
    finally:
        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
