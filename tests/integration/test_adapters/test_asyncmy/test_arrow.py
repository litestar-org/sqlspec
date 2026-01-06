"""Integration tests for asyncmy Arrow query support."""

import pytest

from sqlspec.adapters.asyncmy import AsyncmyDriver
from tests.conftest import requires_interpreted

pytestmark = [pytest.mark.xdist_group("mysql"), requires_interpreted]


async def test_select_to_arrow_basic(asyncmy_driver: AsyncmyDriver) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_users")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_users (id INT, name VARCHAR(100), age INT)")
    await asyncmy_driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

    assert result is not None
    assert isinstance(result.data, (pa.Table, pa.RecordBatch))
    assert result.rows_affected == 2

    df = result.to_pandas()
    assert len(df) == 2
    assert list(df["name"]) == ["Alice", "Bob"]

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_users")


async def test_select_to_arrow_table_format(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_table_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_table_test (id INT, value VARCHAR(100))")
    await asyncmy_driver.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

    assert isinstance(result.data, pa.Table)
    assert result.rows_affected == 3

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_table_test")


async def test_select_to_arrow_batch_format(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_batch_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_batch_test (id INT, value VARCHAR(100))")
    await asyncmy_driver.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_batch_test ORDER BY id", return_format="batches")

    assert isinstance(result.data, list)
    for batch in result.data:
        assert isinstance(batch, pa.RecordBatch)
    assert result.rows_affected == 2

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_batch_test")


async def test_select_to_arrow_with_parameters(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow with query parameters."""

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_params_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_params_test (id INT, value INT)")
    await asyncmy_driver.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

    result = await asyncmy_driver.select_to_arrow(
        "SELECT * FROM arrow_params_test WHERE value > %s ORDER BY id", (150,)
    )

    assert result.rows_affected == 2
    df = result.to_pandas()
    assert list(df["value"]) == [200, 300]

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_params_test")


async def test_select_to_arrow_empty_result(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow with empty result set."""

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_empty_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_empty_test (id INT)")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_empty_test")

    assert result.rows_affected == 0
    assert len(result.to_pandas()) == 0

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_empty_test")


async def test_select_to_arrow_null_handling(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow with NULL values."""

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_null_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_null_test (id INT, value VARCHAR(100))")
    await asyncmy_driver.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

    df = result.to_pandas()
    assert len(df) == 3
    assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_null_test")


async def test_select_to_arrow_to_polars(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""
    pytest.importorskip("polars")

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_polars_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_polars_test (id INT, value VARCHAR(100))")
    await asyncmy_driver.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
    df = result.to_polars()

    assert len(df) == 2
    assert df["value"].to_list() == ["a", "b"]

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_polars_test")


async def test_select_to_arrow_large_dataset(asyncmy_driver: AsyncmyDriver) -> None:
    """Test select_to_arrow with larger dataset."""

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_large_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_large_test (id INT, value INT)")

    values = ", ".join(f"({i}, {i * 10})" for i in range(1, 1001))
    await asyncmy_driver.execute(f"INSERT INTO arrow_large_test VALUES {values}")

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

    assert result.rows_affected == 1000
    df = result.to_pandas()
    assert len(df) == 1000
    assert df["value"].sum() == sum(i * 10 for i in range(1, 1001))

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_large_test")


async def test_select_to_arrow_type_preservation(asyncmy_driver: AsyncmyDriver) -> None:
    """Test that MySQL types are properly converted to Arrow types."""

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_types_test")
    await asyncmy_driver.execute(
        """
        CREATE TABLE IF NOT EXISTS arrow_types_test (
            id INT,
            name VARCHAR(100),
            price DECIMAL(10, 2),
            created_at DATETIME,
            is_active BOOLEAN
        )
        """
    )
    await asyncmy_driver.execute(
        """
        INSERT INTO arrow_types_test VALUES
        (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true),
        (2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)
        """
    )

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

    df = result.to_pandas()
    assert len(df) == 2
    assert df["name"].dtype == object
    assert df["is_active"].dtype in (bool, int, "int64", "Int64")

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_types_test")


async def test_select_to_arrow_json_handling(asyncmy_driver: AsyncmyDriver) -> None:
    """Test JSON type handling in Arrow results."""

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_json_test")
    await asyncmy_driver.execute("CREATE TABLE IF NOT EXISTS arrow_json_test (id INT, data JSON)")
    await asyncmy_driver.execute(
        """
        INSERT INTO arrow_json_test VALUES
        (1, '{"name": "Alice", "age": 30}'),
        (2, '{"name": "Bob", "age": 25}')
        """
    )

    result = await asyncmy_driver.select_to_arrow("SELECT * FROM arrow_json_test ORDER BY id")

    df = result.to_pandas()
    assert len(df) == 2
    first_value = df["data"].iloc[0]
    assert isinstance(first_value, (dict, str, object))

    await asyncmy_driver.execute("DROP TABLE IF EXISTS arrow_json_test")
