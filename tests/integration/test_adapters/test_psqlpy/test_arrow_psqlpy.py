"""Integration tests for psqlpy Arrow support."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy import PsqlpyDriver

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


async def _drop_table(driver: "PsqlpyDriver", table: str) -> None:
    await driver.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


async def test_select_to_arrow_basic(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test basic Arrow extraction."""
    import pyarrow as pa

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_users (id INTEGER, name TEXT, age INTEGER)")
    await driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 2

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]
    finally:
        await _drop_table(driver, "arrow_users")


async def test_select_to_arrow_table_format(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test table return format."""
    import pyarrow as pa

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_table_test (id INTEGER, value TEXT)")
    await driver.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3
    finally:
        await _drop_table(driver, "arrow_table_test")


async def test_select_to_arrow_batch_format(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test batch return format."""
    import pyarrow as pa

    driver = psqlpy_driver
    await driver.execute("DROP TABLE IF EXISTS arrow_batch_test CASCADE")
    await driver.execute("CREATE TABLE arrow_batch_test (id INTEGER, value TEXT)")
    await driver.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_batch_test ORDER BY id", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2
    finally:
        await _drop_table(driver, "arrow_batch_test")


async def test_select_to_arrow_with_parameters(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test Arrow extraction with parameters."""

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_params_test (id INTEGER, value INTEGER)")
    await driver.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_params_test WHERE value > $1 ORDER BY id", (150,))

        df = result.to_pandas()
        assert list(df["value"]) == [200, 300]
    finally:
        await _drop_table(driver, "arrow_params_test")


async def test_select_to_arrow_empty_result(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test empty result extraction."""

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_empty_test (id INTEGER)")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_empty_test WHERE id > 100")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0
    finally:
        await _drop_table(driver, "arrow_empty_test")


async def test_select_to_arrow_null_handling(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test NULL value handling."""

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_null_test (id INTEGER, value TEXT)")
    await driver.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        await _drop_table(driver, "arrow_null_test")


async def test_select_to_arrow_to_polars(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test conversion to Polars."""

    pytest.importorskip("polars")

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_polars_test (id INTEGER, value TEXT)")
    await driver.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["value"].to_list() == ["a", "b"]
    finally:
        await _drop_table(driver, "arrow_polars_test")


async def test_select_to_arrow_large_dataset(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test larger dataset handling."""

    driver = psqlpy_driver
    await driver.execute("CREATE TABLE arrow_large_test (id INTEGER, value INTEGER)")
    values = ", ".join(f"({i}, {i * 10})" for i in range(1, 1001))
    await driver.execute(f"INSERT INTO arrow_large_test VALUES {values}")

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

        assert result.rows_affected == 1000
        df = result.to_pandas()
        assert len(df) == 1000
    finally:
        await _drop_table(driver, "arrow_large_test")


async def test_select_to_arrow_type_preservation(psqlpy_driver: "PsqlpyDriver") -> None:
    """Test that PostgreSQL types map correctly to Arrow."""

    driver = psqlpy_driver
    await driver.execute(
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
    await driver.execute(
        """
            INSERT INTO arrow_types_test VALUES
            (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true),
            (2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)
        """
    )

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Item 1", "Item 2"]
    finally:
        await _drop_table(driver, "arrow_types_test")
