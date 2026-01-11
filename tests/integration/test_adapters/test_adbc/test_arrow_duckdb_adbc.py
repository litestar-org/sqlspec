"""Integration tests for ADBC native Arrow support."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.adbc import AdbcDriver

pytestmark = [pytest.mark.xdist_group("duckdb"), pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")]


def _drop_duckdb_table(driver: "AdbcDriver", table_name: str) -> None:
    driver.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_select_to_arrow_basic(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
    driver.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    try:
        result = driver.select_to_arrow("SELECT * FROM users ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 2

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]
        assert list(df["age"]) == [30, 25]
    finally:
        _drop_duckdb_table(driver, "users")


def test_select_to_arrow_table_format(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow with table return format."""
    import pyarrow as pa

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_table_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3
    finally:
        _drop_duckdb_table(driver, "arrow_table_test")


def test_select_to_arrow_batch_format(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_batch_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_batch_test ORDER BY id", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2
    finally:
        _drop_duckdb_table(driver, "arrow_batch_test")


def test_select_to_arrow_with_parameters(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow with query parameters."""

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_users (id INTEGER, name VARCHAR, age INTEGER)")
    driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_users WHERE age > ?", (25,))

        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["name"]) == {"Alice", "Charlie"}
    finally:
        _drop_duckdb_table(driver, "arrow_users")


def test_select_to_arrow_empty_result(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow with no matching rows."""

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_empty_test (id INTEGER, value VARCHAR)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_empty_test WHERE id > 100")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0
    finally:
        _drop_duckdb_table(driver, "arrow_empty_test")


def test_select_to_arrow_null_handling(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow with NULL values."""

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_null_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        _drop_duckdb_table(driver, "arrow_null_test")


def test_select_to_arrow_to_polars(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""

    pytest.importorskip("polars")

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_polars_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["value"].to_list() == ["a", "b"]
    finally:
        _drop_duckdb_table(driver, "arrow_polars_test")


def test_select_to_arrow_large_dataset(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test select_to_arrow with a larger dataset."""

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_large_test (id INTEGER, value DOUBLE)")
    driver.execute("INSERT INTO arrow_large_test SELECT range AS id, random() FROM range(10000)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_large_test")

        assert result.rows_affected == 10000
        df = result.to_pandas()
        assert len(df) == 10000
    finally:
        _drop_duckdb_table(driver, "arrow_large_test")


def test_select_to_arrow_type_preservation(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Test that Arrow preserves DuckDB column types."""

    driver = adbc_duckdb_driver
    driver.execute(
        """
            CREATE TABLE arrow_types_test (
                id INTEGER,
                name VARCHAR,
                price DOUBLE,
                active BOOLEAN,
                created DATE
            )
        """
    )
    driver.execute(
        """
            INSERT INTO arrow_types_test VALUES
            (1, 'Product A', 19.99, TRUE, DATE '2024-01-01'),
            (2, 'Product B', 29.99, FALSE, DATE '2024-01-02')
        """
    )

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 2
        assert df["price"].dtype == "float64"
        assert df["active"].dtype in (bool, "bool", "boolean")
    finally:
        _drop_duckdb_table(driver, "arrow_types_test")


def test_select_to_arrow_zero_copy_performance(adbc_duckdb_driver: "AdbcDriver") -> None:
    """Smoke-test large Arrow extraction for zero-copy behaviour."""

    driver = adbc_duckdb_driver
    driver.execute("CREATE TABLE arrow_perf_test (id INTEGER, payload VARCHAR)")
    driver.execute("INSERT INTO arrow_perf_test SELECT range, repeat('x', 1024) FROM range(5000)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_perf_test")

        assert result.rows_affected == 5000
        table = result.data
        assert table is not None
        assert table.num_rows == 5000
        assert table.column("payload")[0].as_py() == "x" * 1024
    finally:
        _drop_duckdb_table(driver, "arrow_perf_test")
