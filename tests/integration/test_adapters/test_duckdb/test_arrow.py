"""Integration tests for DuckDB native Arrow support."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb import DuckDBDriver

pytestmark = [
    pytest.mark.xdist_group("duckdb"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


def _drop_table(driver: "DuckDBDriver", table_name: str) -> None:
    driver.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_select_to_arrow_basic(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    driver = duckdb_basic_session
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
        _drop_table(driver, "users")


def test_select_to_arrow_table_format(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_table_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_table_test", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3
    finally:
        _drop_table(driver, "arrow_table_test")


def test_select_to_arrow_batch_format(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_batch_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_batch_test", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2
    finally:
        _drop_table(driver, "arrow_batch_test")


def test_select_to_arrow_with_parameters(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with query parameters."""
    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_users (id INTEGER, name VARCHAR, age INTEGER)")
    driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_users WHERE age > ?", (25,))

        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["name"]) == {"Alice", "Charlie"}
    finally:
        _drop_table(driver, "arrow_users")


def test_select_to_arrow_empty_result(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with no matching rows."""
    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_empty_test (id INTEGER, value VARCHAR)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_empty_test WHERE id > 100")

        assert result.rows_affected == 0
        df = result.to_pandas()
        assert len(df) == 0
    finally:
        _drop_table(driver, "arrow_empty_test")


def test_select_to_arrow_null_handling(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with NULL values."""
    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_null_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        _drop_table(driver, "arrow_null_test")


def test_select_to_arrow_to_polars(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with polars conversion."""
    pytest.importorskip("polars", reason="polars not installed")

    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_polars_test (id INTEGER, value VARCHAR)")
    driver.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")

        pl_df = result.to_polars()
        assert len(pl_df) == 2
        assert list(pl_df["value"]) == ["a", "b"]
    finally:
        _drop_table(driver, "arrow_polars_test")


def test_select_to_arrow_large_dataset(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test select_to_arrow with larger dataset (10K rows)."""
    driver = duckdb_basic_session
    driver.execute("CREATE TABLE arrow_large_test (id INTEGER, value DOUBLE)")
    driver.execute("INSERT INTO arrow_large_test SELECT range AS id, random() FROM range(10000)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_large_test")

        assert result.rows_affected == 10000
        df = result.to_pandas()
        assert len(df) == 10000
    finally:
        _drop_table(driver, "arrow_large_test")


def test_select_to_arrow_type_preservation(duckdb_basic_session: "DuckDBDriver") -> None:
    """Test that Arrow preserves column types correctly."""

    driver = duckdb_basic_session
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
            (1, 'Product A', 19.99, true, '2024-01-01'),
            (2, 'Product B', 29.99, false, '2024-01-02')
        """
    )

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 2
        assert df["id"].dtype == "int32"
        assert df["name"].dtype == "object"
        assert df["price"].dtype == "float64"
        assert df["active"].dtype == "bool"
    finally:
        _drop_table(driver, "arrow_types_test")
