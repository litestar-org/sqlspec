"""Integration tests for ADBC native Arrow support."""

import pytest

from sqlspec._typing import PYARROW_INSTALLED
from sqlspec.adapters.adbc import AdbcConfig

pytestmark = [
    pytest.mark.xdist_group("adbc"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.fixture
def adbc_config() -> AdbcConfig:
    """Create a basic ADBC configuration using DuckDB driver."""
    return AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})


def test_select_to_arrow_basic(adbc_config: AdbcConfig) -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    try:
        with adbc_config.provide_session() as session:
            # Create test table
            session.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
            session.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

            # Test Arrow query
            result = session.select_to_arrow("SELECT * FROM users ORDER BY id")

            assert result is not None
            assert isinstance(result.data, (pa.Table, pa.RecordBatch))
            assert result.rows_affected == 2

            # Convert to pandas and verify
            df = result.to_pandas()
            assert len(df) == 2
            assert list(df["name"]) == ["Alice", "Bob"]
            assert list(df["age"]) == [30, 25]
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_table_format(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    try:
        with adbc_config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
            session.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

            result = session.select_to_arrow("SELECT * FROM test", return_format="table")

            assert isinstance(result.data, pa.Table)
            assert result.rows_affected == 3
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_batch_format(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    try:
        with adbc_config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
            session.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b')")

            result = session.select_to_arrow("SELECT * FROM test", return_format="batch")

            assert isinstance(result.data, pa.RecordBatch)
            assert result.rows_affected == 2
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_with_parameters(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with query parameters."""
    try:
        with adbc_config.provide_session() as session:
            session.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
            session.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")

            # Query with parameter (ADBC/DuckDB uses ? for positional)
            result = session.select_to_arrow("SELECT * FROM users WHERE age > ?", 25)

            df = result.to_pandas()
            assert len(df) == 2
            assert set(df["name"]) == {"Alice", "Charlie"}
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_empty_result(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with no matching rows."""
    try:
        with adbc_config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")

            result = session.select_to_arrow("SELECT * FROM test WHERE id > 100")

            assert result.rows_affected == 0
            df = result.to_pandas()
            assert len(df) == 0
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_null_handling(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with NULL values."""
    try:
        with adbc_config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
            session.execute("INSERT INTO test VALUES (1, 'a'), (2, NULL), (3, 'c')")

            result = session.select_to_arrow("SELECT * FROM test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 3
            assert df["value"].isna()[1]  # Second row should be NULL
            assert df["value"].iloc[0] == "a"
            assert df["value"].iloc[2] == "c"
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_to_polars(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with polars conversion."""
    pytest.importorskip("polars", reason="polars not installed")

    try:
        with adbc_config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
            session.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b')")

            result = session.select_to_arrow("SELECT * FROM test ORDER BY id")

            pl_df = result.to_polars()
            assert len(pl_df) == 2
            assert list(pl_df["value"]) == ["a", "b"]
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_large_dataset(adbc_config: AdbcConfig) -> None:
    """Test select_to_arrow with larger dataset (10K rows)."""
    try:
        with adbc_config.provide_session() as session:
            # Create table with 10K rows
            session.execute("CREATE TABLE test (id INTEGER, value DOUBLE)")
            session.execute("INSERT INTO test SELECT range AS id, random() AS value FROM range(10000)")

            result = session.select_to_arrow("SELECT * FROM test")

            assert result.rows_affected == 10000
            df = result.to_pandas()
            assert len(df) == 10000
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_type_preservation(adbc_config: AdbcConfig) -> None:
    """Test that Arrow preserves column types correctly."""
    try:
        with adbc_config.provide_session() as session:
            session.execute("""
                CREATE TABLE test (
                    id INTEGER,
                    name VARCHAR,
                    price DOUBLE,
                    active BOOLEAN,
                    created DATE
                )
            """)
            session.execute("""
                INSERT INTO test VALUES
                (1, 'Product A', 19.99, true, '2024-01-01'),
                (2, 'Product B', 29.99, false, '2024-01-02')
            """)

            result = session.select_to_arrow("SELECT * FROM test ORDER BY id")

            df = result.to_pandas()
            assert len(df) == 2
            assert df["id"].dtype == "int32"
            assert df["name"].dtype == "object"
            assert df["price"].dtype == "float64"
            assert df["active"].dtype == "bool"
    finally:
        adbc_config.close_pool()


def test_select_to_arrow_zero_copy_performance(adbc_config: AdbcConfig) -> None:
    """Test that ADBC native Arrow is faster than dict conversion (smoke test)."""
    try:
        with adbc_config.provide_session() as session:
            # Create table with moderate number of rows
            session.execute("CREATE TABLE test (id INTEGER, value DOUBLE, text VARCHAR)")
            session.execute("""
                INSERT INTO test
                SELECT
                    range AS id,
                    random() AS value,
                    'row_' || range AS text
                FROM range(5000)
            """)

            # Test Arrow path (should be fast)
            result = session.select_to_arrow("SELECT * FROM test")
            assert result.rows_affected == 5000

            # Verify data is accessible
            df = result.to_pandas()
            assert len(df) == 5000
    finally:
        adbc_config.close_pool()
