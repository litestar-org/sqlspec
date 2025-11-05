"""Integration tests for SQLite Arrow query support."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite import SqliteDriver

pytestmark = pytest.mark.xdist_group("sqlite")


def _drop_table(driver: "SqliteDriver", table_name: str) -> None:
    driver.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_select_to_arrow_basic(sqlite_basic_session: "SqliteDriver") -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_users (id INTEGER, name TEXT, age INTEGER)")
    driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 2

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]
    finally:
        _drop_table(driver, "arrow_users")


def test_select_to_arrow_table_format(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow with table return format."""
    import pyarrow as pa

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_table_test (id INTEGER, value TEXT)")
    driver.execute("INSERT INTO arrow_table_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3
    finally:
        _drop_table(driver, "arrow_table_test")


def test_select_to_arrow_batch_format(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_batch_test (id INTEGER, value TEXT)")
    driver.execute("INSERT INTO arrow_batch_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_batch_test ORDER BY id", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2
    finally:
        _drop_table(driver, "arrow_batch_test")


def test_select_to_arrow_with_parameters(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow with query parameters."""

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_params_test (id INTEGER, value INTEGER)")
    driver.execute("INSERT INTO arrow_params_test VALUES (1, 100), (2, 200), (3, 300)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_params_test WHERE value > ? ORDER BY id", (150,))

        assert result.rows_affected == 2
        df = result.to_pandas()
        assert list(df["value"]) == [200, 300]
    finally:
        _drop_table(driver, "arrow_params_test")


def test_select_to_arrow_empty_result(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow with empty result set."""

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_empty_test (id INTEGER)")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_empty_test WHERE id > 100")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0
    finally:
        _drop_table(driver, "arrow_empty_test")


def test_select_to_arrow_null_handling(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow with NULL values."""

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_null_test (id INTEGER, value TEXT)")
    driver.execute("INSERT INTO arrow_null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        _drop_table(driver, "arrow_null_test")


def test_select_to_arrow_to_polars(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""

    pytest.importorskip("polars")

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_polars_test (id INTEGER, value TEXT)")
    driver.execute("INSERT INTO arrow_polars_test VALUES (1, 'a'), (2, 'b')")

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["value"].to_list() == ["a", "b"]
    finally:
        _drop_table(driver, "arrow_polars_test")


def test_select_to_arrow_large_dataset(sqlite_basic_session: "SqliteDriver") -> None:
    """Test select_to_arrow with larger dataset."""

    driver = sqlite_basic_session
    driver.execute("CREATE TABLE arrow_large_test (id INTEGER, value INTEGER)")
    for i in range(1, 1001):
        driver.execute("INSERT INTO arrow_large_test VALUES (?, ?)", (i, i * 10))

    try:
        result = driver.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

        assert result.rows_affected == 1000
        df = result.to_pandas()
        assert len(df) == 1000
    finally:
        _drop_table(driver, "arrow_large_test")
