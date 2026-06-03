"""Integration tests for SQLite Arrow query support."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite import SqliteDriver

pytestmark = pytest.mark.xdist_group("sqlite")


def _drop_table(driver: "SqliteDriver", table_name: str) -> None:
    driver.execute(f"DROP TABLE IF EXISTS {table_name}")


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
