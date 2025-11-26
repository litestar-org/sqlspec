"""Tests for SQLResult DataFrame conversion methods."""

from typing import Any

import pytest

from sqlspec.core import SQL, SQLResult
from sqlspec.typing import PYARROW_INSTALLED


@pytest.fixture
def sample_data() -> list[dict[str, Any]]:
    """Create sample dict data for testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]


@pytest.fixture
def sql_result(sample_data: list[dict[str, Any]]) -> SQLResult:
    """Create an SQLResult with sample data."""
    stmt = SQL("SELECT * FROM users")
    return SQLResult(statement=stmt, data=sample_data, rows_affected=3)


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_sql_result_to_arrow(sql_result: SQLResult) -> None:
    """Test converting SQLResult to Arrow Table."""
    import pyarrow as pa

    table = sql_result.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert table.column_names == ["id", "name", "age"]


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_sql_result_to_arrow_empty_data() -> None:
    """Test to_arrow() with empty data list."""
    import pyarrow as pa

    stmt = SQL("SELECT * FROM users WHERE 1=0")
    result = SQLResult(statement=stmt, data=[])

    table = result.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 0


def test_sql_result_to_pandas(sql_result: SQLResult) -> None:
    """Test converting SQLResult to pandas DataFrame."""
    pandas = pytest.importorskip("pandas")

    df = sql_result.to_pandas()

    assert isinstance(df, pandas.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "age"]
    assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]


def test_sql_result_to_pandas_empty_data() -> None:
    """Test to_pandas() with empty data list."""
    pandas = pytest.importorskip("pandas")

    stmt = SQL("SELECT * FROM users WHERE 1=0")
    result = SQLResult(statement=stmt, data=[])

    df = result.to_pandas()

    assert isinstance(df, pandas.DataFrame)
    assert len(df) == 0


def test_sql_result_to_pandas_with_null_values() -> None:
    """Test to_pandas() correctly handles NULL values."""
    pandas = pytest.importorskip("pandas")

    data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": None},
        {"id": 3, "name": None, "email": "charlie@example.com"},
    ]
    stmt = SQL("SELECT * FROM users")
    result = SQLResult(statement=stmt, data=data)

    df = result.to_pandas()

    assert pandas.isna(df.loc[1, "email"])
    assert pandas.isna(df.loc[2, "name"])


def test_sql_result_to_polars(sql_result: SQLResult) -> None:
    """Test converting SQLResult to Polars DataFrame."""
    polars = pytest.importorskip("polars")

    df = sql_result.to_polars()

    assert isinstance(df, polars.DataFrame)
    assert len(df) == 3
    assert df.columns == ["id", "name", "age"]
    assert df["name"].to_list() == ["Alice", "Bob", "Charlie"]


def test_sql_result_to_polars_empty_data() -> None:
    """Test to_polars() with empty data list."""
    polars = pytest.importorskip("polars")

    stmt = SQL("SELECT * FROM users WHERE 1=0")
    result = SQLResult(statement=stmt, data=[])

    df = result.to_polars()

    assert isinstance(df, polars.DataFrame)
    assert len(df) == 0


def test_sql_result_to_polars_with_null_values() -> None:
    """Test to_polars() correctly handles NULL values."""
    pytest.importorskip("polars")

    data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": None},
        {"id": 3, "name": None, "email": "charlie@example.com"},
    ]
    stmt = SQL("SELECT * FROM users")
    result = SQLResult(statement=stmt, data=data)

    df = result.to_polars()

    assert df["email"][1] is None
    assert df["name"][2] is None


def test_sql_result_methods_with_none_data_raise() -> None:
    """Test that methods raise ValueError when data is None."""
    stmt = SQL("SELECT * FROM users")
    result = SQLResult(statement=stmt, data=None)

    with pytest.raises(ValueError, match="No data available"):
        result.to_pandas()

    with pytest.raises(ValueError, match="No data available"):
        result.to_polars()


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_sql_result_to_arrow_with_none_data_raises() -> None:
    """Test that to_arrow() raises ValueError when data is None."""
    stmt = SQL("SELECT * FROM users")
    result = SQLResult(statement=stmt, data=None)

    with pytest.raises(ValueError, match="No data available"):
        result.to_arrow()
