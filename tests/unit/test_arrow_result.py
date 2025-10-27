"""Tests for ArrowResult convenience methods."""

from typing import Any, cast

import pytest

from sqlspec.core.statement import SQL
from sqlspec.typing import PYARROW_INSTALLED

pytestmark = pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")


@pytest.fixture
def sample_arrow_table():
    """Create a sample Arrow table for testing."""
    from typing import Any

    import pyarrow as pa

    data: dict[str, Any] = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}
    return pa.Table.from_pydict(data)


@pytest.fixture
def arrow_result(sample_arrow_table):
    """Create an ArrowResult with sample data."""
    from sqlspec.core.result import ArrowResult

    stmt = SQL("SELECT * FROM users")
    return ArrowResult(statement=stmt, data=sample_arrow_table, rows_affected=3)


def test_arrow_result_to_pandas(arrow_result) -> None:
    """Test converting ArrowResult to pandas DataFrame."""
    pandas = pytest.importorskip("pandas")

    df = arrow_result.to_pandas()

    assert isinstance(df, pandas.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "age"]
    assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]


def test_arrow_result_to_polars(arrow_result) -> None:
    """Test converting ArrowResult to Polars DataFrame."""
    polars = pytest.importorskip("polars")

    df = arrow_result.to_polars()

    assert isinstance(df, polars.DataFrame)
    assert len(df) == 3
    assert df.columns == ["id", "name", "age"]
    assert df["name"].to_list() == ["Alice", "Bob", "Charlie"]


def test_arrow_result_to_dict(arrow_result) -> None:
    """Test converting ArrowResult to list of dicts."""
    result = arrow_result.to_dict()

    assert isinstance(result, list)
    assert len(result) == 3
    assert result[0] == {"id": 1, "name": "Alice", "age": 30}
    assert result[1] == {"id": 2, "name": "Bob", "age": 25}
    assert result[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_arrow_result_len(arrow_result) -> None:
    """Test __len__() returns number of rows."""
    assert len(arrow_result) == 3


def test_arrow_result_iter(arrow_result) -> None:
    """Test __iter__() yields rows as dicts."""
    rows = list(arrow_result)

    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_arrow_result_iter_in_for_loop(arrow_result) -> None:
    """Test iterating over ArrowResult in for loop."""
    names = [row["name"] for row in arrow_result]

    assert names == ["Alice", "Bob", "Charlie"]


def test_arrow_result_to_pandas_with_null_values() -> None:
    """Test to_pandas() correctly handles NULL values."""
    pandas = pytest.importorskip("pandas")
    import pyarrow as pa

    from sqlspec.core.result import ArrowResult
    from sqlspec.core.statement import SQL

    data: dict[str, Any] = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", None],
        "email": ["alice@example.com", None, "charlie@example.com"],
    }
    table = pa.Table.from_pydict(data)
    stmt = SQL("SELECT * FROM users")
    result = ArrowResult(statement=stmt, data=table)

    df = result.to_pandas()

    assert pandas.isna(df.loc[1, "email"])
    assert pandas.isna(df.loc[2, "name"])


def test_arrow_result_empty_table() -> None:
    """Test ArrowResult methods with empty table."""
    import pyarrow as pa

    from sqlspec.core.result import ArrowResult
    from sqlspec.core.statement import SQL

    empty_table = pa.Table.from_pydict(cast(dict[str, Any], {}))
    stmt = SQL("SELECT * FROM users WHERE 1=0")
    result = ArrowResult(statement=stmt, data=empty_table)

    assert len(result) == 0
    assert result.to_dict() == []
    assert list(result) == []


def test_arrow_result_methods_with_none_data_raise() -> None:
    """Test that methods raise ValueError when data is None."""
    from sqlspec.core.result import ArrowResult
    from sqlspec.core.statement import SQL

    stmt = SQL("SELECT * FROM users")
    result = ArrowResult(statement=stmt, data=None)

    with pytest.raises(ValueError, match="No Arrow table available"):
        result.to_pandas()

    with pytest.raises(ValueError, match="No Arrow table available"):
        result.to_polars()

    with pytest.raises(ValueError, match="No Arrow table available"):
        result.to_dict()

    with pytest.raises(ValueError, match="No Arrow table available"):
        len(result)

    with pytest.raises(ValueError, match="No Arrow table available"):
        list(result)
