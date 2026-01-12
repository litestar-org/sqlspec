"""Tests for the SQLResult iteration functionality."""

from typing import Any, cast

import pytest

from sqlspec.core import SQL, ArrowResult, SQLResult, StackResult, create_sql_result
from sqlspec.typing import PYARROW_INSTALLED

pytestmark = pytest.mark.xdist_group("core")


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


@pytest.fixture
def sample_arrow_table():
    """Create a sample Arrow table for testing."""
    import pyarrow as pa

    data: dict[str, Any] = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}
    return pa.Table.from_pydict(data)


@pytest.fixture
def arrow_result(sample_arrow_table):
    """Create an ArrowResult with sample data."""
    stmt = SQL("SELECT * FROM users")
    return ArrowResult(statement=stmt, data=sample_arrow_table, rows_affected=3)


def test_sql_result_basic_iteration() -> None:
    """Test basic iteration over SQLResult rows."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=3)

    rows = list(result)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"
    assert rows[2]["name"] == "Charlie"


def test_sql_result_iteration_with_empty_data() -> None:
    """Test iteration when SQLResult has no data."""
    sql_stmt = SQL("SELECT * FROM empty_table")

    result = SQLResult(statement=sql_stmt, data=None, rows_affected=0)
    rows = list(result)
    assert len(rows) == 0

    result = SQLResult(statement=sql_stmt, data=[], rows_affected=0)
    rows = list(result)
    assert len(rows) == 0


def test_sql_result_iteration_with_list_comprehension() -> None:
    """Test that SQLResult works with list comprehensions."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=3)

    names = [row["name"] for row in result]
    assert names == ["Alice", "Bob", "Charlie"]

    ages = [row["age"] for row in result]
    assert ages == [25, 30, 35]


def test_sql_result_iteration_with_filtering() -> None:
    """Test that SQLResult works with filtering during iteration."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "age": 25, "active": True},
        {"id": 2, "name": "Bob", "age": 30, "active": False},
        {"id": 3, "name": "Charlie", "age": 35, "active": True},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=3)

    active_users = [row for row in result if row["active"]]
    assert len(active_users) == 2
    assert active_users[0]["name"] == "Alice"
    assert active_users[1]["name"] == "Charlie"


def test_sql_result_iteration_preserves_existing_functionality() -> None:
    """Test that iteration doesn't break existing SQLResult functionality."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)

    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Bob"
    assert result.get_count() == 2
    first = result.get_first()
    assert first is not None
    assert first["name"] == "Alice"
    assert not result.is_empty()

    for i, row in enumerate(result):
        assert row == result[i]


def test_sql_result_iteration_multiple_times() -> None:
    """Test that SQLResult can be iterated multiple times."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)

    first_iteration = list(result)
    assert len(first_iteration) == 2

    second_iteration = list(result)
    assert len(second_iteration) == 2
    assert first_iteration == second_iteration


def test_sql_result_iterator_protocol() -> None:
    """Test that SQLResult follows the iterator protocol correctly."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=1)

    iterator = iter(result)
    assert hasattr(iterator, "__next__")

    first_item = next(iterator)
    assert first_item == {"id": 1, "name": "Alice"}

    with pytest.raises(StopIteration):
        next(iterator)


def test_create_sql_result_iteration() -> None:
    """Test that create_sql_result function produces iterable results."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}]

    result = create_sql_result(statement=sql_stmt, data=test_data, rows_affected=1)

    rows = list(result)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_sql_result_get_data_with_schema_type() -> None:
    """Test SQLResult.get_data() with schema_type parameter."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str
        email: str

    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)

    # Without schema_type - returns dicts
    data_dicts = result.get_data()
    assert isinstance(data_dicts, list)
    assert len(data_dicts) == 2
    assert isinstance(data_dicts[0], dict)
    assert data_dicts[0]["name"] == "Alice"

    # With schema_type - returns User objects
    users = result.get_data(schema_type=User)
    assert isinstance(users, list)
    assert len(users) == 2
    assert isinstance(users[0], User)
    assert users[0].name == "Alice"
    assert users[0].email == "alice@example.com"
    assert users[1].name == "Bob"


def test_sql_result_get_data_with_typeddict() -> None:
    """Test SQLResult.get_data() with TypedDict schema_type."""
    from typing import TypedDict

    class UserDict(TypedDict):
        id: int
        name: str
        email: str

    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)

    # With TypedDict schema_type
    users = result.get_data(schema_type=UserDict)
    assert isinstance(users, list)
    assert len(users) == 2
    assert isinstance(users[0], dict)  # TypedDict is still a dict at runtime
    assert users[0]["name"] == "Alice"
    assert users[1]["name"] == "Bob"


def test_stack_result_from_sql_result() -> None:
    sql_stmt = SQL("SELECT * FROM users")
    sql_result = SQLResult(statement=sql_stmt, data=[{"id": 1}], rows_affected=1, metadata={"warning": "slow"})

    stack_result = StackResult.from_sql_result(sql_result)

    assert stack_result.rows_affected == 1
    assert stack_result.warning == "slow"
    assert stack_result.result is sql_result
    assert stack_result.get_result() is not None
    assert stack_result.get_result().get_data() == [{"id": 1}]


def test_stack_result_with_error_and_factory() -> None:
    sql_stmt = SQL("SELECT 1")
    sql_result = SQLResult(statement=sql_stmt, data=[{"value": 1}], rows_affected=1)
    stack_result = StackResult(result=sql_result)

    updated = stack_result.with_error(ValueError("boom"))
    assert updated.error is not None
    assert updated.result is sql_result
    assert list(updated.get_result()) == list(stack_result.get_result())

    failure = StackResult.from_error(RuntimeError("stack"))
    assert failure.is_error()
    assert failure.is_sql_result()
    assert failure.get_result().get_data() == []


def test_sql_result_all_with_schema_type() -> None:
    """Test SQLResult.all() with schema_type parameter."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str
        email: str

    sql_stmt = SQL("SELECT * FROM users")
    test_data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)
    users = result.all(schema_type=User)

    assert len(users) == 2
    assert isinstance(users[0], User)
    assert users[0].name == "Alice"
    assert users[1].name == "Bob"


def test_sql_result_one_with_schema_type() -> None:
    """Test SQLResult.one() with schema_type parameter."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str
        email: str

    sql_stmt = SQL("SELECT * FROM users WHERE id = 1")
    test_data = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=1)
    user = result.one(schema_type=User)

    assert isinstance(user, User)
    assert user.name == "Alice"
    assert user.email == "alice@example.com"


def test_sql_result_one_or_none_with_schema_type() -> None:
    """Test SQLResult.one_or_none() with schema_type parameter."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str
        email: str

    sql_stmt = SQL("SELECT * FROM users WHERE id = 1")
    test_data = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=1)
    user = result.one_or_none(schema_type=User)

    assert isinstance(user, User)
    assert user.name == "Alice"

    empty_result = SQLResult(statement=sql_stmt, data=[], rows_affected=0)
    none_user = empty_result.one_or_none(schema_type=User)
    assert none_user is None


def test_sql_result_get_first_with_schema_type() -> None:
    """Test SQLResult.get_first() with schema_type parameter."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str
        email: str

    sql_stmt = SQL("SELECT * FROM users")
    test_data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)
    user = result.get_first(schema_type=User)

    assert isinstance(user, User)
    assert user.name == "Alice"

    empty_result = SQLResult(statement=sql_stmt, data=[], rows_affected=0)
    none_user = empty_result.get_first(schema_type=User)
    assert none_user is None


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


def test_arrow_result_to_pandas(arrow_result: ArrowResult) -> None:
    """Test converting ArrowResult to pandas DataFrame."""
    pandas = pytest.importorskip("pandas")

    df = arrow_result.to_pandas()

    assert isinstance(df, pandas.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "age"]
    assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]


def test_arrow_result_to_polars(arrow_result: ArrowResult) -> None:
    """Test converting ArrowResult to Polars DataFrame."""
    polars = pytest.importorskip("polars")

    df = arrow_result.to_polars()

    assert isinstance(df, polars.DataFrame)
    assert len(df) == 3
    assert df.columns == ["id", "name", "age"]
    assert df["name"].to_list() == ["Alice", "Bob", "Charlie"]


def test_arrow_result_to_dict(arrow_result: ArrowResult) -> None:
    """Test converting ArrowResult to list of dicts."""
    result = arrow_result.to_dict()

    assert isinstance(result, list)
    assert len(result) == 3
    assert result[0] == {"id": 1, "name": "Alice", "age": 30}
    assert result[1] == {"id": 2, "name": "Bob", "age": 25}
    assert result[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_arrow_result_len(arrow_result: ArrowResult) -> None:
    """Test __len__() returns number of rows."""
    assert len(arrow_result) == 3


def test_arrow_result_iter(arrow_result: ArrowResult) -> None:
    """Test __iter__() yields rows as dicts."""
    rows = list(arrow_result)

    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_arrow_result_iter_in_for_loop(arrow_result: ArrowResult) -> None:
    """Test iterating over ArrowResult in for loop."""
    names = [row["name"] for row in arrow_result]

    assert names == ["Alice", "Bob", "Charlie"]


def test_arrow_result_to_pandas_with_null_values() -> None:
    """Test to_pandas() correctly handles NULL values."""
    pandas = pytest.importorskip("pandas")
    import pyarrow as pa

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

    empty_table = pa.Table.from_pydict(cast(dict[str, Any], {}))
    stmt = SQL("SELECT * FROM users WHERE 1=0")
    result = ArrowResult(statement=stmt, data=empty_table)

    assert len(result) == 0
    assert result.to_dict() == []
    assert list(result) == []


def test_arrow_result_methods_with_none_data_raise() -> None:
    """Test that methods raise ValueError when data is None."""
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
