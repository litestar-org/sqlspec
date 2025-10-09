"""Tests for the SQLResult iteration functionality."""

from typing import Any

import pytest

from sqlspec.core.result import SQLResult, create_sql_result
from sqlspec.core.statement import SQL

pytestmark = pytest.mark.xdist_group("core")


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
