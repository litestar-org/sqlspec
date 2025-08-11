"""Tests for the SQLResult iteration functionality."""

from typing import Any

import pytest

from sqlspec.core.result import SQLResult, create_sql_result
from sqlspec.core.statement import SQL


def test_sql_result_basic_iteration() -> None:
    """Test basic iteration over SQLResult rows."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=3)

    # Test basic iteration
    rows = list(result)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"
    assert rows[2]["name"] == "Charlie"


def test_sql_result_iteration_with_empty_data() -> None:
    """Test iteration when SQLResult has no data."""
    sql_stmt = SQL("SELECT * FROM empty_table")

    # Test with None data
    result = SQLResult(statement=sql_stmt, data=None, rows_affected=0)
    rows = list(result)
    assert len(rows) == 0

    # Test with empty list
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

    # Test list comprehension
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

    # Test filtering during iteration
    active_users = [row for row in result if row["active"]]
    assert len(active_users) == 2
    assert active_users[0]["name"] == "Alice"
    assert active_users[1]["name"] == "Charlie"


def test_sql_result_iteration_preserves_existing_functionality() -> None:
    """Test that iteration doesn't break existing SQLResult functionality."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)

    # Test that existing methods still work after adding iteration
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Bob"
    assert result.get_count() == 2
    first = result.get_first()
    assert first is not None
    assert first["name"] == "Alice"
    assert not result.is_empty()

    # Test that iteration works alongside existing methods
    for i, row in enumerate(result):
        assert row == result[i]


def test_sql_result_iteration_multiple_times() -> None:
    """Test that SQLResult can be iterated multiple times."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=2)

    # First iteration
    first_iteration = list(result)
    assert len(first_iteration) == 2

    # Second iteration should work the same
    second_iteration = list(result)
    assert len(second_iteration) == 2
    assert first_iteration == second_iteration


def test_sql_result_iterator_protocol() -> None:
    """Test that SQLResult follows the iterator protocol correctly."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}]

    result = SQLResult(statement=sql_stmt, data=test_data, rows_affected=1)

    # Test that iter() returns an iterator
    iterator = iter(result)
    assert hasattr(iterator, "__next__")

    # Test that we can get items from the iterator
    first_item = next(iterator)
    assert first_item == {"id": 1, "name": "Alice"}

    # Test that StopIteration is raised when no more items
    with pytest.raises(StopIteration):
        next(iterator)


def test_create_sql_result_iteration() -> None:
    """Test that create_sql_result function produces iterable results."""
    sql_stmt = SQL("SELECT * FROM users")
    test_data: list[dict[str, Any]] = [{"id": 1, "name": "Alice"}]

    result = create_sql_result(statement=sql_stmt, data=test_data, rows_affected=1)

    # Test that factory function creates properly iterable result
    rows = list(result)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
