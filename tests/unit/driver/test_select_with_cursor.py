"""Driver cursor paging and single-query contracts."""

from dataclasses import dataclass
from typing import Any

import pytest

from sqlspec.core import SQL, CursorFilter, CursorKey, InCollectionFilter, LimitOffsetFilter, OrderByFilter
from sqlspec.exceptions import ImproperConfigurationError

_ROWS = [(i, i // 5, "a" if i % 2 else None) for i in range(25)]
_KEYS = [CursorKey("v", "desc"), CursorKey("id")]


async def _call(driver: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(driver, method)(*args, **kwargs)
    if hasattr(result, "__await__"):
        return await result
    return result


@pytest.mark.parametrize("async_mode", [False, True])
async def test_walk_schema_and_single_query(
    async_mode: bool, sqlite_sync_driver: Any, aiosqlite_async_driver: Any
) -> None:
    driver = aiosqlite_async_driver if async_mode else sqlite_sync_driver
    await _call(driver, "execute", "CREATE TABLE items(id INTEGER PRIMARY KEY, v INTEGER, n TEXT)")
    await _call(driver, "execute_many", "INSERT INTO items VALUES (?, ?, ?)", _ROWS)
    statements: list[str] = []
    await _call(driver.connection, "set_trace_callback", statements.append)
    expected = [row[0] for row in sorted(_ROWS, key=lambda row: (-row[1], row[0]))]
    page = await _call(driver, "select_with_cursor", "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10))
    assert len([sql for sql in statements if sql.startswith("SELECT")]) == 1
    forward = list(page.items)
    while page.next_cursor:
        page = await _call(
            driver, "fetch_with_cursor", "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10, page.next_cursor)
        )
        forward.extend(page.items)
    assert [row["id"] for row in forward] == expected
    backward = list(page.items)
    while page.previous_cursor:
        page = await _call(
            driver, "select_with_cursor", "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10, page.previous_cursor)
        )
        backward = list(page.items) + backward
    assert [row["id"] for row in backward] == expected

    @dataclass
    class Item:
        id: int
        v: int
        n: str | None

    typed = await _call(
        driver, "select_with_cursor", "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10), schema_type=Item
    )
    assert all(isinstance(row, Item) for row in typed.items)
    assert typed.next_cursor


@pytest.mark.parametrize(
    "filters",
    [
        [],
        [CursorFilter([CursorKey("id")], 1), CursorFilter([CursorKey("id")], 2)],
        [CursorFilter([CursorKey("id")], 1), OrderByFilter("id")],
        [CursorFilter([CursorKey("id")], 1), LimitOffsetFilter(1, 0)],
    ],
)
def test_invalid_filters(sqlite_sync_driver: Any, filters: list[Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        sqlite_sync_driver.select_with_cursor("SELECT id FROM users", *filters)


@pytest.mark.parametrize("extra", [OrderByFilter("id"), LimitOffsetFilter(1, 0), CursorFilter([CursorKey("id")], 2)])
def test_constructor_conflicts(sqlite_sync_driver: Any, extra: Any) -> None:
    with pytest.raises(ImproperConfigurationError):
        sqlite_sync_driver.select_with_cursor(SQL("SELECT id FROM users", extra), CursorFilter([CursorKey("id")], 1))


def test_cursor_runs_after_predicate(sqlite_sync_driver: Any) -> None:
    page = sqlite_sync_driver.select_with_cursor(
        "SELECT name, COUNT(*) AS c FROM users GROUP BY name",
        CursorFilter([CursorKey("name")], 1),
        InCollectionFilter("id", [1]),
    )
    assert page.items == [{"name": "test", "c": 1}]


def test_positional_base_parameters(sqlite_sync_driver: Any) -> None:
    page = sqlite_sync_driver.select_with_cursor(
        "SELECT id FROM users WHERE id > ?", 0, CursorFilter([CursorKey("id")], 1)
    )
    assert page.items == [{"id": 1}]
    page = sqlite_sync_driver.select_with_cursor(
        "SELECT id FROM users WHERE id > ?", 0, CursorFilter([CursorKey("id")], 1, page.next_cursor)
    )
    assert page.items == [{"id": 2}]
