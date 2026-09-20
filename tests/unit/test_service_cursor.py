"""Service cursor paging and single-query contracts."""

from typing import Any

import pytest
from pydantic import BaseModel

from sqlspec.core import (
    SQL,
    CursorFilter,
    CursorKey,
    CursorPagination,
    InCollectionFilter,
    LimitOffsetFilter,
    OffsetPagination,
    OrderByFilter,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

_ROWS = [(i, i // 5, "a" if i % 2 else None) for i in range(25)]
_KEYS = [CursorKey("v", "desc"), CursorKey("id")]


async def _call(target: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    result = getattr(target, method)(*args, **kwargs)
    if hasattr(result, "__await__"):
        return await result
    return result


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("method", ["paginate", "paginate_cursor"])
async def test_walk_schema_and_single_query(
    async_mode: bool, method: str, sqlite_sync_driver: Any, aiosqlite_async_driver: Any
) -> None:
    driver = aiosqlite_async_driver if async_mode else sqlite_sync_driver
    service = SQLSpecAsyncService(session=driver) if async_mode else SQLSpecSyncService(session=driver)
    await _call(driver, "execute", "CREATE TABLE items(id INTEGER PRIMARY KEY, v INTEGER, n TEXT)")
    await _call(driver, "execute_many", "INSERT INTO items VALUES (?, ?, ?)", _ROWS)
    statements: list[str] = []
    await _call(driver.connection, "set_trace_callback", statements.append)
    expected = [row[0] for row in sorted(_ROWS, key=lambda row: (-row[1], row[0]))]
    page = await _call(service, method, "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10))
    assert len([sql for sql in statements if sql.startswith("SELECT")]) == 1
    forward = list(page.items)
    while page.next_cursor:
        page = await _call(service, method, "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10, page.next_cursor))
        forward.extend(page.items)
    assert [row["id"] for row in forward] == expected
    backward = list(page.items)
    while page.previous_cursor:
        page = await _call(service, method, "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10, page.previous_cursor))
        backward = list(page.items) + backward
    assert [row["id"] for row in backward] == expected

    class Item(BaseModel):
        n: str | None

    typed = await _call(service, method, "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10), schema_type=Item)
    assert all(isinstance(row, Item) for row in typed.items)
    assert typed.next_cursor == page.next_cursor
    assert [item.n for item in typed.items] == [row["n"] for row in page.items]
    next_typed = await _call(
        service, method, "SELECT id, v, n FROM items", CursorFilter(_KEYS, 10, typed.next_cursor), schema_type=Item
    )
    assert len(next_typed.items) == 10
    assert next_typed.previous_cursor


@pytest.mark.parametrize(
    "filters",
    [
        [CursorFilter([CursorKey("id")], 1), CursorFilter([CursorKey("id")], 2)],
        [CursorFilter([CursorKey("id")], 1), OrderByFilter("id")],
        [CursorFilter([CursorKey("id")], 1), LimitOffsetFilter(1, 0)],
    ],
)
def test_invalid_filters(sqlite_sync_driver: Any, filters: list[Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        SQLSpecSyncService(session=sqlite_sync_driver).paginate("SELECT id FROM users", *filters)


@pytest.mark.parametrize("extra", [OrderByFilter("id"), LimitOffsetFilter(1, 0), CursorFilter([CursorKey("id")], 2)])
def test_constructor_conflicts(sqlite_sync_driver: Any, extra: Any) -> None:
    with pytest.raises(ImproperConfigurationError):
        SQLSpecSyncService(session=sqlite_sync_driver).paginate(
            SQL("SELECT id FROM users", extra), CursorFilter([CursorKey("id")], 1)
        )


def test_cursor_runs_after_predicate(sqlite_sync_driver: Any) -> None:
    page = SQLSpecSyncService(session=sqlite_sync_driver).paginate(
        "SELECT name, COUNT(*) AS c FROM users GROUP BY name",
        CursorFilter([CursorKey("name")], 1),
        InCollectionFilter("id", [1]),
    )
    assert page.items == [{"name": "test", "c": 1}]


def test_positional_base_parameters(sqlite_sync_driver: Any) -> None:
    service = SQLSpecSyncService(session=sqlite_sync_driver)
    page = service.paginate("SELECT id FROM users WHERE id > ?", 0, CursorFilter([CursorKey("id")], 1))
    assert isinstance(page, CursorPagination)
    assert page.items == [{"id": 1}]
    page = service.paginate(
        "SELECT id FROM users WHERE id > ?", 0, CursorFilter([CursorKey("id")], 1, page.next_cursor)
    )
    assert page.items == [{"id": 2}]


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("method", ["paginate", "paginate_cursor"])
async def test_cursor_forwards_parameters_and_statement_config(
    async_mode: bool, method: str, sqlite_sync_driver: Any, aiosqlite_async_driver: Any
) -> None:
    driver = aiosqlite_async_driver if async_mode else sqlite_sync_driver
    service = SQLSpecAsyncService(session=driver) if async_mode else SQLSpecSyncService(session=driver)
    statements: list[str] = []
    await _call(driver.connection, "set_trace_callback", statements.append)

    def mark_query(sql: str, parameters: Any) -> tuple[str, Any]:
        return sql.replace("SELECT", "SELECT /* per-call */", 1), parameters

    page = await _call(
        service,
        method,
        "SELECT id FROM users WHERE id > :minimum",
        CursorFilter([CursorKey("id")], 1),
        minimum=0,
        statement_config=driver.statement_config.replace(output_transformer=mark_query),
    )
    assert page.items == [{"id": 1}]
    assert page.has_next
    assert len(statements) == 1
    assert "/* per-call */" in statements[0]


@pytest.mark.parametrize("async_mode", [False, True])
async def test_paginate_dispatch_and_pending_filters(
    async_mode: bool, sqlite_sync_driver: Any, aiosqlite_async_driver: Any
) -> None:
    driver = aiosqlite_async_driver if async_mode else sqlite_sync_driver
    service = SQLSpecAsyncService(session=driver) if async_mode else SQLSpecSyncService(session=driver)
    offset = await _call(service, "paginate", "SELECT id FROM users", LimitOffsetFilter(1, 0))
    assert isinstance(offset, OffsetPagination)
    assert offset.total == 2
    unfiltered = await _call(service, "paginate", "SELECT id FROM users")
    assert isinstance(unfiltered, OffsetPagination)
    assert len(unfiltered.items) == unfiltered.total == 2
    cursor_filter = CursorFilter("name", 1)
    statement = SQL("SELECT name, COUNT(*) AS c FROM users GROUP BY name", cursor_filter)
    page = await _call(service, "paginate", statement, InCollectionFilter("id", [1]))
    assert isinstance(page, CursorPagination)
    assert page.items == [{"name": "test", "c": 1}]
    assert statement.filters == [cursor_filter]
    with pytest.raises(ImproperConfigurationError, match="count_with_window"):
        await _call(service, "paginate", statement, count_with_window=True)
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        await _call(service, "paginate", statement, CursorFilter("name", 1))


@pytest.mark.parametrize("async_mode", [False, True])
async def test_explicit_pagination_modes_reject_before_query(
    async_mode: bool, sqlite_sync_driver: Any, aiosqlite_async_driver: Any
) -> None:
    driver = aiosqlite_async_driver if async_mode else sqlite_sync_driver
    service = SQLSpecAsyncService(session=driver) if async_mode else SQLSpecSyncService(session=driver)
    statements: list[str] = []
    await _call(driver.connection, "set_trace_callback", statements.append)
    for method, filters, kwargs in [
        ("paginate_cursor", [], {}),
        ("paginate_cursor", [LimitOffsetFilter(1, 0)], {}),
        ("paginate_cursor", [CursorFilter("id", 1)], {"count_with_window": True}),
        ("paginate_limit_offset", [CursorFilter("id", 1)], {}),
    ]:
        with pytest.raises(ImproperConfigurationError):
            await _call(service, method, "SELECT id FROM users", *filters, **kwargs)
    with pytest.raises(ImproperConfigurationError):
        await _call(service, "paginate_limit_offset", SQL("SELECT id FROM users", CursorFilter("id", 1)))
    assert statements == []
    offset = await _call(service, "paginate_limit_offset", "SELECT id FROM users", count_with_window=True)
    assert isinstance(offset, OffsetPagination)
    assert offset.total == len(offset.items) == 2
    cursor = await _call(service, "paginate_cursor", SQL("SELECT id FROM users", CursorFilter("id", 1)))
    assert isinstance(cursor, CursorPagination)
    assert cursor.items == [{"id": 1}]
