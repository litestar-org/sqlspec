"""Synchronous session accessors on asynchronous configurations across the ASGI frameworks and Flask.

The ASGI plugins return a session that checks out the request connection on its first awaited call,
with the same tenant resolution and commit or rollback handling as the asynchronous accessors. Flask
returns a blocking session whose calls run on the plugin's portal.
"""

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from tests.unit.extensions._framework_apps import FRAMEWORKS, Access, serve
from tests.unit.extensions._tenant_configs import AsyncRecordingStrategy, TenantAiosqliteConfig


def _items_database(tmp_path: Path) -> str:
    database = str(tmp_path / "items.db")
    setup = sqlite3.connect(database)
    setup.execute("CREATE TABLE items (status INTEGER)")
    setup.commit()
    setup.close()
    return database


def _stored(database: str) -> list[int]:
    reader = sqlite3.connect(database)
    try:
        return [row[0] for row in reader.execute("SELECT status FROM items ORDER BY rowid")]
    finally:
        reader.close()


def _counting_config(database: str, framework: str, checkouts: list[int]) -> AiosqliteConfig:
    class CountingConfig(AiosqliteConfig):
        def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
            checkouts.append(1)
            return super().provide_connection(*args, **kwargs)

    return CountingConfig(
        connection_config={"database": database}, extension_config={framework: {"commit_mode": "autocommit"}}
    )


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_sync_session_accessor_opens_async_connection_on_first_await(framework: str, tmp_path: Path) -> None:
    database = _items_database(tmp_path)
    checkouts: list[int] = []
    config = _counting_config(database, framework, checkouts)

    async def nodb(access: Access, status: int) -> Any:
        session = access.session()
        return {"dialect": session.dialect, "same": access.session() is session, "checkouts": len(checkouts)}

    async def write(access: Access, status: int) -> Any:
        session = access.session()
        before = len(checkouts)
        await session.begin()
        await session.execute("INSERT INTO items VALUES (:status)", {"status": status})
        return {
            "before": before,
            "after": len(checkouts),
            "same": access.session() is session,
            "shares_connection": session.connection is access.connection(),
        }

    with serve(framework, config, {"nodb": nodb, "write": write}) as run:
        assert run("nodb", 200) == (200, {"dialect": "sqlite", "same": True, "checkouts": 0})
        assert checkouts == []
        written = {"same": True, "shares_connection": True}
        assert run("write", 201) == (201, {"before": 0, "after": 1, **written})
        assert run("write", 500) == (500, {"before": 1, "after": 2, **written})
    assert _stored(database) == [201]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_sync_session_accessor_streams_and_runs_transactions(framework: str, tmp_path: Path) -> None:
    database = _items_database(tmp_path)
    config = _counting_config(database, framework, [])

    async def stream(access: Access, status: int) -> Any:
        session = access.session()
        async with session.transaction():
            await session.execute("INSERT INTO items VALUES (:status)", {"status": 7})
        streamed = [row["status"] async for row in access.session().select_stream("SELECT status FROM items")]
        async with access.session().select_stream("SELECT status FROM items") as rows:
            entered = [row["status"] async for row in rows]
        return {"streamed": streamed, "entered": entered}

    with serve(framework, config, {"stream": stream}) as run:
        assert run("stream", 200) == (200, {"streamed": [7], "entered": [7]})
    assert _stored(database) == [7]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_sync_session_accessor_resolves_tenant_when_connection_opens(framework: str) -> None:
    strategy = AsyncRecordingStrategy()
    config = TenantAiosqliteConfig(
        connection_config={"database": ":memory:"}, driver_features={"tenant_strategy": strategy}
    )

    async def tenant(access: Access, status: int) -> Any:
        session = access.session()
        access.receipt("acme")
        return {"value": await session.select_value("SELECT 1")}

    with serve(framework, config, {"tenant": tenant}) as run:
        assert run("tenant", 200) == (200, {"value": 1})
    assert strategy.checkouts == ["acme"]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_connection_attributes_before_open_name_the_async_accessor(framework: str, tmp_path: Path) -> None:
    checkouts: list[int] = []
    config = _counting_config(_items_database(tmp_path), framework, checkouts)

    async def unopened(access: Access, status: int) -> Any:
        session = access.session()
        errors: list[str] = []
        reads: list[Callable[[], Any]] = [lambda: session.connection, access.connection]
        for read in reads:
            try:
                read()
            except ImproperConfigurationError as exc:
                errors.append(str(exc))
        await session.select_value("SELECT 1")
        return {"errors": errors, "open": session.connection is access.connection(), "checkouts": len(checkouts)}

    with serve(framework, config, {"unopened": unopened}) as run:
        status, body = run("unopened", 200)
    assert status == 200
    assert body["open"] is True
    assert body["checkouts"] == 1
    session_error, connection_error = body["errors"]
    assert "_session_async(" in session_error
    assert "_connection_async(" in connection_error
