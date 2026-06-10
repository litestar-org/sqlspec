"""Unit tests for the base driver ``select_stream`` API (capability gating + eager fallback)."""

from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError

_SELECT = "select id, name from items order by id"


def _seed_sync(session: Any) -> None:
    session.execute("create table items (id integer primary key, name text)")
    for i in range(25):
        session.execute("insert into items (id, name) values (:id, :name)", {"id": i, "name": f"n{i}"})


async def _seed_async(session: Any) -> None:
    await session.execute("create table items (id integer primary key, name text)")
    for i in range(25):
        await session.execute("insert into items (id, name) values (:id, :name)", {"id": i, "name": f"n{i}"})


# --------------------------------------------------------------------------- #
# Sync (sqlite has no native path in this task -> base path is exercised)
# --------------------------------------------------------------------------- #


def test_sync_select_stream_without_fallback_raises() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        with pytest.raises(ImproperConfigurationError) as excinfo:
            session.select_stream(_SELECT)
        message = str(excinfo.value)
        assert type(session).__name__ in message
        assert "allow_eager_fallback" in message


def test_sync_select_stream_eager_fallback_yields_all_rows_in_order() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        expected = session.execute(_SELECT).get_data()
        with session.select_stream(_SELECT, chunk_size=10, allow_eager_fallback=True) as stream:
            streamed = list(stream)
    assert streamed == expected
    assert len(streamed) == 25


def test_sync_select_stream_close_mid_iteration_stops() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        stream = session.select_stream(_SELECT, chunk_size=10, allow_eager_fallback=True)
        iterator = iter(stream)
        first = next(iterator)
        stream.close()
        with pytest.raises(StopIteration):
            next(iterator)
    assert first["id"] == 0


# --------------------------------------------------------------------------- #
# Async (aiosqlite has no native path in this task -> base path is exercised)
# --------------------------------------------------------------------------- #


async def test_async_select_stream_without_fallback_raises() -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    async with spec.provide_session(config) as session:
        await _seed_async(session)
        with pytest.raises(ImproperConfigurationError) as excinfo:
            session.select_stream(_SELECT)
        message = str(excinfo.value)
        assert type(session).__name__ in message
        assert "allow_eager_fallback" in message


async def test_async_select_stream_eager_fallback_yields_all_rows_in_order() -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    async with spec.provide_session(config) as session:
        await _seed_async(session)
        expected = (await session.execute(_SELECT)).get_data()
        async with session.select_stream(_SELECT, chunk_size=10, allow_eager_fallback=True) as stream:
            streamed = [row async for row in stream]
    assert streamed == expected
    assert len(streamed) == 25
