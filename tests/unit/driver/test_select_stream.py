"""Unit tests for the base driver ``select_stream`` API (native-only gating + eager fallback)."""

from dataclasses import dataclass
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError

_SELECT = "select id, name from items order by id"


@dataclass
class StreamItem:
    """Typed row target for stream schema conversion tests."""

    id: int
    name: str


def _seed_sync(session: Any) -> None:
    session.execute("create table items (id integer primary key, name text)")
    for i in range(25):
        session.execute("insert into items (id, name) values (:id, :name)", {"id": i, "name": f"n{i}"})


async def _seed_async(session: Any) -> None:
    await session.execute("create table items (id integer primary key, name text)")
    for i in range(25):
        await session.execute("insert into items (id, name) values (:id, :name)", {"id": i, "name": f"n{i}"})


def _fail_dispatch(*args: Any, **kwargs: Any) -> None:
    _ = (args, kwargs)
    msg = "dispatch_select_stream should not be called for invalid chunk_size"
    raise AssertionError(msg)


# --------------------------------------------------------------------------- #
# Sync (sqlite has no native path in this task -> base path is exercised)
# --------------------------------------------------------------------------- #


def test_sync_select_stream_native_only_without_native_stream_raises() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        with pytest.raises(ImproperConfigurationError) as excinfo:
            session.select_stream(_SELECT, native_only=True)
        message = str(excinfo.value)
        assert type(session).__name__ in message
        assert "native_only=False" in message


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_sync_select_stream_rejects_non_positive_chunk_size(
    chunk_size: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        monkeypatch.setattr(type(session), "dispatch_select_stream", _fail_dispatch)
        with pytest.raises(ValueError, match="chunk_size must be greater than or equal to 1"):
            session.select_stream(_SELECT, chunk_size=chunk_size, native_only=True)


def test_sync_select_stream_eager_fallback_yields_all_rows_in_order() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        expected = session.execute(_SELECT).get_data()
        with session.select_stream(_SELECT, chunk_size=10) as stream:
            streamed = list(stream)
    assert streamed == expected
    assert len(streamed) == 25


def test_sync_select_stream_eager_fallback_applies_schema_type() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        with session.select_stream(_SELECT, chunk_size=10, schema_type=StreamItem) as stream:
            streamed = list(stream)
    assert len(streamed) == 25
    assert all(isinstance(row, StreamItem) for row in streamed)
    assert [(row.id, row.name) for row in streamed[:3]] == [(0, "n0"), (1, "n1"), (2, "n2")]


def test_sync_select_stream_close_mid_iteration_stops() -> None:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        _seed_sync(session)
        stream = session.select_stream(_SELECT, chunk_size=10)
        iterator = iter(stream)
        first = next(iterator)
        stream.close()
        with pytest.raises(StopIteration):
            next(iterator)
    assert first["id"] == 0


# --------------------------------------------------------------------------- #
# Async (aiosqlite has no native path in this task -> base path is exercised)
# --------------------------------------------------------------------------- #


async def test_async_select_stream_native_only_without_native_stream_raises() -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    async with spec.provide_session(config) as session:
        await _seed_async(session)
        with pytest.raises(ImproperConfigurationError) as excinfo:
            session.select_stream(_SELECT, native_only=True)
        message = str(excinfo.value)
        assert type(session).__name__ in message
        assert "native_only=False" in message


@pytest.mark.parametrize("chunk_size", [0, -1])
async def test_async_select_stream_rejects_non_positive_chunk_size(
    chunk_size: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    async with spec.provide_session(config) as session:
        await _seed_async(session)
        monkeypatch.setattr(type(session), "dispatch_select_stream", _fail_dispatch)
        with pytest.raises(ValueError, match="chunk_size must be greater than or equal to 1"):
            session.select_stream(_SELECT, chunk_size=chunk_size, native_only=True)


async def test_async_select_stream_eager_fallback_yields_all_rows_in_order() -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    async with spec.provide_session(config) as session:
        await _seed_async(session)
        expected = (await session.execute(_SELECT)).get_data()
        async with session.select_stream(_SELECT, chunk_size=10) as stream:
            streamed = [row async for row in stream]
    assert streamed == expected
    assert len(streamed) == 25


async def test_async_select_stream_eager_fallback_applies_schema_type() -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    async with spec.provide_session(config) as session:
        await _seed_async(session)
        async with session.select_stream(_SELECT, chunk_size=10, schema_type=StreamItem) as stream:
            streamed = [row async for row in stream]
    assert len(streamed) == 25
    assert all(isinstance(row, StreamItem) for row in streamed)
    assert [(row.id, row.name) for row in streamed[:3]] == [(0, "n0"), (1, "n1"), (2, "n2")]
