"""Streams end with their session and fail closed afterwards."""

import inspect
import sqlite3
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver._routing import SyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError

_QUERY = "select x from t order by x"


@pytest.fixture(params=["sync", "async"])
def mode(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def database(tmp_path: Path) -> str:
    path = str(tmp_path / "streams.db")
    with sqlite3.connect(path) as connection:
        connection.execute("create table t (x text)")
        connection.executemany("insert into t values (?)", [("A0",), ("A1",), ("A2",)])
    return path


async def resolve(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class StreamExhausted(Exception):
    """Raised by next_row when a sync or async stream ends."""


async def next_row(stream: Any) -> Any:
    try:
        if hasattr(stream, "__anext__"):
            return await stream.__anext__()
        return next(stream)
    except (StopIteration, StopAsyncIteration) as exc:
        raise StreamExhausted from exc


async def drain(stream: Any) -> "list[Any]":
    rows = []
    while True:
        try:
            rows.append(await next_row(stream))
        except StreamExhausted:
            return rows


@asynccontextmanager
async def open_session(mode: str, database: str) -> "AsyncGenerator[Any, None]":
    if mode == "sync":
        config = SqliteConfig(connection_config={"database": database})
        try:
            with config.provide_session() as session:
                yield session
        finally:
            config.close_pool()
    else:
        async_config = AiosqliteConfig(connection_config={"database": database})
        try:
            async with async_config.provide_session() as session:
                yield session
        finally:
            await async_config.close_pool()


@pytest.mark.anyio
@pytest.mark.parametrize("lazy", [False, True], ids=["native", "eager_fallback"])
async def test_closed_driver_stream_raises(
    mode: str, database: str, lazy: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with open_session(mode, database) as session:
        if lazy:
            monkeypatch.setattr(type(session), "dispatch_select_stream", lambda *_args, **_kwargs: None)
        stream = session.select_stream(_QUERY, chunk_size=1)
        assert await next_row(stream) == {"x": "A0"}
        session._mark_closed()
        with pytest.raises(ImproperConfigurationError):
            await next_row(stream)


@pytest.mark.anyio
async def test_lazy_unstarted_stream_raises_after_close(
    mode: str, database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with open_session(mode, database) as session:
        monkeypatch.setattr(type(session), "dispatch_select_stream", lambda *_args, **_kwargs: None)
        stream = session.select_stream(_QUERY, chunk_size=1)
        session._mark_closed()
        with pytest.raises(ImproperConfigurationError):
            await next_row(stream)


@pytest.mark.anyio
async def test_user_closed_stream_still_stops_cleanly(mode: str, database: str) -> None:
    async with open_session(mode, database) as session:
        stream = session.select_stream(_QUERY, chunk_size=1)
        assert await next_row(stream) == {"x": "A0"}
        await resolve(stream.aclose() if mode == "async" else stream.close())
        session._mark_closed()
        assert await drain(stream) == []


@pytest.mark.anyio
async def test_registry_prunes_closed_streams(mode: str, database: str) -> None:
    async with open_session(mode, database) as session:
        for _ in range(5):
            assert len(await drain(session.select_stream(_QUERY, chunk_size=2))) == 3
        session.select_stream(_QUERY, chunk_size=2)
        assert len(session._open_streams) == 1


@pytest.mark.anyio
async def test_close_open_streams_is_idempotent(mode: str, database: str) -> None:
    async with open_session(mode, database) as session:
        stream = session.select_stream(_QUERY, chunk_size=1)
        assert await next_row(stream) == {"x": "A0"}
        await resolve(session._close_open_streams())
        await resolve(session._close_open_streams())
        with pytest.raises(ImproperConfigurationError):
            await next_row(stream)


def test_routing_stream_raises_after_inner_close(database: str) -> None:
    config = SqliteConfig(connection_config={"database": database}, replicas=[{"database": database}])
    try:
        with SyncRoutingSession(config) as session:
            stream = session.select_stream(_QUERY, chunk_size=1)
            assert next(stream) == {"x": "A0"}
            assert session.replica_session is not None
            session.replica_session._mark_closed()
            with pytest.raises(ImproperConfigurationError):
                next(stream)
    finally:
        config.close_pool()


@pytest.mark.anyio
async def test_started_stream_raises_after_session_exit(mode: str, database: str) -> None:
    async with open_session(mode, database) as session:
        stream = session.select_stream(_QUERY, chunk_size=1)
        assert await next_row(stream) == {"x": "A0"}
    with pytest.raises(ImproperConfigurationError):
        await next_row(stream)


@pytest.mark.anyio
async def test_second_session_never_sees_first_stream_rows(mode: str, database: str) -> None:
    seen: list[Any] = []
    async with open_session(mode, database) as first:
        stream = first.select_stream(_QUERY, chunk_size=1)
        seen.append(await next_row(stream))
    async with open_session(mode, database) as second:
        await resolve(second.begin())
        await resolve(second.execute("insert into t values ('B-uncommitted')"))
        with pytest.raises(ImproperConfigurationError):
            seen.extend(await drain(stream))
        await resolve(second.rollback())
    assert seen == [{"x": "A0"}]


@pytest.mark.anyio
async def test_exited_session_and_its_stream_refuse_work(mode: str, database: str) -> None:
    """After session exit the session and its started stream both raise instead of using the released connection."""
    async with open_session(mode, database) as session:
        stream = session.select_stream(_QUERY, chunk_size=1)
        assert await next_row(stream) == {"x": "A0"}
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        await resolve(session.select_value("select 1"))
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        await next_row(stream)
