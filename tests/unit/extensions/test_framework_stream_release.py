"""Request sessions close their open row streams before the request connection is released."""

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from tests.unit.extensions._framework_apps import FRAMEWORKS, Access, serve

QUERY = "SELECT status FROM items ORDER BY status"


class SpyConnectionContext:
    """Connection context that records whether the request stream was closed when it exits."""

    def __init__(self, wrapped: Any, observed: "list[bool]", streams: "list[Any]") -> None:
        self._wrapped = wrapped
        self._observed = observed
        self._streams = streams

    async def __aenter__(self) -> Any:
        return await self._wrapped.__aenter__()

    async def __aexit__(self, *exc_info: Any) -> Any:
        self._observed.append(real_stream(self._streams[0])._closed)
        return await self._wrapped.__aexit__(*exc_info)


def real_stream(stream: Any) -> Any:
    for attribute in ("_target", "_resource"):
        if hasattr(stream, attribute) and getattr(stream, attribute) is not None:
            return getattr(stream, attribute)
    return stream


def build_database(tmp_path: Path) -> str:
    database = str(tmp_path / "streams.db")
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE items (status INTEGER)")
    connection.executemany("INSERT INTO items VALUES (?)", [(1,), (2,), (3,)])
    connection.commit()
    connection.close()
    return database


def spy_config(database: str, framework: str, observed: "list[bool]", streams: "list[Any]") -> AiosqliteConfig:
    class SpyConfig(AiosqliteConfig):
        def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
            return SpyConnectionContext(super().provide_connection(*args, **kwargs), observed, streams)

    return SpyConfig(connection_config={"database": database}, extension_config={framework: {"commit_mode": "manual"}})


async def next_row(stream: Any) -> Any:
    return await stream.__anext__()


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_request_stream_closed_before_release(framework: str, tmp_path: Path) -> None:
    observed: list[bool] = []
    streams: list[Any] = []
    config = spy_config(build_database(tmp_path), framework, observed, streams)

    async def read_one(access: Access, status: int) -> Any:
        stream = access.session().select_stream(QUERY, chunk_size=1)
        row = await next_row(stream)
        streams.append(stream)
        return {"status": row["status"]}

    with serve(framework, config, {"read_one": read_one}) as run:
        assert run("read_one", 200) == (200, {"status": 1})
    assert observed == [True]
    stream = streams[0]
    with pytest.raises(ImproperConfigurationError):
        asyncio.run(next_row(stream))


def test_flask_async_config_stream_closed_through_portal(tmp_path: Path) -> None:
    from flask import Flask

    from sqlspec.extensions.flask import SQLSpecPlugin

    observed: list[bool] = []
    streams: list[Any] = []
    config = spy_config(build_database(tmp_path), "flask", observed, streams)
    app = Flask(__name__)
    plugin = SQLSpecPlugin(config, app)

    @app.get("/read")
    def read() -> Any:
        stream = plugin.get_session().select_stream(QUERY, chunk_size=1)
        row = next(stream)
        streams.append(stream)
        return {"status": row["status"]}

    try:
        assert app.test_client().get("/read").json == {"status": 1}
        assert observed == [True]
        with pytest.raises(ImproperConfigurationError):
            next(streams[0])
    finally:
        plugin.shutdown()
