from __future__ import annotations

import anyio
import pytest

__all__ = ("test_async_connection",)


def test_async_connection() -> None:
    pytest.importorskip("aiosqlite")

    async def _run() -> None:
        # start-example
        from sqlspec import SQLSpec
        from sqlspec.adapters.aiosqlite import AiosqliteConfig

        spec = SQLSpec()
        config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

        async with spec.provide_session(config) as session:
            await session.execute("create table if not exists notes (id integer primary key, body text)")
            await session.execute("insert into notes (body) values (?)", "Hello, Async SQLSpec!")
            result = await session.execute("select id, body from notes")
            print(result.all())
        # end-example

        assert result.all() == [{"id": 1, "body": "Hello, Async SQLSpec!"}]

    anyio.run(_run)
