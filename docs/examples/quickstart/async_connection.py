"""Open an asynchronous SQLite session with AiosqliteConfig and run a query."""

import asyncio
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig

__all__ = ("main",)


async def main() -> list[dict[str, Any]]:
    """Insert and read a note using an asynchronous SQLite session."""
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

    try:
        # --8<-- [start:example]
        async with spec.provide_session(config) as session:
            await session.execute(
                """
                CREATE TABLE notes (
                    id INTEGER PRIMARY KEY,
                    body TEXT
                )
                """
            )
            await session.execute(
                """
                INSERT INTO notes (body)
                VALUES (?)
                """,
                "Hello, Async SQLSpec",
            )
            rows = await session.select(
                """
                SELECT id, body
                FROM notes
                """
            )
            # --8<-- [end:example]
            assert rows == [{"id": 1, "body": "Hello, Async SQLSpec"}]
            return rows
    finally:
        await config.close_pool()


if __name__ == "__main__":
    print(asyncio.run(main()))
