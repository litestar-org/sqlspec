"""Open an asynchronous SQLite session with AiosqliteConfig and run a query."""

import asyncio
from typing import Any

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig


async def main() -> list[dict[str, Any]]:
    """Insert and read a note using an asynchronous SQLite session."""
    spec = SQLSpec()
    db_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    config = spec.add_config(db_config)

    try:
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
            rows = await session.select("SELECT id, body FROM notes")
            assert rows == [{"id": 1, "body": "Hello, Async SQLSpec"}]
            return rows
    finally:
        await config.close_pool()


# --8<-- [end:example]

__all__ = ("main",)


if __name__ == "__main__":
    print(asyncio.run(main()))
