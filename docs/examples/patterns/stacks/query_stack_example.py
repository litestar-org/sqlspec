"""Demonstrate StatementStack usage across sync and async SQLite adapters."""

import asyncio
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementStack

__all__ = ("build_stack", "main", "run_async_example", "run_sync_example")

SCHEMA_SCRIPT = """
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, last_action TEXT);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    action TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL,
    role TEXT NOT NULL
);
"""


def build_stack(user_id: int, action: str) -> "StatementStack":
    """Add audit, update, and select operations to the stack."""
    return (
        StatementStack()
        .push_execute(
            "INSERT INTO audit_log (user_id, action) VALUES (:user_id, :action)", {"user_id": user_id, "action": action}
        )
        .push_execute(
            "UPDATE users SET last_action = :action WHERE id = :user_id", {"action": action, "user_id": user_id}
        )
        .push_execute("SELECT role FROM user_roles WHERE user_id = :user_id ORDER BY role", {"user_id": user_id})
    )


def _seed_sync_tables(session: "Any", user_id: int, roles: "tuple[str, ...]") -> None:
    """Create tables and seed sync demo data."""
    session.execute_script(SCHEMA_SCRIPT)
    session.execute(
        "INSERT INTO users (id, last_action) VALUES (:user_id, :action)", {"user_id": user_id, "action": "start"}
    )
    session.execute_many(
        "INSERT INTO user_roles (user_id, role) VALUES (:user_id, :role)",
        [{"user_id": user_id, "role": role} for role in roles],
    )


async def _seed_async_tables(session: "Any", user_id: int, roles: "tuple[str, ...]") -> None:
    """Create tables and seed async demo data."""
    await session.execute_script(SCHEMA_SCRIPT)
    await session.execute(
        "INSERT INTO users (id, last_action) VALUES (:user_id, :action)", {"user_id": user_id, "action": "start"}
    )
    await session.execute_many(
        "INSERT INTO user_roles (user_id, role) VALUES (:user_id, :role)",
        [{"user_id": user_id, "role": role} for role in roles],
    )


def run_sync_example() -> None:
    """Execute the stack with the synchronous SQLite adapter."""
    registry = SQLSpec()
    config = registry.add_config(SqliteConfig(pool_config={"database": ":memory:"}))
    with registry.provide_session(config) as session:
        _seed_sync_tables(session, 1, ("admin", "editor"))
        results = session.execute_stack(build_stack(user_id=1, action="sync-login"))
        audit_insert, user_update, role_select = results
        print("[sync] rows inserted:", audit_insert.rowcount)
        print("[sync] rows updated:", user_update.rowcount)
        if role_select.raw_result is not None:
            roles = [row["role"] for row in role_select.raw_result.data]
            print("[sync] roles:", roles)


def run_async_example() -> None:
    """Execute the stack with the asynchronous AioSQLite adapter."""

    async def _inner() -> None:
        registry = SQLSpec()
        config = registry.add_config(AiosqliteConfig(pool_config={"database": ":memory:"}))
        async with registry.provide_session(config) as session:
            await _seed_async_tables(session, 2, ("viewer",))
            results = await session.execute_stack(build_stack(user_id=2, action="async-login"))
            audit_insert, user_update, role_select = results
            print("[async] rows inserted:", audit_insert.rowcount)
            print("[async] rows updated:", user_update.rowcount)
            if role_select.raw_result is not None:
                roles = [row["role"] for row in role_select.raw_result.data]
                print("[async] roles:", roles)

    asyncio.run(_inner())


def main() -> None:
    """Run both sync and async StatementStack demonstrations."""
    run_sync_example()
    run_async_example()


if __name__ == "__main__":
    main()
