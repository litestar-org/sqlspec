import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementStack


def build_stack(user_id: int, action: str) -> "StatementStack":
    stack = (
        StatementStack()
        .push_execute(
            "INSERT INTO audit_log (user_id, action) VALUES (:user_id, :action)", {"user_id": user_id, "action": action}
        )
        .push_execute(
            "UPDATE users SET last_action = :action WHERE id = :user_id", {"action": action, "user_id": user_id}
        )
        .push_execute("SELECT role FROM user_roles WHERE user_id = :user_id ORDER BY role", {"user_id": user_id})
    )
    return stack


def run_sync_example() -> None:
    sql = SQLSpec()
    config = SqliteConfig(pool_config={"database": ":memory:"})
    registry = sql.add_config(config)

    with sql.provide_session(registry) as session:
        session.execute_script(
            """
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
            INSERT INTO users (id, last_action) VALUES (1, 'start');
            INSERT INTO user_roles (user_id, role) VALUES (1, 'admin'), (1, 'editor');
            """
        )

        stack = build_stack(user_id=1, action="sync-login")
        results = session.execute_stack(stack)

        audit_insert, user_update, role_select = results
        print("[sync] rows inserted:", audit_insert.rowcount)
        print("[sync] rows updated:", user_update.rowcount)
        if role_select.raw_result is not None:
            print("[sync] roles:", [row["role"] for row in role_select.raw_result.data])


def run_async_example() -> None:
    async def _inner() -> None:
        sql = SQLSpec()
        config = AiosqliteConfig(pool_config={"database": ":memory:"})
        registry = sql.add_config(config)

        async with sql.provide_session(registry) as session:
            await session.execute_script(
                """
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
                INSERT INTO users (id, last_action) VALUES (2, 'start');
                INSERT INTO user_roles (user_id, role) VALUES (2, 'viewer');
                """
            )

            stack = build_stack(user_id=2, action="async-login")
            results = await session.execute_stack(stack, continue_on_error=False)

            audit_insert, user_update, role_select = results
            print("[async] rows inserted:", audit_insert.rowcount)
            print("[async] rows updated:", user_update.rowcount)
            if role_select.raw_result is not None:
                print("[async] roles:", [row["role"] for row in role_select.raw_result.data])

    asyncio.run(_inner())


def main() -> None:
    run_sync_example()
    run_async_example()


if __name__ == "__main__":
    main()
