# Transactions

Database reliability depends on clear, predictable transaction boundaries. SQLSpec provides a context manager (`session.transaction()`), explicit `begin`, `commit`, and `rollback` controls, portable savepoint methods (`create_savepoint`, `rollback_to_savepoint`, `release_savepoint`), atomic `begin_transaction()` helpers on services, and configurable request commit modes in web framework integrations.

## Transaction Context Manager

The recommended way to manage transactions on a driver session is `with session.transaction():` (`async with session.transaction():` on async drivers). Entering `session.transaction()` begins a transaction, commits automatically on normal exit, and rolls back if an exception is raised. When nested inside an already-active transaction (or `service.begin_transaction()`), `session.transaction()` automatically creates, releases, or rolls back a savepoint:

=== "Sync"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.exceptions import SQLSpecError

    spec = SQLSpec()
    config = spec.add_config(
        SqliteConfig(connection_config={"database": ":memory:"})
    )

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE tags (name TEXT PRIMARY KEY)")
        with session.transaction():
            session.execute("INSERT INTO tags (name) VALUES ('python')")
            try:
                with session.transaction():
                    session.execute("INSERT INTO tags (name) VALUES ('python')")
            except SQLSpecError:
                pass
        rows = session.select("SELECT name FROM tags")
        assert rows == [{"name": "python"}]
    ```

=== "Async"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.exceptions import SQLSpecError

    spec = SQLSpec()
    config = spec.add_config(
        AiosqliteConfig(connection_config={"database": ":memory:"})
    )

    async with spec.provide_session(config) as session:
        await session.execute("CREATE TABLE tags (name TEXT PRIMARY KEY)")
        async with session.transaction():
            await session.execute("INSERT INTO tags (name) VALUES ('python')")
            try:
                async with session.transaction():
                    await session.execute(
                        "INSERT INTO tags (name) VALUES ('python')"
                    )
            except SQLSpecError:
                pass
    ```

## Explicit Begin, Commit, and Rollback

When you need imperative control across conditionals or loops, call `session.begin()`, `session.commit()`, and `session.rollback()` (`await session.begin()`, `await session.commit()`, `await session.rollback()` on async drivers):

```{ .python title="docs/examples/drivers/transaction_handling.py" }
--8<-- "docs/examples/drivers/transaction_handling.py:example"
```

If an unhandled exception escapes a `provide_session()` context while a transaction is active, SQLSpec rolls back uncommitted changes before returning the connection to the pool.

## Service-Layer Atomic Blocks

When coordinating writes in domain services, `SQLSpecSyncService.begin_transaction()` and `SQLSpecAsyncService.begin_transaction()` open a session, start a transaction, commit automatically on normal exit, and roll back automatically if any exception is raised:

=== "Sync"

    ```python
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.service import SQLSpecSyncService

    config = SqliteConfig(connection_config={"database": ":memory:"})
    service = SQLSpecSyncService(config=config)

    with service.begin_transaction() as session:
        session.execute(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)"
        )
        session.execute(
            "INSERT INTO accounts (id, balance) VALUES (1, 500), (2, 200)"
        )
        session.execute(
            "UPDATE accounts SET balance = balance - 100 WHERE id = 1"
        )
        session.execute(
            "UPDATE accounts SET balance = balance + 100 WHERE id = 2"
        )
    ```

=== "Async"

    ```python
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.service import SQLSpecAsyncService

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    service = SQLSpecAsyncService(config=config)

    async with service.begin_transaction() as session:
        await session.execute(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)"
        )
        await session.execute(
            "INSERT INTO accounts (id, balance) VALUES (1, 500), (2, 200)"
        )
        await session.execute(
            "UPDATE accounts SET balance = balance - 100 WHERE id = 1"
        )
        await session.execute(
            "UPDATE accounts SET balance = balance + 100 WHERE id = 2"
        )
    ```

## Savepoints and Partial Rollbacks

In addition to automatic nested savepoints via `with session.transaction():`, every driver exposes explicit savepoint methods (`create_savepoint`, `rollback_to_savepoint`, and `release_savepoint`; `await` on async drivers) that validate the savepoint identifier and emit the dialect's savepoint syntax:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError

spec = SQLSpec()
config = spec.add_config(
    SqliteConfig(connection_config={"database": ":memory:"})
)

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE tags (name TEXT PRIMARY KEY)")
    with session.transaction():
        session.execute("INSERT INTO tags (name) VALUES ('python')")
        session.create_savepoint("optional_tag")
        try:
            session.execute("INSERT INTO tags (name) VALUES ('python')")
        except SQLSpecError:
            session.rollback_to_savepoint("optional_tag")
        session.release_savepoint("optional_tag")
```

Note dialect syntax differences if you ever write raw savepoint SQL: SQL Server uses `SAVE TRANSACTION <name>` / `ROLLBACK TRANSACTION <name>`, and IBM Db2 requires `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS`.

## Exception Handling Around Compiled Drivers

When using `sqlspec[mypyc]` compiled wheels, avoid executing fallback database queries directly inside an `except:` block while an active Python exception context (`sys.exception()`) is set. Capture a boolean flag inside `except:`, exit the block so `PyErr_Occurred()` clears, and run the fallback query afterward:

```python
should_retry_fallback = False
try:
    session.execute(
        "INSERT INTO audit_log (event) VALUES (:event)", event="login"
    )
except Exception:
    should_retry_fallback = True

if should_retry_fallback:
    session.rollback()
```

## Framework Commit Modes

Web integrations (`Litestar`, `FastAPI`, `Starlette`, `Flask`, `Sanic`) support these request-scoped commit modes:

- `manual` (default): Handlers or services call `commit()` explicitly; ideal for strict boundary control and read-heavy routes.
- `autocommit`: Commits automatically on `2xx` HTTP responses and rolls back on `4xx`/`5xx` responses or unhandled exceptions.
- `autocommit_include_redirect`: Commits on both `2xx` and `3xx` redirect responses (`301`–`308`), useful for HTML form and HTMX workflows.

## Recap

- Prefer `with session.transaction():` / `async with session.transaction():` (including nested savepoint blocks) or `service.begin_transaction()` for atomic writes.
- Use `session.create_savepoint(name)`, `session.rollback_to_savepoint(name)`, and `session.release_savepoint(name)` when managing named savepoints imperatively.
- Keep transactions short—never hold an open transaction across external HTTP calls or LLM generation requests.
