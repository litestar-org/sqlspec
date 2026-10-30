# Transactions

Database reliability depends on clear, predictable transaction boundaries. SQLSpec provides explicit `begin`, `commit`, and `rollback` controls on every driver session, atomic `begin_transaction()` helpers on services, and configurable request commit modes in web framework integrations.

## Explicit `begin`, `commit`, and `rollback`

Call `session.begin()`, `session.commit()`, and `session.rollback()` (`await session.begin()`, `await session.commit()`, `await session.rollback()` on async drivers) to group multiple statements into an atomic unit of work:

```{ .python title="docs/examples/drivers/transaction_handling.py" }
--8<-- "docs/examples/drivers/transaction_handling.py:example"
```

If an unhandled exception escapes a `provide_session()` context while a transaction is active, SQLSpec rolls back uncommitted changes before returning the connection to the pool.

## Atomic Blocks with `begin_transaction`

When coordinating writes in domain services, `SQLSpecSyncService.begin_transaction()` and `SQLSpecAsyncService.begin_transaction()` open a session, start a transaction, commit automatically on normal exit, and roll back automatically if any exception is raised:

=== "Sync"

    ```python
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.service import SQLSpecSyncService

    config = SqliteConfig(connection_config={"database": ":memory:"})
    service = SQLSpecSyncService(config=config)

    with service.begin_transaction() as session:
        session.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        session.execute("INSERT INTO accounts (id, balance) VALUES (1, 500), (2, 200)")
        session.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
        session.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    ```

=== "Async"

    ```python
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.service import SQLSpecAsyncService

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    service = SQLSpecAsyncService(config=config)

    async with service.begin_transaction() as session:
        await session.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        await session.execute("INSERT INTO accounts (id, balance) VALUES (1, 500), (2, 200)")
        await session.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
        await session.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    ```

## Savepoints and Partial Rollbacks

Inside an outer transaction, standard SQL savepoints let you isolate optional or retryable operations without aborting earlier writes in the same transaction:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE tags (name TEXT PRIMARY KEY)")
    session.begin()
    session.execute("INSERT INTO tags (name) VALUES ('python')")
    session.execute("SAVEPOINT optional_tag")
    try:
        session.execute("INSERT INTO tags (name) VALUES ('python')")
    except SQLSpecError:
        session.execute("ROLLBACK TO SAVEPOINT optional_tag")
    session.execute("RELEASE SAVEPOINT optional_tag")
    session.commit()
```

Note dialect syntax differences when writing raw savepoint statements: SQL Server uses `SAVE TRANSACTION <name>` / `ROLLBACK TRANSACTION <name>`, and IBM Db2 requires `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS`.

## Exception Handling Around Compiled Drivers

When using `sqlspec[mypyc]` compiled wheels, avoid executing fallback database queries directly inside an `except:` block while an active Python exception context (`sys.exception()`) is set. Capture a boolean flag inside `except:`, exit the block so `PyErr_Occurred()` clears, and run the fallback query afterward:

```python
should_retry_fallback = False
try:
    session.execute("INSERT INTO audit_log (event) VALUES (:event)", event="login")
except Exception:
    should_retry_fallback = True

if should_retry_fallback:
    session.rollback()
```

## Framework Commit Modes

Web integrations (`Litestar`, `FastAPI`, `Starlette`, `Flask`, `Sanic`) support three request-scoped commit strategies:

- `manual` (default): Handlers or services call `commit()` explicitly; ideal for strict boundary control and read-heavy routes.
- `autocommit`: Commits automatically on `2xx` HTTP responses and rolls back on `4xx`/`5xx` responses or unhandled exceptions.
- `autocommit_include_redirect`: Commits on both `2xx` and `3xx` redirect responses (`301`–`308`), useful for HTML form and HTMX workflows.

## Recap

- Keep transactions as short as possible—never hold an open transaction across external HTTP calls or LLM generation requests.
- Use `service.begin_transaction()` for self-contained domain commands and `manual` commit mode when mixing read-only and write endpoints.
- On Cloud Spanner, wrap contended read-write mutations in `driver.run_in_transaction(...)` so `Aborted` (`409`) conflicts retry automatically.
