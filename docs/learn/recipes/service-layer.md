# Service Layer

`SQLSpecSyncService` and `SQLSpecAsyncService` (`sqlspec.service`) wrap an adapter configuration (or an already-checked-out driver session) with ergonomic helpers for single-row lookups (`get_one`), existence checks (`exists`), offset and cursor pagination (`paginate`, `paginate_limit_offset`, `paginate_cursor`), and atomic write blocks (`begin_transaction`).

## Full Sync and Async Service Example

When constructed with `config=`, a service checks out a short-lived pooled session automatically for standalone read helpers (`service.get_one(...)`, `service.exists(...)`, `service.paginate(...)`) and opens an explicit commit-or-rollback session inside `with service.begin_transaction() as session:`:

```{ .python title="docs/examples/drivers/service_config.py" hl_lines="19 23 27 28 29 31 57 61" }
--8<-- "docs/examples/drivers/service_config.py"
```

## Core Service Methods

| Method | Purpose |
| --- | --- |
| `get_one(statement, *filters, session=None, **params)` | Fetch a single row (or typed `schema_type` instance), raising `NotFoundError` when absent. |
| `exists(statement, *filters, session=None, **params)` | Return `bool` indicating whether at least one row matches the query. |
| `paginate(statement, *filters, session=None, **params)` | Dispatch automatically to limit/offset or cursor pagination based on attached filters. |
| `paginate_limit_offset(statement, filter, ...)` | Return an `OffsetPagination` envelope (`items`, `limit`, `offset`, `total`). |
| `paginate_cursor(statement, filter, ...)` | Return a `CursorPagination` envelope (`items`, `next_cursor`, `previous_cursor`). |
| `provide_session()` | Context manager yielding a pooled read/write driver session. |
| `begin_transaction()` | Context manager yielding a session inside an active transaction that commits on exit and rolls back on exception. |

## Structuring Domain Services

Subclass or compose `SQLSpecAsyncService` (or `SQLSpecSyncService`) around your domain's `.sql` file loader so route handlers express domain operations rather than SQL mechanics:

```python
import msgspec

from sqlspec import SQLFileLoader, sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.core import LimitOffsetFilter, OffsetPagination
from sqlspec.service import SQLSpecAsyncService


class Customer(msgspec.Struct, frozen=True, gc=False):
    id: int
    email: str
    tier: str


class CustomerService:
    def __init__(self, config: AiosqliteConfig, loader: SQLFileLoader) -> None:
        self._db = SQLSpecAsyncService(config=config, loader=loader)

    async def get_by_email(self, email: str) -> Customer:
        query = sql.select("id", "email", "tier").from_("customers").where_eq("email", email)
        row = await self._db.get_one(query)
        return msgspec.convert(row, type=Customer)

    async def list_customers(self, *, limit: int = 25, offset: int = 0) -> OffsetPagination[dict[str, object]]:
        query = sql.select("id", "email", "tier").from_("customers").order_by("id")
        return await self._db.paginate_limit_offset(query, LimitOffsetFilter(limit=limit, offset=offset))

    async def upgrade_tier(self, customer_id: int, new_tier: str) -> None:
        async with self._db.begin_transaction() as session:
            await session.execute(
                "UPDATE customers SET tier = :tier WHERE id = :id", tier=new_tier, id=customer_id
            )
            await session.execute(
                "INSERT INTO tier_audit (customer_id, new_tier) VALUES (:id, :tier)",
                id=customer_id,
                tier=new_tier,
            )
```

## Sharing an Existing Session Across Multiple Services

Pass `session=session` to `get_one`, `exists`, or `paginate` (or construct `SQLSpecAsyncService(session)`) when composing multiple domain services inside a single outer `begin_transaction()` block so every read and write participates in the same atomic transaction:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.service import SQLSpecSyncService

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
orders_svc = SQLSpecSyncService(config=config)
ledger_svc = SQLSpecSyncService(config=config)

with orders_svc.begin_transaction() as session:
    session.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    session.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, order_id INTEGER, amount INTEGER)")
    session.execute("INSERT INTO orders (id, total) VALUES (:id, :total)", id=101, total=2500)

    order = orders_svc.get_one("SELECT id, total FROM orders WHERE id = :id", session=session, id=101)
    session.execute(
        "INSERT INTO ledger (order_id, amount) VALUES (:order_id, :amount)",
        order_id=order["id"],
        amount=order["total"],
    )
    assert ledger_svc.exists("SELECT 1 FROM ledger WHERE order_id = :id", session=session, id=101)
```

## Recap

- Construct `SQLSpecSyncService(config=...)` or `SQLSpecAsyncService(config=...)` to eliminate repetitive `with config.provide_session()` boilerplate on single-statement reads.
- Group multi-statement domain mutations inside `service.begin_transaction()` for automatic commit on success and rollback on failure.
- Pass `session=session` when coordinating multiple domain services within one shared transaction.
