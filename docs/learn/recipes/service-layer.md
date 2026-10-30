# Service Layer

`SQLSpecSyncService` and `SQLSpecAsyncService` (`sqlspec.service`) wrap an adapter configuration (or an already-checked-out driver session) with ergonomic helpers for single-row lookups (`get_one`), existence checks (`exists`), offset and cursor pagination (`paginate`, `paginate_limit_offset`, `paginate_cursor`), and atomic write blocks (`begin_transaction`).

## Full Sync and Async Service Example

When constructed with `config=`, a service checks out a short-lived pooled session automatically for standalone read helpers (`service.get_one(...)`, `service.exists(...)`, `service.paginate(...)`) and opens an explicit commit-or-rollback session inside `with service.begin_transaction() as session:`:

```python
--8 < --"docs/examples/drivers/service_config.py:example"
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

## Production Service Architecture Patterns

In real-world architectures, services fall into two lifetime patterns:

### Pattern 1: Bounded-Session Domain Services (`config=` + `provide_session()`)

When a service coordinates external asynchronous I/O (such as password hashing, third-party REST APIs, or LLM embeddings), do not keep a database connection checked out during the entire operation. Instead, initialize the service with `config=` and check out short-lived sessions only when executing database commands:

```python
"""Bounded-session customer domain service using AsyncpgConfig and SQLFileLoader."""

from collections.abc import Sequence
import msgspec

from sqlspec import SQLFileLoader, StatementStack
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.core import LimitOffsetFilter, OffsetPagination
from sqlspec.service import SQLSpecAsyncService


class Customer(msgspec.Struct, frozen=True, gc=False):
    """Customer entity representation."""

    id: int
    email: str
    tier: str


class CustomerService(SQLSpecAsyncService[AsyncpgDriver]):
    """Domain service managing customer accounts and tier lifecycle."""

    def __init__(self, config: AsyncpgConfig, loader: SQLFileLoader) -> None:
        super().__init__(config=config, loader=loader)

    async def get_by_email(self, email: str) -> Customer:
        """Fetch customer by email, checking out a short-lived connection."""
        query = self.get_sql("get-customer-by-email").where("email = :email")
        return await self.get_one(query, email=email, schema_type=Customer)

    async def list_customers(self, *, limit: int = 25, offset: int = 0) -> OffsetPagination[Customer]:
        """Paginate customers without holding a connection between pages."""
        query = self.get_sql("list-customers")
        return await self.paginate_limit_offset(
            query, LimitOffsetFilter(limit=limit, offset=offset), schema_type=Customer
        )

    async def upgrade_tier_with_audit(self, customer_id: int, new_tier: str, audit_events: Sequence[str]) -> None:
        """Execute atomic multi-table updates inside an explicit transaction."""
        stack = (
            StatementStack()
            .push_execute(
                """
                UPDATE customers
                SET tier = :tier
                WHERE id = :id
                """,
                tier=new_tier,
                id=customer_id,
            )
            .push_execute_many(
                """
                INSERT INTO tier_audit (customer_id, new_tier, event_type)
                VALUES (:id, :tier, :event)
                """,
                [{"id": customer_id, "tier": new_tier, "event": evt} for evt in audit_events],
            )
        )
        async with self.begin_transaction() as session:
            await session.execute_stack(stack)
```

### Pattern 2: Request-Scoped Driver Services (`session=driver`)

When using dependency injection frameworks like Dishka or Litestar where a driver connection is already checked out for the lifetime of an HTTP request, inject the `driver` directly into `SQLSpecAsyncService(session)`:

```python
"""Request-scoped product service receiving an injected driver instance."""

import msgspec

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import FilterTypes, OffsetPagination
from sqlspec.service import SQLSpecAsyncService


class Product(msgspec.Struct, frozen=True, gc=False):
    """Product domain entity."""

    id: int
    sku: str
    name: str
    price_cents: int


class ProductService(SQLSpecAsyncService[AsyncpgDriver]):
    """Request-scoped service operating on a checked-out driver session."""

    def __init__(self, driver: AsyncpgDriver, spec: SQLSpec) -> None:
        super().__init__(driver)
        self.spec = spec

    async def get_by_id(self, product_id: int) -> Product:
        """Retrieve a product by ID or raise NotFoundError."""
        query = self.spec.get_sql("get-product").where("id = :id")
        return await self.get_one(query, id=product_id, schema_type=Product)

    async def list_products(self, *filters: FilterTypes) -> OffsetPagination[Product]:
        """Filter and paginate products using precompiled SQL templates."""
        query = self.spec.get_sql("list-products")
        return await self.paginate(query, *filters, schema_type=Product)
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
    session.execute(
        """
        CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)
        """
    )
    session.execute(
        """
        CREATE TABLE ledger (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            amount INTEGER
        )
        """
    )
    session.execute(
        """
        INSERT INTO orders (id, total)
        VALUES (:id, :total)
        """,
        id=101,
        total=2500,
    )

    order = orders_svc.get_one(
        """
        SELECT id, total
        FROM orders
        WHERE id = :id
        """,
        session=session,
        id=101,
    )
    session.execute(
        """
        INSERT INTO ledger (order_id, amount)
        VALUES (:order_id, :amount)
        """,
        order_id=order["id"],
        amount=order["total"],
    )
    assert ledger_svc.exists(
        """
        SELECT 1
        FROM ledger
        WHERE order_id = :id
        """,
        session=session,
        id=101,
    )
```

## Recap

- Construct `SQLSpecSyncService(config=...)` or `SQLSpecAsyncService(config=...)` to eliminate repetitive `with config.provide_session()` boilerplate on single-statement reads.
- Group multi-statement domain mutations inside `service.begin_transaction()` or `session.execute_stack(...)` for atomic execution.
- Pass `session=session` when coordinating multiple domain services within one shared transaction.
