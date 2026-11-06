# Service Layer

`SQLSpecSyncService` and `SQLSpecAsyncService` (`sqlspec.service`) wrap an adapter configuration (or an already-checked-out driver session) with ergonomic helpers for single-row lookups (`get_one`), existence checks (`exists`), offset and cursor pagination (`paginate`, `paginate_limit_offset`, `paginate_cursor`), and atomic write blocks (`begin_transaction`).

## Full Sync and Async Service Example

When constructed with `config=`, a service checks out a short-lived pooled session automatically for standalone read helpers (`service.get_one(...)`, `service.exists(...)`, `service.paginate(...)`) and opens an explicit commit-or-rollback session inside `with service.begin_transaction() as session:`:

```python
--8<-- "docs/examples/drivers/service_config.py:example"
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

## Service Session Lifetimes

Services use either a bounded-session or a request-scoped lifetime:

### Bounded-Session Domain Services

When a service coordinates external asynchronous I/O (such as password hashing, third-party REST APIs, or LLM embeddings), do not keep a database connection checked out during the entire operation. Initialize the service with `config=` and `loader=`, and access named SQL via `self.loader.get_sql(...)`:

=== "SQL (`app/sql/customers.sql`)"

    ```sql
    -- name: get-customer-by-email
    SELECT id, email, tier
    FROM customers;

    -- name: list-customers
    SELECT id, email, tier
    FROM customers
    ORDER BY id;
    ```

=== "Python (`app/services/customers.py`)"

    ```python
    """Bounded-session customer service using AsyncpgConfig and SQLFileLoader."""

    from collections.abc import Sequence
    import msgspec

    from sqlspec import SQLFileLoader, StatementStack
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
    from sqlspec.core import LimitOffsetFilter, OffsetPagination
    from sqlspec.service import SQLSpecAsyncService


    class Customer(msgspec.Struct):
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
            base_query = self.loader.get_sql("get-customer-by-email")
            query = base_query.where("email = :email")
            return await self.get_one(query, email=email, schema_type=Customer)

        async def list_customers(
            self, *, limit: int = 25, offset: int = 0
        ) -> OffsetPagination[Customer]:
            """Paginate customers without holding a connection between pages."""
            query = self.loader.get_sql("list-customers")
            return await self.paginate_limit_offset(
                query,
                LimitOffsetFilter(limit=limit, offset=offset),
                schema_type=Customer,
            )

        async def upgrade_tier_with_audit(
            self, customer_id: int, new_tier: str, audit_events: Sequence[str]
        ) -> None:
            """Execute atomic multi-table updates inside an explicit transaction."""
            stack = (
                StatementStack()
                .push_execute(
                    "UPDATE customers SET tier = :tier WHERE id = :id",
                    tier=new_tier,
                    id=customer_id,
                )
                .push_execute_many(
                    """
                    INSERT INTO tier_audit (customer_id, new_tier, event_type)
                    VALUES (:id, :tier, :event)
                    """,
                    [
                        {"id": customer_id, "tier": new_tier, "event": evt}
                        for evt in audit_events
                    ],
                )
            )
            async with self.begin_transaction() as session:
                await session.execute_stack(stack)
    ```

### Request-Scoped Driver Services

When using dependency injection frameworks like Dishka or Litestar where a driver connection is already checked out for the lifetime of an HTTP request, inject `driver` directly into `SQLSpecAsyncService(driver)` (or pass `session=session` to `get_one`, `exists`, and `paginate` when sharing an outer `begin_transaction()` block across multiple services):

```python
import msgspec

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import FilterTypes, OffsetPagination
from sqlspec.service import SQLSpecAsyncService


class Product(msgspec.Struct):
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

    async def list_products(
        self, *filters: FilterTypes
    ) -> OffsetPagination[Product]:
        """Filter and paginate products using precompiled SQL templates."""
        query = self.spec.get_sql("list-products")
        return await self.paginate(query, *filters, schema_type=Product)
```

## Production Composition Root

In multi-pool applications (separating request-scoped web pools from background worker pools), assemble `SQLSpec`, `bind_key` configs, and `load_sql_files` in a dedicated `app/config.py` composition root and register `config = "app.config:db"` under `[tool.sqlspec]` in `pyproject.toml`:

```python
from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

BASE_DIR = Path(__file__).resolve().parent.parent
DSN = "postgresql://user:pass@localhost:5432/app"

db_manager = SQLSpec()
db = db_manager.add_config(
    AsyncpgConfig(
        bind_key="app-web",
        connection_config={
            "dsn": DSN,
            "min_size": 5,
            "max_size": 20,
            "command_timeout": 30.0,
        },
        migration_config={"script_location": str(BASE_DIR / "db" / "migrations")},
    )
)
worker_db = db_manager.add_config(
    AsyncpgConfig(
        bind_key="app-worker",
        connection_config={"dsn": DSN, "min_size": 2, "max_size": 10},
    )
)
db_manager.load_sql_files(BASE_DIR / "db" / "sql")
```

## Recap

- Construct `SQLSpecSyncService(config=..., loader=...)` or `SQLSpecAsyncService(config=..., loader=...)` and call `self.loader.get_sql(...)` for bounded-session domain services.
- Group multi-statement mutations inside `service.begin_transaction()` or `session.execute_stack(...)`, and pass `session=session` when coordinating multiple services inside one transaction.
