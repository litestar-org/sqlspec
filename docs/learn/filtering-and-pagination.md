# Filtering and Pagination

List endpoints in APIs routinely combine pagination, search, date-range bounds, collection membership, and multi-column sorting. SQLSpec provides composable `StatementFilter` value objects (`FilterTypes`) in `sqlspec.core` along with offset (`OffsetPagination[T]`) and keyset cursor (`CursorPagination[T]`) pagination helpers.

## Built-In Statement Filters

Pass any combination of `StatementFilter` instances as positional arguments to `session.select(...)`, `session.select_with_total(...)`, or `service.paginate(...)`:

<!-- GENERATED:START filter-catalog -->
| Filter Class | Constructor Signature | SQL Clause Generated | Description |
| --- | --- | --- | --- |
| `BeforeAfterFilter` | `BeforeAfterFilter(field_name, before=None, after=None)` | `WHERE col < :col_before AND col > :col_after` | Filter for datetime range queries. |
| `OnBeforeAfterFilter` | `OnBeforeAfterFilter(field_name, on_or_before=None, on_or_after=None)` | `WHERE col <= :col_on_or_before AND col >= :col_on_or_after` | Filter for inclusive datetime range queries. |
| `InCollectionFilter` | `InCollectionFilter(field_name, values=None)` | `WHERE col IN (:col_in_0, :col_in_1, ...)` | Filter for IN clause queries. |
| `NotInCollectionFilter` | `NotInCollectionFilter(field_name, values=None)` | `WHERE col NOT IN (:col_notin_0, :col_notin_1, ...)` | Filter for NOT IN clause queries. |
| `AnyCollectionFilter` | `AnyCollectionFilter(field_name, values=None)` | `WHERE col = ANY ([:col_any_0, :col_any_1, ...])` | Filter for PostgreSQL-style ANY clause queries. |
| `NotAnyCollectionFilter` | `NotAnyCollectionFilter(field_name, values=None)` | `WHERE NOT (col = ANY ([:col_not_any_0, ...]))` | Filter for PostgreSQL-style NOT ANY clause queries. |
| `ChoicesFilter` | `ChoicesFilter(field_name, values=None)` | `WHERE col IN (:col_choices_0, :col_choices_1, ...)` | Filter for field matching a defined set of choice values. |
| `LimitOffsetFilter` | `LimitOffsetFilter(limit, offset)` | `LIMIT :limit OFFSET :offset` | Filter for LIMIT and OFFSET clauses. |
| `CursorFilter` | `CursorFilter(keys, limit, cursor=None, secret=None)` | `WHERE (col, id) > (:cursor_k0, :cursor_k1) ORDER BY col ASC, id ASC LIMIT :cursor_limit` | Paginate by ordered keys whose last key uniquely identifies each row. |
| `OrderByFilter` | `OrderByFilter(field_name, sort_order='asc', nulls=None)` | `ORDER BY col ASC / DESC [NULLS FIRST / LAST]` | Filter for ORDER BY clauses. |
| `SearchFilter` | `SearchFilter(field_name, value, ignore_case=False)` | `WHERE col LIKE :col_search (or ILIKE when ignore_case=True)` | Filter for text search queries. |
| `NotInSearchFilter` | `NotInSearchFilter(field_name, value, ignore_case=False)` | `WHERE col NOT LIKE :col_not_search (or NOT ILIKE when ignore_case=True)` | Filter for negated text search queries. |
| `NullFilter` | `NullFilter(field_name)` | `WHERE col IS NULL` | Filter for IS NULL queries. |
| `NotNullFilter` | `NotNullFilter(field_name)` | `WHERE col IS NOT NULL` | Filter for IS NOT NULL queries. |
| `BooleanFilter` | `BooleanFilter(field_name, value)` | `WHERE col = :col_boolean` | Filter for boolean comparison queries. |
<!-- GENERATED:END filter-catalog -->

- **Multi-column and escaped `LIKE` search**: `SearchFilter` and `NotInSearchFilter` accept `set[str]` to filter across multiple columns (`OR` for `SearchFilter`, `AND` for `NotInSearchFilter`). Call `SearchFilter.escape_like_value(raw)` to escape literal `%`, `_`, and `\` wildcards before passing user input.
- **Named parameter binding**: Because `StatementFilter` objects bind named parameters (`:limit`, `:offset`, etc.), always bind any additional base-query parameters as **named** keyword arguments (`status="active"`), never positional tuples.

### Combining Multiple Filters on One Query

Filters compose in any order without mutating the underlying `SQL` statement:

```python
import datetime as dt

from sqlspec import SQL, SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import (
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    OrderByFilter,
    SearchFilter,
)

spec = SQLSpec()
config = spec.add_config(
    SqliteConfig(connection_config={"database": ":memory:"})
)

with spec.provide_session(config) as session:
    session.execute(
        "CREATE TABLE articles ("
        "id INTEGER PRIMARY KEY, title TEXT, status TEXT, published_at TIMESTAMP)"
    )
    rows = session.select(
        SQL("SELECT id, title, status FROM articles"),
        InCollectionFilter("status", ["published"]),
        SearchFilter(
            {"title", "status"},
            SearchFilter.escape_like_value("Pool"),
            ignore_case=True,
        ),
        BeforeAfterFilter(
            "published_at",
            after=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        ),
        OrderByFilter("published_at", sort_order="desc"),
        LimitOffsetFilter(limit=10, offset=0),
    )
```

## Offset Pagination and Total Counts

Chain `.paginate(page=..., page_size=...)` on a `SQL` statement and call `session.select_with_total(..., count_with_window=True)` to compute `COUNT(*) OVER()` in a single database round-trip (or omit `count_with_window` to run a separate scalar `COUNT(*)` query). Service layers expose `SQLSpecSyncService.paginate_limit_offset` and `SQLSpecAsyncService.paginate_limit_offset` to return an `OffsetPagination[T]` envelope directly:

```python
--8<-- "docs/examples/patterns/pagination.py:example"
```

## Keyset Cursor Pagination

Large tables degrade under deep `OFFSET 100000` scans because the engine must read and discard skipped rows. `CursorFilter` and `CursorKey(field_name, sort_order="asc", nulls=None, result_name=None)` implement indexed keyset seek pagination with opaque forward (`next_cursor`) and backward (`previous_cursor`) tokens:

```python
--8<-- "docs/examples/patterns/cursor_pagination.py:example"
```

- **Unique tie-breaker key**: Always include a unique column (such as `CursorKey("id", "desc")`) as the final key in `CursorKeys` so rows with identical timestamps paginate deterministically.
- **HMAC-SHA256 cursor signing**: Pass `secret="your-signing-key"` to `CursorFilter(keys, limit=20, cursor=token, secret=...)` so tampered tokens or sort-order mismatches raise `InvalidCursorError`.
- **Direct driver vs service assembly**: Call `cursor_filter.build_page(rows)` when executing via a raw driver session (`session.select(query, cursor_filter)`), or call `service.paginate_cursor(query, cursor_filter)` on `SQLSpecSyncService` / `SQLSpecAsyncService`.

## Declarative HTTP Filter Providers (Litestar)

In Litestar services, `create_filter_dependencies` generates typed query-parameter dependency providers (`id_filter`, `created_filter`, `updated_filter`, `search_filter`, `order_by_filter`, `limit_offset_filter`) plus an aggregate `filters` dependency from a single `FilterConfig` dictionary:

```python
--8<-- "docs/examples/patterns/filter_dependencies.py:example"
```

### Wiring Filters in a Litestar Controller

Mount `create_filter_dependencies({...})` on `Controller.dependencies` and pass `*filters` into `await db_session.select_with_total(..., count_with_window=True)` (or `await service.paginate(statement, *filters, schema_type=Product)` when using a `SQLSpecAsyncService` subclass):

```python
from typing import Annotated

import msgspec
from litestar import Controller, get
from litestar.params import Dependency

from sqlspec import SQL
from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import FilterTypes, LimitOffsetFilter, OffsetPagination
from sqlspec.extensions.litestar.providers import create_filter_dependencies


class Product(msgspec.Struct):
    id: int
    name: str
    price_cents: int


class ProductController(Controller):
    path = "/products"
    dependencies = create_filter_dependencies({
        "sort_field": ["name", "price_cents", "created_at"],
        "sort_order": "asc",
        "pagination_type": "limit_offset",
        "pagination_size": 25,
        "search": "name",
    })

    @get()
    async def list_products(
        self,
        db_session: AsyncpgDriver,
        filters: Annotated[list[FilterTypes], Dependency(skip_validation=True)],
    ) -> OffsetPagination[Product]:
        """Filter, sort, and paginate products."""
        items, total = await db_session.select_with_total(
            SQL("SELECT id, name, price_cents FROM products"),
            *filters,
            schema_type=Product,
            count_with_window=True,
        )
        page = next(f for f in filters if isinstance(f, LimitOffsetFilter))
        return OffsetPagination(
            items=items, limit=page.limit, offset=page.offset, total=total
        )
```
