# Filtering and Pagination

List endpoints in APIs routinely combine pagination, search, date-range bounds, collection membership, and multi-column sorting. SQLSpec provides composable `StatementFilter` value objects in `sqlspec.core` along with offset and keyset cursor pagination helpers.

## Built-In `StatementFilter` Classes

Pass any combination of `StatementFilter` instances as positional arguments to `session.select(...)`, `session.select_with_total(...)`, or `service.paginate(...)`:

| Filter Class | Appended SQL Semantics | Key Arguments |
| --- | --- | --- |
| `LimitOffsetFilter` | `LIMIT :limit OFFSET :offset` (or `FETCH FIRST` on Oracle/Db2/MSSQL) | `limit`, `offset` |
| `OrderByFilter` | `ORDER BY <field> ASC\|DESC [NULLS FIRST\|LAST]` | `field_name`, `sort_order="asc"`, `nulls=None` |
| `SearchFilter` | Case-sensitive or case-insensitive `LIKE` / `ILIKE` across one or more columns | `field_name`, `value`, `ignore_case=False` |
| `BeforeAfterFilter` | `< :before` and/or `> :after` timestamp range bounds (`OnBeforeAfterFilter` for inclusive `<=`/`>=`) | `field_name`, `before=None`, `after=None` |
| `InCollectionFilter` | `WHERE <field> IN (:p1, ...)` (`NotInCollectionFilter` for `NOT IN`) | `field_name`, `values` |
| `NullFilter` / `NotNullFilter` | `WHERE <field> IS NULL` / `IS NOT NULL` | `field_name` |

Because `StatementFilter` objects bind named parameters (`:limit`, `:offset`, etc.), always bind any additional base-query parameters as **named** keyword arguments (`status="active"`), never positional tuples.

### Combining Multiple Filters on One Query

Filters compose in any order without mutating the underlying `SQL` statement:

```python
from datetime import UTC, datetime

from sqlspec import SQL, SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import BeforeAfterFilter, InCollectionFilter, LimitOffsetFilter, OrderByFilter, SearchFilter

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, status TEXT, published_at TEXT)")
    session.execute_many(
        "INSERT INTO articles (title, status, published_at) VALUES (:title, :status, :published_at)",
        [
            {"title": "SQLite WAL Tuning", "status": "published", "published_at": "2026-03-01T10:00:00Z"},
            {"title": "Postgres Poolers", "status": "published", "published_at": "2026-04-15T12:00:00Z"},
            {"title": "Draft Notes", "status": "draft", "published_at": "2026-04-20T09:00:00Z"},
        ],
    )

    base_query = SQL("SELECT id, title, status FROM articles")
    rows = session.select(
        base_query,
        InCollectionFilter("status", ["published"]),
        SearchFilter("title", "Pool", ignore_case=True),
        BeforeAfterFilter("published_at", after=datetime(2026, 1, 1, tzinfo=UTC).isoformat()),
        OrderByFilter("published_at", sort_order="desc"),
        LimitOffsetFilter(limit=10, offset=0),
    )
    assert [row["title"] for row in rows] == ["Postgres Poolers"]
```

## Offset Pagination and Total Counts

For classic page-number navigation, chain `.paginate(page=..., page_size=...)` on a `SQL` statement and call `session.select_with_total(...)` (or `service.paginate_limit_offset(...)` to receive an `OffsetPagination` envelope):

```{ .python title="docs/examples/patterns/pagination.py" hl_lines="21 22 23 24 25 26" }
--8<-- "docs/examples/patterns/pagination.py"
```

When paginating complex queries containing `JOIN`, `GROUP BY`, or CTEs with named parameters, running an explicit scalar `SELECT COUNT(*)` query alongside `session.select(query, LimitOffsetFilter(...), **named_params)` avoids wrapping large projections inside a derived count subquery.

## Keyset Cursor Pagination

Large tables degrade under deep `OFFSET 100000` scans because the database must read and discard skipped rows. `CursorFilter` and `SQLSpecSyncService.paginate_cursor` / `SQLSpecAsyncService.paginate_cursor` implement indexed keyset seek pagination with opaque forward (`next_cursor`) and backward (`previous_cursor`) tokens:

```{ .python title="docs/examples/patterns/cursor_pagination.py" hl_lines="17 28 29 30 31" }
--8<-- "docs/examples/patterns/cursor_pagination.py"
```

Always include a unique tie-breaker column (such as `("id", "desc")`) as the final key in `CursorKeys` so rows with identical timestamps paginate deterministically without duplicates or gaps.

## Declarative HTTP Filter Providers (Litestar)

In Litestar services, `create_filter_dependencies` generates typed query-parameter dependency providers (`id_filter`, `created_at`, `updated_at`, `search`, `sort_field`, and `limit_offset`) from a single `FilterConfig` dictionary:

```{ .python title="docs/examples/patterns/filter_dependencies.py" hl_lines="12 15 18 20 25" }
--8<-- "docs/examples/patterns/filter_dependencies.py"
```

Always pass an explicit `sort_field` allowlist (and optional `sort_field_aliases`) so untrusted query strings can only sort by indexed, permitted columns.

## Recap

- Compose `SearchFilter`, `BeforeAfterFilter`, `InCollectionFilter`, `OrderByFilter`, and `LimitOffsetFilter` onto base `SQL` or builder queries instead of hand-rolling conditional SQL strings.
- Keep all base-query parameters named (`**params`) whenever attaching `StatementFilter` instances.
- Prefer `CursorFilter` keyset pagination with a unique tie-breaker key for infinite-scroll feeds and high-cardinality tables.
