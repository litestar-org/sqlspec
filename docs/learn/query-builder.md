# Query Builder: SELECT, Filtering and Joins

The `sql` factory builds SQLGlot ASTs with automatic parameter binding and dialect transpilation. You can start from a fluent chain or parse existing SQL and keep extending it.

## Inspecting and Executing Built Queries

`.build(dialect=...)` returns a `BuiltQuery` (`sql`, `parameters`, `dialect`). `.to_sql(show_parameters=True)` renders SQL with values inlined for debugging, `.to_statement()` returns a `SQL` statement, and `.apply_filters(*filters)` applies `StatementFilter` instances and returns a `SQL` statement. Pass builders directly to `session.select(query)` or `session.execute(query)`. `sql("SELECT ...")`, `sql.select("SELECT ...")`, and `SQL("SELECT ...").builder()` parse raw SQL into a chainable builder:

```python
from sqlspec import SQL, sql
from sqlspec.core import LimitOffsetFilter

query = sql("SELECT id, name FROM users").where_eq("active", True)
built = query.build()
# SQL: SELECT id, name FROM users WHERE active = :active
# Parameters: {'active': True}

paged = query.apply_filters(LimitOffsetFilter(limit=10, offset=0))
# SQL: SELECT id, name FROM users WHERE active = :active LIMIT :limit OFFSET :offset
# Parameters: {'active': True, 'limit': 10, 'offset': 0}

stmt = SQL("SELECT id FROM users").builder().where_eq("id", 1).to_statement()
preview = query.to_sql(show_parameters=True)
# SQL: SELECT id, name FROM users WHERE active = TRUE
```

!!! note "Reading the SQL comments"
    The `# SQL:` comments on this page omit the identifier quoting and table qualification (`"users"."id" AS "id"`) that the builder's optimizer adds by default. Builds without a dialect use `:name` placeholders; dialects such as `postgres` render the same parameters in their native style (`%(name)s`).

```{ .python title="docs/examples/builder/select_query.py" }
--8<-- "docs/examples/builder/select_query.py:example"
```

## Projections and Column Expressions

Use `sql.select(...)` to add projections, `.select_only(...)` to replace them, `.distinct()` to deduplicate rows, and `sql.column(name, table=...)` for typed column expressions:

```python
from sqlspec import sql

u_name = sql.column("name", table="u")
proj = (
    sql
    .select("u.id", "u.email")
    .from_("users", alias="u")
    .select_only("u.id", u_name.as_("user_name"))
    .distinct()
    .order_by(u_name.asc(nulls="last"))
)
# SQL: SELECT DISTINCT u.id, u.name AS user_name FROM users AS u
# SQL: ORDER BY u.name ASC NULLS LAST
# Parameters: {}
```

Values compared through `Column` operators bind as generated parameters (`:where_param_1`, `:where_param_2`, ...):

| Category | Python Syntax | Compiled SQL |
| --- | --- | --- |
| Equality | `col == "Ada"`, `col != "Ada"`, `col == None` | `col = :p`, `col <> :p`, `col IS NULL` |
| Ordering | `col < 10`, `col <= 10`, `col > 0`, `col >= 0` | `col < :p`, `col <= :p`, `col > :p`, `col >= :p` |
| Logical | `&` for AND, the pipe operator for OR, `~` for NOT | `a = :p AND b = :p`, `a = :p OR b = :p`, `NOT ...` |
| Membership | `col.in_(["a", "b"])`, `col.not_in(["a", "b"])` | `col IN (:p1, :p2)`, `NOT col IN (:p1, :p2)` |
| Array Any | `col.any_(["a", "b"])`, `col.not_any_(["a", "b"])` | `col = ANY(ARRAY(:p1, :p2))`, `col <> ALL(ARRAY(:p1, :p2))` |
| Range and Null | `col.between(1, 5)`, `col.is_null()`, `col.is_not_null()` | `BETWEEN` bounds, `col IS NULL`, `NOT col IS NULL` |
| Pattern | `col.like("%x%")`, `col.not_like(...)`, `col.ilike(...)`, `col.not_ilike(...)` | `col LIKE :p`, `NOT col LIKE :p`, `col ILIKE :p` |
| Sort and Alias | `col.asc(nulls="last")`, `col.desc(nulls="first")`, `col.alias("n")`, `col.as_("n")` | `col ASC NULLS LAST`, `col AS n` |

!!! warning "Operator precedence and grouping"
    Python evaluates `&`, `|`, and `~` before `==`, `<`, or `>=`, so wrap every comparison in parentheses. Never use `and` / `or`; they raise `TypeError` on column expressions. `&` parenthesizes `OR` operands and `~` parenthesizes `AND` / `OR` operands, so the Python grouping carries into SQL:

    ```python
    from sqlspec import sql

    age = sql.column("age")
    role = sql.column("role")
    q = (
        sql
        .select("id")
        .from_("users")
        .where((age >= 18) & ((role == "admin") | (role == "owner")))
    )
    # SQL: SELECT id FROM users WHERE age >= :where_param_1
    # SQL: AND (role = :where_param_2 OR role = :where_param_3)
    # Parameters: {'where_param_1': 18, 'where_param_2': 'admin',
    # Parameters:  'where_param_3': 'owner'}
    ```

## WHERE and OR WHERE Predicates

Chained `.where_*()` calls combine with `AND`; `.or_where_*()` and `.or_where(...)` combine with `OR`, and `.where_or(...)` adds one parenthesized `OR` group. `.where()` also accepts a 2-tuple `("status", "active")` or a 3-tuple `("score", ">=", 50)`:

```python
from sqlspec import sql

filtered = (
    sql
    .select("id")
    .from_("users")
    .where(("status", "active"))
    .where(("score", ">=", 50))
    .where_or(("role", "admin"), ("role", "owner"))
)
# SQL: SELECT id FROM users WHERE (role = :role OR role = :role_1)
# SQL: AND score >= :score AND status = :status
# Parameters: {'status': 'active', 'score': 50, 'role': 'admin', 'role_1': 'owner'}
```

| AND Method | OR Method | Compiled SQL |
| --- | --- | --- |
| `.where_eq(c, v)` / `.where_neq(c, v)` | `.or_where_eq(c, v)` / `.or_where_neq(c, v)` | `c = :c` / `c <> :c`; `None` binds as a value, so use `.where_is_null(c)` for null checks |
| `.where_lt(c, v)` / `.where_lte(c, v)` | `.or_where_lt(c, v)` / `.or_where_lte(c, v)` | `c < :c` / `c <= :c` |
| `.where_gt(c, v)` / `.where_gte(c, v)` | `.or_where_gt(c, v)` / `.or_where_gte(c, v)` | `c > :c` / `c >= :c` |
| `.where_between(c, lo, hi)` | `.or_where_between(c, lo, hi)` | `BETWEEN` bounds bound as `:c_low` / `:c_high` |
| `.where_like(c, p)` / `.where_not_like(c, p)` | `.or_where_like(c, p)` / `.or_where_not_like(c, p)` | `c LIKE :c` / `NOT c LIKE :c` |
| `.where_ilike(c, p)` | `.or_where_ilike(c, p)` | `c ILIKE :c` |
| `.where_like_any(c, ps)` | None | `c LIKE :c OR c LIKE :c_1` |
| `.where_is_null(c)` / `.where_is_not_null(c)` | `.or_where_is_null(c)` / `.or_where_is_not_null(c)` | `c IS NULL` / `NOT c IS NULL` |
| `.where_in(c, vs)` / `.where_not_in(c, vs)` | `.or_where_in(c, vs)` / `.or_where_not_in(c, vs)` | `c IN (:c_1, :c_2)` / `NOT c IN (...)`; also accepts a `Select` subquery |
| `.where_any(c, vs)` / `.where_not_any(c, vs)` | `.or_where_any(c, vs)` / `.or_where_not_any(c, vs)` | `c = ANY(ARRAY(:c_any_1, ...))` / `c <> ALL(ARRAY(...))` |
| `.where_exists(subq)` / `.where_not_exists(subq)` | `.or_where_exists(subq)` / `.or_where_not_exists(subq)` | `EXISTS (SELECT ...)` |

## Joins and Time-Travel Queries

Chain `.inner_join()`, `.left_join()`, `.right_join()`, `.full_join()`, `.cross_join()`, `.lateral_join()`, `.left_lateral_join()`, or `.cross_lateral_join()`, or build join clauses with `sql.inner_join_`, `sql.left_join_`, and `sql.lateral_join_`. Pass `as_of` to `.from_()` or call `.as_of()` on a join builder for time-travel reads. The timestamp renders as a literal: `FOR SYSTEM_TIME AS OF` on BigQuery, `AS OF TIMESTAMP` on Oracle, `AS OF SYSTEM TIME` on CockroachDB, and `AT (TIMESTAMP => ...)` on DuckDB and Snowflake. Lateral joins render as `JOIN LATERAL`, `LEFT JOIN LATERAL`, or `CROSS JOIN LATERAL`, with `ON TRUE` when no condition is given:

```{ .python title="docs/examples/builder/complex_joins.py" }
--8<-- "docs/examples/builder/complex_joins.py:example"
```

```python
from sqlspec import sql

joined = (
    sql
    .select("u.id", "o.total")
    .from_("users", alias="u")
    .join(
        sql
        .inner_join_("orders", alias="o")
        .as_of("2026-01-01 00:00:00")
        .on("u.id = o.user_id")
    )
)
built = joined.build(dialect="bigquery")
# SQL: SELECT u.id, o.total FROM users AS u JOIN orders AS o
# SQL: FOR SYSTEM_TIME AS OF '2026-01-01 00:00:00' ON u.id = o.user_id
# Parameters: {}
```

## Subqueries, CTEs and Set Operations

Use `.where_exists(subq)`, `.where(sql.exists_(subq))`, or `.where_in(col, subq)` to embed subqueries; their parameters merge into the outer query. Attach CTEs with `.with_(name, query, recursive=False, columns=[...])` and combine queries with `.union(other, all_=True)`, `.intersect(other)`, or `.except_(other)`:

```{ .python title="docs/examples/builder/subquery_cte.py" }
--8<-- "docs/examples/builder/subquery_cte.py:example"
```

```python
from sqlspec import sql

subq = sql.select("user_id").from_("orders").where_gte("total", 500)
combined = (
    sql
    .select("id")
    .from_("users")
    .where_exists(subq)
    .union(sql.select("id").from_("vip_users"), all_=True)
)
# SQL: SELECT id FROM users WHERE EXISTS (SELECT user_id FROM orders
# SQL: WHERE total >= :total) UNION ALL SELECT id FROM vip_users
# Parameters: {'total': 500}
```

## Row Locking, Hints, PIVOT and EXPLAIN

The builder also supports row locks (`.for_update(skip_locked=True, nowait=False, of="orders")`, `.for_share()`, `.for_key_share()`, `.for_no_key_update()`), optimizer hints (`.with_hint(...)`, `.approx_limit(...)`), `.pivot(...)` / `.unpivot(...)`, and execution plans (`sql.explain(...)` / `query.explain(...)`). Hint text renders verbatim inside `/*+ ... */`. Lock clauses render for builds without a dialect and for dialects with row locking; dialects without it, such as `sqlite`, raise `SQLBuilderError`:

```python
from sqlspec import sql

locked = (
    sql
    .select("id")
    .from_("orders")
    .where_eq("status", "pending")
    .for_update(skip_locked=True, of="orders")
)
built = locked.build(dialect="postgres")
# SQL: SELECT id FROM orders WHERE status = %(status)s
# SQL: FOR UPDATE OF orders SKIP LOCKED
# Parameters: {'status': 'pending'}

plan = sql.explain(locked, analyze=True, format="json", dialect="postgres").build()
# SQL: EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM orders
# SQL: WHERE status = %(status)s FOR UPDATE OF orders SKIP LOCKED
# Parameters: {'status': 'pending'}
```

Continue to [Expressions, Window Functions and Vector Search](query-builder-expressions.md) and [DML, Upserts, MERGE and DDL Schema Builders](query-builder-dml-ddl.md).
