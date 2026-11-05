# Query Builder: SELECT, Filtering and Joins

SQLSpec's fluent `sql` factory constructs SQLGlot AST queries with automatic parameter binding, dialect transpilation, and parse-and-extend composition.

## Inspecting and Executing Built Queries

Every builder compiles via `.build(dialect=...)` into a `BuiltQuery` (`sql`, `parameters`, `dialect`), renders SQL via `.to_sql(dialect=..., show_parameters=True)`, converts to a `SQL` statement via `.to_statement()`, or applies `StatementFilter` instances via `.apply_filters(*filters)`. Pass builders directly to `session.select(query)` or `session.execute(query)`. Calling `sql("SELECT ...")`, `sql.select("SELECT ...")`, or `SQL("SELECT ...").builder()` parses raw SQL into a chainable builder:

```python
from sqlspec import SQL, OffsetPagination, sql

query = sql("SELECT id, name FROM users").where_eq("active", True)
paged = query.apply_filters(OffsetPagination(limit=10, offset=0))
built = paged.build(dialect="postgres")
# SQL: SELECT id, name FROM users WHERE active = :active LIMIT :limit OFFSET :offset
# Parameters: {'active': True, 'limit': 10, 'offset': 0}

stmt = SQL("SELECT id FROM users").builder().where_eq("id", 1).to_statement()
preview = query.to_sql(dialect="postgres", show_parameters=True)
# SQL: SELECT id, name FROM users WHERE active = TRUE
# Parameters: {'active': True}
```

```{ .python title="docs/examples/builder/select_query.py" }
--8<-- "docs/examples/builder/select_query.py:example"
```

## Core SELECT Projections and Column Expressions

Use `sql.select(...)` to append projections, `.select_only(...)` to replace existing projections, `.distinct()` to deduplicate rows, and `sql.column(name, table=...)` (`Column` / `ColumnExpression`) for typed expressions:

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

| Category | Python Syntax | Compiled SQL | Bound Parameters |
| --- | --- | --- | --- |
| Equality | `col == "Ada"` / `col != "Ada"` | `col = :col` / `col != :col` | `{'col': 'Ada'}` |
| Ordering | `col < 10`, `col <= 10`, `col > 0`, `col >= 0` | `col < :col`, `col >= :col` | `{'col': 10}` |
| Logical | `(col_a == 1) & (col_b == 2)`, `col_a.__or__(...)`, `~(col == 1)` | `(col_a = :a AND col_b = :b)`, `NOT (col = :a)` | `{'a': 1, 'b': 2}` |
| Membership | `col.in_(["a", "b"])` / `col.not_in(["a", "b"])` | `col IN (:col_1, :col_2)` / `NOT IN (...)` | `{'col_1': 'a', 'col_2': 'b'}` |
| Array Any | `col.any_(["a", "b"])` / `col.not_any_(["a", "b"])` | `col = ANY(:col)` / `NOT (col = ANY(:col))` | `{'col': ['a', 'b']}` |
| Range & Null | `col.between(1, 5)`, `col.is_null()`, `col.is_not_null()` | `col BETWEEN :low AND :high`, `col IS NOT NULL` | `{'low': 1, 'high': 5}` |
| Pattern | `col.like("%x%")`, `col.not_like(...)`, `col.ilike(...)`, `col.not_ilike(...)` | `col LIKE :col` / `col ILIKE :col` | `{'col': '%x%'}` |
| Sort & Alias | `col.asc(nulls="last")`, `col.desc(nulls="first")`, `col.alias("n")` / `col.as_("n")` | `col ASC NULLS LAST`, `col AS n` | `{}` |

!!! warning "Bitwise Operator Precedence and Python Booleans"
    Python evaluates bitwise `&`, `|`, and `~` before `==`, `<`, or `>=`. Always wrap each comparison in parentheses, and never use `and` or `or` (which short-circuit and raise `TypeError` on `ColumnExpression`):

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
    # SQL: SELECT id FROM users
    # SQL: WHERE (age >= :age_1 AND (role = :role_1 OR role = :role_2))
    # Parameters: {'age_1': 18, 'role_1': 'admin', 'role_2': 'owner'}
    ```

## WHERE and OR WHERE Predicates

Chained `.where_*()` calls combine with `AND`; `.or_where_*()`, `.where_or(...)`, and `.or_where(...)` combine with `OR`. `.where()` also accepts 2-tuple `("status", "active")` and 3-tuple `("created_at", ">=", "2026-01-01")` shorthands:

```python
from sqlspec import sql

filtered = (
    sql
    .select("id")
    .from_("users")
    .where(("status", "active"), ("score", ">=", 50))
    .where_or(("role", "admin"), ("role", "owner"))
    .or_where_eq("override", True)
)
# SQL: SELECT id FROM users WHERE status = :status AND score >= :score
# SQL: AND (role = :role_1 OR role = :role_2) OR override = :override
# Parameters: {'status': 'active', 'score': 50, 'role_1': 'admin',
# Parameters:  'role_2': 'owner', 'override': True}
```

| AND Method | OR Method | Compiled SQL | Notes |
| --- | --- | --- | --- |
| `.where_eq(c, v)` / `.where_neq(c, v)` | `.or_where_eq(c, v)` / `.or_where_neq(c, v)` | `c = :c` / `c != :c` | Auto-switches `None` to `IS NULL` / `IS NOT NULL` |
| `.where_lt(c, v)` / `.where_lte(c, v)` | `.or_where_lt(c, v)` / `.or_where_lte(c, v)` | `c < :c` / `c <= :c` | Parameterized upper bound |
| `.where_gt(c, v)` / `.where_gte(c, v)` | `.or_where_gt(c, v)` / `.or_where_gte(c, v)` | `c > :c` / `c >= :c` | Parameterized lower bound |
| `.where_between(c, lo, hi)` | `.or_where_between(c, lo, hi)` | `c BETWEEN :lo AND :hi` | Inclusive range predicate |
| `.where_like(c, p)` / `.where_not_like(c, p)` | `.or_where_like(c, p)` / `.or_where_not_like(c, p)` | `c LIKE :p` / `NOT (c LIKE :p)` | Case-sensitive pattern match |
| `.where_ilike(c, p)` / `.where_like_any(c, ps)` | `.or_where_ilike(c, p)` / `.or_where_like_any(c, ps)` | `c ILIKE :p` / `(c LIKE :p1 OR ...)` | Case-insensitive or multi-pattern |
| `.where_is_null(c)` / `.where_is_not_null(c)` | `.or_where_is_null(c)` / `.or_where_is_not_null(c)` | `c IS NULL` / `c IS NOT NULL` | Null check without parameters |
| `.where_in(c, vs)` / `.where_not_in(c, vs)` | `.or_where_in(c, vs)` / `.or_where_not_in(c, vs)` | `c IN (...)` / `NOT (c IN (...))` | Accepts sequence or `Select` subquery |
| `.where_any(c, vs)` / `.where_not_any(c, vs)` | `.or_where_any(c, vs)` / `.or_where_not_any(c, vs)` | `c = ANY(:vs)` / `NOT (c = ANY(:vs))` | Array/set `ANY` comparison |
| `.where_exists(subq)` / `.where_not_exists(subq)` | `.or_where_exists(subq)` / `.or_where_not_exists(subq)` | `EXISTS (SELECT ...)` | Correlated or uncorrelated subquery |

## Joins, Lateral Joins and Time-Travel Queries

Chain `.inner_join()`, `.left_join()`, `.right_join()`, `.full_join()`, `.cross_join()`, `.lateral_join()`, `.left_lateral_join()`, or `.cross_lateral_join()`, or construct join clauses with `sql.inner_join_`, `sql.left_join_`, and `sql.lateral_join_` (`JoinBuilder`). Pass `as_of` on `.from_()` or `JoinBuilder.as_of()` for time-travel queries (compiling to `FOR SYSTEM_TIME AS OF` on BigQuery, `AS OF TIMESTAMP` on Oracle, `AS OF SYSTEM TIME` on CockroachDB/Postgres, and `AT (TIMESTAMP => ...)` on DuckDB/Snowflake):

```{ .python title="docs/examples/builder/complex_joins.py" }
--8<-- "docs/examples/builder/complex_joins.py:example"
```

```python
from sqlspec import sql

joined = (
    sql
    .select("u.id", "o.total")
    .from_("users", alias="u", as_of="2026-01-01 00:00:00")
    .join(
        sql
        .inner_join_("orders", alias="o")
        .as_of("2026-01-01 00:00:00")
        .on("u.id = o.user_id")
    )
    .left_lateral_join("unnest(u.tags)", alias="t")
)
# SQL: SELECT u.id, o.total FROM users AS u FOR SYSTEM_TIME AS OF :as_of_1
# SQL: INNER JOIN orders AS o FOR SYSTEM_TIME AS OF :as_of_2 ON u.id = o.user_id
# SQL: LEFT JOIN LATERAL unnest(u.tags) AS t ON TRUE
# Parameters: {'as_of_1': '2026-01-01 00:00:00', 'as_of_2': '2026-01-01 00:00:00'}
```

## Subqueries, CTEs and Set Operations

Wrap subqueries with `sql.exists_(subq)`, `sql.in_(subq)`, `sql.any_(subq)`, or `sql.all_(subq)`. Attach CTEs via `.with_(name, query, recursive=False, columns=[...])` and combine queries with `.union(other, all_=True)`, `.intersect(other)`, or `.except_(other)`:

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
    .where(sql.exists_(subq))
    .union(sql.select("id").from_("vip_users"), all_=True)
)
# SQL: SELECT id FROM users WHERE EXISTS (SELECT user_id FROM orders
# SQL: WHERE total >= :total) UNION ALL SELECT id FROM vip_users
# Parameters: {'total': 500}
```

## Row Locking, Hints, PIVOT and EXPLAIN

Apply row locks (`.for_update(skip_locked=True, nowait=False, of="orders")`, `.for_share()`, `.for_key_share()`, `.for_no_key_update()`), optimizer hints (`.with_hint(...)`, `.approx_limit(...)`), `.pivot(...)` / `.unpivot(...)`, and execution plan builders (`sql.explain(...)` / `query.explain(...)`):

```python
from sqlspec import sql

locked = (
    sql
    .select("id")
    .from_("orders")
    .with_hint("INDEX(orders idx_orders_status)", dialect="oracle")
    .where_eq("status", "pending")
    .for_update(skip_locked=True, of="orders")
)
# SQL: SELECT id FROM orders WHERE status = :status
# SQL: FOR UPDATE OF orders SKIP LOCKED
# Parameters: {'status': 'pending'}

plan = locked.explain(analyze=True, format="json")
# SQL: EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM orders ...
# Parameters: {'status': 'pending'}
```

Continue to [Expressions, Window Functions and Vector Search](query-builder-expressions.md) and [DML, Upserts, MERGE and DDL Schema Builders](query-builder-dml-ddl.md).
