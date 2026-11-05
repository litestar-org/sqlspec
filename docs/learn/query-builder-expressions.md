# Expressions, Window Functions and Vector Search

Compose typed aggregate, scalar, conditional, window, and vector search expressions using either `sql.*` factory methods or `Column` descriptors (`sql.column(...)`).

## Aggregations and GROUP BY Extensions

Aggregate helpers are available on `sql` (`sql.count()`, `sql.count_distinct()`, `sql.sum()`, `sql.avg()`, `sql.min()`, `sql.max()`) and on `Column` (`col.count()`, `col.count_distinct()`, `col.sum()`, `col.avg()`, `col.min()`, `col.max()`, `Column.count_all()`). Group rows with `.group_by(...)`, `.group_by_rollup(...)`, `.group_by_cube(...)`, `.group_by_grouping_sets(...)`, or `sql.rollup()` / `sql.cube()` / `sql.grouping_sets()`, and filter aggregates with `.having(...)`:

```python
from sqlspec import sql

amt = sql.column("amount")
rollup_q = (
    sql
    .select(
        "region",
        "year",
        sql.count_distinct("user_id").as_("buyers"),
        amt.sum().as_("total"),
    )
    .from_("orders")
    .group_by_rollup("region", "year")
    .having((amt.sum(), ">", 1000))
)
# SQL: SELECT region, year, COUNT(DISTINCT user_id) AS buyers,
# SQL: SUM(amount) AS total FROM orders
# SQL: GROUP BY ROLLUP (region, year) HAVING SUM(amount) > :amount_1
# Parameters: {'amount_1': 1000}
```

## Scalar, String, Math and Type Conversion Functions

| Category | SQLFactory Helpers | Column Methods |
| --- | --- | --- |
| Type Conversion | `sql.cast(col, dtype)`, `sql.to_literal(val)` | `col.cast(dtype)` |
| NULL Handling | `sql.coalesce(*exprs)`, `sql.nvl(c, v)`, `sql.nvl2(c, a, b)`, `sql.decode(c, s1, r1, ...)` | `col.coalesce(*vals)` |
| String Functions | `sql.concat(*exprs)`, `sql.upper(c)`, `sql.lower(c)`, `sql.length(c)` | `col.lower()`, `col.upper()`, `col.length()`, `col.trim()`, `col.substring(pos, len)` |
| Math Functions | `sql.round(c, decimals)` | `col.abs()`, `col.round(decimals)`, `col.floor()`, `col.ceil()` |

```python
from sqlspec import sql

email = sql.column("email")
price = sql.column("price")
scalar_q = sql.select(
    email.trim().lower().as_("norm_email"),
    sql.coalesce("nickname", "email").as_("display_name"),
    price.round(2).cast("NUMERIC(10, 2)").as_("rounded_price"),
).from_("users")
# SQL: SELECT LOWER(TRIM(email)) AS norm_email,
# SQL: COALESCE(nickname, email) AS display_name,
# SQL: CAST(ROUND(price, 2) AS NUMERIC(10, 2)) AS rounded_price FROM users
# Parameters: {}
```

## Conditional CASE Expressions

Build `CASE WHEN ... THEN ... ELSE ... END` expressions with `sql.case()` or the `sql.case_` property:

```python
from sqlspec import sql

amt = sql.column("amount")
tier = (
    sql
    .case()
    .when(amt >= 1000, "enterprise")
    .when(amt >= 100, "pro")
    .else_("free")
    .end()
    .as_("tier")
)
case_q = sql.select("id", tier).from_("orders")
# SQL: SELECT id, CASE WHEN amount >= :amount_1 THEN :when_1
# SQL: WHEN amount >= :amount_2 THEN :when_2 ELSE :else_1 END AS tier
# SQL: FROM orders
# Parameters: {'amount_1': 1000, 'when_1': 'enterprise', 'amount_2': 100,
# Parameters:  'when_2': 'pro', 'else_1': 'free'}
```

## Window and Analytic Functions

SQLSpec provides both direct factory methods and fluent `WindowFunctionBuilder` properties (`.partition_by(...).order_by(...).as_(...)`):

| Function | Direct Factory Method | Fluent Property Builder |
| --- | --- | --- |
| `ROW_NUMBER` / `RANK` / `DENSE_RANK` | `sql.row_number(...)`, `sql.rank(...)`, `sql.dense_rank(...)` | `sql.row_number_`, `sql.rank_`, `sql.dense_rank_` |
| `LAG` / `LEAD` | `sql.lag(col, offset, default, ...)`, `sql.lead(col, ...)` | `sql.lag_`, `sql.lead_` |
| `COUNT` / `SUM` `OVER` | `sql.count_over(col, ...)`, `sql.sum_over(col, ...)` | `sql.count_over_`, `sql.sum_over_` |
| `AVG` / `MIN` / `MAX` `OVER` | `sql.avg_over(col, ...)`, `sql.min_over(...)`, `sql.max_over(...)` | `sql.avg_over_`, `sql.min_over_`, `sql.max_over_` |

```python
from sqlspec import sql

salary = sql.column("salary")
win_q = sql.select(
    "name",
    sql.row_number_.partition_by("dept").order_by(salary.desc()).as_("rn"),
    sql.sum_over("salary", partition_by="dept", alias="dept_payroll"),
).from_("employees")
# SQL: SELECT name, ROW_NUMBER() OVER (PARTITION BY dept
# SQL: ORDER BY salary DESC) AS rn,
# SQL: SUM(salary) OVER (PARTITION BY dept) AS dept_payroll FROM employees
# Parameters: {}
```

## Cross-Dialect Vector Distance and Oracle Hybrid RRF

Call `Column.vector_distance(query_vec, metric=...)` (`"euclidean"`, `"cosine"`, `"inner_product"`, `"euclidean_squared"`) or `Column.cosine_similarity(query_vec)` to compile native vector distance operators per dialect:

| Dialect | Metrics Supported | Compiled SQL Output |
| --- | --- | --- |
| `postgres` | `euclidean` (`<->`), `cosine` (`<=>`), `inner_product` (`<#>`) | `(embedding <=> CAST(ARRAY[0.1, 0.2, 0.3] AS VECTOR))` |
| `oracle` | `euclidean`, `cosine`, `inner_product`, `euclidean_squared` | `VECTOR_DISTANCE(embedding, TO_VECTOR('[0.1, 0.2, 0.3]'), COSINE)` |
| `mysql` | `euclidean`, `cosine`, `inner_product` | `DISTANCE(embedding, STRING_TO_VECTOR('[0.1, 0.2, 0.3]'), 'COSINE')` |
| `duckdb` | `euclidean`, `cosine`, `inner_product` | `array_cosine_distance(embedding, CAST([0.1, 0.2, 0.3] AS DOUBLE[3]))` |
| `bigquery` | `euclidean`, `cosine`, `inner_product` | `COSINE_DISTANCE(embedding, [0.1, 0.2, 0.3])` |
| `tsql` / generic | `euclidean`, `cosine`, `inner_product`, `euclidean_squared` | `VECTOR_DISTANCE(embedding, [0.1, 0.2, 0.3], 'COSINE')` |

For Oracle 26ai, `build_oracle_hnsw_subquery()` isolates `ORDER BY VECTOR_DISTANCE(...) FETCH APPROX FIRST :k ROWS ONLY` in a derived table with outer `ROWNUM` so the CBO preserves `VECTOR INDEX HNSW SCAN` (which `ROW_NUMBER() OVER (...)` disables), and `build_oracle_hybrid_rrf_sql()` fuses HNSW vector search with Oracle Text `CONTAINS(..., 1) > 0` / `SCORE(1)` via Reciprocal Rank Fusion:

```python
from sqlspec import sql
from sqlspec.builder import build_oracle_hybrid_rrf_sql

emb = sql.column("embedding")
vec_q = (
    sql
    .select("id", emb.cosine_similarity([0.1, 0.2, 0.3]).as_("score"))
    .from_("docs")
    .order_by(emb.vector_distance([0.1, 0.2, 0.3], metric="cosine").asc())
    .limit(5)
)
# SQL: SELECT id, 1 - (embedding <=> CAST(ARRAY[0.1, 0.2, 0.3] AS VECTOR))
# SQL: AS score FROM docs ORDER BY (embedding <=> ...) ASC LIMIT :limit
# Parameters: {'limit': 5}

rrf_sql = build_oracle_hybrid_rrf_sql(
    "docs", vector_column="embedding", text_column="body", id_column="id"
)
# SQL: WITH vec_candidates AS (...), fts_candidates AS (...) SELECT ...
# Parameters: {'query_vec': ..., 'fts_query': ..., 'top_n': ...}
```

## Parameterized Raw SQL Escape Hatches

Use `sql.raw(fragment, **params)` to inject dialect-specific SQL expressions with bound named parameters, or `sql.to_literal(val)` for AST literals. Never interpolate untrusted input into `sql.raw()`:

```{ .python title="docs/examples/builder/raw_expressions.py" }
--8<-- "docs/examples/builder/raw_expressions.py:example"
```

Continue to [DML, Upserts, MERGE and DDL Schema Builders](query-builder-dml-ddl.md).
