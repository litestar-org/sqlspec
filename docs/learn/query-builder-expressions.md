# Expressions, Window Functions and Vector Search

Compose typed aggregate, scalar, conditional, window, and vector search expressions using either `sql.*` factory methods or `Column` descriptors (`sql.column(...)`).

## Aggregations and GROUP BY Extensions

Aggregate helpers are available on `sql` (`sql.count()`, `sql.count_distinct()`, `sql.sum()`, `sql.avg()`, `sql.min()`, `sql.max()`) and on `Column` (`col.count()`, `col.count_distinct()`, `col.sum()`, `col.avg()`, `col.min()`, `col.max()`, `Column.count_all()`). Group rows with `.group_by(...)`, `.group_by_rollup(...)`, `.group_by_cube(...)`, `.group_by_grouping_sets(...)`, or `sql.rollup()` / `sql.cube()` / `sql.grouping_sets()`, and filter aggregates with `.having(...)`. The `# SQL:` comments omit identifier quoting and table qualification, as on the [SELECT page](query-builder.md#inspecting-and-executing-built-queries):

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
    .having("SUM(amount) > 1000")
)
# SQL: SELECT region, year, COUNT(DISTINCT user_id) AS buyers,
# SQL: SUM(amount) AS total FROM orders
# SQL: GROUP BY ROLLUP (region, year) HAVING SUM(amount) > 1000
# Parameters: {}
```

## Scalar, String, Math and Type Conversion Functions

| Category | SQLFactory Helpers | Column Methods |
| --- | --- | --- |
| Type Conversion | `sql.cast(col, dtype)`, `sql.to_literal(val)` | `col.cast(dtype)` |
| NULL Handling | `sql.coalesce(*exprs)`, `sql.nvl(c, v)`, `sql.nvl2(c, a, b)`, `sql.decode(c, s1, r1, ...)` | `col.coalesce(*vals)` |
| String Functions | `sql.concat(*exprs)`, `sql.upper(c)`, `sql.lower(c)`, `sql.length(c)` | `col.lower()`, `col.upper()`, `col.length()`, `col.trim()`, `col.substring(pos, len)` |
| Math Functions | `sql.round(c, decimals)` | `col.abs()`, `col.round(decimals)`, `col.floor()`, `col.ceil()` |

Column methods return a `FunctionColumn`, which supports `.cast()`, `.as_()`, ordering, and comparison helpers but not further string or math methods, so `email.trim().lower()` is not available:

```python
from sqlspec import sql

email = sql.column("email")
price = sql.column("price")
scalar_q = sql.select(
    email.lower().as_("norm_email"),
    sql.coalesce("nickname", "email").as_("display_name"),
    price.round(2).cast("NUMERIC(10, 2)").as_("rounded_price"),
).from_("users")
# SQL: SELECT LOWER(email) AS norm_email,
# SQL: COALESCE(nickname, email) AS display_name,
# SQL: CAST(ROUND(price, 2) AS DECIMAL(10, 2)) AS rounded_price FROM users
# Parameters: {}
```

## Conditional CASE Expressions

Build `CASE WHEN ... THEN ... ELSE ... END` expressions with `sql.case()` or the `sql.case_` property. `.when()` takes a SQL condition string or a SQLGlot expression, and result values render as literals:

```python
from sqlspec import sql

tier = (
    sql
    .case()
    .when("amount >= 1000", "enterprise")
    .when("amount >= 100", "pro")
    .else_("free")
    .end()
    .as_("tier")
)
case_q = sql.select("id", tier).from_("orders")
# SQL: SELECT id, CASE WHEN amount >= 1000 THEN 'enterprise'
# SQL: WHEN amount >= 100 THEN 'pro' ELSE 'free' END AS tier
# SQL: FROM orders
# Parameters: {}
```

## Window and Analytic Functions

SQLSpec provides both direct factory methods and fluent `WindowFunctionBuilder` properties (`.partition_by(...).order_by(...).as_(...)`):

| Function | Direct Factory Method | Fluent Property Builder |
| --- | --- | --- |
| `ROW_NUMBER` / `RANK` / `DENSE_RANK` | `sql.row_number(...)`, `sql.rank(...)`, `sql.dense_rank(...)` | `sql.row_number_`, `sql.rank_`, `sql.dense_rank_` |
| `LAG` / `LEAD` | `sql.lag(col, offset, default, ...)`, `sql.lead(col, ...)` | `sql.lag_`, `sql.lead_` |
| `COUNT` / `SUM` `OVER` | `sql.count_over(col, ...)`, `sql.sum_over(col, ...)` | `sql.count_over_`, `sql.sum_over_` |
| `AVG` / `MIN` / `MAX` `OVER` | `sql.avg_over(col, ...)`, `sql.min_over(...)`, `sql.max_over(...)` | `sql.avg_over_`, `sql.min_over_`, `sql.max_over_` |

Direct factory methods accept `partition_by=` and `order_by=`; alias the result with `.as_(...)`:

```python
from sqlspec import sql

salary = sql.column("salary")
win_q = sql.select(
    "name",
    sql.row_number_.partition_by("dept").order_by(salary.desc()).as_("rn"),
    sql.sum_over("salary", partition_by="dept").as_("dept_payroll"),
).from_("employees")
# SQL: SELECT name, ROW_NUMBER() OVER (PARTITION BY dept
# SQL: ORDER BY salary DESC) AS rn,
# SQL: SUM(salary) OVER (PARTITION BY dept) AS dept_payroll FROM employees
# Parameters: {}
```

## Cross-Dialect Vector Distance and Oracle Hybrid RRF

Call `Column.vector_distance(query_vec, metric=...)` (`"euclidean"`, `"cosine"`, `"inner_product"`, `"euclidean_squared"`) or `Column.cosine_similarity(query_vec)` to compile native vector distance operators per dialect. The cosine metric compiles to:

| Dialect | Native Metrics | Compiled SQL Output (cosine) |
| --- | --- | --- |
| `postgres` | `euclidean` (`<->`), `cosine` (`<=>`), `inner_product` (`<#>`) | `embedding <=> CAST(ARRAY[0.1, 0.2, 0.3] AS VECTOR)` |
| `oracle` | `euclidean`, `cosine`, `inner_product` (`DOT`), `euclidean_squared` | `VECTOR_DISTANCE(embedding, TO_VECTOR('[0.1, 0.2, 0.3]'), COSINE)` |
| `mysql` | `euclidean`, `cosine`, `inner_product` (`DOT`) | `DISTANCE(embedding, STRING_TO_VECTOR('[0.1, 0.2, 0.3]'), 'COSINE')` |
| `duckdb` | `euclidean`, `cosine`, `inner_product` | `array_cosine_distance(embedding, CAST([0.1, 0.2, 0.3] AS DOUBLE[3]))` |
| `bigquery` | `euclidean`, `cosine`, `inner_product` | `COSINE_DISTANCE(embedding, [0.1, 0.2, 0.3])` |
| No dialect | All four | `VECTOR_DISTANCE(embedding, ARRAY(0.1, 0.2, 0.3), 'COSINE')` |

Metrics without a native form for a dialect fall back to the generic `VECTOR_DISTANCE(...)` call.

For Oracle 26ai, `build_oracle_hnsw_subquery()` isolates `ORDER BY VECTOR_DISTANCE(...) FETCH APPROX FIRST :top_k ROWS ONLY` in a derived table and assigns `ROWNUM` in the outer query, because `ROW_NUMBER() OVER (...)` in the same query block disables the `VECTOR INDEX HNSW SCAN` plan. `build_oracle_hybrid_rrf_sql()` returns a SQL string that fuses HNSW vector search with Oracle Text `CONTAINS(..., 1) > 0` / `SCORE(1)` through Reciprocal Rank Fusion:

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
built = vec_q.build(dialect="postgres")
# SQL: SELECT id, 1 - (embedding <=> CAST(ARRAY[0.1, 0.2, 0.3] AS VECTOR)) AS score
# SQL: FROM docs ORDER BY embedding <=> CAST(ARRAY[0.1, 0.2, 0.3] AS VECTOR) LIMIT 5
# Parameters: {}

rrf_sql = build_oracle_hybrid_rrf_sql(
    "docs", vector_column="embedding", text_column="body", id_column="id"
)
# SQL: WITH vec_candidates AS (...), fts_candidates AS (...), rrf_fused AS (...)
# SQL: SELECT base.*, rrf.rrf_score ... FETCH FIRST :final_limit ROWS ONLY
# Binds: :query_vec, :fts_query, :candidate_limit, :final_limit
```

## Parameterized Raw SQL Escape Hatches

Use `sql.raw(fragment, **params)` to inject dialect-specific SQL expressions with bound named parameters, or `sql.to_literal(val)` for AST literals. Never interpolate untrusted input into `sql.raw()`:

```{ .python title="docs/examples/builder/raw_expressions.py" }
--8<-- "docs/examples/builder/raw_expressions.py:example"
```

Continue to [DML, Upserts, MERGE and DDL Schema Builders](query-builder-dml-ddl.md).
