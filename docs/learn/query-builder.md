# Query Builder

When queries depend on conditional joins, dynamic column lists, cross-dialect upsert syntax, or portable DDL generation, SQLSpec's fluent `sql` builder constructs SQLGlot AST statements that compile cleanly to your active adapter's SQL dialect.

## Building `SELECT`, `INSERT`, and `UPDATE` Queries

Import `sql` from `sqlspec` to compose `SELECT`, `INSERT`, `UPDATE`, and `DELETE` queries with automatic parameter binding:

=== "SELECT"

    ```{ .python title="docs/examples/builder/select_query.py" }
    --8<-- "docs/examples/builder/select_query.py:example"
    ```

=== "INSERT"

    ```{ .python title="docs/examples/builder/insert_query.py" }
    --8<-- "docs/examples/builder/insert_query.py:example"
    ```

=== "UPDATE"

    ```{ .python title="docs/examples/builder/update_query.py" }
    --8<-- "docs/examples/builder/update_query.py:example"
    ```

=== "DELETE"

    ```python
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(
        SqliteConfig(connection_config={"database": ":memory:"})
    )

    with spec.provide_session(config) as session:
        session.execute(
            "CREATE TABLE sessions (id INTEGER PRIMARY KEY, expired INTEGER)"
        )
        session.execute_many(
            "INSERT INTO sessions (expired) VALUES (?)", [(0,), (1,), (1,)]
        )

        delete_expired = sql.delete().from_("sessions").where_eq("expired", 1)
        result = session.execute(delete_expired)
        assert result.rows_affected == 2
    ```

Common predicate helpers on `sql.select(...)`, `sql.update(...)`, and `sql.delete(...)` include:

- `.where(...)`, `.where_eq(col, val)`, `.where_neq(col, val)`, `.where_like(col, pattern)`
- `.where_in(col, values)`, `.where_not_in(col, values)`
- `.where_is_null(col)`, `.where_is_not_null(col)` (note the `_is_` verb)
- `.distinct()`, `.group_by(...)`, `.having(...)`, `.order_by(...)`, `.limit(n)`, `.offset(m)`

## Joins and Common Table Expressions

Chain `.join(...)`, `.left_join(...)`, `.right_join(...)`, or `.cross_join(...)` to combine tables with explicit join predicates, and attach `.with_(alias, subquery)` for readable Common Table Expressions (CTEs):

```{ .python title="docs/examples/builder/complex_joins.py" }
--8<-- "docs/examples/builder/complex_joins.py:example"
```

```python
high_value = (
    sql
    .select("customer_id", "amount")
    .from_("orders")
    .where("amount >= :min_amount", min_amount=100)
)
summary = (
    sql
    .select("customer_id", sql.raw("COUNT(*) AS order_count"))
    .with_("high_value", high_value)
    .from_("high_value")
    .group_by("customer_id")
)
```

## Upserts (`ON CONFLICT` and `sql.merge`)

Use `.on_conflict(...).do_nothing()` or `.on_conflict(...).do_update(...)` for conflict-tolerant inserts on SQLite, PostgreSQL, CockroachDB, and DuckDB, or `sql.merge(...)` for ANSI `MERGE INTO` workflows on Oracle, SQL Server, BigQuery, and PostgreSQL 15+:

```{ .python title="docs/examples/builder/upsert.py" }
--8<-- "docs/examples/builder/upsert.py:example"
```

## DDL Builders and Schema Inference

Construct portable `CREATE TABLE` (including generated/computed columns), covering `CREATE INDEX ... INCLUDE (...)`, and `CREATE SEQUENCE` statements—or synthesize `CreateTable` builders directly from catalog metadata, PyArrow schemas, or Python record sequences:

```python
from sqlspec import sql
from sqlspec.builder import CreateTable

users_ddl = (
    sql
    .create_table("users", dialect="postgres")
    .column("id", "BIGINT", primary_key=True, identity="ALWAYS")
    .column("email", "TEXT", not_null=True)
    .column(
        "email_lower",
        "TEXT",
        generated_expression="LOWER(email)",
        persisted=True,
    )
    .to_sql()
)

covering_idx = (
    sql
    .create_index("idx_users_email", dialect="postgres")
    .on_table("users")
    .columns("email_lower")
    .include("id")
    .unique()
    .to_sql()
)

seq_ddl = sql.create_sequence(
    "order_num_seq", dialect="postgres", start_with=1000, increment_by=1
).to_sql()

events_ddl = CreateTable.from_records(
    "events",
    [{"id": 1, "event_type": "signup", "active": True}],
    primary_key="id",
    dialect="postgres",
).to_sql()
```

`CreateTable.from_metadata`, `CreateTable.from_arrow_schema`, and `CreateTable.from_records` map types and constraints into dialect-valid SQL across PostgreSQL, CockroachDB, SQLite, DuckDB, MySQL, MariaDB, Oracle, SQL Server, Sybase ASE, SAP SQL Anywhere, IBM Db2, Spanner, and BigQuery.

## Raw SQL Fragments and `RETURNING`

Embed dialect-specific aggregate or window expressions inside builder projections with `sql.raw(...)`, and chain `.returning(...)` on `INSERT`, `UPDATE`, or `DELETE` builders to fetch mutated columns in a single round-trip:

```{ .python title="docs/examples/builder/raw_expressions.py" }
--8<-- "docs/examples/builder/raw_expressions.py:example"
```

Only pass trusted, static SQL expressions to `sql.raw(...)`; bind user-supplied values via builder parameters.

## Dialect Transpilation and Optimization Notes

- **Automatic Dialect Targeting**: When executed through `session.execute(query)`, the builder compiles against the session's configured dialect (`postgres`, `cockroachdb`, `sqlite`, `oracle`, `bigquery`, `spanner`, `spangres`, `duckdb`, `db2`, `tsql`, `sybase`, `sqlanywhere`, `mysql`, `mariadb`) automatically.
- **Preserving Verbatim Identifier Casing**: By default, `StatementConfig(enable_optimization=True)` runs lightweight SQLGlot qualification and simplification rules. On engines like BigQuery where optimizer qualification lowercases column aliases, pass `StatementConfig(dialect="bigquery", enable_optimization=False)` if exact input casing must be preserved verbatim.
- **`sqlglot[c]` Subclassing Compatibility**: When `sqlspec[mypyc]` (`sqlglot[c]`) is installed, SQLGlot's core AST classes are compiled C extensions; compose queries with `sql.*` and `sql.raw(...)` rather than subclassing `sqlglot.exp` nodes at runtime.

## Recap

- Reach for `sql.select`, `sql.insert`, `sql.update`, `sql.delete`, and `sql.merge` when predicates, projections, or conflict clauses vary dynamically at runtime.
- Use `sql.create_table`, `sql.create_index`, `sql.create_sequence`, and `CreateTable.from_*` for dialect-portable schema DDL and data-driven bootstrapping.
- Combine `sql.raw(...)` with `.returning(...)` when mixing custom SQL expressions into structured DML builders.
