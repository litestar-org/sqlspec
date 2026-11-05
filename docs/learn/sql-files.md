# SQL Files

Keeping non-trivial queries in dedicated `.sql` files gives you full editor syntax highlighting, formatter support, and DBA reviewability while retaining SQLSpec's parameter binding, runtime type validation, and AST composition. `SQLFileLoader` and `SQLSpec` parse structured comment directives and inline markers without custom template engines.

## Directive and Marker Cheat Sheet

| Directive or Marker | Scope | Syntax | Purpose |
| --- | --- | --- | --- |
| `-- name:` | Statement header | `-- name: list-users` | Starts a named query block; hyphens normalize to underscores (`list_users`) on lookup. |
| `-- dialect:` | Statement header | `-- dialect: postgres` | Sets the SQLGlot dialect for expression slots, `.builder()`, and `.explain()`. |
| `-- param:` | Statement header | `-- param: status str? Optional filter` | Declares parameter name, runtime type (`?` = optional, defaults to `None`), and description. |
| `-- fragment:` | File section | `-- fragment: active_users_cte` | Declares a reusable static SQL fragment (cannot declare `-- dialect:`, `-- param:`, or `-- slot:`). |
| `/* include: */` | Statement or fragment body | `/* include: active_users_cte */` | Splices a registered fragment (supports nested includes and `<namespace>.<name>` lookup). |
| `-- slot:` | Statement header | `-- slot: ordering = id ASC` | Declares an optional default SQL fill for a matching `/* slot: name */` marker in the body. |
| `/* slot: */` | Statement or fragment body | `/* slot: ordering */` | Reserves a fill point replaced at `spec.get_sql(name, **slots)` retrieval time. |

## Loading SQL Files and Directory Namespaces

Load `.sql` files or directory trees with `spec.load_sql_files(...)` (or register inline strings via `spec.add_named_sql(...)`):

=== "queries.sql"

    ```sql
    -- name: list_teams
    SELECT id, name
    FROM teams
    ORDER BY id;
    ```

=== "app.py"

    ```python
    --8<-- "docs/examples/sql_files/load_sql_files.py:example"
    ```

=== "Inline Registration"

    ```python
    --8<-- "docs/examples/sql_files/named_queries.py:example"
    ```

When `spec.load_sql_files(Path("app/sql"))` scans a directory tree, top-level files register bare query names while nested subdirectories prepend dot-separated relative directory namespaces:

```text
app/sql/
├── users.sql            # -- name: list-users   -> "list_users"
└── billing/
    ├── invoices.sql     # -- name: list-overdue -> "billing.list_overdue"
    └── reports/
        └── mrr.sql      # -- name: monthly-mrr  -> "billing.reports.monthly_mrr"
```

- `spec.list_sql_queries()` / `spec.has_sql_query(name)` / `spec.get_sql_files()`: Inspect registered query names and file paths at startup.
- `spec.reload_sql_files(reload=True)`: Re-reads only `.sql` files whose MD5 checksum changed and invalidates cached statement instances.
- `SQLFileLoader(storage_registry=...)`: Loads `.sql` bundles from S3, GCS, Azure Blob Storage, or `fsspec` aliases registered in `StorageRegistry`.

## Dialect Declarations

Add `-- dialect: <name>` under `-- name:` so SQLGlot expression slots, `SQL.builder()`, and `SQL.explain()` target the exact engine syntax:

```sql
-- name: upsert_subscription
-- dialect: postgres
INSERT INTO subscriptions (account_id, plan)
VALUES (:account_id, :plan)
ON CONFLICT (account_id) DO UPDATE SET plan = EXCLUDED.plan;
```

The loader normalizes these aliases (`DIALECT_ALIASES`) before handing the name to SQLGlot; any other value passes through unchanged:

| Input Alias | Normalized Dialect |
| --- | --- |
| `postgresql`, `pg`, `pgplsql` | `postgres` |
| `plsql`, `oracledb` | `oracle` |
| `tsql` | `mssql` |

<span id="anatomy-of-a-multi-query-sql-file"></span>

## Parameter Type Declarations {#parameter-type-declarations}

Declare `-- param: <name> <type>[?] <description>` headers below `-- name:` to document parameters and validate them:

=== "teams.sql"

    ```sql
    -- name: get_team_by_name
    -- param: name str  The team name to look up
    SELECT id, name FROM teams WHERE name = :name;

    -- name: list_teams
    -- param: name str?  Optional team name filter
    SELECT id, name FROM teams
    WHERE (:name IS NULL OR name = :name)
    ORDER BY id;
    ```

=== "app.py"

    ```python
    --8<-- "docs/examples/sql_files/declared_params.py:example"
    ```

- **Placeholder drift check at load time**: The loader raises `SQLFileParseError` when a declared `-- param:` name is missing from the query's `:placeholder` tokens or positional counts differ. Queries that use fragments or slots are checked at `get_sql()` time instead.
- **Required vs optional at execution**: Omitting a required parameter raises `SQLSpecError` before a cursor is acquired. A `?` type suffix (or a description ending in `(optional)`) defaults an omitted keyword argument to `None`, so `(:name IS NULL OR name = :name)` or `COALESCE(:name, name)` predicates reuse one prepared-statement plan.
- **Runtime type checks**: Non-`None` values are checked against the declared type: `str`, `int`, `float`, `bool`, `bytes`, `date`, `datetime`, `time`, `decimal`, `uuid`, `dict`, `json`, `jsonb`, `list`, `list[int]`, `list[str]`, `list[float]`, `list[bool]`, or `tuple`. Unknown type strings are documentation only unless registered with `register_param_type("money", Decimal)`. Pass `SQLFileLoader(strict_parameter_annotations=True)` to raise `SQLFileParseError` on malformed `-- param:` lines.

<span id="sql-fragments-and-slots"></span>

## Fragments and Slots

Reuse static SQL blocks across queries with `-- fragment: <name>` and `/* include: <name> */`, and fill controlled dynamic SQL markers at retrieval time with `-- slot: <name> = <default>` and `/* slot: <name> */`:

=== "teams.sql"

    ```sql
    --8<-- "docs/examples/sql_files/fragments_and_slots.sql:queries"
    ```

=== "app.py"

    ```python
    --8<-- "docs/examples/sql_files/fragments_and_slots.py:example"
    ```

| Value Type | Example | Splicing Semantics | Parameter Binding |
| --- | --- | --- | --- |
| `str` | `ordering="t.name DESC"` | Spliced verbatim as trusted SQL text (use allowlists for identifiers and directions). | None (bind user data separately). |
| `sqlglot.exp.Expr` | `ordering=exp.column("name", table="t").desc()` | Rendered to SQL in the query's `-- dialect:`. | None. |
| `SQL` | `predicates=SQL("t.name = :team_name", team_name="SQLSpec")` | Splices `SQL.sql` into the marker and merges `SQL.named_parameters` onto the returned statement. | Carries named parameters (`:team_name`) safely into execution. |

!!! warning "Slot Validation and SQLSlotError Rules"
    - **Missing or unknown slots**: A `/* slot: name */` marker without a `-- slot: name = default` header or `get_sql()` keyword argument, or an unknown slot keyword, raises `SQLSlotError`.
    - **Positional binds and filters**: Passing a `SQL` slot value with positional parameters or attached `StatementFilter` objects raises `SQLSlotError` (attach filters to the completed `SQL` returned by `get_sql()`).
    - **Parameter collisions**: Overlapping parameter names between a `SQL` slot and the outer query (or between two slots with unequal values) raise `SQLSlotError`.
    - **Fragment headers and cycles**: Fragments cannot declare `-- dialect:`, `-- param:`, or `-- slot:` headers (`SQLFileParseError`), and cyclic `/* include: */` chains raise `SQLFileParseError`.

## Statement Modifiers and the Builder Bridge

`spec.get_sql(...)` returns an immutable `SQL` instance that supports runtime predicate chaining (`.where_eq()`, `.where_in()`, `.where_like()`, `.order_by()`, `.paginate()`), column projection narrowing (`.select_only(*cols, prune_columns=True)`), execution plan inspection (`.explain(analyze=True, format="json")`), and conversion into a fluent SQLGlot AST builder via `.builder()`:

=== "Dynamic Modifiers"

    ```python
    --8<-- "docs/examples/sql_files/dynamic_where.py:example"
    ```

=== "Schema Projection & Builder Bridge"

    ```python
    from pathlib import Path
    import msgspec

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig


    class Product(msgspec.Struct):
        id: int
        sku: str
        name: str
        price_cents: int

    spec = SQLSpec()
    config = spec.add_config(
        SqliteConfig(connection_config={"database": ":memory:"})
    )
    spec.load_sql_files(Path("sql"))
    with spec.provide_session(config) as session:
        product = session.select_one(
            spec.get_sql("get-product"), id=1, schema_type=Product
        )
        catalog = session.select(
            spec.get_sql("list-products")
            .where("price_cents >= :min_price")
            .select_only("id", "sku", "name", "price_cents", prune_columns=True),
            min_price=1000,
            sku=None,
            schema_type=Product,
        )
        plan = session.select(spec.get_sql("list-products").explain(), sku=None)
        ranked_query = (
            spec.get_sql("list-products")
            .builder()
            .join("inventory", on="inventory.product_id = products.id")
            .build()
        )
    ```

<span id="composition-rules"></span>

## Query Composition Decision Matrix

| Mechanism | Phase | Mutates SQL Structure? | Parameter-Safe? | Best For |
| --- | --- | --- | --- | --- |
| `-- param:` (`:name`, `str?`) | Execution bind | No (fixed SQL plan) | Yes (driver bind) | Scalar filters and optional `(:p IS NULL OR col = :p)` predicates that maximize prepared-statement cache hits. |
| `-- fragment:` + `/* include: */` | Load / `get_sql()` | Static splice | N/A (template text) | Sharing CTEs, `FROM` / `JOIN` blocks, or column lists across queries in `.sql` files. |
| `-- slot:` + `/* slot: */` | `get_sql(**slots)` | Controlled marker fill | Yes when filled with `SQL(..., key=val)` | Swapping trusted `ORDER BY` expressions, table/view names, or optional multi-clause predicates at retrieval time. |
| `SQL.where*()` / `StatementFilter` | Pre-execution AST | Appends `WHERE` / `ORDER BY` / `LIMIT` | Yes (auto-named binds) | Attaching HTTP query filters (`SearchFilter`, `InCollectionFilter`, `CursorFilter`) or `.select_only()` onto a base query. |
| [`sql.select()` / `SQL.builder()`](query-builder.md) | Runtime AST builder | Full AST construction | Yes (auto-named binds) | Dynamic multi-join queries, conditional `GROUP BY` or window functions, cross-dialect vector distance, and programmatic DDL/DML. |
