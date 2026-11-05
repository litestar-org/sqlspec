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

| Input Alias (`DIALECT_ALIASES`) | Normalized SQLGlot Dialect | Canonical SQLSpec Dialects |
| --- | --- | --- |
| `postgresql`, `pg`, `pgplsql` | `postgres` | `postgres`, `mysql`, `mariadb`, `oracle`, `mssql`, `sqlite`, `duckdb` |
| `plsql`, `oracledb` | `oracle` | `bigquery`, `spanner`, `spangres`, `cockroach`, `db2`, `sybase`, `sqlanywhere` |
| `tsql` | `mssql` | — |

<span id="anatomy-of-a-multi-query-sql-file"></span>

## Parameter Type Declarations {#parameter-type-declarations}

Declare `-- param: <name> <type>[?] <description>` headers below `-- name:` to document parameters and enforce three validation layers:

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

1. **Load-Time Placeholder Drift Check** (`SQLFileLoader._check_declared_parameters`): Raises `SQLFileParseError` if a declared `-- param:` name is absent from the query's `:placeholder` tokens or positional counts mismatch (evaluated at `get_sql()` time when a query uses fragments or slots).
2. **Runtime Required vs Nullable (`?`) Execution**: Omitting a required parameter raises `SQLSpecError` before acquiring a cursor. Suffixing the type with `?` (or trailing `(optional)` in the description) defaults omitted keyword arguments to `None` (`NULL`), enabling `(:name IS NULL OR name = :name)` or `COALESCE(:name, name)` predicates that reuse a single prepared-statement execution plan.
3. **Runtime Type Registry & Custom Types**: Non-`None` values are checked against `_TYPE_REGISTRY` (`str`, `int`, `float`, `bool`, `bytes`, `date`, `datetime`, `time`, `decimal`, `uuid`, `dict`, `json`, `jsonb`, `list`, `list[int]`, `list[str]`, `list[float]`, `list[bool]`, `tuple`). Unknown type strings act as documentation-only unless registered via `register_param_type("money", Decimal)`. Pass `SQLFileLoader(strict_parameter_annotations=True)` to raise `SQLFileParseError` on malformed `-- param:` lines.

<span id="sql-fragments-and-slots"></span>

## Fragments and Slots

Reuse a query's SQL structure with **fragments**, and choose parts of that SQL at retrieval time with **slots**. Both features work with `SQLFileLoader` and `SQLSpec`.

Declare a shared section with `-- fragment: name`, then insert it with `/* include: name */`. In this example, both queries use the same common table expression to select teams in one workspace:

```sql
--8<-- "docs/examples/sql_files/fragments_and_slots.sql:queries"
```

A `/* slot: name */` marker reserves a place for SQL text. Put `-- slot: name = default SQL` in the query's leading header block to supply a default. Here, `list_teams` defaults to all teams ordered by ID. The `count_teams` predicate has no default, so callers must fill it.

Save that SQL as `teams.sql` and pass its path as `sql_file` in the following example:

```python
from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL

sql_file = Path("teams.sql")

--8<-- "docs/examples/sql_files/fragments_and_slots.py:example"
```

The default query returns the two teams in workspace 42. Changing `ordering` puts SQLSpec first. Filling `predicates` with the bound `SQL` value selects SQLSpec, and the count query returns `1`. The third team belongs to another workspace and stays outside both results.

Pass slot values to `get_sql()`, then pass the query's remaining data values to `session.select()` or another execution method. A `SQL` slot carries its own named bindings; `team_name` travels with the predicate while `workspace_id` is bound at execution.

### Choosing Safe Slot Values

Slots accept trusted SQL strings, SQLGlot expressions, and `SQL` objects. Strings are inserted as SQL text. Use a `SQL` object with named placeholders for user data, as shown above. Choose ordering expressions and identifiers from trusted code or an allowlist.

A missing required slot or unknown slot name raises `SQLSlotError`. Positional bindings and statement filters on a `SQL` slot also raise this error. Apply filters to the completed statement returned by `get_sql()` instead. Use distinct names for slot bindings and the outer query's placeholders, such as `team_name` and `workspace_id`. Reusing an outer placeholder name in a bound slot raises `SQLSlotError`.

Fragments may include other fragments, and a fragment may contain slot markers. Defaults belong to the named query's header; fragments cannot declare `-- slot:`, `-- param:`, or `-- dialect:` headers. Load all referenced fragments before retrieving a query.

For namespaced files, lookup rules, inspection methods, and cache behavior, see the [loader composition reference](../reference/loader.md#composition-rules).

## Composing Dynamic WHERE Clauses onto Named SQL

Static `.sql` files often need optional runtime predicates for search, tenant scoping, or pagination. Because `spec.get_sql(...)` returns an immutable `SQL` statement object, calling `.where(...)`, `.where_eq(...)`, `.where_like(...)`, or `.paginate(...)` returns a new derived `SQL` instance without mutating the cached base template:

=== "SQL (`users.sql`)"

    ```sql
    -- name: list_users
    SELECT id, name, status
    FROM users
    ```

=== "Python"

    ```python
    --8<-- "docs/examples/sql_files/dynamic_where.py:example"
    ```

You can also pass `StatementFilter` objects (`LimitOffsetFilter`, `SearchFilter`, `OrderByFilter`) alongside `spec.get_sql("...")` inside `session.select(...)`—remembering to keep all bound parameters named when filters are attached.

## Kebab-Case Named Queries and Schema Projection

In production services, organize queries using `kebab-case` identifiers (`-- name: list-products`, `-- name: get-product`) matching domain entity routes. Combine loaded statements with `.where(...)` and decode rows directly into immutable `msgspec.Struct` models:

=== "SQL (`products.sql`)"

    ```sql
    -- name: list-products
    -- param: sku str?  Optional SKU match
    SELECT id, sku, name, price_cents
    FROM products
    WHERE (:sku IS NULL OR sku = :sku)
    ORDER BY id;

    -- name: get-product
    -- param: id int  Product identifier
    SELECT id, sku, name, price_cents
    FROM products
    WHERE id = :id;
    ```

=== "Python"

    ```python
    from pathlib import Path
    import msgspec

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig


    class Product(msgspec.Struct):
        """Typed product projection."""

        id: int
        sku: str
        name: str
        price_cents: int


    spec = SQLSpec()
    config = spec.add_config(
        SqliteConfig(connection_config={"database": ":memory:"})
    )
    spec.load_sql_files(Path("products.sql"))

    with spec.provide_session(config) as session:
        query = spec.get_sql("get-product")
        product = session.select_one(query, id=1, schema_type=Product)

        active_query = spec.get_sql("list-products").where(
            "price_cents >= :min_price"
        )
        catalog = session.select(
            active_query, min_price=1000, sku=None, schema_type=Product
        )
    ```

## Recap

- Store multi-line CTEs, window functions, and analytical queries in `.sql` files under `-- name:` blocks and load them once at startup via `spec.load_sql_files(...)`.
- Declare `-- param:` headers on shared SQL queries to catch missing required arguments early and expose parameter metadata via `spec.get_query_parameters(...)`.
- Chain `.where_eq(...)`, `.where_like(...)`, or `StatementFilter` arguments onto `spec.get_sql(...)` instead of concatenating raw `WHERE` strings in Python.
