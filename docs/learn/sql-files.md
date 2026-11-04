# SQL Files

Keeping non-trivial queries in dedicated `.sql` files gives you full editor syntax highlighting, formatter support, DBA reviewability, and zero Python string-escaping noise while retaining SQLSpec's parameter binding, validation, and dynamic predicate composition.

## Inline Named Queries

For small query registries, register named statements directly on `SQLSpec` with `add_named_sql` and retrieve a reusable `SQL` object via `get_sql`:

```python
--8<-- "docs/examples/sql_files/named_queries.py:example"
```

## Loading `.sql` Files from Disk or Storage

Group related queries inside `.sql` files using `-- name: <query_name>` comment headers and load individual files or entire directories with `spec.load_sql_files(...)` (or `SQLFileLoader` directly):

=== "SQL (`queries.sql`)"

    ```sql
    -- name: list_teams
    SELECT id, name
    FROM teams
    ORDER BY id;
    ```

=== "Python"

    ```python
    --8<-- "docs/examples/sql_files/load_sql_files.py:example"
    ```

Query names inside nested directories can be organized by namespace (or hyphenated identifiers normalized automatically when accessed) and shared across any session opened from `spec`. `SQLFileLoader` also integrates with `StorageRegistry` so SQL bundles can be loaded from local directories or registered object storage aliases.

### Organizing Query Directories

When `spec.load_sql_files(Path("app/sql"))` scans a directory tree, you can keep queries organized per domain (`app/sql/billing.sql`, `app/sql/users.sql`) and inspect loaded definitions at startup:

- `spec.list_sql_queries()`: Returns all registered query names so startup tests can assert expected SQL assets are present.
- `spec.get_sql("list_customer_invoices")`: Returns the immutable `SQL` template ready for execution or filter composition.
- `SQLFileLoader(storage_registry=...)`: Loads `.sql` files from S3, GCS, Azure Blob Storage, or embedded package paths registered in `StorageRegistry`.

<span id="anatomy-of-a-multi-query-sql-file"></span>

## Declared Parameter Headers (`-- param:`)

Each `-- name:` block can start with `-- dialect:` and `-- param:` headers. The following file groups two related queries.

Add `-- param: <name> <type>[?]  <description>` headers immediately below `-- name:` to document expected arguments and enforce automatic required-parameter validation before a query ever touches the database connection:

=== "SQL (`teams.sql`)"

    ```sql
    -- name: get_team_by_name
    -- param: name str  The team name to look up
    SELECT id, name
    FROM teams
    WHERE name = :name;

    -- name: list_teams
    -- param: name str?  Optional team name filter
    SELECT id, name
    FROM teams
    WHERE (:name IS NULL OR name = :name)
    ORDER BY id;
    ```

=== "Python"

    ```python
    --8<-- "docs/examples/sql_files/declared_params.py:example"
    ```

Suffixing the type with `?` (such as `str?` or `int?`) marks the parameter optional and defaults omitted keyword arguments to `None` (`NULL`), enabling clean `(:name IS NULL OR name = :name)` filter patterns in pure SQL. Omitting a required parameter raises `SQLSpecError` immediately with the missing parameter name.

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

## Composing Dynamic `WHERE` Clauses onto Named SQL

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


    class Product(msgspec.Struct, frozen=True, gc=False):
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
