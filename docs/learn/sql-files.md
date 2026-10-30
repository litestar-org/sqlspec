# SQL Files

Keeping non-trivial queries in dedicated `.sql` files gives you full editor syntax highlighting, formatter support, DBA reviewability, and zero Python string-escaping noise while retaining SQLSpec's parameter binding, validation, and dynamic predicate composition.

## Inline Named Queries

For small query registries, register named statements directly on `SQLSpec` with `add_named_sql` and retrieve a reusable `SQL` object via `get_sql`:

```python
--8 < --"docs/examples/sql_files/named_queries.py:example"
```

## Loading `.sql` Files from Disk or Storage

Group related queries inside `.sql` files using `-- name: <query_name>` comment headers and load individual files or entire directories with `spec.load_sql_files(...)` (or `SQLFileLoader` directly):

```python
--8 < --"docs/examples/sql_files/load_sql_files.py:example"
```

Query names inside nested directories can be organized by namespace (or hyphenated identifiers normalized automatically when accessed) and shared across any session opened from `spec`. `SQLFileLoader` also integrates with `StorageRegistry` so SQL bundles can be loaded from local directories or registered object storage aliases.

### Anatomy of a Multi-Query `.sql` File

A single `.sql` file can group all reads and writes for a domain module under distinct `-- name:` headers, with optional dialect annotations and `-- param:` declarations:

```sql
-- name: create_invoices_table
CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL
);

-- name: insert_invoice
-- param: customer_id int  Account identifier
-- param: status str       Initial invoice state
-- param: total_cents int  Invoice total in minor currency units
INSERT INTO invoices (customer_id, status, total_cents)
VALUES (:customer_id, :status, :total_cents);

-- name: list_customer_invoices
-- param: customer_id int  Account identifier
-- param: status str?      Optional status filter
SELECT id, customer_id, status, total_cents
FROM invoices
WHERE customer_id = :customer_id
  AND (:status IS NULL OR status = :status)
ORDER BY id DESC;
```

### Organizing Query Directories

When `spec.load_sql_files(Path("app/sql"))` scans a directory tree, you can keep queries organized per domain (`app/sql/billing.sql`, `app/sql/users.sql`) and inspect loaded definitions at startup:

- `spec.list_sql_queries()`: Returns all registered query names so startup tests can assert expected SQL assets are present.
- `spec.get_sql("list_customer_invoices")`: Returns the immutable `SQL` template ready for execution or filter composition.
- `SQLFileLoader(storage_registry=...)`: Loads `.sql` files from S3, GCS, Azure Blob Storage, or embedded package paths registered in `StorageRegistry`.

## Declared Parameter Headers (`-- param:`)

Add `-- param: <name> <type>[?]  <description>` headers immediately below `-- name:` to document expected arguments and enforce automatic required-parameter validation before a query ever touches the database connection:

```python
--8 < --"docs/examples/sql_files/declared_params.py:example"
```

Suffixing the type with `?` (such as `str?` or `int?`) marks the parameter optional and defaults omitted keyword arguments to `None` (`NULL`), enabling clean `(:name IS NULL OR name = :name)` filter patterns in pure SQL. Omitting a required parameter raises `SQLSpecError` immediately with the missing parameter name.

## Composing Dynamic `WHERE` Clauses onto Named SQL

Static `.sql` files often need optional runtime predicates for search, tenant scoping, or pagination. Because `spec.get_sql(...)` returns an immutable `SQL` statement object, calling `.where(...)`, `.where_eq(...)`, `.where_like(...)`, or `.paginate(...)` returns a new derived `SQL` instance without mutating the cached base template:

```python
--8 < --"docs/examples/sql_files/dynamic_where.py:example"
```

You can also pass `StatementFilter` objects (`LimitOffsetFilter`, `SearchFilter`, `OrderByFilter`) alongside `spec.get_sql("...")` inside `session.select(...)`—remembering to keep all bound parameters named when filters are attached.

## Kebab-Case Named Queries and Schema Projection

In production services, organize queries using `kebab-case` identifiers (`-- name: list-products`, `-- name: get-product`) matching domain entity routes. Combine loaded statements with `.where(...)` and decode rows directly into immutable `msgspec.Struct` models:

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
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    # Retrieve pre-compiled kebab-case query template
    query = spec.get_sql("get-product")
    product = session.select_one(query, id=1, schema_type=Product)

    # Chain runtime predicates onto kebab-case named query
    active_query = spec.get_sql("list-products").where("price_cents >= :min_price")
    catalog = session.select(active_query, min_price=1000, sku=None, schema_type=Product)
```

## Recap

- Store multi-line CTEs, window functions, and analytical queries in `.sql` files under `-- name:` blocks and load them once at startup via `spec.load_sql_files(...)`.
- Declare `-- param:` headers on shared SQL queries to catch missing required arguments early and expose parameter metadata via `spec.get_query_parameters(...)`.
- Chain `.where_eq(...)`, `.where_like(...)`, or `StatementFilter` arguments onto `spec.get_sql(...)` instead of concatenating raw `WHERE` strings in Python.
