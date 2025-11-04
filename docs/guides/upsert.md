# Unified Upsert API

SQLSpec provides a unified `sql.upsert()` API that automatically selects the appropriate database strategy for "insert or update" operations across all supported databases.

## Overview

The upsert operation handles the common pattern of "insert if not exists, otherwise update". Different databases implement this using different SQL syntax:

- **MERGE-based** (PostgreSQL 15+, Oracle, BigQuery): Use MERGE statements
- **INSERT ON CONFLICT** (SQLite, DuckDB, MySQL): Use INSERT with conflict handling

SQLSpec abstracts these differences with a single `sql.upsert()` factory method that automatically selects the correct builder based on your database dialect.

## Quick Start

### Basic Usage

```python
from sqlspec import sql

# Automatically selects the right strategy for your database
upsert_query = (
    sql.upsert("products", dialect="postgres")
    .using([{"id": 1, "name": "Widget", "price": 19.99}], alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(upsert_query)
```

### Dialect Selection

The `sql.upsert()` method automatically returns the appropriate builder:

| Database | Builder Type | SQL Strategy |
|----------|-------------|--------------|
| PostgreSQL 15+ | `Merge` | MERGE statement |
| Oracle | `Merge` | MERGE statement |
| BigQuery | `Merge` | MERGE statement |
| SQLite | `Insert` | INSERT ON CONFLICT |
| DuckDB | `Insert` | INSERT ON CONFLICT |
| MySQL | `Insert` | INSERT ON DUPLICATE KEY UPDATE |

```python
# PostgreSQL - returns Merge builder
pg_upsert = sql.upsert("products", dialect="postgres")
assert isinstance(pg_upsert, Merge)

# SQLite - returns Insert builder
sqlite_upsert = sql.upsert("products", dialect="sqlite")
assert isinstance(sqlite_upsert, Insert)
```

## MERGE-Based Upserts (PostgreSQL, Oracle, BigQuery)

For databases that support MERGE statements, `sql.upsert()` returns a `Merge` builder.

### Single Row Upsert

```python
from sqlspec import sql

# PostgreSQL upsert
upsert_query = (
    sql.upsert("products", dialect="postgres")
    .using({"id": 1, "name": "Widget", "price": 19.99}, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(upsert_query)
```

**Generated SQL (PostgreSQL)**:
```sql
MERGE INTO products AS t
USING (
  SELECT * FROM jsonb_to_recordset(:data) AS src(id INTEGER, name TEXT, price NUMERIC)
) AS src
ON t.id = src.id
WHEN MATCHED THEN
  UPDATE SET name = src.name, price = src.price
WHEN NOT MATCHED THEN
  INSERT (id, name, price)
  VALUES (src.id, src.name, src.price)
```

### Bulk Upsert (High Performance)

Upsert multiple rows efficiently using JSON-based strategies:

```python
from decimal import Decimal

# Prepare bulk data
products = [
    {"id": 1, "name": "Widget", "price": Decimal("19.99")},
    {"id": 2, "name": "Gadget", "price": Decimal("29.99")},
    {"id": 3, "name": "Doohickey", "price": Decimal("39.99")},
]

# Bulk upsert
bulk_upsert = (
    sql.upsert("products", dialect="postgres")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(bulk_upsert)
```

**Performance**: Handles 1000+ rows efficiently with a single database round-trip.

### Oracle Upsert

Oracle uses `JSON_TABLE` for bulk operations:

```python
# Oracle upsert with JSON_TABLE strategy
oracle_upsert = (
    sql.upsert("products", dialect="oracle")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(oracle_upsert)
```

**Generated SQL (Oracle)**:
```sql
MERGE INTO products t
USING (
  SELECT * FROM JSON_TABLE(:data, '$[*]'
    COLUMNS(
      id NUMBER PATH '$.id',
      name VARCHAR2(4000) PATH '$.name',
      price NUMBER PATH '$.price'
    )
  )
) src
ON (t.id = src.id)
WHEN MATCHED THEN
  UPDATE SET name = src.name, price = src.price
WHEN NOT MATCHED THEN
  INSERT (id, name, price)
  VALUES (src.id, src.name, src.price)
```

### BigQuery Upsert

BigQuery uses `UNNEST` for array processing:

```python
# BigQuery upsert
bq_upsert = (
    sql.upsert("products", dialect="bigquery")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(bq_upsert)
```

## INSERT ON CONFLICT Upserts (SQLite, DuckDB, MySQL)

For databases without MERGE support, `sql.upsert()` returns an `Insert` builder configured for conflict handling.

### SQLite/DuckDB Upsert

```python
# SQLite upsert
sqlite_upsert = (
    sql.upsert("products", dialect="sqlite")
    .values(id=1, name="Widget", price=19.99)
    .on_conflict("id")
    .do_update(name="EXCLUDED.name", price="EXCLUDED.price")
)

result = await session.execute(sqlite_upsert)
```

**Generated SQL (SQLite)**:
```sql
INSERT INTO products (id, name, price)
VALUES (:id, :name, :price)
ON CONFLICT (id)
DO UPDATE SET
  name = EXCLUDED.name,
  price = EXCLUDED.price
```

### MySQL Upsert

MySQL uses `ON DUPLICATE KEY UPDATE`:

```python
# MySQL upsert
mysql_upsert = (
    sql.upsert("products", dialect="mysql")
    .values(id=1, name="Widget", price=19.99)
    .on_duplicate_key_update(name="VALUES(name)", price="VALUES(price)")
)

result = await session.execute(mysql_upsert)
```

**Generated SQL (MySQL)**:
```sql
INSERT INTO products (id, name, price)
VALUES (:id, :name, :price)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  price = VALUES(price)
```

## Handling NULL Values

Both MERGE and INSERT ON CONFLICT handle NULL values correctly:

```python
# Upsert with NULL values
upsert_with_nulls = (
    sql.upsert("products", dialect="postgres")
    .using([
        {"id": 1, "name": "Widget", "price": None},
        {"id": 2, "name": "Gadget", "price": 29.99},
    ], alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(upsert_with_nulls)
```

NULL values are preserved in both insert and update operations.

## Conditional Upserts

### MERGE with Additional Conditions

Add conditions to match and non-match clauses:

```python
# Only update if new price is lower
conditional_upsert = (
    sql.upsert("products", dialect="postgres")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(
        name="src.name",
        price="src.price",
        where_condition="src.price < t.price"
    )
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)
```

### INSERT ON CONFLICT with WHERE

```python
# Only update active products
conditional_sqlite = (
    sql.upsert("products", dialect="sqlite")
    .values(id=1, name="Widget", price=19.99)
    .on_conflict("id")
    .do_update(
        name="EXCLUDED.name",
        price="EXCLUDED.price",
        where="products.active = 1"
    )
)
```

## Multi-Database Applications

Use the factory default dialect or specify explicitly:

```python
from sqlspec import sql, SQLSpec, AsyncpgConfig, SqliteConfig

# Configure multi-database setup
sqlspec = SQLSpec()
sqlspec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/main"}))
sqlspec.add_config(SqliteConfig(pool_config={"database": "cache.db"}))

# Use default dialect from factory
default_upsert = sql.upsert("products")  # Uses first configured dialect

# Override dialect for specific database
pg_upsert = sql.upsert("products", dialect="postgres")
sqlite_upsert = sql.upsert("products", dialect="sqlite")
```

## Best Practices

### 1. Always Specify ON Condition

```python
# Good - Explicit ON condition
good_upsert = (
    sql.upsert("products", dialect="postgres")
    .using(data, alias="src")
    .on("t.id = src.id")  # Clear match condition
    .when_matched_then_update(name="src.name")
    .when_not_matched_then_insert(id="src.id", name="src.name")
)
```

### 2. Use Bulk Operations for Performance

```python
# Efficient - Single query for 1000 rows
bulk_data = [{"id": i, "name": f"Product {i}"} for i in range(1000)]
bulk_upsert = (
    sql.upsert("products", dialect="postgres")
    .using(bulk_data, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name")
    .when_not_matched_then_insert(id="src.id", name="src.name")
)

# Inefficient - 1000 separate queries
for row in bulk_data:
    single_upsert = sql.upsert("products", dialect="postgres").using(row, alias="src")...
```

### 3. Handle Type Conversions

Use appropriate Python types for database compatibility:

```python
from decimal import Decimal
from datetime import datetime

# Use Decimal for precise numeric values
products = [
    {
        "id": 1,
        "name": "Widget",
        "price": Decimal("19.99"),  # Not float
        "created_at": datetime.now(),
    }
]

upsert = (
    sql.upsert("products", dialect="postgres")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(
        id="src.id",
        name="src.name",
        price="src.price",
        created_at="src.created_at"
    )
)
```

### 4. Debug with to_sql()

Use `to_sql()` to inspect generated SQL:

```python
upsert = sql.upsert("products", dialect="postgres").using(data, alias="src")...

# View SQL with placeholders
print(upsert.to_sql())

# View SQL with actual values (for debugging only!)
print(upsert.to_sql(show_parameters=True))
```

**Warning**: Never use `to_sql(show_parameters=True)` for actual query execution. Always use parameterized queries for security.

## Performance Considerations

### Batch Sizes

Different databases have different optimal batch sizes:

| Database | Recommended Batch Size | Notes |
|----------|----------------------|-------|
| PostgreSQL | 1000-5000 rows | Limited by parameter count (32767) |
| Oracle | 500-1000 rows | JSON_TABLE efficient, but watch for LOB limits |
| BigQuery | 10000+ rows | Optimized for large datasets |
| SQLite | 100-500 rows | In-memory faster for small batches |
| DuckDB | 1000-5000 rows | Columnar storage efficient |
| MySQL | 100-1000 rows | Watch for max_allowed_packet |

### Memory Usage

Bulk operations load all data into memory. For very large datasets:

```python
# Process in chunks for datasets > 100K rows
chunk_size = 5000

for i in range(0, len(large_dataset), chunk_size):
    chunk = large_dataset[i:i + chunk_size]

    upsert = (
        sql.upsert("products", dialect="postgres")
        .using(chunk, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    await session.execute(upsert)
```

## Error Handling

### Dialect Validation

The builder validates dialect support at build time:

```python
from sqlspec.exceptions import DialectNotSupportedError

try:
    # This will raise when build() is called
    invalid = (
        sql.merge(dialect="mysql")  # MySQL doesn't support MERGE
        .into("products")
        .using(data, alias="src")
        .on("t.id = src.id")
    )
    invalid.build()  # Raises DialectNotSupportedError
except DialectNotSupportedError as e:
    print(f"Error: {e}")
    # Use INSERT ON CONFLICT instead
    valid = sql.upsert("products", dialect="mysql")  # Returns Insert builder
```

### Constraint Violations

Handle unique constraint violations:

```python
from sqlspec.exceptions import IntegrityError

try:
    upsert = (
        sql.upsert("products", dialect="sqlite")
        .values(id=1, name="Widget", price=19.99)
        .on_conflict("id")
        .do_update(name="EXCLUDED.name")
    )
    result = await session.execute(upsert)
except IntegrityError as e:
    # Handle constraint violation
    logger.error(f"Constraint violation: {e}")
```

## Migration Examples

### From Raw SQL to sql.upsert()

**Before** (PostgreSQL):
```python
# Raw SQL - fragile, dialect-specific
await session.execute("""
    MERGE INTO products AS t
    USING (SELECT * FROM jsonb_to_recordset(:data) AS src(id INTEGER, name TEXT)) AS src
    ON t.id = src.id
    WHEN MATCHED THEN UPDATE SET name = src.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (src.id, src.name)
""", {"data": json.dumps(products)})
```

**After**:
```python
# Type-safe, portable, maintainable
upsert = (
    sql.upsert("products", dialect="postgres")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name")
    .when_not_matched_then_insert(id="src.id", name="src.name")
)
result = await session.execute(upsert)
```

### From INSERT ON CONFLICT to sql.upsert()

**Before** (SQLite):
```python
# Manual conflict handling
await session.execute("""
    INSERT INTO products (id, name, price)
    VALUES (:id, :name, :price)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      price = EXCLUDED.price
""", {"id": 1, "name": "Widget", "price": 19.99})
```

**After**:
```python
# Unified API
upsert = (
    sql.upsert("products", dialect="sqlite")
    .values(id=1, name="Widget", price=19.99)
    .on_conflict("id")
    .do_update(name="EXCLUDED.name", price="EXCLUDED.price")
)
result = await session.execute(upsert)
```

## See Also

- [MERGE Builder Guide](./builder/merge.md) - Detailed MERGE statement documentation
- [INSERT Builder Guide](./builder/insert.md) - INSERT statement documentation
- [Builder API Overview](./builder/index.md) - General builder pattern documentation
- [Type System](./types.md) - Type handling and conversions
