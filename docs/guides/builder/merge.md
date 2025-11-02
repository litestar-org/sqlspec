# MERGE Statement Builder Guide

## Overview

The MERGE statement (also known as UPSERT) allows you to insert, update, or delete rows in a target table based on a join condition with a source table or dataset. SQLSpec provides a fluent builder API for constructing type-safe MERGE statements with automatic parameter binding.

## When to Use MERGE vs INSERT ON CONFLICT

**Use MERGE when**:
- You need conditional updates or deletes based on match status
- You want to handle "not matched by source" cases (SQL Server pattern)
- Your database doesn't support INSERT ON CONFLICT (Oracle, BigQuery, SQL Server)
- You need complex multi-condition logic in a single statement

**Use INSERT ON CONFLICT when**:
- Simple upsert: insert if not exists, update if exists
- PostgreSQL or SQLite database
- You want simpler, more readable syntax for basic upserts

**Example comparison**:

```python
# Simple upsert - use INSERT ON CONFLICT
await session.execute(
    sql.insert_into("products")
       .values(id=1, name="Widget", price=29.99)
       .on_conflict("id")
       .do_update(name="EXCLUDED.name", price="EXCLUDED.price")
)

# Complex conditional logic - use MERGE
await session.execute(
    sql.merge()
       .into("products", alias="t")
       .using({"id": 1, "name": "Widget", "price": 29.99}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(
           condition="src.price < t.price",  # Only update if cheaper
           price="src.price"
       )
       .when_not_matched_then_insert(columns=["id", "name", "price"])
)
```

## Database Compatibility

| Database   | MERGE Support | Minimum Version | Notes |
|------------|---------------|-----------------|-------|
| PostgreSQL | ✅ Yes | 15.0+ | Full MERGE support |
| Oracle     | ✅ Yes | 9i+ | Full MERGE support, production-proven |
| BigQuery   | ✅ Yes | All versions | Full MERGE support |
| SQL Server | ✅ Yes | 2008+ | Supports WHEN NOT MATCHED BY SOURCE |
| MySQL      | ❌ No | N/A | Use INSERT ON DUPLICATE KEY UPDATE |
| SQLite     | ❌ No | N/A | Use INSERT ON CONFLICT |
| DuckDB     | ❌ No | N/A | Use INSERT ON CONFLICT |

**For unsupported databases**: SQLSpec will raise `DialectNotSupportedError` with helpful alternatives.

## Basic MERGE Examples

### Simple Upsert

Insert new products or update existing ones:

```python
from sqlspec import sql

# Single row upsert
query = (
    sql.merge()
       .into("products", alias="t")
       .using({"id": 1, "name": "Widget", "price": 29.99}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name", price="src.price")
       .when_not_matched_then_insert(columns=["id", "name", "price"])
)

result = await session.execute(query)
print(f"Rows affected: {result.rows_affected}")
```

### Using Table as Source

Merge from a staging table:

```python
query = (
    sql.merge()
       .into("products", alias="t")
       .using("staging_products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(
           name="s.name",
           price="s.price",
           updated_at="CURRENT_TIMESTAMP"
       )
       .when_not_matched_then_insert(
           columns=["id", "name", "price", "created_at"],
           values=["s.id", "s.name", "s.price", "CURRENT_TIMESTAMP"]
       )
)

result = await session.execute(query)
```

### Using Subquery as Source

Merge from a SELECT query:

```python
source_query = (
    sql.select()
       .columns("id", "name", "price")
       .from_("external_products")
       .where("active = true")
)

query = (
    sql.merge()
       .into("products", alias="t")
       .using(source_query, alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(name="s.name", price="s.price")
       .when_not_matched_then_insert(columns=["id", "name", "price"])
)

result = await session.execute(query)
```

## Conditional Updates and Deletes

### Update Only When Condition Met

Update price only if the new price is lower:

```python
# Using merge_ property (cleaner syntax - recommended)
query = (
    sql.merge_
       .into("products", alias="t")
       .using({"id": 1, "price": 24.99}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(
           condition="src.price < t.price",  # Only if cheaper
           price="src.price",
           updated_at="CURRENT_TIMESTAMP"
       )
)

# Or use merge() method (equivalent)
query = sql.merge().into("products", alias="t").using(...)

result = await session.execute(query)
```

### Delete Matched Rows

Delete products that match certain conditions:

```python
query = (
    sql.merge()
       .into("products", alias="t")
       .using("discontinued_products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_delete(condition="s.discontinued_date < CURRENT_DATE - INTERVAL '90 days'")
)

result = await session.execute(query)
```

### Multiple WHEN Clauses

You can combine multiple WHEN clauses for complex logic:

```python
query = (
    sql.merge()
       .into("inventory", alias="t")
       .using("new_stock", alias="s")
       .on("t.product_id = s.product_id")
       # Update if quantity changed significantly
       .when_matched_then_update(
           condition="ABS(s.quantity - t.quantity) > 10",
           quantity="s.quantity",
           updated_at="CURRENT_TIMESTAMP"
       )
       # Delete if quantity is zero
       .when_matched_then_delete(condition="s.quantity = 0")
       # Insert new products
       .when_not_matched_then_insert(
           columns=["product_id", "quantity", "created_at"],
           values=["s.product_id", "s.quantity", "CURRENT_TIMESTAMP"]
       )
)

result = await session.execute(query)
```

## SQL Server: BY SOURCE Pattern

SQL Server supports `WHEN NOT MATCHED BY SOURCE` for handling rows that exist in the target but not in the source:

```python
# Mark products as inactive if not in the import
query = (
    sql.merge()
       .into("products", alias="t")
       .using("imported_products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(name="s.name", price="s.price")
       .when_not_matched_then_insert(columns=["id", "name", "price"])
       .when_not_matched_by_source_then_update(active=False)
)

result = await session.execute(query)
```

**Note**: PostgreSQL, Oracle, and BigQuery do not support `WHEN NOT MATCHED BY SOURCE`. Use a separate UPDATE or DELETE statement instead.

## NULL Value Handling

SQLSpec automatically handles NULL values in MERGE statements by inferring types from non-NULL values:

```python
# Some rows have NULL values
data = [
    {"id": 1, "name": "Widget", "price": 29.99},
    {"id": 2, "name": "Gadget", "price": None},  # NULL price
    {"id": 3, "name": None, "price": 19.99},     # NULL name
]

query = (
    sql.merge()
       .into("products", alias="t")
       .using(data, alias="src")  # Bulk operation (coming in Phase 2)
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name", price="src.price")
       .when_not_matched_then_insert(columns=["id", "name", "price"])
)

result = await session.execute(query)
```

SQLSpec scans all records to find non-NULL values for type inference. If all values are NULL, it defaults to NUMERIC (PostgreSQL) or VARCHAR2 (Oracle).

## Parameter Binding

All values in MERGE statements are automatically parameterized for safety:

```python
# Dict values are parameterized
query = (
    sql.merge()
       .into("users", alias="t")
       .using({"id": user_id, "name": user_name}, alias="src")  # Parameterized
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name")  # src.name is a column reference
       .when_not_matched_then_insert(columns=["id", "name"])
)

# Inspect parameters
stmt = query.build()
print(stmt.parameters)  # {'id': 123, 'name': 'Alice'}
print(stmt.sql)         # Shows placeholders
```

**Column references vs literals**:
- `"src.name"` - Column reference (not parameterized)
- `"CURRENT_TIMESTAMP"` - SQL function (not parameterized)
- `user_name` - Python variable (parameterized)
- `42` - Literal value (parameterized)

## Performance Characteristics

### Single-Row Operations

For single-row upserts, MERGE and INSERT ON CONFLICT have similar performance:

```python
# MERGE - ~1-2ms
result = await session.execute(
    sql.merge()
       .into("products", alias="t")
       .using({"id": 1, "name": "Widget"}, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(name="src.name")
       .when_not_matched_then_insert(columns=["id", "name"])
)

# INSERT ON CONFLICT - ~1-2ms (PostgreSQL, SQLite)
result = await session.execute(
    sql.insert_into("products")
       .values(id=1, name="Widget")
       .on_conflict("id")
       .do_update(name="EXCLUDED.name")
)
```

**Recommendation**: Use INSERT ON CONFLICT for simple upserts when available (PostgreSQL, SQLite).

### Bulk Operations (Coming in Phase 2)

Bulk MERGE operations will use database-specific optimizations:

- **PostgreSQL**: `jsonb_to_recordset()` for N≥500 rows
- **Oracle**: `JSON_TABLE()` for N≥1000 rows or when hitting parameter limits
- **BigQuery**: `UNNEST(ARRAY<STRUCT>)` for all bulk operations
- **MySQL/SQLite**: Multi-row VALUES with chunking

Expected performance: 5-10x faster than looping `execute()` for N≥100 rows.

## Best Practices

### 1. Always Use Aliases

Aliases make ON and WHEN clauses more readable:

```python
# GOOD - Clear which table is being referenced
sql.merge()
   .into("products", alias="t")
   .using(data, alias="src")
   .on("t.id = src.id")
   .when_matched_then_update(name="src.name")

# BAD - Ambiguous without aliases
sql.merge()
   .into("products")
   .using(data)
   .on("id = id")  # Which id?
```

### 2. Use Keyword Arguments

Keyword arguments are more readable than positional:

```python
# GOOD
.when_matched_then_update(name="src.name", price="src.price")

# OK but less clear
.when_matched_then_update({"name": "src.name", "price": "src.price"})
```

### 3. Specify Columns Explicitly

Always specify columns in INSERT clauses for maintainability:

```python
# GOOD - Explicit columns
.when_not_matched_then_insert(
    columns=["id", "name", "price"],
    values=["src.id", "src.name", "src.price"]
)

# BAD - Relies on column order
.when_not_matched_then_insert(
    values=["src.id", "src.name", "src.price"]
)
```

### 4. Handle All Cases

Consider all possible outcomes:

```python
# GOOD - Handles all cases
sql.merge()
   .into("products", alias="t")
   .using(source, alias="s")
   .on("t.id = s.id")
   .when_matched_then_update(...)      # Existing products
   .when_not_matched_then_insert(...)  # New products
   # Optionally: when_matched_then_delete() for cleanup
```

### 5. Use Transactions

Wrap MERGE in transactions for atomicity:

```python
async with session.begin():
    result = await session.execute(merge_query)
    if result.rows_affected > 1000:
        await session.rollback()  # Safety check
        raise ValueError("Too many rows affected")
```

## Common Patterns

### Upsert with Timestamps

Track when records were created and updated:

```python
query = (
    sql.merge()
       .into("products", alias="t")
       .using(data, alias="src")
       .on("t.id = src.id")
       .when_matched_then_update(
           name="src.name",
           price="src.price",
           updated_at="CURRENT_TIMESTAMP"
       )
       .when_not_matched_then_insert(
           columns=["id", "name", "price", "created_at", "updated_at"],
           values=["src.id", "src.name", "src.price", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"]
       )
)
```

### Soft Delete Pattern

Mark deleted records instead of removing them:

```python
query = (
    sql.merge()
       .into("products", alias="t")
       .using("active_products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(name="s.name", is_deleted=False)
       .when_not_matched_by_source_then_update(is_deleted=True)  # SQL Server only
)
```

For PostgreSQL/Oracle, use a separate UPDATE:

```python
# First, merge active products
await session.execute(merge_query)

# Then, mark missing products as deleted
await session.execute(
    sql.update("products")
       .set(is_deleted=True)
       .where("id NOT IN (SELECT id FROM active_products)")
)
```

### Incremental ETL

Load only changed records:

```python
query = (
    sql.merge()
       .into("warehouse.products", alias="t")
       .using("staging.products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(
           condition="s.updated_at > t.updated_at",  # Only if newer
           name="s.name",
           price="s.price",
           updated_at="s.updated_at"
       )
       .when_not_matched_then_insert(columns=["id", "name", "price", "updated_at"])
)
```

## Troubleshooting

### Error: "Dialect not supported"

**Problem**: MySQL, SQLite, or DuckDB don't support MERGE.

**Solution**: Use INSERT ON CONFLICT or INSERT ON DUPLICATE KEY:

```python
# PostgreSQL, SQLite
sql.insert_into("products")
   .values(id=1, name="Widget")
   .on_conflict("id")
   .do_update(name="EXCLUDED.name")

# MySQL
sql.raw(
    "INSERT INTO products (id, name) VALUES (:id, :name) "
    "ON DUPLICATE KEY UPDATE name = VALUES(name)",
    id=1, name="Widget"
)
```

### Error: "Could not parse ON condition"

**Problem**: Invalid SQL syntax in ON clause.

**Solution**: Check your condition syntax:

```python
# BAD
.on("id = id")  # Ambiguous

# GOOD
.on("t.id = src.id")  # Fully qualified
```

### Performance Issues

**Problem**: MERGE is slower than expected for large datasets.

**Solution**: Wait for Phase 2 bulk operations, or use staging table pattern:

```python
# Create temporary staging table
await session.execute("CREATE TEMP TABLE staging_products AS SELECT * FROM products LIMIT 0")

# Bulk insert into staging
await session.execute_many(
    "INSERT INTO staging_products VALUES (:id, :name, :price)",
    data
)

# MERGE from staging (single statement)
await session.execute(
    sql.merge()
       .into("products", alias="t")
       .using("staging_products", alias="s")
       .on("t.id = s.id")
       .when_matched_then_update(name="s.name", price="s.price")
       .when_not_matched_then_insert(columns=["id", "name", "price"])
)
```

## Next Steps

- **Bulk Operations**: Phase 2 will add support for `.using([{}, {}, {}])` with automatic strategy selection
- **Unified API**: Phase 3 will add `session.upsert(table, data, on=["id"])` for database-agnostic upserts
- **Performance Guide**: Benchmark results and optimization tips

## See Also

- [INSERT ON CONFLICT Guide](insert-on-conflict.md)
- [Bulk Operations Guide](bulk-operations.md) (Coming in Phase 2)
- [Upsert API Guide](../upsert.md) (Coming in Phase 3)
- [PostgreSQL Adapter](../adapters/asyncpg.md)
- [Oracle Adapter](../adapters/oracledb.md)
- [BigQuery Adapter](../adapters/bigquery.md)
