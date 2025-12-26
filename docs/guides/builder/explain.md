# EXPLAIN Plan Builder Guide

## Overview

The EXPLAIN statement allows you to analyze how a database will execute a query, showing the query execution plan, estimated costs, and optimization decisions. SQLSpec provides a fluent builder API for constructing EXPLAIN statements with dialect-aware SQL generation that automatically adapts to your target database.

## When to Use EXPLAIN

**Use EXPLAIN when**:
- Debugging slow queries to understand why they are slow
- Verifying that indexes are being used correctly
- Understanding join strategies and table scan behavior
- Comparing different query approaches before choosing one
- Learning how your database processes SQL

**Use EXPLAIN ANALYZE when**:
- You need actual runtime statistics (not just estimates)
- Verifying that estimated costs match reality
- Profiling query performance with real data

**Warning**: EXPLAIN ANALYZE executes the query, which can modify data (for INSERT/UPDATE/DELETE) and incur costs (BigQuery). Use with caution in production.

## Database Compatibility

| Database   | EXPLAIN | EXPLAIN ANALYZE | Formats | Notes |
|------------|---------|-----------------|---------|-------|
| PostgreSQL | Yes | Yes | TEXT, JSON, XML, YAML | Full options support (BUFFERS, TIMING, etc.) |
| MySQL      | Yes | Yes | TRADITIONAL, JSON, TREE | ANALYZE always uses TREE format |
| SQLite     | Yes (QUERY PLAN) | No | TEXT | Output format varies by SQLite version |
| DuckDB     | Yes | Yes | TEXT, JSON | JSON format via (FORMAT JSON) |
| Oracle     | Yes | No | TEXT | Two-step process (EXPLAIN PLAN FOR + DBMS_XPLAN) |
| BigQuery   | Yes | Yes | TEXT | ANALYZE executes query and incurs costs |
| Spanner    | N/A | N/A | N/A | Uses API-based query mode parameter |

## Basic Usage

### SQL Class Method

The simplest way to get an explain plan for an existing SQL statement:

```python
from sqlspec.core import SQL

stmt = SQL("SELECT * FROM users WHERE status = :status", {"status": "active"})
explain_stmt = stmt.explain()

# Execute with any adapter
async with config.provide_session() as session:
    result = await session.execute(explain_stmt)
    for row in result.data:
        print(row)
```

### SQLFactory Method

Use `sql.explain()` to wrap any statement:

```python
from sqlspec.builder import sql

# Explain a raw SQL string
explain = sql.explain("SELECT * FROM users WHERE id = :id", id=1)
explain_sql = explain.build()

# Explain a query builder
query = sql.select("*").from_("users").where_eq("id", 1)
explain = sql.explain(query, analyze=True)
explain_sql = explain.build()
```

### QueryBuilder Integration

All query builders (Select, Insert, Update, Delete, Merge) have an `.explain()` method:

```python
from sqlspec.builder import Select

query = (
    Select("*", dialect="postgres")
    .from_("users")
    .where("status = :status", status="active")
    .order_by("created_at DESC")
)

# Get explain plan
explain = query.explain(analyze=True, format="json")
explain_sql = explain.build()

# Execute
async with config.provide_session() as session:
    result = await session.execute(explain_sql)
    plan = result.data[0]  # JSON plan data
```

## Fluent API

The `Explain` builder provides a fluent interface for configuring EXPLAIN options:

```python
from sqlspec.builder import Explain

explain = (
    Explain("SELECT * FROM users", dialect="postgres")
    .analyze()      # Execute and show actual runtime stats
    .verbose()      # Show additional information
    .format("json") # Output format
    .buffers()      # Show buffer usage (PostgreSQL)
    .timing()       # Show timing information (PostgreSQL)
    .costs()        # Show cost estimates
    .build()
)
```

### Available Methods

| Method | Description | Databases |
|--------|-------------|-----------|
| `.analyze()` | Execute query and show actual statistics | PostgreSQL, MySQL, DuckDB, BigQuery |
| `.verbose()` | Show additional information | PostgreSQL |
| `.format(fmt)` | Set output format | PostgreSQL, MySQL, DuckDB |
| `.costs()` | Include cost estimates | PostgreSQL |
| `.buffers()` | Include buffer usage statistics | PostgreSQL (requires ANALYZE) |
| `.timing()` | Include actual timing | PostgreSQL (requires ANALYZE) |
| `.summary()` | Include summary information | PostgreSQL |
| `.memory()` | Include memory usage | PostgreSQL 17+ |
| `.settings()` | Include configuration parameters | PostgreSQL 12+ |
| `.wal()` | Include WAL usage | PostgreSQL 13+ (requires ANALYZE) |
| `.generic_plan()` | Generate plan ignoring parameter values | PostgreSQL 16+ |

## Dialect-Specific Examples

### PostgreSQL

PostgreSQL has the most comprehensive EXPLAIN support:

```python
from sqlspec.builder import Explain, Select
from sqlspec.core.explain import ExplainFormat

# Basic explain
query = Select("*", dialect="postgres").from_("users")
explain = query.explain().build()
# EXPLAIN SELECT * FROM users

# Full analysis with all options
explain = (
    Explain("SELECT * FROM orders WHERE total > 100", dialect="postgres")
    .analyze()
    .verbose()
    .buffers()
    .timing()
    .format(ExplainFormat.JSON)
    .build()
)
# EXPLAIN (ANALYZE, VERBOSE, BUFFERS TRUE, TIMING TRUE, FORMAT JSON) SELECT ...

# PostgreSQL 16+ generic plan
explain = (
    Explain("SELECT * FROM users WHERE id = $1", dialect="postgres")
    .generic_plan()
    .build()
)
# EXPLAIN (GENERIC_PLAN TRUE) SELECT * FROM users WHERE id = $1
```

### MySQL

MySQL supports FORMAT options and ANALYZE (which always uses TREE format):

```python
from sqlspec.builder import Explain

# JSON format for structured output
explain = (
    Explain("SELECT * FROM users", dialect="mysql")
    .format("json")
    .build()
)
# EXPLAIN FORMAT = JSON SELECT * FROM users

# TREE format for hierarchical view
explain = (
    Explain("SELECT * FROM users", dialect="mysql")
    .format("tree")
    .build()
)
# EXPLAIN FORMAT = TREE SELECT * FROM users

# ANALYZE (always uses TREE, ignores format option)
explain = (
    Explain("SELECT * FROM users", dialect="mysql")
    .analyze()
    .build()
)
# EXPLAIN ANALYZE SELECT * FROM users
```

### SQLite

SQLite only supports EXPLAIN QUERY PLAN (no ANALYZE or format options):

```python
from sqlspec.builder import Explain

explain = (
    Explain("SELECT * FROM users WHERE id = ?", dialect="sqlite")
    .build()
)
# EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = ?

# Note: analyze and format options are ignored for SQLite
explain = (
    Explain("SELECT * FROM users", dialect="sqlite")
    .analyze()  # Ignored
    .format("json")  # Ignored
    .build()
)
# EXPLAIN QUERY PLAN SELECT * FROM users
```

**Warning**: SQLite's EXPLAIN QUERY PLAN output format may change between versions. Avoid parsing it programmatically.

### DuckDB

DuckDB supports ANALYZE and JSON format:

```python
from sqlspec.builder import Explain

# Basic explain
explain = Explain("SELECT * FROM users", dialect="duckdb").build()
# EXPLAIN SELECT * FROM users

# With ANALYZE
explain = (
    Explain("SELECT * FROM users", dialect="duckdb")
    .analyze()
    .build()
)
# EXPLAIN ANALYZE SELECT * FROM users

# JSON format
explain = (
    Explain("SELECT * FROM users", dialect="duckdb")
    .format("json")
    .build()
)
# EXPLAIN (FORMAT JSON) SELECT * FROM users
```

### Oracle

Oracle uses a two-step process - the builder generates the first step:

```python
from sqlspec.builder import Explain

explain = (
    Explain("SELECT * FROM users", dialect="oracle")
    .build()
)
# EXPLAIN PLAN FOR SELECT * FROM users

# To view the plan, execute a second query:
# SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())
```

Note: Oracle's EXPLAIN PLAN does not execute the query - it just stores the plan in PLAN_TABLE.

### BigQuery

```python
from sqlspec.builder import Explain

# Estimated plan (no cost)
explain = Explain("SELECT * FROM users", dialect="bigquery").build()
# EXPLAIN SELECT * FROM users

# Actual execution statistics (incurs query costs!)
explain = (
    Explain("SELECT * FROM users", dialect="bigquery")
    .analyze()
    .build()
)
# EXPLAIN ANALYZE SELECT * FROM users
```

**Warning**: EXPLAIN ANALYZE in BigQuery executes the query and incurs costs.

## ExplainOptions and ExplainFormat

For programmatic configuration, use `ExplainOptions` and `ExplainFormat`:

```python
from sqlspec.core.explain import ExplainOptions, ExplainFormat
from sqlspec.builder import Explain

# Create reusable options
options = ExplainOptions(
    analyze=True,
    verbose=True,
    format=ExplainFormat.JSON,
    buffers=True,
    timing=True,
)

# Apply to multiple queries
explain1 = Explain("SELECT * FROM users", dialect="postgres", options=options).build()
explain2 = Explain("SELECT * FROM orders", dialect="postgres", options=options).build()

# Copy and modify options
debug_options = options.copy(summary=True, memory=True)
```

### ExplainFormat Enum

```python
from sqlspec.core.explain import ExplainFormat

ExplainFormat.TEXT         # Default text output
ExplainFormat.JSON         # JSON (PostgreSQL, MySQL, DuckDB)
ExplainFormat.XML          # XML (PostgreSQL only)
ExplainFormat.YAML         # YAML (PostgreSQL only)
ExplainFormat.TREE         # Tree format (MySQL, DuckDB)
ExplainFormat.TRADITIONAL  # Tabular format (MySQL)
```

## Working with Parameters

EXPLAIN preserves parameters from the underlying statement:

```python
from sqlspec.core import SQL
from sqlspec.builder import Explain

# Parameters are preserved
stmt = SQL("SELECT * FROM users WHERE id = :id", {"id": 42})
explain = Explain(stmt).build()

print(explain.named_parameters)  # {'id': 42}

# Execute with parameters
async with config.provide_session() as session:
    result = await session.execute(explain)
```

## Best Practices

### 1. Use JSON Format for Programmatic Analysis

JSON output is easier to parse and analyze programmatically:

```python
explain = query.explain(format="json").build()
result = await session.execute(explain)

# Parse JSON plan
import json
plan = json.loads(result.data[0]["QUERY PLAN"])
print(f"Estimated cost: {plan[0]['Plan']['Total Cost']}")
```

### 2. Always Test with Representative Data

Query plans depend on table statistics. Test with production-like data volumes:

```python
# Good: Test with realistic data
explain = query.explain(analyze=True).build()
result = await session.execute(explain)

# Check actual vs estimated rows
# Significant differences indicate stale statistics
```

### 3. Use ANALYZE Sparingly in Production

EXPLAIN ANALYZE executes the query, which can:
- Modify data (INSERT/UPDATE/DELETE)
- Hold locks
- Consume resources
- Incur costs (BigQuery)

```python
# Development: Use ANALYZE freely
explain = query.explain(analyze=True).build()

# Production: Use plain EXPLAIN
explain = query.explain().build()  # Estimates only, no execution
```

### 4. Compare Query Approaches

Use EXPLAIN to compare different query strategies:

```python
# Approach 1: Subquery
query1 = sql.select("*").from_("orders").where("user_id IN (SELECT id FROM users WHERE active)")

# Approach 2: JOIN
query2 = sql.select("orders.*").from_("orders").join("users", "users.id = orders.user_id").where("users.active")

# Compare plans
explain1 = query1.explain().build()
explain2 = query2.explain().build()

# Execute both and compare costs
```

### 5. Check Index Usage

Verify indexes are being used:

```python
explain = (
    sql.select("*")
    .from_("orders")
    .where("customer_id = :id", id=123)
    .explain(format="json")
    .build()
)

result = await session.execute(explain)
plan = json.loads(result.data[0]["QUERY PLAN"])

# Look for "Index Scan" vs "Seq Scan" in plan
```

## Common Patterns

### Debugging Slow Queries

```python
async def debug_slow_query(session, query):
    """Analyze why a query is slow."""
    explain = (
        Explain(query, dialect="postgres")
        .analyze()
        .buffers()
        .timing()
        .format("json")
        .build()
    )

    result = await session.execute(explain)
    plan = json.loads(result.data[0]["QUERY PLAN"])[0]

    print(f"Total Time: {plan['Execution Time']}ms")
    print(f"Planning Time: {plan['Planning Time']}ms")

    # Find slow nodes
    def find_slow_nodes(node, threshold_ms=100):
        if node.get("Actual Total Time", 0) > threshold_ms:
            yield node
        for child in node.get("Plans", []):
            yield from find_slow_nodes(child, threshold_ms)

    for slow_node in find_slow_nodes(plan["Plan"]):
        print(f"Slow: {slow_node['Node Type']} - {slow_node['Actual Total Time']}ms")
```

### Query Plan Comparison

```python
async def compare_plans(session, query1, query2, dialect="postgres"):
    """Compare execution plans of two queries."""
    explain1 = Explain(query1, dialect=dialect).format("json").build()
    explain2 = Explain(query2, dialect=dialect).format("json").build()

    result1 = await session.execute(explain1)
    result2 = await session.execute(explain2)

    plan1 = json.loads(result1.data[0]["QUERY PLAN"])[0]["Plan"]
    plan2 = json.loads(result2.data[0]["QUERY PLAN"])[0]["Plan"]

    print(f"Query 1 - Cost: {plan1['Total Cost']}, Rows: {plan1['Plan Rows']}")
    print(f"Query 2 - Cost: {plan2['Total Cost']}, Rows: {plan2['Plan Rows']}")

    if plan1["Total Cost"] < plan2["Total Cost"]:
        print("Query 1 is more efficient")
    else:
        print("Query 2 is more efficient")
```

## Troubleshooting

### Error: "Unknown format option"

**Problem**: Database doesn't support the requested format.

**Solution**: Check database compatibility table above. Use TEXT for maximum compatibility.

### Error: "EXPLAIN ANALYZE not supported"

**Problem**: SQLite and Oracle don't support EXPLAIN ANALYZE.

**Solution**: For SQLite, use EXPLAIN QUERY PLAN without ANALYZE. For Oracle, use DBMS_XPLAN.DISPLAY_CURSOR after executing the query.

### Plan Shows Sequential Scan Instead of Index Scan

**Problem**: Database is not using an index.

**Possible causes**:
1. Table is small (sequential scan is faster)
2. No suitable index exists
3. Statistics are out of date
4. Query predicate doesn't match index

**Solution**:
```sql
-- Update statistics (PostgreSQL)
ANALYZE table_name;

-- Check available indexes
\di table_name  -- psql

-- Force index usage (testing only)
SET enable_seqscan = off;
```

## See Also

- [PostgreSQL EXPLAIN Documentation](https://www.postgresql.org/docs/current/sql-explain.html)
- [MySQL EXPLAIN Statement](https://dev.mysql.com/doc/refman/8.0/en/explain.html)
- [SQLite EXPLAIN QUERY PLAN](https://www.sqlite.org/eqp.html)
- [DuckDB EXPLAIN](https://duckdb.org/docs/guides/meta/explain)
- [Oracle DBMS_XPLAN](https://docs.oracle.com/database/121/ARPLS/d_xplan.htm)
- [BigQuery Query Plans](https://cloud.google.com/bigquery/docs/query-plan-explanation)
- [Query Builder Guide](./merge.md)
- [Performance Tuning Guide](../performance/sqlglot.md)
