# SQLSpec Adapter Skills

Individual skills for each database adapter with adapter-specific guidance.

## Available Adapter Skills

### PostgreSQL Adapters

- **[asyncpg.md](asyncpg.md)** - AsyncPG (async, high performance) ✅
- **[psycopg.md](psycopg.md)** - Psycopg (sync/async, feature-rich) ✅
- **[psqlpy.md](psqlpy.md)** - Psqlpy (Rust-based, extreme performance) ✅

### SQLite Adapters

- **[sqlite.md](sqlite.md)** - SQLite (sync, embedded) ✅
- **[aiosqlite.md](aiosqlite.md)** - AioSQLite (async, embedded) ✅

### Analytics & OLAP

- **[duckdb.md](duckdb.md)** - DuckDB (columnar, analytics) ✅

### Oracle

- **[oracledb.md](oracledb.md)** - Oracle Database (sync/async, enterprise) ✅

### MySQL/MariaDB

- **[asyncmy.md](asyncmy.md)** - Asyncmy (async MySQL) ✅

### Cloud & Multi-Database

- **[bigquery.md](bigquery.md)** - Google BigQuery (data warehouse) ✅
- **[adbc.md](adbc.md)** - ADBC (Arrow-native, multi-database) ✅

## Adapter Selection Guide

| Use Case | Recommended Adapter | Skill File |
|----------|-------------------|-----------|
| PostgreSQL async | AsyncPG | [asyncpg.md](asyncpg.md) |
| PostgreSQL sync | Psycopg | psycopg.md |
| PostgreSQL extreme perf | Psqlpy | psqlpy.md |
| Embedded database | SQLite or DuckDB | sqlite.md, duckdb.md |
| Analytics queries | DuckDB | duckdb.md |
| Oracle enterprise | OracleDB | oracledb.md |
| MySQL/MariaDB | Asyncmy | asyncmy.md |
| Cloud data warehouse | BigQuery | bigquery.md |
| Multi-database | ADBC | adbc.md |
| Arrow ecosystem | ADBC or DuckDB | adbc.md, duckdb.md |

## Skill Template

Each adapter skill covers:

1. When to use this adapter
2. Configuration examples
3. Parameter binding style
4. Adapter-specific features
5. Performance optimization
6. Best practices
7. Common issues
8. Real-world examples

## Contributing

To add a new adapter skill:

1. Copy `asyncpg.md` as a template
2. Fill in adapter-specific details
3. Add to this README
4. Link from main skill.md
