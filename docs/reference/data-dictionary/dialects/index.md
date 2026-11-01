# Dialect SQL Catalogs

```python
from sqlspec.data_dictionary import DialectConfig, get_data_dictionary_loader, get_dialect_config
```

SQLSpec ships 13 dialect-specific SQL catalog packs under `sqlspec/data_dictionary/dialects/` containing named [`SQLFileLoader`](../../loader.md) queries (`-- name: ...`), version-gated feature profiles (`DialectConfig`), and logical-to-physical type mappings. See the [Data Dictionary Guide](../../../learn/data-dictionary.md) and [Data Dictionary Reference](../../data-dictionary.md) for programmatic usage.

## Dialect Catalog Matrix

| Dialect | Default Schema | DDL Resolution | Domain `.sql` Files | Named Queries | Reference |
| --- | --- | --- | --- | --- | --- |
| **PostgreSQL** (`postgres`) | `public` | AST synthesis + `pg_get_*def` catalog functions | `21` | `26` | [PostgreSQL Catalog](postgres.md) |
| **CockroachDB** (`cockroachdb`) | `public` | Native `crdb_internal.create_statement` + AST fallback | `15` | `18` | [CockroachDB Catalog](cockroachdb.md) |
| **SQLite** (`sqlite`) | `None` | Native `sqlite_master.sql` + `PRAGMA` synthesis | `15` | `23` | [SQLite Catalog](sqlite.md) |
| **DuckDB** (`duckdb`) | `None` | Native `duckdb_*().sql` catalog functions + AST synthesis | `17` | `24` | [DuckDB Catalog](duckdb.md) |
| **MySQL** (`mysql`) | `None` | Native `SHOW CREATE` + `information_schema` synthesis | `18` | `27` | [MySQL Catalog](mysql.md) |
| **MariaDB** (`mariadb`) | `None` | Native `SHOW CREATE` + MariaDB sequence & constraint reflection | `17` | `27` | [MariaDB Catalog](mariadb.md) |
| **Oracle Database** (`oracle`) | `None` | Native `DBMS_METADATA.GET_DDL` + AST fallback | `22` | `36` | [Oracle Database Catalog](oracle.md) |
| **SQL Server** (`mssql`) | `dbo` | AST synthesis + `OBJECT_DEFINITION()` for views/routines/triggers | `19` | `31` | [SQL Server Catalog](mssql.md) |
| **Sybase ASE** (`sybase`) | `dbo` | AST synthesis + `dbo.syscomments` object text reassembly | `9` | `16` | [Sybase ASE Catalog](sybase.md) |
| **SAP SQL Anywhere** (`sqlanywhere`) | `DBA` | AST synthesis + `SYS.SYSTAB` / `SYS.SYSPROCEDURE` definitions | `9` | `16` | [SAP SQL Anywhere Catalog](sqlanywhere.md) |
| **IBM Db2** (`db2`) | `None` | AST synthesis + `SYSCAT` catalog text definitions | `9` | `14` | [IBM Db2 Catalog](db2.md) |
| **Google BigQuery** (`bigquery`) | `None` | Native `INFORMATION_SCHEMA.TABLES.ddl` + AST fallback | `14` | `30` | [Google BigQuery Catalog](bigquery.md) |
| **Google Cloud Spanner** (`spanner`) | `None` | AST synthesis with `INTERLEAVE IN PARENT` & `ROW DELETION POLICY` (`GoogleSQL` & `PostgreSQL` modes) | `5` | `8` | [Google Cloud Spanner Catalog](spanner.md) |
| **Total (`13` dialects)** | - | - | **`190`** | **`296`** | - |

## Cross-Dialect Capability Matrix

Each dialect pack declares boolean `feature_flags` and version-gated `feature_versions` inside its `DialectConfig`:

| Dialect | Schemas | Returning | Upsert | Window | CTE | JSON | Arrays | Vectors | Transactions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [PostgreSQL](postgres.md) | `Yes` | `>=8.2.0` | `>=9.5.0` | `>=8.4.0` | `>=8.4.0` | `>=9.2.0` | `Yes` | `No` | `Yes` |
| [CockroachDB](cockroachdb.md) | `Yes` | `>=20.1.0` | `>=19.2.0` | `>=19.1.0` | `>=19.1.0` | `>=20.1.0` | `Yes` | `No` | `Yes` |
| [SQLite](sqlite.md) | `No` | `>=3.35.0` | `>=3.24.0` | `>=3.25.0` | `>=3.8.3` | `>=3.38.0` | `No` | `No` | `Yes` |
| [DuckDB](duckdb.md) | `Yes` | `>=0.8.0` | `>=0.8.0` | `Yes` | `Yes` | `Yes` | `Yes` | `No` | `Yes` |
| [MySQL](mysql.md) | `Yes` | `No` | `Yes` | `>=8.0.2` | `>=8.0.1` | `>=5.7.8` | `No` | `No` | `Yes` |
| [MariaDB](mariadb.md) | `Yes` | `Yes` | `Yes` | `>=10.2.0` | `>=10.2.1` | `>=10.2.7` | `No` | `No` | `Yes` |
| [Oracle Database](oracle.md) | `Yes` | `No` | `No` | `No` | `No` | `No` | `No` | `No` | `Yes` |
| [SQL Server](mssql.md) | `Yes` | `No` | `No` | `No` | `No` | `No` | `No` | `No` | `Yes` |
| [Sybase ASE](sybase.md) | `Yes` | `No` | `No` | `No` | `No` | `No` | `No` | `No` | `Yes` |
| [SAP SQL Anywhere](sqlanywhere.md) | `Yes` | `No` | `Yes` | `>=10.0.0` | `>=10.0.0` | `No` | `No` | `No` | `Yes` |
| [IBM Db2](db2.md) | `Yes` | `No` | `No` | `>=8.1.0` | `>=8.1.0` | `No` | `No` | `No` | `Yes` |
| [Google BigQuery](bigquery.md) | `Yes` | `No` | `Yes` | `Yes` | `Yes` | `Yes` | `Yes` | `No` | `Yes` |
| [Google Cloud Spanner](spanner.md) | `No` | `No` | `No` | `No` | `No` | `Yes` | `No` | `No` | `No` |

## Logical-to-Physical Type Mappings

`driver.data_dictionary.get_optimal_type(driver, type_category)` resolves portable logical type categories using each dialect's `DialectConfig.type_mappings` and active engine version:

| Dialect | `json` | `uuid` | `boolean` | `timestamp` | `text` | `blob` | `vector` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [PostgreSQL](postgres.md) | `JSONB` | `UUID` | `BOOLEAN` | `TIMESTAMP WITH TIME ZONE` | `TEXT` | `BYTEA` | `-` |
| [CockroachDB](cockroachdb.md) | `JSONB` | `UUID` | `BOOL` | `TIMESTAMPTZ` | `STRING` | `BYTES` | `-` |
| [SQLite](sqlite.md) | `JSON` | `TEXT` | `INTEGER` | `TIMESTAMP` | `TEXT` | `BLOB` | `-` |
| [DuckDB](duckdb.md) | `JSON` | `UUID` | `BOOLEAN` | `TIMESTAMP` | `TEXT` | `BLOB` | `-` |
| [MySQL](mysql.md) | `JSON` | `VARCHAR(36)` | `TINYINT(1)` | `TIMESTAMP` | `TEXT` | `BLOB` | `-` |
| [MariaDB](mariadb.md) | `JSON` | `VARCHAR(36)` | `TINYINT(1)` | `TIMESTAMP` | `TEXT` | `BLOB` | `-` |
| [Oracle Database](oracle.md) | `JSON` | `RAW(16)` | `NUMBER(1)` | `TIMESTAMP` | `CLOB` | `BLOB` | `-` |
| [SQL Server](mssql.md) | `NVARCHAR(MAX)` | `UNIQUEIDENTIFIER` | `BIT` | `DATETIME2(6)` | `NVARCHAR(MAX)` | `VARBINARY(MAX)` | `-` |
| [Sybase ASE](sybase.md) | `TEXT` | `VARCHAR(36)` | `BIT` | `BIGDATETIME` | `TEXT` | `IMAGE` | `-` |
| [SAP SQL Anywhere](sqlanywhere.md) | `LONG VARCHAR` | `UNIQUEIDENTIFIER` | `BIT` | `TIMESTAMP` | `LONG VARCHAR` | `LONG BINARY` | `-` |
| [IBM Db2](db2.md) | `CLOB` | `VARCHAR(36)` | `BOOLEAN` | `TIMESTAMP` | `CLOB` | `BLOB` | `-` |
| [Google BigQuery](bigquery.md) | `JSON` | `STRING` | `BOOL` | `TIMESTAMP` | `STRING` | `BYTES` | `-` |
| [Google Cloud Spanner](spanner.md) | `JSON` | `BYTES(16)` | `BOOL` | `TIMESTAMP` | `STRING(MAX)` | `BYTES(MAX)` | `-` |
