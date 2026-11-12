# SQLGlot Dialects

```python
from sqlspec.dialects import (
    DB2,
    ParadeDB,
    PGTextSearch,
    PGVector,
    Spangres,
    Spanner,
    SQLAnywhere,
    Sybase,
    Teradata,
)
```

Custom SQLGlot dialect implementations registered automatically via Python entry points (`sqlglot.dialects`) when SQLSpec is installed. See the [Query Builder Guide](../learn/query-builder.md) for syntax translations and dialect features.

## Cloud Spanner Dialects

::: sqlspec.dialects.spanner.Spanner
    options:
      heading_level: 3

::: sqlspec.dialects.spanner.Spangres
    options:
      heading_level: 3

## Sybase ASE & SAP SQL Anywhere Dialects

::: sqlspec.dialects.sybase.Sybase
    options:
      heading_level: 3

::: sqlspec.dialects.sybase.SQLAnywhere
    options:
      heading_level: 3

## IBM Db2 Dialect

::: sqlspec.dialects.db2.DB2
    options:
      heading_level: 3

## Teradata Vantage Dialect

SQLSpec registers its Teradata dialect with sqlglot as `vantage` and leaves sqlglot's built-in `teradata` dialect unchanged. SQLSpec builders, statement configs, and the Teradata adapters resolve `teradata` to `vantage`; call sqlglot with `vantage` directly to get the same output.

::: sqlspec.dialects.teradata.Vantage
    options:
      heading_level: 3

## PostgreSQL Search & Vector Dialects

::: sqlspec.dialects.postgres.PGVector
    options:
      heading_level: 3

::: sqlspec.dialects.postgres.ParadeDB
    options:
      heading_level: 3

::: sqlspec.dialects.postgres.PGTextSearch
    options:
      heading_level: 3
