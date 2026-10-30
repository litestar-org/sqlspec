# SQLGlot Dialects

```python
from sqlspec.dialects import DB2, ParadeDB, PGTextSearch, PGVector, Spangres, Spanner
```

Custom SQLGlot dialect implementations registered automatically via Python entry points (`sqlglot.dialects`) when SQLSpec is installed. See the [Query Builder Guide](../learn/query-builder.md) for syntax translations and dialect features.

## Cloud Spanner Dialects

::: sqlspec.dialects.spanner.Spanner
    options:
      heading_level: 3

::: sqlspec.dialects.spanner.Spangres
    options:
      heading_level: 3

## IBM Db2 Dialect

::: sqlspec.dialects.db2.DB2
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
