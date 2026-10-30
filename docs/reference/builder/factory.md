# SQL Factory & Base Builder

```python
from sqlspec.builder import (
    BuiltQuery,
    Explain,
    ExplainMixin,
    ExpressionBuilder,
    QueryBuilder,
    SQLFactory,
    build_copy_from_statement,
    build_copy_statement,
    build_copy_to_statement,
    sql,
)
```

Entry-point `sql` factory (`SQLFactory`), base `QueryBuilder` class, `Explain` builder, and `COPY` statement helpers. See the [Query Builder Guide](../../learn/query-builder.md) for usage examples.

## SQL Factory

::: sqlspec.builder._factory.SQLFactory
    options:
      heading_level: 3
      members:
        - select
        - insert
        - update
        - delete
        - merge
        - upsert
        - explain
        - create_table
        - create_view
        - create_index
        - create_schema
        - create_sequence
        - alter_table
        - drop_table
        - drop_schema
        - drop_sequence
        - truncate
        - raw
        - column

## Base Query Builder

::: sqlspec.builder._base.QueryBuilder
    options:
      heading_level: 3
      members:
        - dialect
        - parameters
        - build
        - to_sql
        - to_statement

::: sqlspec.builder._base.BuiltQuery
    options:
      heading_level: 3
      members: false

::: sqlspec.builder._base.ExpressionBuilder
    options:
      heading_level: 3
      members: false

## Explain & COPY Helpers

::: sqlspec.builder._explain.Explain
    options:
      heading_level: 3
      members: false

::: sqlspec.builder._explain.ExplainMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._factory.build_copy_statement
    options:
      heading_level: 4

::: sqlspec.builder._factory.build_copy_from_statement
    options:
      heading_level: 4

::: sqlspec.builder._factory.build_copy_to_statement
    options:
      heading_level: 4
