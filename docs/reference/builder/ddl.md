# DDL Builders

```python
from sqlspec.builder import (
    AlterOperation,
    AlterTable,
    ColumnDefinition,
    CommentOn,
    ConstraintDefinition,
    CreateIndex,
    CreateMaterializedView,
    CreateSchema,
    CreateTable,
    CreateTableAsSelect,
    CreateView,
    DDLBuilder,
    DropIndex,
    DropMaterializedView,
    DropSchema,
    DropTable,
    DropView,
    RenameTable,
    Truncate,
    create_temporal_table,
)
```

Schema, table, column, constraint, index, view, and materialized view DDL builders.

## Table & Schema DDL

::: sqlspec.builder._ddl.DDLBuilder
    options:
      heading_level: 3
      members: false

::: sqlspec.builder._ddl.CreateTable
    options:
      heading_level: 3
      members:
        - column
        - primary_key_constraint
        - foreign_key_constraint
        - unique_constraint
        - check_constraint
        - in_schema
        - partition_by

::: sqlspec.builder._ddl.CreateTableAsSelect
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.AlterTable
    options:
      heading_level: 3
      members:
        - add_column
        - drop_column
        - alter_column_type
        - rename_column
        - drop_constraint

::: sqlspec.builder._ddl.DropTable
    options:
      heading_level: 3
      members: false

::: sqlspec.builder._ddl.RenameTable
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.Truncate
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.CreateSchema
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.DropSchema
    options:
      heading_level: 4
      members: false

## Columns, Constraints & Comments

::: sqlspec.builder._ddl.ColumnDefinition
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.ConstraintDefinition
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.AlterOperation
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.CommentOn
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._temporal.create_temporal_table
    options:
      heading_level: 4

## Index & View DDL

::: sqlspec.builder._ddl.CreateIndex
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.DropIndex
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.CreateView
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.DropView
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.CreateMaterializedView
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.DropMaterializedView
    options:
      heading_level: 4
      members: false
