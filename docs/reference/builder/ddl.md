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
    CreateSequence,
    CreateTable,
    CreateTableAsSelect,
    CreateView,
    DDLBuilder,
    DropIndex,
    DropMaterializedView,
    DropSchema,
    DropSequence,
    DropTable,
    DropView,
    RenameTable,
    Truncate,
    compile_table_ddl_results,
    create_temporal_table,
)
```

Schema, sequence, table, column, constraint, index, view, materialized view, and metadata-to-DDL builders.

## Table, Sequence & Schema DDL

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
        - from_metadata
        - from_arrow_schema
        - from_records

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

::: sqlspec.builder._ddl.CreateSequence
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.DropSequence
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._ddl.compile_table_ddl_results
    options:
      heading_level: 4

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
      members:
        - on_table
        - columns
        - include
        - unique
        - where
        - from_metadata

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
