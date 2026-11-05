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
    register_version_generators,
)
```

Schema, sequence, table, column, constraint, index, view, materialized view, and metadata-to-DDL builders. See [DML, Upserts, MERGE and DDL Schema Builders](../../learn/query-builder-dml-ddl.md) for usage examples.

## Table, Sequence and Schema DDL

::: sqlspec.builder._ddl.DDLBuilder
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._ddl.CreateTable
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false
      show_docstring_examples: false
      show_docstring_functions: false
      show_docstring_attributes: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.CreateTableAsSelect
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false
      show_docstring_functions: false
      show_docstring_attributes: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.AlterTable
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false
      show_docstring_examples: false
      show_docstring_functions: false
      show_docstring_attributes: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.DropTable
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.RenameTable
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.Truncate
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.CreateSchema
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.DropSchema
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.CreateSequence
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.DropSequence
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.compile_table_ddl_results
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

## Columns, Constraints and Comments

::: sqlspec.builder._ddl.ColumnDefinition
    options:
      heading_level: 4
      members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_attributes: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.ConstraintDefinition
    options:
      heading_level: 4
      members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_attributes: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.AlterOperation
    options:
      heading_level: 4
      members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_attributes: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.CommentOn
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._temporal.create_temporal_table
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

::: sqlspec.builder._temporal.register_version_generators
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

## Index and View DDL

::: sqlspec.builder._ddl.CreateIndex
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false
      show_docstring_examples: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.DropIndex
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.CreateView
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.DropView
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.CreateMaterializedView
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true

::: sqlspec.builder._ddl.DropMaterializedView
    options:
      heading_level: 4
      inherited_members: false
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      summary:
        attributes: true
