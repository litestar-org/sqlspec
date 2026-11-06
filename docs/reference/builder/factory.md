# SQL Factory and Base Builder

```python
from sqlspec.builder import (
    BuiltQuery,
    Explain,
    ExplainMixin,
    ExpressionBuilder,
    QueryBuilder,
    SQLBuilderError,
    SQLFactory,
    build_copy_from_statement,
    build_copy_statement,
    build_copy_to_statement,
    build_explain_sql,
    build_oracle_display_cursor,
    normalize_dialect_name,
)
```

Entry-point `sql` factory (`SQLFactory`), base `QueryBuilder` class, `BuiltQuery` result container, `Explain` builder, and `COPY` statement helpers. See [Query Builder: SELECT, Filtering and Joins](../../learn/query-builder.md) for usage examples.

## SQL Factory

::: sqlspec.builder._factory.SQLFactory
    options:
      heading_level: 3
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

## Base Query Builder

::: sqlspec.builder._base.QueryBuilder
    options:
      heading_level: 3
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

::: sqlspec.builder._base.BuiltQuery
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._base.ExpressionBuilder
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.exceptions.SQLBuilderError
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._explain.normalize_dialect_name
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

## Explain and COPY Helpers

::: sqlspec.builder._explain.Explain
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

::: sqlspec.builder._explain.ExplainMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false
      show_docstring_examples: false

::: sqlspec.builder._explain.build_explain_sql
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._explain.build_oracle_display_cursor
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._factory.build_copy_statement
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._factory.build_copy_from_statement
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._factory.build_copy_to_statement
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
