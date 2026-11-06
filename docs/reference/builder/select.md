# Select and Clause Builders

```python
from sqlspec.builder import (
    Case,
    CaseBuilder,
    CommonTableExpressionMixin,
    HavingClauseMixin,
    JoinBuilder,
    LimitOffsetClauseMixin,
    OrderByClauseMixin,
    PivotClauseMixin,
    Select,
    SelectClauseMixin,
    SetOperationMixin,
    SubqueryBuilder,
    UnpivotClauseMixin,
    WhereClauseMixin,
    WindowFunctionBuilder,
)
from sqlspec.builder._join import JoinClauseMixin
```

Fluent `SELECT` query builder supporting joins, CTEs, window functions, `CASE` expressions, subqueries, `PIVOT`/`UNPIVOT`, and set operations (`UNION`, `INTERSECT`, `EXCEPT`). See [Query Builder: SELECT, Filtering and Joins](../../learn/query-builder.md) and [Expressions, Window Functions and Vector Search](../../learn/query-builder-expressions.md) for usage examples.

## Select Builder

::: sqlspec.builder._select.Select
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false
      show_docstring_examples: false

## Join, Window, Case and Subquery Builders

::: sqlspec.builder._join.JoinBuilder
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

::: sqlspec.builder._select.WindowFunctionBuilder
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

::: sqlspec.builder._select.CaseBuilder
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._select.Case
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false
      summary:
        attributes: true

::: sqlspec.builder._select.SubqueryBuilder
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

## Clause Mixins

::: sqlspec.builder._select.SelectClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

::: sqlspec.builder._select.WhereClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

::: sqlspec.builder._join.JoinClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false

::: sqlspec.builder._select.OrderByClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._select.LimitOffsetClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._select.HavingClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._select.CommonTableExpressionMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._select.SetOperationMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._select.PivotClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false

::: sqlspec.builder._select.UnpivotClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_description: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_examples: false
