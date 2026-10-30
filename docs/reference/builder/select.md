# Select & Clause Builders

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
```

Fluent `SELECT` query builder supporting joins, CTEs, window functions, `CASE` expressions, subqueries, `PIVOT`/`UNPIVOT`, and set operations (`UNION`, `INTERSECT`, `EXCEPT`). See the [Query Builder Guide](../../learn/query-builder.md) for usage examples.

## Select Builder

::: sqlspec.builder._select.Select
    options:
      heading_level: 3

## Join, Window, Case & Subquery Builders

::: sqlspec.builder._join.JoinBuilder
    options:
      heading_level: 3
      members:
        - "on"

::: sqlspec.builder._select.WindowFunctionBuilder
    options:
      heading_level: 3
      members:
        - partition_by
        - order_by
        - build
        - as_

::: sqlspec.builder._select.CaseBuilder
    options:
      heading_level: 3
      members: false

::: sqlspec.builder._select.Case
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.SubqueryBuilder
    options:
      heading_level: 3
      members: false

## Clause Mixins

::: sqlspec.builder._select.SelectClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.WhereClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.OrderByClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.LimitOffsetClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.HavingClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.CommonTableExpressionMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.SetOperationMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.PivotClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._select.UnpivotClauseMixin
    options:
      heading_level: 4
      members: false
