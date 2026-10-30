# Columns & Expressions

```python
from sqlspec.builder import (
    AggregateExpression,
    Column,
    ColumnExpression,
    ConversionExpression,
    ExpressionWrapper,
    FunctionColumn,
    FunctionExpression,
    MathExpression,
    StringExpression,
    VectorDistance,
    extract_expression,
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
    to_expression,
)
```

Typed column objects, aggregate/math/string/conversion expression wrappers, vector distance expressions, and SQLGlot expression parsing helpers.

## Column Descriptors

::: sqlspec.builder._column.Column
    options:
      heading_level: 3

::: sqlspec.builder._column.ColumnExpression
    options:
      heading_level: 3

::: sqlspec.builder._column.FunctionColumn
    options:
      heading_level: 3

## Expression Wrappers & Vector Distance

::: sqlspec.builder._expression_wrappers.ExpressionWrapper
    options:
      heading_level: 3

::: sqlspec.builder._expression_wrappers.AggregateExpression
    options:
      heading_level: 4

::: sqlspec.builder._expression_wrappers.FunctionExpression
    options:
      heading_level: 4

::: sqlspec.builder._expression_wrappers.MathExpression
    options:
      heading_level: 4

::: sqlspec.builder._expression_wrappers.StringExpression
    options:
      heading_level: 4

::: sqlspec.builder._expression_wrappers.ConversionExpression
    options:
      heading_level: 4

::: sqlspec.builder._vector_distance.VectorDistance
    options:
      heading_level: 3

## Expression Parsing Helpers

::: sqlspec.builder._parsing_utils.to_expression
    options:
      heading_level: 4

::: sqlspec.builder._parsing_utils.extract_expression
    options:
      heading_level: 4

::: sqlspec.builder._parsing_utils.parse_column_expression
    options:
      heading_level: 4

::: sqlspec.builder._parsing_utils.parse_condition_expression
    options:
      heading_level: 4

::: sqlspec.builder._parsing_utils.parse_order_expression
    options:
      heading_level: 4

::: sqlspec.builder._parsing_utils.parse_table_expression
    options:
      heading_level: 4
