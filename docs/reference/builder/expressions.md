# Columns and Expressions

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
    build_oracle_hnsw_subquery,
    build_oracle_hybrid_rrf_query,
    build_oracle_hybrid_rrf_sql,
    extract_expression,
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
    to_expression,
)
```

Typed column objects, aggregate/math/string/conversion expression wrappers, cross-dialect vector distance expressions, Oracle 26ai Hybrid RRF helpers, and SQLGlot expression parsing helpers. See [Expressions, Window Functions and Vector Search](../../learn/query-builder-expressions.md) for usage examples.

## Column Descriptors

::: sqlspec.builder._column.Column
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._column.ColumnExpression
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._column.FunctionColumn
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

## Expression Wrappers and Vector Distance

::: sqlspec.builder._expression_wrappers.ExpressionWrapper
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._expression_wrappers.AggregateExpression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._expression_wrappers.FunctionExpression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._expression_wrappers.MathExpression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._expression_wrappers.StringExpression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._expression_wrappers.ConversionExpression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._vector_distance.VectorDistance
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._vector_distance.build_oracle_hnsw_subquery
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._vector_distance.build_oracle_hybrid_rrf_sql
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._vector_distance.build_oracle_hybrid_rrf_query
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

## Expression Parsing Helpers

::: sqlspec.builder._parsing_utils.to_expression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._parsing_utils.extract_expression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._parsing_utils.parse_column_expression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._parsing_utils.parse_condition_expression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._parsing_utils.parse_order_expression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false

::: sqlspec.builder._parsing_utils.parse_table_expression
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
