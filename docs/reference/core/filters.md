# Query Filters

```python
from sqlspec.core import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    BooleanFilter,
    ChoicesFilter,
    ConditionFactory,
    CursorFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    NullsPlacement,
    OrderByFilter,
    SearchFilter,
    StatementFilter,
    apply_filter,
    canonicalize_filters,
)
```

Composable `StatementFilter` classes that append `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, and keyset cursor predicates to `SQL` and `Select` statements. See [Filtering & Pagination](../../learn/filtering-and-pagination.md) for usage examples.

## Filter Protocol & Pagination Filters

::: sqlspec.core.filters.StatementFilter
    options:
      heading_level: 3
      members:
        - append_to_statement
        - extract_parameters
        - get_cache_key

::: sqlspec.core.filters.LimitOffsetFilter
    options:
      heading_level: 3
      members:
        - limit
        - offset
        - append_to_statement

::: sqlspec.core.filters.CursorFilter
    options:
      heading_level: 3
      members:
        - keys
        - limit
        - cursor
        - backward
        - encode
        - build_page
        - append_to_statement

::: sqlspec.core.filters.OrderByFilter
    options:
      heading_level: 3
      members:
        - field_name
        - sort_order
        - nulls
        - append_to_statement

::: sqlspec.core._ordering.NullsPlacement
    options:
      heading_level: 4
      members: false

## Predicate & Collection Filters

::: sqlspec.core.filters.BeforeAfterFilter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.SearchFilter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.InCollectionFilter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.NotInCollectionFilter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.AnyCollectionFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.ChoicesFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.BooleanFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.NullFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.NotNullFilter
    options:
      heading_level: 4
      members: false

## Filter Application & Condition Factory

::: sqlspec.core.filters.apply_filter
    options:
      heading_level: 4

::: sqlspec.core.filters.canonicalize_filters
    options:
      heading_level: 4

::: sqlspec.core.query_modifiers.ConditionFactory
    options:
      heading_level: 3
      members: false
