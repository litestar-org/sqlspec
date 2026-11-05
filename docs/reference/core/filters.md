# Query Filters

```python
from sqlspec.core import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    BooleanFilter,
    ChoicesFilter,
    ConditionFactory,
    CursorFilter,
    CursorKey,
    CursorKeys,
    CursorPagination,
    FilterTypes,
    FilterTypeT,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    NullsPlacement,
    OffsetPagination,
    OrderByFilter,
    Pagination,
    SearchFilter,
    StatementFilter,
    apply_filter,
    canonicalize_filters,
    normalize_cursor_keys,
)
from sqlspec.core.filters import (
    InAnyFilter,
    NotAnyCollectionFilter,
    NotInSearchFilter,
    OnBeforeAfterFilter,
    PaginationFilter,
    find_filter,
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
      members: false

::: sqlspec.core.filters.CursorFilter
    options:
      heading_level: 3
      members:
        - encode
        - build_page
        - append_to_statement

::: sqlspec.core.filters.CursorKey
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.OrderByFilter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.PaginationFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.CursorKeys
    options:
      heading_level: 4

::: sqlspec.core._pagination.OffsetPagination
    options:
      heading_level: 4
      members: false

::: sqlspec.core._pagination.CursorPagination
    options:
      heading_level: 4
      members: false

::: sqlspec.core._pagination.Pagination
    options:
      heading_level: 4

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
      members:
        - escape_like_value
        - like_pattern

::: sqlspec.core.filters.InCollectionFilter
    options:
      heading_level: 3
      members: false

::: sqlspec.core.filters.OnBeforeAfterFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.NotInSearchFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.InAnyFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.NotInCollectionFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.AnyCollectionFilter
    options:
      heading_level: 4
      members: false

::: sqlspec.core.filters.NotAnyCollectionFilter
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

## Filter Application & Helpers

::: sqlspec.core.filters.FilterTypes
    options:
      heading_level: 4

::: sqlspec.core.filters.FilterTypeT
    options:
      heading_level: 4

::: sqlspec.core.filters.apply_filter
    options:
      heading_level: 4

::: sqlspec.core.filters.find_filter
    options:
      heading_level: 4

::: sqlspec.core.filters.canonicalize_filters
    options:
      heading_level: 4

::: sqlspec.core.filters.normalize_cursor_keys
    options:
      heading_level: 4

::: sqlspec.core.query_modifiers.ConditionFactory
    options:
      heading_level: 4
      members: false
