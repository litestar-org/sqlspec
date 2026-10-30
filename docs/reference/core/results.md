# Results & Pagination

```python
from sqlspec.core import (
    ArrowResult,
    CursorPagination,
    DMLResult,
    OffsetPagination,
    Pagination,
    SQLResult,
    StackResult,
    StatementResult,
    create_arrow_result,
    create_sql_result,
)
```

Result containers for row dictionaries, hydrated schema models, scalar values, Apache Arrow tables/batches, statement stacks, and offset/cursor pagination envelopes. See [Querying](../../learn/querying.md) and [Arrow & Storage](../../learn/arrow-and-storage.md).

## Statement Results

::: sqlspec.core.result.StatementResult
    options:
      heading_level: 3
      members: false

::: sqlspec.core.result.SQLResult
    options:
      heading_level: 3
      members:
        - all
        - one
        - one_or_none
        - scalar
        - scalar_or_none
        - to_arrow
        - to_pandas
        - to_polars

::: sqlspec.core.result.ArrowResult
    options:
      heading_level: 3
      members: false

::: sqlspec.core.result.DMLResult
    options:
      heading_level: 3
      members: false

::: sqlspec.core.result.StackResult
    options:
      heading_level: 3
      members: false

## Pagination Envelopes

::: sqlspec.core._pagination.Pagination
    options:
      heading_level: 3
      members: false

::: sqlspec.core._pagination.OffsetPagination
    options:
      heading_level: 3
      members: false

::: sqlspec.core._pagination.CursorPagination
    options:
      heading_level: 3
      members: false
