# Core SQL Pipeline

```python
from sqlspec.core import (
    SQL,
    ArrowResult,
    CacheConfig,
    CompiledSQL,
    CursorPagination,
    LimitOffsetFilter,
    OffsetPagination,
    ParameterStyle,
    ParameterStyleConfig,
    SQLProcessor,
    SQLResult,
    StackResult,
    StatementConfig,
    StatementStack,
)
```

Core SQL compilation, parameter conversion, result hydration, query filtering, and LRU caching pipeline.

## Reference Pages

| Page | Primary Symbols | Description |
| --- | --- | --- |
| [Statements & Stack](statement.md) | `SQL`, `Statement`, `StatementConfig`, `ProcessedState`, `StatementStack`, `StackOperation`, `ExplainOptions` | SQL statement encapsulation, dialect configuration, batched statement stacks, and `EXPLAIN` options. |
| [Results & Pagination](results.md) | `SQLResult`, `ArrowResult`, `StackResult`, `DMLResult`, `OffsetPagination`, `CursorPagination` | Row/model hydration, Apache Arrow results, stack execution outputs, and pagination envelopes. |
| [Parameters & Coercion](parameters.md) | `ParameterStyle`, `ParameterStyleConfig`, `ParameterProcessor`, `ParameterConverter`, `ParameterValidator`, `TypedParameter` | Placeholder style conversion (`$1`, `:name`, `?`, `@p`), type coercion, and parameter validation. |
| [Query Filters](filters.md) | `StatementFilter`, `LimitOffsetFilter`, `OrderByFilter`, `SearchFilter`, `BeforeAfterFilter`, `InCollectionFilter`, `CursorFilter` | Composable statement filters and condition factories for pagination, sorting, and search. |
| [Caching & Hashing](cache.md) | `CacheConfig`, `CacheStats`, `LRUCache`, `NamespacedCache`, `get_cache_config`, `update_cache_config` | Multi-tier statement, AST expression, and compiled SQL caching with hit/miss telemetry. |
| [Compiler & Splitter](compiler.md) | `SQLProcessor`, `CompiledSQL`, `OperationProfile`, `OperationType`, `split_sql_script` | SQLGlot AST parsing, dialect translation, operation classification, and script splitting. |
