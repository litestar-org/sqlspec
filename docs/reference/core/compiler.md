# Compiler & Splitter

```python
from sqlspec.core import (
    CompiledSQL,
    CorrelationExtractor,
    OperationProfile,
    OperationType,
    SQLProcessor,
    is_copy_from_operation,
    is_copy_operation,
    is_copy_to_operation,
    split_sql_script,
)
```

SQLGlot AST compilation, dialect translation, operation classification (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DDL`, `COPY`, `COMMAND`), comment correlation extraction, and dialect-aware multi-statement script splitting.

## SQL Processor & Compiled Output

::: sqlspec.core.compiler.SQLProcessor
    options:
      heading_level: 3

::: sqlspec.core.compiler.CompiledSQL
    options:
      heading_level: 3

::: sqlspec.core.compiler.OperationProfile
    options:
      heading_level: 3

::: sqlspec.core.compiler.OperationType
    options:
      heading_level: 3

## Script Splitting & Operation Helpers

::: sqlspec.core.splitter.split_sql_script
    options:
      heading_level: 3

::: sqlspec.core.correlation.CorrelationExtractor
    options:
      heading_level: 3

::: sqlspec.core.compiler.is_copy_operation
    options:
      heading_level: 4

::: sqlspec.core.compiler.is_copy_from_operation
    options:
      heading_level: 4

::: sqlspec.core.compiler.is_copy_to_operation
    options:
      heading_level: 4
