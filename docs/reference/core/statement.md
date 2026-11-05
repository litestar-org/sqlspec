# Statements & Stack

```python
from sqlspec.core import (
    SQL,
    ExplainFormat,
    ExplainOptions,
    ProcessedState,
    StackExecutionMetrics,
    StackOperation,
    Statement,
    StatementConfig,
    StatementStack,
    get_default_config,
    get_default_parameter_config,
)
```

SQL statement encapsulation, dialect configuration, batched `StatementStack` pipelines, and `EXPLAIN` options. See [Querying](../../learn/querying.md) and [Transactions](../../learn/transactions.md) for usage examples.

## Statement & Configuration

::: sqlspec.core.statement.SQL
    options:
      heading_level: 3
      members:
        - sql
        - raw_sql
        - parameters
        - dialect
        - compile
        - paginate

::: sqlspec.core.statement.Statement
    options:
      heading_level: 3
      members: false

::: sqlspec.core.statement.StatementConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.core.statement.ProcessedState
    options:
      heading_level: 3
      members: false

::: sqlspec.core.statement.get_default_config
    options:
      heading_level: 4

::: sqlspec.core.statement.get_default_parameter_config
    options:
      heading_level: 4

## Statement Stacks

::: sqlspec.core.stack.StatementStack
    options:
      heading_level: 3
      members:
        - push_execute
        - push_execute_many
        - push_execute_script
        - extend

::: sqlspec.core.stack.StackOperation
    options:
      heading_level: 3
      members: false

::: sqlspec.core.metrics.StackExecutionMetrics
    options:
      heading_level: 3
      members: false

## Explain Options

::: sqlspec.core.explain.ExplainOptions
    options:
      heading_level: 3
      members: false

::: sqlspec.core.explain.ExplainFormat
    options:
      heading_level: 3
      members: false
