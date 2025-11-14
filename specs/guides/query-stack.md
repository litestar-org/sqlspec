# Query Stack Guide

Query Stack executes multiple SQL statements in a single driver call while preserving raw SQL semantics. Each stack is immutable, MyPy-friendly, and can be shared across asyncio tasks or worker threads without synchronization.

## When to Use Query Stack

- Multi-step workflows (audit insert + update + permission read) that would otherwise require multiple round-trips.
- Adapter-specific native pipelines (Oracle 23ai+, psycopg pipeline mode, asyncpg batch execution) where batching reduces latency.
- Sequential fallback adapters (SQLite, DuckDB, BigQuery, ADBC, AsyncMy) when you still want the ergonomic benefits of a single API call.
- Continue-on-error workflows that need to run every statement but report failures alongside successful operations.

## Building StatementStack Instances

1. Start with an empty stack: `stack = StatementStack()`.
2. Add operations via the push helpers (each returns a new instance):
   - `.push_execute(sql, parameters, *, statement_config=None, **kwargs)`
   - `.push_execute_many(sql, parameter_sets, *filters, statement_config=None, **kwargs)`
   - `.push_execute_script(sql, *filters, statement_config=None, **kwargs)`
   - `.push_execute_arrow(sql, *filters, statement_config=None, **kwargs)`
3. Use `.extend()` or `StatementStack.from_operations()` to combine stacks.
4. Store stacks at module scope or factory functions—the tuple-based storage makes them hashable and thread-safe.

## Execution Modes

`Session.execute_stack(stack, continue_on_error=False)` mirrors the driver’s transaction rules:

- **Fail-fast (default):** The driver creates a transaction if one is not already active. Any failure raises `StackExecutionError` and rolls back the transaction.
- **Continue-on-error:** Each operation commits immediately. Failures still raise `StackExecutionError`, but execution continues and the error is preserved on the corresponding `StackResult`.

When using adapters with native pipelines (Oracle, psycopg, asyncpg), continue-on-error downgrades to sequential mode if the native API cannot honor the semantics (e.g., psycopg pipeline requires fail-fast).

## Transaction Boundaries

- Existing transactions are respected—`execute_stack()` never commits or rolls back a transaction it did not create.
- For fail-fast stacks, drivers call `begin()`/`commit()` (or `rollback()` on error) only when no transaction is active.
- Continue-on-error uses commit/rollback hooks after each operation to keep the connection clean.

## Arrow Operations

`push_execute_arrow()` delegates to `select_to_arrow()` when the adapter implements Arrow support (DuckDB, BigQuery, ADBC, etc.). The returned `StackResult.result` is an `ArrowResult`, so downstream helpers like `to_pandas()` or `to_polars()` continue to work.

## Telemetry and Tracing

Every stack execution routes through `StackExecutionObserver`, which provides:

- `StackExecutionMetrics`: increments `stack.execute.*` counters (invocations, statements, partial errors, duration) for any observability runtime (built-in logger, OTLP exporter, Prometheus bridge, etc.).
- `sqlspec.stack.execute` tracing spans containing adapter, statement count, native pipeline flag, continue-on-error, and hashed SQL identifiers.
- Structured DEBUG/ERROR logs (`stack.execute.start`, `stack.execute.complete`, `stack.execute.failed`).

Adapters only need to report whether they used a native pipeline; the observer handles the rest.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `ValueError: Cannot execute an empty StatementStack` | Stack has zero operations | Ensure you push at least one statement before calling `execute_stack()` |
| `StackExecutionError(operation_index=1, ...)` | Driver error on a specific statement | Inspect `StackResult.error` to see the wrapped exception; use `StackResult.result` to inspect partial data |
| `push_execute_many` raising `TypeError` | Parameter payload not a sequence | Pass an actual list/tuple of parameter sets |
| Continue-on-error seems to run sequentially on psycopg | Psycopg pipeline mode does not support partial failures | Expected—SQLSpec downgrades to sequential mode automatically |

## Related Resources

- [Query Stack API Reference](/reference/query-stack)
- :doc:`/examples/patterns/stacks/query_stack_example`
- [Adapter Guides](/guides/adapters/) for native vs. fallback behavior per database

Use the new :doc:`/reference/query-stack` page for low-level API details and :doc:`/examples/patterns/stacks/query_stack_example` to see the end-to-end workflow.
