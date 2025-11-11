# Architecture Patterns

This guide captures the key patterns introduced by Query Stack. Use it as the canonical reference when extending the feature or reviewing adapter contributions.

## Immutable Stack Builder

- ``StatementStack`` stores operations as tuples (method, statement, args, kwargs). Every mutating helper returns a **new** instance.
- Avoid dataclasses—``__slots__`` keeps the builder MyPy-friendly and mypyc-compatible.
- Share stacks freely across tasks/programs. There is no internal mutation after construction.
- Validation happens at push time (empty SQL, invalid execute_many payloads, nested stacks). Drivers can assume well-formed operations.

## Native vs Sequential Branching

- Base drivers (sync + async) handle sequential execution, transaction management, continue-on-error commits, and rollback safety.
- Adapter overrides should be thin wrappers that:
  1. Decide whether a native pipeline is available (version checks, capability flags).
  2. Fall back to ``super().execute_stack()`` immediately when native mode is unavailable.
  3. Convert native driver results back into ``StackResult`` without copying data.
- Keep capability gating deterministic—one probe function per adapter (e.g., Oracle’s pipeline version check, psycopg’s ``has_pipeline`` flag).

## StackExecutionObserver Contract

- Always wrap adapter-specific overrides with ``StackExecutionObserver`` using the correct ``native_pipeline`` flag.
- The observer emits:
  - ``stack.execute.*`` metrics (invocations, statements, duration, partial errors, forced overrides)
  - ``sqlspec.stack.execute`` tracing spans with hashed SQL identifiers
  - Structured DEBUG/ERROR logs
- Adapters should **not** emit their own stack metrics; they only pass the correct context (continue_on_error, native pipeline flag).

## Error Handling Pattern

- Wrap driver exceptions in ``StackExecutionError`` with:
  - ``operation_index``
  - ``sql`` summary (`describe_stack_statement`)
  - ``adapter`` name
  - ``mode`` (``fail-fast`` or ``continue-on-error``)
- Continue-on-error flows append ``StackResult.from_error(error)`` and keep executing.
- Fail-fast flows immediately raise the wrapped error after rolling back / cleaning state.

## Adapter Checklist

1. **Version / capability gate** native execution.
2. **Respect ``stack_native_disabled`` driver feature** if provided manually (useful for integration tests).
3. **Never mutate stack operations**—always compile to driver-specific statements first.
4. **Preserve ``StackResult.raw_result``** when possible (call ``StackResult.from_sql_result`` / ``from_arrow_result``).
5. **Guarantee cleanup** (`commit()`/`rollback()` in `finally` blocks) even for native pipelines.
