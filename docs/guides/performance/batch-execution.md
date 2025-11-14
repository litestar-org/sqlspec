# Batch Execution Strategies

Query Stack complements (not replaces) ``execute_many``. Use this guide to choose the right batching strategy per workload.

## Query Stack vs ``execute_many``

| Scenario | Use Query Stack | Use ``execute_many`` |
| --- | --- | --- |
| Heterogeneous statements (audit INSERT + UPDATE + SELECT) | ✅ | ❌ |
| Single statement + many parameter sets | ❌ | ✅ |
| Need per-statement telemetry and error attribution | ✅ | ❌ |
| Simple bulk insert without control flow | ❌ | ✅ |

## Adapter Optimizations

- **Oracle 23ai+** – Uses ``oracledb.create_pipeline()`` / ``run_pipeline()`` for true single round-trips.
- **Psycopg 3 (libpq 14+)** – Uses pipeline mode to enqueue statements without waiting for results.
- **AsyncPG** – Reuses libpq’s extended protocol and caches prepared statements for repeated stacks.
- **Fallback Adapters** – Execute sequentially but still gain transactional bundling and telemetry.

## Measuring Benefits

1. Run workloads with `StackExecutionMetrics` enabled (the default) and export `stack.execute.*` counters.
2. Compare average duration in milliseconds between native vs sequential paths.
3. Use tracing spans to verify pipeline usage—``sqlspec.stack.native_pipeline=true`` indicates the optimized path.
4. Set up canaries with `driver_features={"stack_native_disabled": True}` if you need to toggle native mode manually during incident response.

## Tuning Recommendations

- **Group dependent statements**: keep related DML/SELECT blocks inside one stack to avoid extra round-trips.
- **Limit stack size**: avoid 100+ statement stacks on fallback adapters—split into logical phases so rollbacks stay manageable.
- **Watch transactions**: fail-fast stacks run inside a transaction when the driver is not already in one. Continue-on-error stacks auto-commit after each success.
- **Mix Arrow with SQL sparingly**: ``push_execute_arrow`` is available, but only include Arrow operations when the adapter supports it, or the driver will raise `StackExecutionError`.

## Benchmark Template

Use the following structure when adding performance tests (see Task 6.6):

```python
from sqlspec import StatementStack

stack = (
    StatementStack()
    .push_execute("INSERT INTO audit_log (action) VALUES (:action)", {"action": "login"})
    .push_execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = :id", {"id": 1})
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = :id", {"id": 1})
)
```

Measure wall-clock time for native vs sequential execution, record round-trip counts (database logs or tracing), and publish the findings in ``docs/benchmarks/`` when Task 6.6 is complete.
