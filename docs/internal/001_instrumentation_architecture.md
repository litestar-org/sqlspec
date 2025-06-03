## [REF-001] Instrumentation Architecture: Context Managers vs Decorators

**DECISION**: Migrated from decorator-based to context manager-based instrumentation.

**IMPLEMENTATION**:

- **Protocol Layer**: Public methods (`execute`, `execute_many`, `execute_script`) use context managers
- **Driver Layer**: Private methods (`_execute_statement`, `_wrap_select_result`) use context managers
- **Context Managers**: `instrument_operation()` (sync) and `instrument_operation_async()` (async)

**USER BENEFIT**:

- Clean type signatures (no decorator interference)
- Multi-level telemetry (API + driver level)
- Comprehensive tracking of database operations

**CODE EXAMPLES**:

```python
# User calls this
result = driver.execute("SELECT * FROM users")

# Results in telemetry hierarchy:
# 1. High-level: "execute" operation (API usage)
# 2. Low-level: "psycopg_execute" operation (database access)
# 3. Low-level: "psycopg_wrap_select" operation (result processing)
```

**TELEMETRY COVERAGE**:

- OpenTelemetry spans with proper attributes
- Prometheus metrics (counters, histograms, gauges)
- Structured logging with context
- Error tracking and latency monitoring

---
