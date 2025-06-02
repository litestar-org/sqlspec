## [REF-003] Connection Pool Lifecycle and Instrumentation

**DECISION**: Instrument pool operations for observability into connection management.

**IMPLEMENTATION**:

- Pool creation: `_create_pool_impl()` with timing and logging
- Pool closure: `_close_pool_impl()` with cleanup tracking
- Connection provision: Context managers for connection lifecycle
- Session provision: Context managers for driver instances

**USER BENEFIT**:

- Visibility into pool health and performance
- Connection leak detection capabilities
- Pool sizing optimization data

**CONFIG PATTERNS**:

```python
# TypedDict approach for clean configuration
config = PsycopgAsyncConfig(
    pool_config={
        "min_size": 5,
        "max_size": 20,
        "max_lifetime": 3600,
    },
    instrumentation=InstrumentationConfig(
        log_pool_operations=True,
        enable_prometheus=True,
    )
)

# Usage
async with config.provide_session() as driver:
    result = await driver.execute("SELECT 1")
```

**INSTRUMENTATION POINTS**:

- Pool create/destroy operations
- Connection acquire/release timing
- Pool size and utilization metrics
- Connection error rates and types

---
