## [REF-006] Configuration Design: TypedDict Approach

**DECISION**: Use TypedDict for database configuration instead of dataclasses.

**IMPLEMENTATION**:

- `PsycopgConnectionConfig`: Basic connection parameters
- `PsycopgPoolConfig`: Pool-specific configuration (inherits connection params)
- `NotRequired` fields for optional parameters
- Validation happens at runtime, not definition time

**USER BENEFIT**:

- Better IDE support and auto-completion
- Clear documentation of available options
- Type safety without runtime overhead
- Flexible configuration merging

**CONFIG EXAMPLE**:

```python
from sqlspec.adapters.psycopg import PsycopgAsyncConfig

config = PsycopgAsyncConfig(
    pool_config={
        "host": "localhost",
        "port": 5432,
        "user": "myapp",
        "password": "secret",
        "dbname": "production",
        "min_size": 5,
        "max_size": 20,
        "max_lifetime": 3600.0,
    },
    instrumentation=InstrumentationConfig(
        enable_opentelemetry=True,
        enable_prometheus=True,
        service_name="myapp-db",
    )
)
```

---
