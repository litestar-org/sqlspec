## [REF-010] AioSQL Integration: File-Based SQL with Full SQLSpec Power

**DECISION**: Complete integration with AioSQL enabling file-based SQL organization while preserving all SQLSpec capabilities.

**IMPLEMENTATION**:

### Integration Architecture

- **Singleton Caching**: `AiosqlLoader` parses SQL files once and caches forever using metaclass
- **Typed Query Objects**: `AiosqlQuery` wraps SQL with type annotations and builder API support
- **Adapter Bridge**: `AiosqlSyncAdapter` and `AiosqlAsyncAdapter` implement aiosql protocol with SQLSpec drivers
- **Service Layer**: `AiosqlService` provides high-level abstractions and advanced configuration
- **Builder API Magic**: Loaded queries support full SQLSpec builder patterns (.where(), .limit(), .order_by())
- **Filter Integration**: Special `_sqlspec_filters` parameter for dynamic filter application

### Core Design Principles

- **Best of Both Worlds**: Combine aiosql's file organization with SQLSpec's power
- **Zero Compromise**: Full compatibility with entire SQLSpec ecosystem
- **Performance First**: Singleton caching and optimized query execution
- **Type Safety**: Full type annotation support with return type inference

**USER BENEFIT**:

- **File Organization**: Organize SQL in files using familiar aiosql conventions
- **Performance**: Singleton caching eliminates re-parsing overhead
- **Builder Integration**: Use SQLSpec builder API on file-loaded queries seamlessly
- **Full Ecosystem**: Access to all SQLSpec features (filters, instrumentation, validation)
- **Migration Path**: Easy migration from existing aiosql setups to SQLSpec power
- **Developer Experience**: Type-safe queries with IDE support and autocompletion

**AIOSQL INTEGRATION PATTERNS**:

### File-Based Query Organization

```sql
# queries.sql - Standard aiosql format
-- name: get_all_users^
SELECT
    id, name, email, department, age, salary, hire_date, active,
    ARRAY['SQL', 'Python', 'Analytics'] as skills
FROM users
WHERE active = TRUE
ORDER BY hire_date DESC

-- name: get_user_by_id$
SELECT * FROM users WHERE id = :user_id AND active = TRUE

-- name: create_user<!
INSERT INTO users (name, email, department, age, salary, hire_date, active)
VALUES (:name, :email, :department, :age, :salary, :hire_date, :active)
RETURNING id, name, email, department

-- name: search_high_performers^
SELECT * FROM users
WHERE salary > :min_salary
  AND active = TRUE
  AND (name ILIKE '%' || :search_term || '%'
       OR email ILIKE '%' || :search_term || '%')
```

### Singleton Caching Power

```python
from sqlspec.extensions.aiosql import AiosqlLoader

# First load - parses file and caches
loader1 = AiosqlLoader("queries.sql", dialect="postgresql")

# Second load - instant retrieval from cache
loader2 = AiosqlLoader("queries.sql", dialect="postgresql")

# loader1 is loader2 -> True (same instance!)
print(f"Cached loading is {first_load_time / cached_load_time:.1f}x faster!")
```

### Typed Query Objects with Builder API Magic

```python
from sqlspec.extensions.aiosql import AiosqlLoader
from sqlspec import sql

# Load with type annotations
loader = AiosqlLoader("queries.sql", dialect="postgresql")
get_users = loader.get_query("get_all_users", return_type=User)

# Use as-is
result = driver.execute(get_users, schema_type=User)

# OR enhance with builder API magic
enhanced_query = (
    get_users
    .where("salary > 75000")  # Add dynamic conditions
    .where_eq("department", "Engineering")  # Builder method chaining
    .order_by("salary DESC", "hire_date ASC")  # Complex ordering
    .limit(10)  # Pagination
)

# Execute enhanced query
top_engineers = driver.execute(enhanced_query, schema_type=User)
```

### Advanced Filter Integration

```python
from sqlspec.statement.filters import SearchFilter, LimitOffsetFilter

# Traditional aiosql execution with SQLSpec filters
loader = AiosqlLoader("queries.sql", dialect="postgresql")

with driver.provide_connection() as conn:
    # Special _sqlspec_filters parameter
    result = queries.get_all_users(
        conn,
        department="Engineering",
        _sqlspec_filters=[
            SearchFilter("name", "John"),      # Text search
            LimitOffsetFilter(10, 0),          # Pagination
        ]
    )
```

### Service Layer for Advanced Workflows

```python
from sqlspec.extensions.aiosql import AiosqlService
from sqlspec.statement.filters import LimitOffsetFilter

# Create service with default configuration
service = AiosqlService(
    driver,
    default_filters=[LimitOffsetFilter(limit=100, offset=0)],  # Auto-pagination
    allow_sqlspec_filters=True  # Enable _sqlspec_filters
)

# Load queries through service (with caching)
queries = service.load_queries("user_queries.sql")

# Execute with service enhancements
result = service.execute_query_with_filters(
    queries.get_users_by_department,
    connection=None,
    parameters={"department": "Engineering"},
    filters=[SearchFilter("email", "@company.com")],
    schema_type=User
)
```

**ADAPTER PATTERN BRIDGE**:

### Native AioSQL Protocol Support

```python
import aiosql
from sqlspec.extensions.aiosql import AiosqlSyncAdapter, AiosqlAsyncAdapter

# Create SQLSpec driver
config = PsycopgAsyncConfig(pool_config={"host": "localhost", "dbname": "myapp"})
driver = config.create_driver()

# Bridge to aiosql
adapter = AiosqlAsyncAdapter(
    driver,
    default_filters=[LimitOffsetFilter(limit=1000)],  # Default pagination
    allow_sqlspec_filters=True  # Enable advanced filtering
)

# Use with standard aiosql
queries = aiosql.from_path("queries.sql", adapter)

# Now all queries have SQLSpec power under the hood!
users = await queries.get_all_users(
    conn,
    department="Engineering",
    _sqlspec_filters=[SearchFilter("name", "John")]  # SQLSpec magic!
)
```

### Seamless Migration from AioSQL

```python
# Before: Standard aiosql
import aiosql
import psycopg2

queries = aiosql.from_path("queries.sql", "psycopg2")
result = queries.get_users(conn, department="Engineering")

# After: AioSQL + SQLSpec (drop-in replacement!)
from sqlspec.extensions.aiosql import AiosqlSyncAdapter
from sqlspec.adapters.psycopg import PsycopgSyncConfig

driver = PsycopgSyncConfig(...).create_driver()
adapter = AiosqlSyncAdapter(driver)
queries = aiosql.from_path("queries.sql", adapter)

# Same interface, but now with SQLSpec power!
result = queries.get_users(
    conn,
    department="Engineering",
    _sqlspec_filters=[LimitOffsetFilter(50, 0)]  # NEW: SQLSpec filters!
)
```

**COMPREHENSIVE QUERY LIFECYCLE**:

### File to Execution Flow

#### 1. File-based SQL organization (aiosql format)

The contents of `analytics.sql`:

```sql
-- name: complex_analytics^
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MAX(hire_date) as newest_hire
FROM users
WHERE active = TRUE
GROUP BY department
HAVING COUNT(*) > :min_employees
```

```python
# 2. Singleton-cached loading
loader = AiosqlLoader("analytics.sql", dialect="postgresql")
analytics_query = loader.get_query("complex_analytics", return_type=Analytics)

# 3. Builder API enhancement
enhanced_query = (
    analytics_query
    .where("department IN ('Engineering', 'Product')")  # Additional filters
    .order_by("avg_salary DESC")  # Dynamic ordering
    .limit(5)  # Top 5 departments
)

# 4. Advanced filter application
filters = [
    SearchFilter("department", "eng"),  # Text search in department
    LimitOffsetFilter(3, 0),           # Override limit for pagination
]

# 5. Type-safe execution with full ecosystem
result = await driver.execute(
    enhanced_query,
    parameters={"min_employees": 5},
    filters=filters,
    schema_type=Analytics  # Type-safe result conversion
)
```

### Real-World Complex Scenario

```python
# Enterprise data pipeline with file-based queries
from sqlspec.extensions.aiosql import AiosqlLoader, AiosqlService
from sqlspec.statement.filters import SearchFilter, LimitOffsetFilter
from datetime import datetime, timedelta

# Load different query collections
user_loader = AiosqlLoader("user_queries.sql", dialect="postgresql")
analytics_loader = AiosqlLoader("analytics_queries.sql", dialect="postgresql")
reporting_loader = AiosqlLoader("reporting_queries.sql", dialect="postgresql")

# Create service for advanced workflows
service = AiosqlService(
    driver,
    default_filters=[LimitOffsetFilter(limit=1000, offset=0)],
    allow_sqlspec_filters=True
)

# Complex data pipeline
async def generate_department_report(department: str, start_date: datetime):
    # 1. Get users with builder enhancement
    users_query = user_loader.get_query("get_users_by_department", return_type=User)
    users = await driver.execute(
        users_query
        .where("hire_date >= :start_date")
        .where_eq("active", True)
        .order_by("salary DESC"),
        parameters={"department": department, "start_date": start_date},
        schema_type=User
    )

    # 2. Analytics with filters
    analytics_query = analytics_loader.get_query("department_performance", return_type=Analytics)
    performance = await driver.execute(
        analytics_query,
        parameters={"department": department, "period_days": 90},
        filters=[LimitOffsetFilter(10, 0)],
        schema_type=Analytics
    )

    # 3. Complex reporting with service
    reports = service.load_queries("complex_reports.sql")
    revenue_data = await service.execute_query_with_filters(
        reports.department_revenue_analysis,
        connection=None,
        parameters={"department": department},
        filters=[SearchFilter("status", "completed")],
        schema_type=RevenueReport
    )

    return {
        "users": users.rows,
        "performance": performance.rows,
        "revenue": revenue_data
    }
```

**PERFORMANCE CHARACTERISTICS**:

### Optimization Features

```python
# Singleton caching - parse once, use forever
loader1 = AiosqlLoader("huge_file.sql")  # ~100ms first time
loader2 = AiosqlLoader("huge_file.sql")  # ~0.1ms cached (1000x faster!)

# Memory efficiency - shared instances
assert loader1 is loader2  # Same object in memory

# Lazy query enhancement
query = loader.get_query("complex_query")
# Enhancement only happens when executed
enhanced = query.where("active = true").limit(10)  # Fast transformation
```

**KEY POINTS FOR DOCS**:

- **Zero Migration Cost**: Drop-in replacement for existing aiosql setups
- **Performance Revolution**: Singleton caching provides massive speedups
- **Builder API Magic**: Enhance file-loaded queries with SQLSpec builder patterns
- **Filter Ecosystem**: Use powerful SQLSpec filters through `_sqlspec_filters` parameter
- **Type Safety**: Full type annotation support with return type inference
- **Service Abstractions**: High-level service for complex workflows and default configurations
- **Comprehensive Integration**: Works with ALL SQLSpec features (drivers, validation, instrumentation)
- **File Organization**: Maintain clean separation of SQL logic in organized files
- **Developer Experience**: Best-in-class IDE support, autocompletion, and error detection

---
