# AioSQL Integration

## Introduction

The AioSQL integration allows SQLSpec to work seamlessly with SQL files, providing a clean separation between SQL code and Python code. This integration combines AioSQL's file-based query management with SQLSpec's powerful execution engine, validation, and type safety.

## Overview

AioSQL lets you organize SQL queries in `.sql` files with special comments that define query names and types. SQLSpec enhances this by adding:

- Automatic parameter validation and binding
- Query result type safety
- SQL validation and transformation
- Performance monitoring
- Multi-database support

## Basic Usage

### SQL File Format

Create SQL files with named queries:

```sql
-- name: get_user_by_id^
-- Get a single user by ID
SELECT id, name, email, created_at
FROM users
WHERE id = :user_id;

-- name: list_active_users
-- List all active users with pagination
SELECT id, name, email, last_login
FROM users
WHERE active = true
  AND last_login > CURRENT_DATE - INTERVAL '30 days'
ORDER BY last_login DESC
LIMIT :limit OFFSET :offset;

-- name: create_user<!
-- Create a new user and return the ID
INSERT INTO users (name, email, password_hash)
VALUES (:name, :email, :password_hash)
RETURNING id;

-- name: update_user_email!
-- Update user email address
UPDATE users
SET email = :email,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :user_id;

-- name: bulk_deactivate_users*!
-- Deactivate multiple users
UPDATE users
SET active = false,
    deactivated_at = CURRENT_TIMESTAMP
WHERE id = :user_id;
```

### Query Type Indicators

- `^` - Returns single row (one)
- No suffix - Returns multiple rows (all)
- `!` - Executes without returning rows (execute)
- `*` - Prepared for multiple parameter sets (execute_many)
- `<!` - Insert returning generated keys

### Loading and Using Queries

```python
from sqlspec.extensions.aiosql import AioSQLAdapter
from sqlspec import SQLSpec

# Initialize SQLSpec
sqlspec = SQLSpec()
sqlspec.register_config(PostgreSQLConfig(...), "primary")

# Create adapter
aiosql = AioSQLAdapter(sqlspec)

# Load queries from files
queries = aiosql.from_path("sql/queries.sql")

# Use queries with automatic SQLSpec integration
async with sqlspec.get_session("primary") as session:
    # Single row query
    user = await queries.get_user_by_id(session, user_id=123)

    # Multiple rows
    users = await queries.list_active_users(
        session,
        limit=10,
        offset=0
    )

    # Execute with returned ID
    new_user_id = await queries.create_user(
        session,
        name="Alice",
        email="alice@example.com",
        password_hash="..."
    )

    # Bulk operation
    await queries.bulk_deactivate_users(
        session,
        [{"user_id": 1}, {"user_id": 2}, {"user_id": 3}]
    )
```

## Advanced Features

### Type-Safe Queries

```python
from typing import TypedDict, List
from datetime import datetime

class User(TypedDict):
    id: int
    name: str
    email: str
    created_at: datetime

# Create type-safe adapter
aiosql = AioSQLAdapter(
    sqlspec,
    type_mappings={
        "get_user_by_id": User,
        "list_active_users": List[User]
    }
)

queries = aiosql.from_path("sql/users.sql")

# Now queries return typed results
async with sqlspec.get_session("primary") as session:
    user: User = await queries.get_user_by_id(session, user_id=123)
    users: List[User] = await queries.list_active_users(session, limit=10)
```

### Query Validation

```python
from sqlspec.config import SQLConfig
from sqlspec.statement.pipelines.validators import (
    SecurityValidator,
    PerformanceValidator
)

# Configure validation for AioSQL queries
aiosql = AioSQLAdapter(
    sqlspec,
    sql_config=SQLConfig(
        validators=[
            SecurityValidator(),
            PerformanceValidator(max_join_count=5)
        ],
        strict_mode=True  # Fail on validation warnings
    )
)

# Queries are validated when loaded
try:
    queries = aiosql.from_path("sql/queries.sql")
except ValidationError as e:
    print(f"Query validation failed: {e}")
```

### Dynamic Query Building

```python
-- name: search_users
-- Search users with dynamic filters
SELECT id, name, email, created_at
FROM users
WHERE 1=1
  {% if name %}
  AND name ILIKE :name_pattern
  {% endif %}
  {% if email %}
  AND email ILIKE :email_pattern
  {% endif %}
  {% if created_after %}
  AND created_at >= :created_after
  {% endif %}
ORDER BY created_at DESC
LIMIT :limit;
```

```python
# Use with conditional parameters
results = await queries.search_users(
    session,
    name="John",           # Will add name filter
    name_pattern="%John%",
    email=None,           # Will skip email filter
    created_after=date(2024, 1, 1),
    limit=20
)
```

### Query Composition

```python
-- name: _user_base_query
-- Base query for user selection (private, not exposed)
SELECT
    u.id,
    u.name,
    u.email,
    u.created_at,
    COUNT(o.id) as order_count,
    COALESCE(SUM(o.total), 0) as total_spent

-- name: get_user_with_stats^
-- Get user with order statistics
{_user_base_query}
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.id = :user_id
GROUP BY u.id;

-- name: get_top_users
-- Get top users by spending
{_user_base_query}
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.active = true
GROUP BY u.id
HAVING COUNT(o.id) > 0
ORDER BY total_spent DESC
LIMIT :limit;
```

## Integration Architecture

### Adapter Implementation

```python
from typing import Dict, Any, Optional, Type
from pathlib import Path
import aiosql

class AioSQLAdapter:
    """Adapter integrating AioSQL with SQLSpec."""

    def __init__(
        self,
        sqlspec: SQLSpec,
        sql_config: Optional[SQLConfig] = None,
        type_mappings: Optional[Dict[str, Type]] = None
    ):
        self.sqlspec = sqlspec
        self.sql_config = sql_config or SQLConfig()
        self.type_mappings = type_mappings or {}
        self._query_cache = {}

    def from_path(
        self,
        sql_path: Union[str, Path],
        driver_name: str = "sqlspec"
    ) -> QueryLibrary:
        """Load queries from SQL file."""
        # Load with aiosql
        raw_queries = aiosql.from_path(
            sql_path,
            driver_name
        )

        # Wrap queries with SQLSpec integration
        return self._wrap_queries(raw_queries, sql_path)

    def _wrap_queries(
        self,
        raw_queries: Any,
        source_path: Path
    ) -> QueryLibrary:
        """Wrap AioSQL queries with SQLSpec functionality."""
        library = QueryLibrary(self.sqlspec, source_path)

        for query_name in raw_queries.available_queries:
            if query_name.startswith("_"):
                continue  # Skip private queries

            query_fn = getattr(raw_queries, query_name)
            wrapped_fn = self._create_wrapper(
                query_name,
                query_fn,
                source_path
            )

            setattr(library, query_name, wrapped_fn)

        return library

    def _create_wrapper(
        self,
        query_name: str,
        query_fn: Callable,
        source_path: Path
    ) -> Callable:
        """Create SQLSpec wrapper for AioSQL query."""

        # Get query metadata
        sql_text = self._extract_sql(query_fn)
        return_type = self.type_mappings.get(query_name)

        # Create wrapper based on query type
        if hasattr(query_fn, "_is_one_query"):
            return self._wrap_one_query(
                query_name, sql_text, return_type
            )
        elif hasattr(query_fn, "_is_many_query"):
            return self._wrap_many_query(
                query_name, sql_text, return_type
            )
        else:
            return self._wrap_execute_query(
                query_name, sql_text, return_type
            )
```

### Query Execution Wrapper

```python
def _wrap_one_query(
    self,
    query_name: str,
    sql_text: str,
    return_type: Optional[Type]
) -> Callable:
    """Wrap single-row query."""

    async def wrapped(
        session: SQLSpecSession,
        **params
    ) -> Optional[Any]:
        # Create SQL object with validation
        sql = SQL(sql_text, params, config=self.sql_config)

        # Add metadata
        sql.metadata["query_name"] = query_name
        sql.metadata["source"] = "aiosql"

        # Execute with type safety
        result = await session.execute(
            sql,
            schema_type=return_type
        )

        return result.one_or_none()

    # Preserve metadata
    wrapped.__name__ = query_name
    wrapped.__doc__ = self._extract_doc(sql_text)
    wrapped.sql = sql_text

    return wrapped
```

## Configuration Options

### Adapter Configuration

```python
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

@dataclass
class AioSQLConfig:
    """Configuration for AioSQL adapter."""

    # Query loading
    query_paths: List[str]
    query_loader_kwargs: Dict[str, Any] = field(default_factory=dict)

    # Validation
    validate_on_load: bool = True
    sql_config: Optional[SQLConfig] = None

    # Type safety
    enable_type_checking: bool = True
    type_mappings: Dict[str, Type] = field(default_factory=dict)

    # Performance
    cache_compiled_queries: bool = True
    cache_size: int = 1000

    # Security
    allowed_query_types: List[str] = field(
        default_factory=lambda: ["SELECT", "INSERT", "UPDATE", "DELETE"]
    )
    parameter_sanitization: bool = True

    # Monitoring
    track_query_usage: bool = True
    slow_query_threshold_ms: float = 1000.0

# Use configuration
config = AioSQLConfig(
    query_paths=["sql/queries/", "sql/reports/"],
    sql_config=SQLConfig(
        strict_mode=True,
        validators=[SecurityValidator()]
    ),
    type_mappings={
        "get_user": User,
        "list_orders": List[Order]
    }
)

adapter = AioSQLAdapter.from_config(config)
```

### Environment-Specific Queries

```python
-- name: get_user_data
-- Get user data with environment-specific fields
SELECT
    id,
    name,
    email,
    {% if env == 'development' %}
    -- Development-only fields
    debug_info,
    internal_notes,
    {% endif %}
    {% if features.get('analytics', False) %}
    -- Analytics fields when feature enabled
    last_utm_source,
    conversion_attribution,
    {% endif %}
    created_at
FROM users
WHERE id = :user_id;
```

```python
# Configure environment-aware loading
adapter = AioSQLAdapter(
    sqlspec,
    template_vars={
        "env": os.getenv("ENVIRONMENT", "production"),
        "features": {
            "analytics": True,
            "beta": False
        }
    }
)
```

## Query Organization

### Recommended File Structure

```
sql/
├── migrations/          # Database migrations
├── queries/            # Application queries
│   ├── users.sql       # User-related queries
│   ├── orders.sql      # Order-related queries
│   └── reports.sql     # Reporting queries
├── admin/              # Admin-only queries
│   └── maintenance.sql
└── analytics/          # Analytics queries
    ├── metrics.sql
    └── aggregates.sql
```

### Query Naming Conventions

```sql
-- Entity queries (users.sql)
-- name: get_user_by_id^         -- Single entity by ID
-- name: get_user_by_email^      -- Single entity by unique field
-- name: list_users              -- List with filters
-- name: search_users            -- Full-text search
-- name: create_user<!           -- Create with returned ID
-- name: update_user!            -- Update entity
-- name: delete_user!            -- Delete entity
-- name: bulk_update_users*!     -- Bulk operations

-- Relationship queries (orders.sql)
-- name: get_user_orders         -- One-to-many
-- name: get_order_with_items    -- One with related
-- name: count_user_orders^      -- Aggregation

-- Reporting queries (reports.sql)
-- name: daily_sales_summary     -- Time-based aggregation
-- name: top_products_by_revenue -- Ranked results
-- name: user_cohort_analysis    -- Complex analytics
```

## Performance Optimization

### Query Caching

```python
class CachedAioSQLAdapter(AioSQLAdapter):
    """AioSQL adapter with query result caching."""

    def __init__(self, *args, cache_ttl: int = 300, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = TTLCache(maxsize=1000, ttl=cache_ttl)

    def _wrap_cacheable_query(
        self,
        query_name: str,
        sql_text: str,
        return_type: Optional[Type]
    ) -> Callable:
        """Wrap query with caching logic."""

        async def wrapped(session: SQLSpecSession, **params):
            # Generate cache key
            cache_key = self._generate_cache_key(query_name, params)

            # Check cache
            if cache_key in self.cache:
                return self.cache[cache_key]

            # Execute query
            result = await self._execute_query(
                session, sql_text, params, return_type
            )

            # Cache result
            self.cache[cache_key] = result

            return result

        # Mark as cacheable
        wrapped.cacheable = True
        wrapped.cache_key_fn = lambda **p: self._generate_cache_key(query_name, p)

        return wrapped
```

### Prepared Statements

```python
-- name: get_user_by_id_prepared^
-- prepare: true
-- Prepared statement for frequent user lookups
SELECT id, name, email, created_at
FROM users
WHERE id = :user_id;
```

```python
class PreparedAioSQLAdapter(AioSQLAdapter):
    """Support for prepared statements."""

    def _should_prepare(self, sql_text: str) -> bool:
        """Check if query should be prepared."""
        return "-- prepare: true" in sql_text

    async def _prepare_query(
        self,
        session: SQLSpecSession,
        query_name: str,
        sql_text: str
    ) -> PreparedStatement:
        """Prepare statement for reuse."""
        clean_sql = self._clean_sql(sql_text)

        stmt = await session.prepare(clean_sql)

        # Cache prepared statement
        self._prepared_statements[query_name] = stmt

        return stmt
```

## Testing Support

### Mock Query Library

```python
from unittest.mock import Mock

def create_mock_queries(query_results: Dict[str, Any]) -> Mock:
    """Create mock query library for testing."""
    mock_queries = Mock()

    for query_name, result in query_results.items():
        query_mock = Mock(return_value=result)
        setattr(mock_queries, query_name, query_mock)

    return mock_queries

# Use in tests
async def test_user_service():
    # Mock query results
    mock_queries = create_mock_queries({
        "get_user_by_id": User(id=1, name="Test User"),
        "list_active_users": [User(id=1), User(id=2)]
    })

    # Test service
    service = UserService(mock_queries)
    user = await service.get_user(1)

    assert user.name == "Test User"
    mock_queries.get_user_by_id.assert_called_once_with(
        ANY,  # Session
        user_id=1
    )
```

### Query Testing

```python
import pytest
from sqlspec.testing import DatabaseTestCase

class TestUserQueries(DatabaseTestCase):
    """Test user queries."""

    @pytest.fixture
    def queries(self, test_db):
        """Load queries for testing."""
        adapter = AioSQLAdapter(test_db)
        return adapter.from_path("sql/users.sql")

    async def test_get_user_by_id(self, queries, test_session):
        """Test single user retrieval."""
        # Insert test data
        await test_session.execute(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            (1, "Test User", "test@example.com")
        )

        # Test query
        user = await queries.get_user_by_id(test_session, user_id=1)

        assert user is not None
        assert user["name"] == "Test User"

    async def test_create_user(self, queries, test_session):
        """Test user creation."""
        user_id = await queries.create_user(
            test_session,
            name="New User",
            email="new@example.com",
            password_hash="xxx"
        )

        assert isinstance(user_id, int)

        # Verify creation
        result = await test_session.execute(
            "SELECT name FROM users WHERE id = ?",
            (user_id,)
        )
        assert result.scalar() == "New User"
```

## Integration Patterns

### Service Layer Integration

```python
from typing import Optional

class UserService:
    """Service layer using AioSQL queries."""

    def __init__(self, queries: QueryLibrary):
        self.queries = queries

    async def get_user(
        self,
        session: SQLSpecSession,
        user_id: int
    ) -> Optional[User]:
        """Get user with caching."""
        return await self.queries.get_user_by_id(
            session,
            user_id=user_id
        )

    async def search_users(
        self,
        session: SQLSpecSession,
        search_term: Optional[str] = None,
        active_only: bool = True,
        limit: int = 20
    ) -> List[User]:
        """Search users with filters."""
        return await self.queries.search_users(
            session,
            search_term=search_term,
            search_pattern=f"%{search_term}%" if search_term else None,
            active_only=active_only,
            limit=limit
        )

    async def create_user(
        self,
        session: SQLSpecSession,
        user_data: UserCreate
    ) -> User:
        """Create user with validation."""
        # Hash password
        password_hash = self._hash_password(user_data.password)

        # Create user
        user_id = await self.queries.create_user(
            session,
            name=user_data.name,
            email=user_data.email,
            password_hash=password_hash
        )

        # Return created user
        return await self.get_user(session, user_id)
```

### Repository Pattern

```python
class UserRepository:
    """Repository using AioSQL queries."""

    def __init__(self, sqlspec: SQLSpec, queries: QueryLibrary):
        self.sqlspec = sqlspec
        self.queries = queries

    async def find_by_id(self, user_id: int) -> Optional[User]:
        async with self.sqlspec.get_session() as session:
            return await self.queries.get_user_by_id(
                session,
                user_id=user_id
            )

    async def find_by_email(self, email: str) -> Optional[User]:
        async with self.sqlspec.get_session() as session:
            return await self.queries.get_user_by_email(
                session,
                email=email
            )

    async def save(self, user: User) -> User:
        async with self.sqlspec.get_session() as session:
            if user.get("id"):
                # Update existing
                await self.queries.update_user(
                    session,
                    user_id=user["id"],
                    **user
                )
            else:
                # Create new
                user["id"] = await self.queries.create_user(
                    session,
                    **user
                )

            return user
```

## Best Practices

### 1. Organize Queries Logically

```sql
-- ❌ Bad: Mixed concerns in one file
-- name: get_user^
-- name: create_order<!
-- name: update_product!
-- name: delete_invoice!

-- ✅ Good: Separate files by domain
-- users.sql
-- name: get_user^
-- name: create_user<!
-- name: update_user!

-- orders.sql
-- name: get_order^
-- name: create_order<!
-- name: update_order_status!
```

### 2. Use Consistent Naming

```sql
-- ✅ Good: Consistent naming pattern
-- name: get_user_by_id^
-- name: get_order_by_id^
-- name: get_product_by_id^

-- ❌ Bad: Inconsistent naming
-- name: fetch_user^
-- name: order_by_id^
-- name: load_product_data^
```

### 3. Document Queries

```sql
-- name: calculate_user_lifetime_value^
-- Calculate the total lifetime value of a user
--
-- This includes:
-- - Total order amount
-- - Number of orders
-- - Average order value
-- - Days since first order
--
-- Parameters:
--   :user_id - The user's ID
--
-- Returns:
--   Single row with aggregated metrics
SELECT
    u.id as user_id,
    COUNT(o.id) as total_orders,
    COALESCE(SUM(o.total), 0) as lifetime_value,
    COALESCE(AVG(o.total), 0) as average_order_value,
    EXTRACT(DAY FROM (NOW() - MIN(o.created_at))) as customer_days
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.id = :user_id
GROUP BY u.id;
```

### 4. Handle NULLs Properly

```sql
-- name: get_user_profile^
-- Get complete user profile with defaults for NULL values
SELECT
    id,
    name,
    email,
    COALESCE(bio, '') as bio,
    COALESCE(avatar_url, '/default-avatar.png') as avatar_url,
    COALESCE(preferences, '{}'::jsonb) as preferences,
    created_at
FROM users
WHERE id = :user_id;
```

## Summary

The AioSQL integration provides:

- **Clean separation** of SQL and Python code
- **Type-safe** query execution with SQLSpec
- **Automatic validation** of SQL queries
- **Performance features** including caching and prepared statements
- **Testing utilities** for query verification
- **Flexible organization** of SQL files

This integration combines the simplicity of AioSQL's file-based approach with SQLSpec's powerful features for a best-of-both-worlds solution.

---

[← Litestar Integration](./17-litestar-integration.md) | [Back to Index →](../README.md)
