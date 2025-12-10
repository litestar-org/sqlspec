# Testing Patterns

Best practices for testing with SQLSpec.

## Test Isolation with Pooling

**❌ WRONG - :memory: with pooling causes test failures:**
```python
def test_something():
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    # Shared state across parallel tests!
```

**✅ CORRECT - Use temp files:**
```python
import tempfile

def test_something():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        # Each test gets isolated database
```

## pytest-databases Integration

```python
import pytest
from pytest_databases.docker.postgres import PostgresService
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

@pytest.fixture(scope="session")
def asyncpg_config(postgres_service: PostgresService):
    return AsyncpgConfig(
        connection_config={"dsn": postgres_service.connection_url()}
    )

@pytest.fixture(scope="session")
def asyncpg_driver(asyncpg_config):
    spec = SQLSpec()
    config = spec.add_config(asyncpg_config)

    # Apply schema once per session
    with spec.provide_session(config) as session:
        session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)

    with spec.provide_session(config) as driver:
        yield driver

@pytest.fixture
def clean_users(asyncpg_driver):
    """Clean users table before each test."""
    yield
    asyncpg_driver.execute("DELETE FROM users")
```

## Parallel Test Execution

```bash
# Run tests in parallel
uv run pytest -n auto --dist=loadgroup

# Specific markers
uv run pytest -m postgres -n auto --dist=loadgroup
```

**Requirements for parallel execution:**
- Session-scoped fixtures for containers
- Idempotent DDL (`CREATE TABLE IF NOT EXISTS`)
- Unique temp files for SQLite pooling tests
- Clean data between tests (fixtures or transactions)

## Transaction Rollback Pattern

```python
@pytest.fixture
async def db_session(asyncpg_driver):
    """Provide session with automatic rollback."""
    async with asyncpg_driver.begin_transaction():
        yield asyncpg_driver
        # Auto-rollback on fixture teardown
```

## Testing Framework Extensions

```python
import tempfile
from starlette.testclient import TestClient

def test_starlette_autocommit():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit"}}
        )
        sql.add_config(config)
        plugin = SQLSpecPlugin(sql)

        async def create_user(request):
            session = plugin.get_session(request)
            await session.execute(
                "INSERT INTO users (name) VALUES (?)", "Alice"
            )
            return JSONResponse({"created": True})

        app = Starlette(routes=[Route("/create", create_user)])
        plugin.init_app(app)

        with TestClient(app) as client:
            response = client.post("/create")
            assert response.status_code == 200
```

## Testing Migrations

```python
def test_migration_up_down(config):
    # Apply migration
    config.migrate_up("0001")

    # Verify table exists
    with config.provide_session() as session:
        result = session.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        assert result.one_or_none() is not None

    # Rollback
    config.migrate_down("base")

    # Verify table removed
    with config.provide_session() as session:
        result = session.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        assert result.one_or_none() is None
```

## Best Practices

1. **Use tempfile for SQLite pooling tests** - Never :memory:
2. **Session-scoped fixtures for containers** - Start once, reuse
3. **Idempotent DDL** - CREATE TABLE IF NOT EXISTS
4. **Clean data between tests** - Fixtures, transactions, or DELETE
5. **Mark adapter-specific tests** - @pytest.mark.postgres
6. **Run parallel execution** - pytest -n auto --dist=loadgroup
7. **Test both success and failure paths** - Exceptions, rollbacks
