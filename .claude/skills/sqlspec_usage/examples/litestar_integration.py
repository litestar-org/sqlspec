"""Complete Litestar + SQLSpec integration example.

Demonstrates:
- Multi-database setup (PostgreSQL + DuckDB)
- Dependency injection
- Transaction management
- Type-safe schema mapping
- Error handling
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from litestar import Litestar, delete, get, post
from litestar.exceptions import NotFoundException, ValidationException
from pydantic import BaseModel, EmailStr

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.exceptions import IntegrityError
from sqlspec.extensions.litestar import SQLSpecPlugin

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


# Pydantic models
class UserCreate(BaseModel):
    name: str
    email: EmailStr


class User(BaseModel):
    id: int
    name: str
    email: str


class AnalyticsEvent(BaseModel):
    event_type: str
    user_id: int
    metadata: dict


# Configure SQLSpec
spec = SQLSpec()

# Primary database (PostgreSQL)
primary_db = spec.add_config(
    AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/myapp", "min_size": 10, "max_size": 20},
        extension_config={
            "litestar": {
                "session_key": "primary_db",  # Custom key
                "commit_mode": "autocommit",  # Auto-commit on success
                "enable_correlation_middleware": True,  # Request tracking
            }
        },
        driver_features={
            "enable_pgvector": True,  # Auto-enabled if installed
            "enable_json_codecs": True,  # Auto-enabled
        },
    )
)

# Analytics database (DuckDB)
analytics_db = spec.add_config(
    DuckDBConfig(
        connection_config={"database": "analytics.duckdb", "config": {"memory_limit": "4GB"}},
        extension_config={
            "litestar": {
                "session_key": "analytics_db",  # Unique key
                "commit_mode": "autocommit",
            }
        },
    )
)


# Route handlers
@get("/users")
async def list_users(primary_db: AsyncpgDriver) -> list[User]:
    """List all users from primary database."""
    return await primary_db.select("SELECT id, name, email FROM users ORDER BY id", schema_type=User)


@get("/users/{user_id:int}")
async def get_user(user_id: int, primary_db: AsyncpgDriver) -> User:
    """Get single user by ID."""
    user = await primary_db.select_one_or_none(
        "SELECT id, name, email FROM users WHERE id = $1", user_id, schema_type=User
    )
    if user is None:
        raise NotFoundException(detail=f"User {user_id} not found")
    return user


@post("/users")
async def create_user(data: UserCreate, primary_db: AsyncpgDriver) -> User:
    """Create new user."""
    try:
        result = await primary_db.execute(
            """
            INSERT INTO users (name, email)
            VALUES ($1, $2)
            RETURNING id, name, email
            """,
            data.name,
            data.email,
        )
        return result.one(schema_type=User)
    except IntegrityError as e:
        raise ValidationException(detail="Email already exists") from e


@delete("/users/{user_id:int}")
async def delete_user(user_id: int, primary_db: AsyncpgDriver) -> dict:
    """Delete user by ID."""
    result = await primary_db.execute("DELETE FROM users WHERE id = $1", user_id)

    if result.rows_affected == 0:
        raise NotFoundException(detail=f"User {user_id} not found")

    return {"deleted": user_id}


@post("/events")
def log_event(event: AnalyticsEvent, analytics_db: DuckDBDriver) -> dict:
    """Log analytics event to DuckDB."""
    analytics_db.execute(
        """
        INSERT INTO events (event_type, user_id, metadata, created_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """,
        event.event_type,
        event.user_id,
        event.metadata,
    )
    return {"logged": True}


@get("/analytics/user-stats")
async def user_stats(primary_db: AsyncpgDriver, analytics_db: DuckDBDriver) -> dict[str, Any]:
    """Aggregate stats from both databases."""
    # Get user count from PostgreSQL
    user_count = await primary_db.select_value("SELECT COUNT(*) FROM users")

    # Get event stats from DuckDB (sync - call directly, no await)
    event_stats = analytics_db.select(
        """
        SELECT
            event_type,
            COUNT(*) as count
        FROM events
        GROUP BY event_type
        ORDER BY count DESC
        """
    )

    return {"total_users": user_count, "event_stats": event_stats}


@get("/users/{user_id:int}/with-events")
async def user_with_events(user_id: int, primary_db: AsyncpgDriver, analytics_db: DuckDBDriver) -> dict[str, Any]:
    """Get user with their events from both databases."""
    # Get user from PostgreSQL
    user = await primary_db.select_one_or_none(
        "SELECT id, name, email FROM users WHERE id = $1", user_id, schema_type=User
    )
    if user is None:
        raise NotFoundException(detail=f"User {user_id} not found")

    # Get user's events from DuckDB (sync - call directly, no await)
    events = analytics_db.select(
        """
        SELECT event_type, metadata, created_at
        FROM events
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT 10
        """,
        user_id,
    )

    return {"user": user.model_dump(), "recent_events": events}


# Lifespan context manager for schema initialization
@asynccontextmanager
async def lifespan(app: Litestar) -> AsyncGenerator[None, None]:
    """Initialize database schemas on startup."""
    # PostgreSQL schema
    async with spec.provide_session(primary_db) as session:
        await session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await session.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)
        """)

    # DuckDB schema (sync - DuckDB is not async)
    with spec.provide_session(analytics_db) as session:
        session.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_type TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                metadata JSON,
                created_at TIMESTAMP NOT NULL
            )
        """)

    yield


# Application setup
app = Litestar(
    route_handlers=[list_users, get_user, create_user, delete_user, log_event, user_stats, user_with_events],
    plugins=[SQLSpecPlugin(sqlspec=spec)],
    lifespan=[lifespan],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)  # nosec: B104
