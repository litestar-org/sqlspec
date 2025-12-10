"""Multi-database configuration example.

Shows how to configure and use multiple databases simultaneously.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig


# Define models
class User(BaseModel):
    id: int
    name: str
    email: str


class OrderStats(BaseModel):
    total_orders: int
    total_spent: float
    avg_order_value: float


class Order(BaseModel):
    id: int
    user_id: int
    amount: float


# Initialize SQLSpec
spec = SQLSpec()

# Production database (PostgreSQL)
production_db = spec.add_config(
    AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/production", "min_size": 20, "max_size": 50})
)

# Cache database (SQLite)
cache_db = spec.add_config(
    SqliteConfig(connection_config={"database": "/var/lib/myapp/cache.db", "check_same_thread": False})
)

# Analytics database (DuckDB)
analytics_db = spec.add_config(
    DuckDBConfig(
        connection_config={
            "database": "/var/lib/myapp/analytics.duckdb",
            "config": {"memory_limit": "8GB", "threads": 4},
        }
    )
)


async def get_user_with_analytics(user_id: int) -> dict[str, Any]:
    """Get user from production DB with analytics from DuckDB."""
    # Get user from PostgreSQL (async)
    async with spec.provide_session(production_db) as pg_session:
        user_result = await pg_session.execute("SELECT id, name, email FROM users WHERE id = $1", user_id)
        user = user_result.one(schema_type=User)

    # Get user's order stats from DuckDB (sync - DuckDB is not async)
    with spec.provide_session(analytics_db) as duck_session:
        stats_result = duck_session.execute(
            """
            SELECT
                COUNT(*) as total_orders,
                SUM(amount) as total_spent,
                AVG(amount) as avg_order_value
            FROM orders
            WHERE user_id = ?
        """,
            user_id,
        )
        stats = stats_result.one(schema_type=OrderStats)

    # Check cache for recent activity (sync)
    with spec.provide_session(cache_db) as cache_session:
        cache_result = cache_session.execute("SELECT activity_data FROM user_cache WHERE user_id = ?", user_id)
        cached_activity = cache_result.one_or_none()

    return {"user": user, "order_stats": stats, "cached_activity": cached_activity}


async def sync_to_analytics() -> None:
    """Sync production data to analytics database."""
    # Read from production (async)
    async with spec.provide_session(production_db) as pg_session:
        result = await pg_session.execute("SELECT * FROM orders WHERE synced_at IS NULL")
        orders = result.all()

    if not orders:
        return

    # Write to analytics (sync - DuckDB is not async)
    with spec.provide_session(analytics_db) as duck_session:
        duck_session.execute_many(
            """
            INSERT INTO orders (id, user_id, amount, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [(o["id"], o["user_id"], o["amount"], o["created_at"]) for o in orders],
        )

    # Mark as synced (async)
    async with spec.provide_session(production_db) as pg_session:
        order_ids = [o["id"] for o in orders]
        await pg_session.execute("UPDATE orders SET synced_at = NOW() WHERE id = ANY($1)", order_ids)


async def cache_user_activity(user_id: int, activity_data: dict[str, Any]) -> None:
    """Cache user activity in SQLite (sync)."""
    with spec.provide_session(cache_db) as cache_session:
        cache_session.execute(
            """
            INSERT OR REPLACE INTO user_cache (user_id, activity_data, cached_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
            user_id,
            activity_data,
        )


async def main() -> None:
    """Example usage of multi-database configuration."""
    # Example usage
    await get_user_with_analytics(123)

    # Sync to analytics
    await sync_to_analytics()

    # Cache activity
    await cache_user_activity(123, {"last_action": "view_product"})

    # Cleanup
    await spec.close_all_pools()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

__all__ = ("Order", "OrderStats", "User", "cache_user_activity", "get_user_with_analytics", "main", "sync_to_analytics")
