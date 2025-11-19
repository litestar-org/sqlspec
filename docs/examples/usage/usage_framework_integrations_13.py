# start-example
__all__ = ("generate_report", "get_analytics_db", "get_main_db", "test_stub")


# Main database
from typing import Annotated

from fastapi import Depends, FastAPI

from sqlspec import AsyncDriverAdapterBase, SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

spec = SQLSpec()
main_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/main"}))

# Analytics database
analytics_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/analytics"}))


# Dependency functions
async def get_main_db():
    async with spec.provide_session(main_db) as session:
        yield session


async def get_analytics_db():
    async with spec.provide_session(analytics_db) as session:
        yield session


app = FastAPI()


# Use in handlers
@app.get("/report")
async def generate_report(
    main_db: Annotated[AsyncDriverAdapterBase, Depends(get_main_db)],
    analytics_db: Annotated[AsyncDriverAdapterBase, Depends(get_analytics_db)],
) -> dict:
    users = await main_db.execute("SELECT COUNT(*) FROM users")
    events = await analytics_db.execute("SELECT COUNT(*) FROM events")
    return {"users": users.scalar(), "events": events.scalar()}


# end-example


def test_stub() -> None:
    assert True
