"""FastAPI + SQLSpec integration example.

Demonstrates:
- Dependency injection
- Type-safe responses
- Error handling
- Transaction management
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, EmailStr

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.exceptions import IntegrityError
from sqlspec.extensions.fastapi import SQLSpecPlugin

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


# Models
class UserCreate(BaseModel):
    name: str
    email: EmailStr


class User(BaseModel):
    id: int
    name: str
    email: str


# Configure SQLSpec
spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/myapp", "min_size": 5, "max_size": 10},
        extension_config={
            "starlette": {  # FastAPI uses starlette config key
                "commit_mode": "autocommit",
                "session_key": "db_session",
            }
        },
    )
)

# Initialize plugin
plugin = SQLSpecPlugin(spec)


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize database schema on startup."""
    async with spec.provide_session(db) as session:
        await session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )
        """)
    yield


# Create app with lifespan
app = FastAPI(title="FastAPI + SQLSpec Example", lifespan=lifespan)

# Initialize plugin with app
plugin.init_app(app)


# Dependency type alias
DBSession = Annotated[AsyncpgDriver, Depends(plugin.provide_session(db))]


# Routes
@app.get("/users", response_model=list[User])
async def list_users(db: DBSession) -> list[User]:
    """Get all users."""
    return await db.select("SELECT id, name, email FROM users ORDER BY id", schema_type=User)


@app.get("/users/{user_id}", response_model=User)
async def get_user(user_id: int, db: DBSession) -> User:
    """Get user by ID."""
    user = await db.select_one_or_none("SELECT id, name, email FROM users WHERE id = $1", user_id, schema_type=User)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")
    return user


@app.post("/users", response_model=User, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate, db: DBSession) -> User:
    """Create new user."""
    try:
        result = await db.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email", user.name, user.email
        )
        return result.one(schema_type=User)
    except IntegrityError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already exists") from exc


@app.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: int, db: DBSession) -> None:
    """Delete user."""
    result = await db.execute("DELETE FROM users WHERE id = $1", user_id)

    if result.rows_affected == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)  # nosec: B104

__all__ = ("User", "UserCreate", "create_user", "delete_user", "get_user", "lifespan", "list_users")
