# Dishka Dependency Injection

[Dishka](https://dishka.readthedocs.io/) is a scoped IoC container for Python applications that share dependencies across HTTP servers (Litestar, FastAPI, Starlette, Flask, Sanic), background worker queues, and CLI entrypoints. SQLSpec's explicit separation between application-scoped `*Config` pools and request-scoped `*Driver` sessions maps directly onto Dishka's `Scope.APP` and `Scope.REQUEST` scopes.

## Installation

```bash
uv add sqlspec dishka
```

## Provider Pattern (`Scope.APP` + `Scope.REQUEST`)

Register `SQLSpec` and your adapter `*Config` at `Scope.APP` (created once per process, closing the pool when the application container closes) and yield the driver session from `sqlspec.provide_session(config)` at `Scope.REQUEST`:

```python
from collections.abc import AsyncIterator

from dishka import Provider, Scope, provide

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver


class DatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_config(self) -> AsyncIterator[tuple[SQLSpec, AsyncpgConfig]]:
        spec = SQLSpec()
        config = spec.add_config(
            AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb", "min_size": 2, "max_size": 20})
        )
        try:
            yield spec, config
        finally:
            await config.close_pool()

    @provide(scope=Scope.REQUEST)
    async def provide_driver(self, app_deps: tuple[SQLSpec, AsyncpgConfig]) -> AsyncIterator[AsyncpgDriver]:
        spec, config = app_deps
        async with spec.provide_session(config) as driver:
            yield driver
```

## Injecting Drivers into Domain Services

Combine `FromDishka[DriverT]` with `SQLSpecAsyncService` (or `SQLSpecSyncService`) so domain services receive a request-scoped driver automatically:

```python
from typing import Any

from dishka import FromDishka, Provider, Scope, make_async_container, provide

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.service import SQLSpecAsyncService


class UserService(SQLSpecAsyncService[AsyncpgDriver]):
    def __init__(self, driver: FromDishka[AsyncpgDriver]) -> None:
        super().__init__(session=driver)

    async def get_user(self, user_id: int) -> dict[str, Any] | None:
        return await self.get_one_or_none("SELECT id, email FROM users WHERE id = :id", id=user_id)


class ServiceProvider(Provider):
    user_service = provide(UserService, scope=Scope.REQUEST)
```

## Reusing the Container Across Entrypoints

```python
from dishka import make_async_container

container = make_async_container(DatabaseProvider(), ServiceProvider())

async with container() as request_container:
    users = await request_container.get(UserService)
    user = await users.get_user(1)

await container.close()
```

## Recap

- Provide `SQLSpec` and `*Config` at `Scope.APP` (closing pools in `finally:`) and yield `spec.provide_session(config)` at `Scope.REQUEST`.
- Pair `FromDishka` with [Service Layer](../learn/recipes/service-layer.md) classes (`SQLSpecAsyncService` / `SQLSpecSyncService`) to share query logic across web routes, CLI commands, and background workers.
