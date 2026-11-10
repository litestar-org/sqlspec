"""CockroachDB session tenant identity with verified empty-string restoration."""

import asyncio
from typing import Any, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.adapters.asyncpg.tenant import AsyncpgRLSStrategy
from sqlspec.utils.tenant_asyncpg import await_owned_release

__all__ = ("CockroachAsyncpgRLSStrategy", "build_cockroach_asyncpg_tenant_lease")


@mypyc_attr(allow_interpreted_subclasses=True)
class CockroachAsyncpgRLSStrategy(AsyncpgRLSStrategy):
    """Apply tenant settings and clear unset CockroachDB values with empty strings."""

    __slots__ = ()
    _unset_restore_value: ClassVar[str] = ""


def build_cockroach_asyncpg_tenant_lease(config: Any) -> Any:
    """Wrap the adapter pool callbacks in the shared fail-closed lease."""
    from asyncpg import InterfaceError

    from sqlspec.driver._tenant import DEFAULT_TENANT_CLEANUP_TIMEOUT, AsyncTenantLease

    pool: Any = None
    cleanup_deadline: float | None = None

    async def release(connection: Any) -> None:
        timeout = (
            DEFAULT_TENANT_CLEANUP_TIMEOUT
            if cleanup_deadline is None
            else max(0.0, cleanup_deadline - asyncio.get_running_loop().time())
        )
        await await_owned_release(pool.release(connection, timeout=timeout))

    async def invalidate(connection: Any) -> None:
        try:
            connection.terminate()
        except InterfaceError:
            pass
        finally:
            await release(connection)

    async def acquire() -> Any:
        nonlocal pool
        pool = await config.provide_pool()
        return await pool.acquire()

    async def finish_transaction(connection: Any, _exc_type: type[BaseException] | None) -> None:
        nonlocal cleanup_deadline
        cleanup_deadline = asyncio.get_running_loop().time() + DEFAULT_TENANT_CLEANUP_TIMEOUT
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")

    return AsyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish_transaction,
        cleanup_timeout=DEFAULT_TENANT_CLEANUP_TIMEOUT,
    )
