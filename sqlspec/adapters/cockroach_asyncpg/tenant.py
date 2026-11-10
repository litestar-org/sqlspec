"""CockroachDB session tenant identity with verified empty-string restoration."""

from typing import Any, ClassVar

from sqlspec.adapters.asyncpg import AsyncpgRLSStrategy


class CockroachAsyncpgRLSStrategy(AsyncpgRLSStrategy):
    """Apply tenant settings and clear unset CockroachDB values with empty strings."""

    __slots__ = ()
    _unset_restore_value: ClassVar[str] = ""


def build_cockroach_asyncpg_tenant_lease(config: Any) -> Any:
    """Wrap the adapter pool callbacks in the shared fail-closed lease."""
    from asyncpg import InterfaceError

    from sqlspec.driver._tenant import AsyncTenantLease

    pool: Any = None

    async def invalidate(connection: Any) -> None:
        try:
            connection.terminate()
        except InterfaceError:
            pass
        finally:
            await pool.release(connection)

    async def acquire() -> Any:
        nonlocal pool
        pool = await config.provide_pool()
        return await pool.acquire()

    async def release(connection: Any) -> None:
        await pool.release(connection)

    async def finish_transaction(connection: Any, _exc_type: type[BaseException] | None) -> None:
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")

    return AsyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish_transaction,
    )
