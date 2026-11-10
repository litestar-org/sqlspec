"""CockroachDB psycopg session tenant strategies and native lease callbacks."""

from typing import Any, ClassVar

from sqlspec.adapters.psycopg import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy
from sqlspec.exceptions import TenantCleanupError

__all__ = (
    "CockroachPsycopgAsyncRLSStrategy",
    "CockroachPsycopgSyncRLSStrategy",
    "build_cockroach_psycopg_async_tenant_lease",
    "build_cockroach_psycopg_sync_tenant_lease",
)


class CockroachPsycopgSyncRLSStrategy(PsycopgSyncRLSStrategy):
    """Apply and restore synchronous CockroachDB session settings."""

    __slots__ = ()
    _unset_restore_value: ClassVar[str] = ""


class CockroachPsycopgAsyncRLSStrategy(PsycopgAsyncRLSStrategy):
    """Apply and restore asynchronous CockroachDB session settings."""

    __slots__ = ()
    _unset_restore_value: ClassVar[str] = ""


def build_cockroach_psycopg_sync_tenant_lease(config: Any, *, rollback_on_exit: bool = False) -> Any:
    """Adapt the native pool context to synchronous lease callbacks."""
    from sqlspec.driver._tenant import SyncTenantLease

    context: Any = None
    pool: Any = None

    def acquire() -> Any:
        nonlocal context, pool
        pool = config.provide_pool()
        context = pool.connection()
        return context.__enter__()

    def release(_connection: Any) -> None:
        nonlocal context
        current, context = context, None
        if current is not None:
            current.__exit__(None, None, None)

    def invalidate(connection: Any) -> None:
        try:
            connection.close()
        except BaseException as close_error:
            try:
                pool.close(timeout=0)
            except BaseException as quarantine_error:
                msg = f"Physical close failed: {close_error!r}; pool quarantine failed: {quarantine_error!r}"
                raise TenantCleanupError(msg) from close_error
            raise
        release(connection)

    def finish_transaction(connection: Any, exc_type: type[BaseException] | None) -> None:
        if exc_type is None and not rollback_on_exit:
            connection.commit()
        else:
            connection.rollback()

    return SyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish_transaction,
    )


def build_cockroach_psycopg_async_tenant_lease(config: Any, *, rollback_on_exit: bool = False) -> Any:
    """Adapt the native pool context to asynchronous lease callbacks."""
    from sqlspec.driver._tenant import AsyncTenantLease

    context: Any = None
    pool: Any = None

    async def acquire() -> Any:
        nonlocal context, pool
        pool = await config.provide_pool()
        context = pool.connection()
        return await context.__aenter__()

    async def release(_connection: Any) -> None:
        nonlocal context
        current, context = context, None
        if current is not None:
            await current.__aexit__(None, None, None)

    async def invalidate(connection: Any) -> None:
        try:
            await connection.close()
        except BaseException as close_error:
            try:
                await pool.close(timeout=0)
            except BaseException as quarantine_error:
                msg = f"Physical close failed: {close_error!r}; pool quarantine failed: {quarantine_error!r}"
                raise TenantCleanupError(msg) from close_error
            raise
        await release(connection)

    async def finish_transaction(connection: Any, exc_type: type[BaseException] | None) -> None:
        if exc_type is None and not rollback_on_exit:
            await connection.commit()
        else:
            await connection.rollback()

    return AsyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish_transaction,
    )
