"""CockroachDB psycopg session tenant strategies and native lease callbacks."""

from typing import Any, ClassVar

from sqlspec.adapters.psycopg import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy


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

    def acquire() -> Any:
        nonlocal context
        context = config.provide_pool().connection()
        return context.__enter__()

    def release(_connection: Any) -> None:
        nonlocal context
        current, context = context, None
        if current is not None:
            current.__exit__(None, None, None)

    def invalidate(connection: Any) -> None:
        try:
            connection.close()
        finally:
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

    async def acquire() -> Any:
        nonlocal context
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
        finally:
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
