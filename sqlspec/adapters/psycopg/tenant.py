"""Session RLS identity for synchronous and asynchronous psycopg pools."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from psycopg import pq
from psycopg.rows import tuple_row

from sqlspec.core.tenant import TenantLeaseState, require_session_pooler
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant_postgres import PostgresTenantSettings, psycopg_setting_query

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = ("PsycopgAsyncRLSStrategy", "PsycopgSyncRLSStrategy")


class PsycopgSyncRLSStrategy(PostgresTenantSettings):
    """Capture, apply and verify synchronous PostgreSQL session settings."""

    __slots__ = ()

    def checkout(self, connection: "PsycopgSyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Apply identity and finish only the management transaction."""
        if connection.info.transaction_status != pq.TransactionStatus.IDLE:
            msg = "Tenant checkout requires an idle connection"
            raise TenantContextError(msg)
        sql, params = self.prepare_checkout(payload)
        with connection.cursor(row_factory=tuple_row) as cursor:
            query, binds = psycopg_setting_query(sql, params)
            cursor.execute(query.encode("ascii"), binds)
            state = self.capture(payload, params, cursor.fetchone())
        if not connection.autocommit:
            connection.commit()
        return state

    def checkin(self, connection: "PsycopgSyncConnection", state: TenantLeaseState) -> None:
        """Restore owned settings after the lease has finished application work."""
        if not state.baseline:
            return
        try:
            if connection.info.transaction_status != pq.TransactionStatus.IDLE:
                connection.rollback()
            sql, params = self.prepare_checkin(state)
            with connection.cursor(row_factory=tuple_row) as cursor:
                query, binds = psycopg_setting_query(sql, params)
                cursor.execute(query.encode("ascii"), binds)
                self.verify_checkin(params, cursor.fetchone())
            if not connection.autocommit:
                connection.commit()
        except Exception as exc:
            if isinstance(exc, TenantCleanupError):
                raise
            msg = "Tenant session restoration failed"
            raise TenantCleanupError(msg) from exc


class PsycopgAsyncRLSStrategy(PostgresTenantSettings):
    """Capture, apply and verify asynchronous PostgreSQL session settings."""

    __slots__ = ()

    async def checkout(self, connection: "PsycopgAsyncConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Apply identity and finish only the management transaction."""
        if connection.info.transaction_status != pq.TransactionStatus.IDLE:
            msg = "Tenant checkout requires an idle connection"
            raise TenantContextError(msg)
        sql, params = self.prepare_checkout(payload)
        async with connection.cursor(row_factory=tuple_row) as cursor:
            query, binds = psycopg_setting_query(sql, params)
            await cursor.execute(query.encode("ascii"), binds)
            state = self.capture(payload, params, await cursor.fetchone())
        if not connection.autocommit:
            await connection.commit()
        return state

    async def checkin(self, connection: "PsycopgAsyncConnection", state: TenantLeaseState) -> None:
        """Restore owned settings after the lease has finished application work."""
        if not state.baseline:
            return
        try:
            if connection.info.transaction_status != pq.TransactionStatus.IDLE:
                await connection.rollback()
            sql, params = self.prepare_checkin(state)
            async with connection.cursor(row_factory=tuple_row) as cursor:
                query, binds = psycopg_setting_query(sql, params)
                await cursor.execute(query.encode("ascii"), binds)
                self.verify_checkin(params, await cursor.fetchone())
            if not connection.autocommit:
                await connection.commit()
        except Exception as exc:
            if isinstance(exc, TenantCleanupError):
                raise
            msg = "Tenant session restoration failed"
            raise TenantCleanupError(msg) from exc


def validate_psycopg_tenant_config(
    driver_features: Mapping[str, Any], connection_config: Mapping[str, Any], connection_instance: Any, adapter: str
) -> str:
    """Require session affinity and physical close semantics on invalidation."""
    mode = require_session_pooler(driver_features.get("pooler_mode"), adapter)
    if connection_config.get("close_returns") or getattr(connection_instance, "close_returns", False):
        msg = "Tenant configurations require close_returns=False for physical disposal"
        raise ImproperConfigurationError(msg)
    return mode


def build_psycopg_sync_tenant_lease(config: Any, *, rollback_on_exit: bool = False) -> Any:
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


def build_psycopg_async_tenant_lease(config: Any, *, rollback_on_exit: bool = False) -> Any:
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
