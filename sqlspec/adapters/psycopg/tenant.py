"""Session RLS identity for synchronous and asynchronous psycopg pools."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

from sqlspec.adapters.psycopg._typing import psycopg_pq as pq
from sqlspec.adapters.psycopg._typing import psycopg_tuple_row as tuple_row
from sqlspec.core.tenant.base import TenantLeaseState, require_session_pooler, with_cause
from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant._postgres import PostgresTenantSettings, pyformat_binds, pyformat_query

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection
    from sqlspec.core.tenant.base import AsyncTenantStrategy, SyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "PsycopgAsyncRLSStrategy",
    "PsycopgAsyncTenantLease",
    "PsycopgSyncRLSStrategy",
    "PsycopgSyncTenantLease",
    "validate_psycopg_tenant_config",
)


@mypyc_attr(allow_interpreted_subclasses=True)
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
            cursor.execute(pyformat_query(sql), pyformat_binds(params))
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
                cursor.execute(pyformat_query(sql), pyformat_binds(params))
                self.verify_checkin(params, cursor.fetchone())
            if not connection.autocommit:
                connection.commit()
        except TenantCleanupError:
            raise
        except Exception as exc:
            msg = "Tenant session restoration failed"
            raise with_cause(TenantCleanupError(msg), exc)


@mypyc_attr(allow_interpreted_subclasses=True)
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
            await cursor.execute(pyformat_query(sql), pyformat_binds(params))
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
                await cursor.execute(pyformat_query(sql), pyformat_binds(params))
                self.verify_checkin(params, await cursor.fetchone())
            if not connection.autocommit:
                await connection.commit()
        except TenantCleanupError:
            raise
        except Exception as exc:
            msg = "Tenant session restoration failed"
            raise with_cause(TenantCleanupError(msg), exc)


def validate_psycopg_tenant_config(
    driver_features: Mapping[str, Any], connection_config: Mapping[str, Any], connection_instance: Any, adapter: str
) -> str:
    """Require session affinity and physical close semantics on invalidation."""
    mode = require_session_pooler(driver_features.get("pooler_mode"), adapter)
    if connection_config.get("close_returns") or getattr(connection_instance, "close_returns", False):
        msg = "Tenant configurations require close_returns=False for physical disposal"
        raise ImproperConfigurationError(msg)
    return mode


def _quarantine_error(close_error: BaseException, quarantine_error: BaseException) -> TenantCleanupError:
    msg = f"Physical close failed: {close_error!r}; pool quarantine failed: {quarantine_error!r}"
    error = TenantCleanupError(msg)
    error.__cause__ = close_error
    return error


@mypyc_attr(allow_interpreted_subclasses=True)
class PsycopgSyncTenantLease(SyncTenantLease):
    """Adapt the native pool context to a synchronous fail-closed lease.

    Like the native pool context, a clean exit commits and an exception rolls back.
    """

    __slots__ = ("_config", "_context", "_pool")

    def __init__(self, config: Any, strategy: "SyncTenantStrategy") -> None:
        super().__init__(strategy)
        self._config = config
        self._context: Any = None
        self._pool: Any = None

    def _acquire(self) -> Any:
        self._pool = self._config.provide_pool()
        self._context = self._pool.connection()
        return self._context.__enter__()

    def _release(self, connection: Any) -> None:
        current, self._context = self._context, None
        if current is not None:
            current.__exit__(None, None, None)

    def _invalidate(self, connection: Any) -> None:
        try:
            connection.close()
        except BaseException as close_error:
            try:
                self._pool.close(timeout=0)
            except BaseException as quarantine_error:
                raise _quarantine_error(close_error, quarantine_error)
            raise
        self._release(connection)

    def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        if exc_type is None:
            connection.commit()
        else:
            connection.rollback()


@mypyc_attr(allow_interpreted_subclasses=True)
class PsycopgAsyncTenantLease(AsyncTenantLease):
    """Adapt the native pool context to an asynchronous fail-closed lease.

    Like the native pool context, a clean exit commits and an exception rolls back.
    """

    __slots__ = ("_config", "_context", "_pool")

    def __init__(self, config: Any, strategy: "AsyncTenantStrategy") -> None:
        super().__init__(strategy)
        self._config = config
        self._context: Any = None
        self._pool: Any = None

    async def _acquire(self) -> Any:
        self._pool = await self._config.provide_pool()
        self._context = self._pool.connection()
        return await self._context.__aenter__()

    async def _release(self, connection: Any) -> None:
        current, self._context = self._context, None
        if current is not None:
            await current.__aexit__(None, None, None)

    async def _invalidate(self, connection: Any) -> None:
        try:
            await connection.close()
        except BaseException as close_error:
            try:
                await self._pool.close(timeout=0)
            except BaseException as quarantine_error:
                raise _quarantine_error(close_error, quarantine_error)
            raise
        await self._release(connection)

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        if exc_type is None:
            await connection.commit()
        else:
            await connection.rollback()
