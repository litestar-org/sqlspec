"""Session-scoped PostgreSQL RLS identity for asyncpg connections."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from sqlspec.core.tenant import TenantLeaseState, require_session_pooler, validate_tenant_session_baseline
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant_postgres import PostgresTenantSettings

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._typing import AsyncpgConnection
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = ("AsyncpgRLSStrategy",)


class AsyncpgRLSStrategy(PostgresTenantSettings):
    """Bind tenant identity and restore owned session settings in one query each."""

    __slots__ = ()

    async def checkout(self, connection: "AsyncpgConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Capture and apply identity before any application transaction."""
        if connection.is_in_transaction():
            msg = "Tenant checkout requires an idle connection"
            raise TenantContextError(msg)
        sql, params = self.prepare_checkout(payload)
        row = await connection.fetchrow(sql, *params)
        return self.capture(payload, params, tuple(row) if row is not None else None)

    async def checkin(self, connection: "AsyncpgConnection", state: TenantLeaseState) -> None:
        """Restore the baseline and verify it before the pool sees the connection."""
        if not state.baseline:
            return
        sql, params = self.prepare_checkin(state)
        try:
            row = await connection.fetchrow(sql, *params)
            self.verify_checkin(params, tuple(row) if row is not None else None)
        except Exception as exc:
            if isinstance(exc, TenantCleanupError):
                raise
            msg = "Tenant session restoration failed"
            raise TenantCleanupError(msg) from exc


async def apply_session_baseline(connection: "AsyncpgConnection", baseline: Mapping[str, str]) -> None:
    """Replay explicitly declared pool settings in one bound query."""
    if baseline:
        sql = "SELECT " + ", ".join(f"set_config(${2 * i + 1}, ${2 * i + 2}, false)" for i in range(len(baseline)))
        await connection.fetchrow(sql, *(part for pair in baseline.items() for part in pair))


def validate_asyncpg_tenant_config(
    driver_features: Mapping[str, Any], connection_config: Mapping[str, Any]
) -> dict[str, str]:
    """Validate affinity and the explicit declaration required by setup hooks."""
    require_session_pooler(driver_features.get("pooler_mode"), "AsyncpgConfig")
    if (
        driver_features.get("on_connection_create") is not None
        or connection_config.get("init") is not None
        or connection_config.get("setup") is not None
    ) and "tenant_session_baseline" not in driver_features:
        msg = "Tenant connection callbacks require an explicit tenant_session_baseline"
        raise ImproperConfigurationError(msg)
    return validate_tenant_session_baseline(
        driver_features.get("tenant_session_baseline", {}), connection_config.get("server_settings")
    )


def build_asyncpg_tenant_lease(config: Any) -> Any:
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
        connection = await pool.acquire()
        try:
            await apply_session_baseline(connection, config.driver_features.get("tenant_session_baseline", {}))
        except BaseException as exc:
            try:
                await invalidate(connection)
            except BaseException as cleanup:
                exc.add_note(f"Tenant connection disposal failed: {type(cleanup).__name__}")
            raise
        return connection

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
