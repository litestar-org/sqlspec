"""Session-scoped PostgreSQL RLS identity for asyncpg connections."""

import asyncio
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

from sqlspec.adapters.asyncpg._typing import AsyncpgInterfaceError
from sqlspec.core.tenant.base import (
    TenantLeaseState,
    require_session_pooler,
    validate_tenant_session_baseline,
    with_cause,
)
from sqlspec.driver._tenant import DEFAULT_TENANT_CLEANUP_TIMEOUT, AsyncTenantLease, await_owned_release
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant._postgres import PostgresTenantSettings

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._typing import AsyncpgConnection
    from sqlspec.core.tenant.base import AsyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = ("AsyncpgRLSStrategy", "AsyncpgTenantLease", "apply_session_baseline", "validate_asyncpg_tenant_config")


@mypyc_attr(allow_interpreted_subclasses=True)
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
        except TenantCleanupError:
            raise
        except Exception as exc:
            msg = "Tenant session restoration failed"
            raise with_cause(TenantCleanupError(msg), exc)


async def apply_session_baseline(connection: "AsyncpgConnection", baseline: Mapping[str, str]) -> None:
    """Replay explicitly declared pool settings in one bound query."""
    if baseline:
        sql = "SELECT " + ", ".join(f"set_config(${2 * i + 1}, ${2 * i + 2}, false)" for i in range(len(baseline)))
        await connection.fetchrow(sql, *(part for pair in baseline.items() for part in pair))


def validate_asyncpg_tenant_config(
    driver_features: Mapping[str, Any], connection_config: Mapping[str, Any]
) -> dict[str, str]:
    """Validate affinity and the explicit declaration required by pool setup hooks."""
    require_session_pooler(driver_features.get("pooler_mode"), "AsyncpgConfig")
    if (
        connection_config.get("init") is not None or connection_config.get("setup") is not None
    ) and "tenant_session_baseline" not in driver_features:
        msg = "Tenant connection callbacks require an explicit tenant_session_baseline"
        raise ImproperConfigurationError(msg)
    return validate_tenant_session_baseline(
        driver_features.get("tenant_session_baseline", {}), connection_config.get("server_settings")
    )


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncpgTenantLease(AsyncTenantLease):
    """Fail-closed tenant lease over an asyncpg pool.

    Release shares one deadline with transaction cleanup, and every release is
    joined through caller cancellation before the connection changes owner.
    """

    __slots__ = ("_cleanup_deadline", "_config", "_pool")

    def __init__(self, config: Any, strategy: "AsyncTenantStrategy", *, session: bool = False) -> None:
        super().__init__(strategy, cleanup_timeout=DEFAULT_TENANT_CLEANUP_TIMEOUT)
        self._config = config
        self._pool: Any = None
        self._cleanup_deadline: float | None = None

    async def _acquire(self) -> Any:
        self._pool = await self._config.provide_pool()
        connection = await self._pool.acquire()
        setup_error: BaseException | None = None
        try:
            await apply_session_baseline(connection, self._config.driver_features.get("tenant_session_baseline", {}))
        except BaseException as exc:
            setup_error = exc
        if setup_error is not None:
            try:
                await self._invalidate(connection)
            except BaseException as cleanup:
                setup_error.add_note(f"Tenant connection disposal failed: {type(cleanup).__name__}")
            raise setup_error
        return connection

    async def _release(self, connection: Any) -> None:
        deadline = self._cleanup_deadline
        timeout = (
            DEFAULT_TENANT_CLEANUP_TIMEOUT
            if deadline is None
            else max(0.0, deadline - asyncio.get_running_loop().time())
        )
        await await_owned_release(self._pool.release(connection, timeout=timeout))

    async def _invalidate(self, connection: Any) -> None:
        try:
            connection.terminate()
        except AsyncpgInterfaceError:
            pass
        finally:
            await self._release(connection)

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        self._cleanup_deadline = asyncio.get_running_loop().time() + DEFAULT_TENANT_CLEANUP_TIMEOUT
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")
