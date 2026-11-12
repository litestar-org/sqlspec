"""Verified session GUC identity and fail-closed lease disposal for psqlpy."""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import TenantLeaseState, with_cause
from sqlspec.driver._tenant import AsyncTenantLease, await_owned_release
from sqlspec.exceptions import TenantCleanupError
from sqlspec.utils.tenant._postgres import PostgresTenantSettings

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.core.tenant.base import AsyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

PSQLPY_DISPOSAL_TIMEOUT_S: Final = 2.0

__all__ = ("PsqlpyRLSStrategy", "PsqlpyTenantLease")


@mypyc_attr(allow_interpreted_subclasses=True)
class PsqlpyRLSStrategy(PostgresTenantSettings):
    """Apply and restore only owned settings through one bound query each."""

    __slots__ = ()

    async def checkout(self, connection: "PsqlpyConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Capture the previous session state before applying tenant identity."""
        sql, params = self.prepare_checkout(payload)
        result = await connection.fetch_row(sql, list(params))
        return self.capture(payload, params, result.result(as_tuple=True))

    async def checkin(self, connection: "PsqlpyConnection", state: TenantLeaseState) -> None:
        """Restore and verify the captured session state before pool release."""
        if not state.baseline:
            return
        sql, params = self.prepare_checkin(state)
        try:
            result = await connection.fetch_row(sql, list(params))
            self.verify_checkin(params, result.result(as_tuple=True))
        except TenantCleanupError:
            raise
        except Exception as exc:
            msg = "Tenant session restoration failed"
            raise with_cause(TenantCleanupError(msg), exc)


async def _terminate_and_confirm(connection: Any) -> bool:
    with suppress(Exception):
        await connection.execute("SELECT pg_terminate_backend(pg_backend_pid())")
    try:
        await connection.execute("SELECT 1")
    except Exception:
        return bool(connection.is_closed())
    return False


@mypyc_attr(allow_interpreted_subclasses=True)
class PsqlpyTenantLease(AsyncTenantLease):
    """Own one pool context, transaction finish, and verified disposal."""

    __slots__ = ("_config", "_context")

    def __init__(self, config: Any, strategy: "AsyncTenantStrategy", *, session: bool = False) -> None:
        super().__init__(strategy)
        self._config = config
        self._context: Any = None

    async def _acquire(self) -> Any:
        pool = await self._config.provide_pool()
        self._context = pool.acquire()
        connection = await self._context.__aenter__()
        setup_error: BaseException | None = None
        try:
            await self._config._ensure_connection(connection)
        except BaseException as exc:
            setup_error = exc
        if setup_error is not None:
            try:
                await await_owned_release(self._invalidate(connection))
            except BaseException as disposal_error:
                setup_error.add_note(f"Tenant connection invalidation failed: {disposal_error!r}")
            raise setup_error
        return connection

    async def _release(self, connection: Any) -> None:
        current, self._context = self._context, None
        if current is not None:
            await current.__aexit__(None, None, None)

    async def _invalidate(self, connection: Any) -> None:
        try:
            closed = await asyncio.wait_for(_terminate_and_confirm(connection), PSQLPY_DISPOSAL_TIMEOUT_S)
        except Exception:
            closed = False
        if not closed:
            await self._config._close_pool()
        with suppress(Exception):
            await self._release(connection)

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        await connection.execute("ROLLBACK")
