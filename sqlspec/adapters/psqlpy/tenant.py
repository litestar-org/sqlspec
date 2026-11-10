"""Verified session GUC identity and fail-closed lease disposal for psqlpy."""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import TenantLeaseState
from sqlspec.exceptions import TenantCleanupError
from sqlspec.utils.tenant_postgres import PostgresTenantSettings

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.utils.tenant import TenantContextPayload

PSQLPY_DISPOSAL_TIMEOUT_S: Final = 2.0

__all__ = ("PsqlpyRLSStrategy",)


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
        except Exception as exc:
            if isinstance(exc, TenantCleanupError):
                raise
            msg = "Tenant session restoration failed"
            error = TenantCleanupError(msg)
            error.__cause__ = exc
            raise error


def build_psqlpy_tenant_lease(config: Any) -> Any:
    """Own one pool context, transaction finish, and verified disposal."""
    from sqlspec.driver._tenant import AsyncTenantLease

    context: Any = None

    async def acquire() -> Any:
        nonlocal context
        pool = await config.provide_pool()
        context = pool.acquire()
        connection = await context.__aenter__()
        setup_error: BaseException | None = None
        try:
            await config._ensure_connection(connection)
        except BaseException as exc:
            setup_error = exc
        if setup_error is not None:
            cleanup = asyncio.create_task(invalidate(connection))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            try:
                cleanup.result()
            except BaseException as disposal_error:
                setup_error.add_note(f"Tenant acquire connection disposal failed: {type(disposal_error).__name__}")
            raise setup_error
        return connection

    async def release(_connection: Any) -> None:
        nonlocal context
        current, context = context, None
        if current is not None:
            await current.__aexit__(None, None, None)

    async def invalidate(connection: Any) -> None:
        async def terminate_and_confirm() -> bool:
            with suppress(Exception):
                await connection.execute("SELECT pg_terminate_backend(pg_backend_pid())")
            try:
                await connection.execute("SELECT 1")
            except Exception:
                return bool(connection.is_closed())
            return False

        try:
            closed = await asyncio.wait_for(terminate_and_confirm(), PSQLPY_DISPOSAL_TIMEOUT_S)
        except Exception:
            closed = False
        if not closed:
            await config._close_pool()
        with suppress(Exception):
            await release(connection)

    async def finish_transaction(connection: Any, _exc_type: type[BaseException] | None) -> None:
        await connection.execute("ROLLBACK")

    return AsyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish_transaction,
    )
