"""Fail-closed tenant leases around adapter-owned connection callbacks."""

import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final

from sqlspec.core.tenant.base import (
    AsyncTenantStrategy,
    SyncTenantStrategy,
    TenantLeaseState,
    TenantStrategy,
    TenantStrategyShorthand,
    require_session_pooler,
)
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

__all__ = (
    "AsyncTenantLease",
    "AsyncTenantStrategy",
    "SyncTenantLease",
    "SyncTenantStrategy",
    "TenantLeaseState",
    "TenantStrategy",
    "TenantStrategyShorthand",
    "require_session_pooler",
)

logger = get_logger("sqlspec.driver")
DEFAULT_TENANT_CLEANUP_TIMEOUT: Final = 5.0


def _lease_state(payload: TenantContextPayload, state: TenantLeaseState | Mapping[str, str | None]) -> TenantLeaseState:
    return TenantLeaseState(payload, state.baseline if isinstance(state, TenantLeaseState) else state)


def _report_cleanup_failure(
    cause: BaseException, body_error: BaseException | None, invalidation_error: BaseException | None
) -> None:
    error = body_error if body_error is not None else TenantCleanupError("Tenant lease cleanup failed")
    if body_error is not None:
        error.add_note(f"Tenant lease cleanup failed: {cause!r}")
        logger.warning("Tenant lease cleanup failed while handling an existing exception")
    if invalidation_error is not None:
        error.add_note(f"Tenant connection invalidation failed: {invalidation_error!r}")
    if body_error is None:
        error.__cause__ = cause
        raise error


class SyncTenantLease:
    """Capture tenant identity and dispose of connections whose cleanup fails."""

    __slots__ = ("_acquire", "_connection", "_finish_transaction", "_invalidate", "_release", "_state", "_strategy")

    def __init__(
        self,
        strategy: SyncTenantStrategy,
        *,
        acquire: Callable[[], Any],
        release: Callable[[Any], None],
        invalidate: Callable[[Any], None],
        finish_transaction: Callable[[Any, type[BaseException] | None], None],
    ) -> None:
        self._strategy = strategy
        self._acquire = acquire
        self._release = release
        self._invalidate = invalidate
        self._finish_transaction = finish_transaction
        self._connection: Any = None
        self._state: TenantLeaseState | None = None

    def acquire(self) -> Any:
        """Acquire and initialize one connection using the caller's tenant snapshot."""
        if self._state is not None:
            msg = "Tenant lease already owns a connection"
            raise TenantContextError(msg)
        payload = TenantContext.get()
        if payload is None:
            msg = "A tenant scope or explicit bypass is required before acquiring a connection"
            raise TenantContextError(msg)
        connection = self._acquire()
        try:
            state = (
                TenantLeaseState(payload)
                if payload.is_bypass
                else _lease_state(payload, self._strategy.checkout(connection, payload))
            )
        except BaseException as exc:
            try:
                self._invalidate(connection)
            except BaseException as invalidation_error:
                exc.add_note(f"Tenant connection invalidation failed: {invalidation_error!r}")
            raise
        self._connection, self._state = connection, state
        return connection

    def release(self, _conn: Any = None, **kwargs: Any) -> None:
        """Finish the transaction, restore tenant state, and release or invalidate."""
        connection, state = self._connection, self._state
        self._connection, self._state = None, None
        if state is None:
            return
        cause: BaseException | None = None
        try:
            self._finish_transaction(connection, kwargs.get("exc_type"))
            if not state.payload.is_bypass:
                self._strategy.checkin(connection, state)
            self._release(connection)
        except BaseException as exc:
            cause = exc
        if cause is not None:
            invalidation_error: BaseException | None = None
            try:
                self._invalidate(connection)
            except BaseException as exc:
                invalidation_error = exc
            _report_cleanup_failure(cause, kwargs.get("exc_val"), invalidation_error)


async def _await_owned(
    task: asyncio.Future[None], timeout: float | None = None
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    cancellation: asyncio.CancelledError | None = None
    deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
    while not task.done():
        try:
            remaining = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
            await asyncio.wait_for(asyncio.shield(task), remaining)
        except asyncio.CancelledError as exc:
            if not task.cancelled():
                cancellation = exc
        except BaseException as exc:
            return exc, cancellation
    try:
        task.result()
    except BaseException as exc:
        return exc, cancellation
    return None, cancellation


class AsyncTenantLease:
    """Capture tenant identity and shield adapter-owned disposal from cancellation."""

    __slots__ = (
        "_acquire",
        "_cleanup_timeout",
        "_connection",
        "_finish_transaction",
        "_invalidate",
        "_release",
        "_state",
        "_strategy",
    )

    def __init__(
        self,
        strategy: AsyncTenantStrategy,
        *,
        acquire: Callable[[], Awaitable[Any]],
        release: Callable[[Any], Awaitable[None]],
        invalidate: Callable[[Any], Awaitable[None]],
        finish_transaction: Callable[[Any, type[BaseException] | None], Awaitable[None]],
        cleanup_timeout: float = DEFAULT_TENANT_CLEANUP_TIMEOUT,
    ) -> None:
        if not math.isfinite(cleanup_timeout) or cleanup_timeout <= 0:
            msg = "Tenant cleanup timeout must be finite and positive"
            raise ImproperConfigurationError(msg)
        self._strategy = strategy
        self._acquire = acquire
        self._release = release
        self._invalidate = invalidate
        self._finish_transaction = finish_transaction
        self._cleanup_timeout = cleanup_timeout
        self._connection: Any = None
        self._state: TenantLeaseState | None = None

    async def acquire(self) -> Any:
        """Acquire a connection and apply a captured tenant or explicit bypass."""
        if self._state is not None:
            msg = "Tenant lease already owns a connection"
            raise TenantContextError(msg)
        payload = TenantContext.get()
        if payload is None:
            msg = "A tenant scope or explicit bypass is required before acquiring a connection"
            raise TenantContextError(msg)
        connection = await self._acquire()
        try:
            state = (
                TenantLeaseState(payload)
                if payload.is_bypass
                else _lease_state(payload, await self._strategy.checkout(connection, payload))
            )
        except BaseException as exc:
            checkout_error = exc
        else:
            self._connection, self._state = connection, state
            return connection
        disposal = asyncio.create_task(self._dispose(connection))
        invalidation_error, _ = await _await_owned(disposal)
        if invalidation_error is not None:
            checkout_error.add_note(f"Tenant connection invalidation failed: {invalidation_error!r}")
        raise checkout_error

    async def _dispose(self, connection: Any) -> None:
        await self._invalidate(connection)

    async def _cleanup(self, connection: Any, state: TenantLeaseState, exc_type: type[BaseException] | None) -> None:
        await self._finish_transaction(connection, exc_type)
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        if not state.payload.is_bypass:
            await self._strategy.checkin(connection, state)
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        await self._release(connection)

    async def release(self, _conn: Any = None, **kwargs: Any) -> None:
        """Complete bounded cleanup, waiting for owned tasks even after cancellation."""
        connection, state = self._connection, self._state
        self._connection, self._state = None, None
        if state is None:
            return
        cleanup = asyncio.create_task(self._cleanup(connection, state, kwargs.get("exc_type")))
        cause, cancellation = await _await_owned(cleanup, self._cleanup_timeout)
        if not cleanup.done():
            cleanup.cancel()
            _, pending = await _await_owned(cleanup)
            cancellation = cancellation or pending
        if cause is not None:
            disposal = asyncio.create_task(self._dispose(connection))
            invalidation_error, pending = await _await_owned(disposal)
            cancellation = cancellation or pending
            _report_cleanup_failure(cause, cancellation or kwargs.get("exc_val"), invalidation_error)
        if cancellation is not None:
            raise cancellation
