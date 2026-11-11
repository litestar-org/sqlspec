"""Fail-closed tenant leases around adapter-owned connection hooks."""

import asyncio
import math
from collections.abc import Awaitable, Mapping
from typing import Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import AsyncTenantStrategy, SyncTenantStrategy, TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.tenant._context import TenantContext, TenantContextPayload

__all__ = ("DEFAULT_TENANT_CLEANUP_TIMEOUT", "AsyncTenantLease", "SyncTenantLease", "await_owned_release")

logger = get_logger("sqlspec.driver")
DEFAULT_TENANT_CLEANUP_TIMEOUT: Final = 5.0


def _lease_state(payload: TenantContextPayload, state: TenantLeaseState | Mapping[str, str | None]) -> TenantLeaseState:
    if isinstance(state, TenantLeaseState):
        return state if state.payload is payload else TenantLeaseState(payload, state.baseline)
    return TenantLeaseState(payload, state)


def _require_payload() -> TenantContextPayload:
    payload = TenantContext.get()
    if payload is None:
        msg = "A tenant scope or explicit bypass is required before acquiring a connection"
        raise TenantContextError(msg)
    return payload


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


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncTenantLease:
    """Capture tenant identity and dispose of connections whose cleanup fails.

    Adapters subclass the lease and implement the four connection hooks.
    """

    __slots__ = ("_connection", "_state", "_strategy")

    def __init__(self, strategy: SyncTenantStrategy) -> None:
        self._strategy = strategy
        self._connection: Any = None
        self._state: TenantLeaseState | None = None

    def _acquire(self) -> Any:
        """Return one connection from the adapter pool."""
        raise NotImplementedError

    def _release(self, connection: Any) -> None:
        """Return a verified connection to the adapter pool."""
        raise NotImplementedError

    def _invalidate(self, connection: Any) -> None:
        """Physically dispose of a connection that cannot be reused."""
        raise NotImplementedError

    def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        """Finish application work before tenant state is restored."""
        raise NotImplementedError

    def acquire(self) -> Any:
        """Acquire and initialize one connection using the caller's tenant snapshot."""
        if self._state is not None:
            msg = "Tenant lease already owns a connection"
            raise TenantContextError(msg)
        payload = _require_payload()
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
    task: "asyncio.Future[Any]", timeout: float | None = None
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


async def await_owned_release(operation: Awaitable[Any]) -> None:
    """Join adapter-owned release work before reporting its error or the caller's cancellation.

    Cancellation of the caller never abandons the operation. Its own failure
    takes precedence over a cancellation observed while waiting.
    """
    error, cancellation = await _await_owned(asyncio.ensure_future(operation))
    if error is not None:
        raise error
    if cancellation is not None:
        raise cancellation


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncTenantLease:
    """Capture tenant identity and shield adapter-owned disposal from cancellation.

    Adapters subclass the lease and implement the four connection hooks.
    """

    __slots__ = ("_cleanup_timeout", "_connection", "_state", "_strategy")

    def __init__(
        self, strategy: AsyncTenantStrategy, *, cleanup_timeout: float = DEFAULT_TENANT_CLEANUP_TIMEOUT
    ) -> None:
        if not math.isfinite(cleanup_timeout) or cleanup_timeout <= 0:
            msg = "Tenant cleanup timeout must be finite and positive"
            raise ImproperConfigurationError(msg)
        self._strategy = strategy
        self._cleanup_timeout = cleanup_timeout
        self._connection: Any = None
        self._state: TenantLeaseState | None = None

    async def _acquire(self) -> Any:
        """Return one connection from the adapter pool."""
        raise NotImplementedError

    async def _release(self, connection: Any) -> None:
        """Return a verified connection to the adapter pool."""
        raise NotImplementedError

    async def _invalidate(self, connection: Any) -> None:
        """Physically dispose of a connection that cannot be reused."""
        raise NotImplementedError

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        """Finish application work before tenant state is restored."""
        raise NotImplementedError

    async def acquire(self) -> Any:
        """Acquire a connection and apply a captured tenant or explicit bypass."""
        if self._state is not None:
            msg = "Tenant lease already owns a connection"
            raise TenantContextError(msg)
        payload = _require_payload()
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
        disposal = asyncio.create_task(self._invalidate(connection))
        invalidation_error, _ = await _await_owned(disposal)
        if invalidation_error is not None:
            checkout_error.add_note(f"Tenant connection invalidation failed: {invalidation_error!r}")
        raise checkout_error

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
            disposal = asyncio.create_task(self._invalidate(connection))
            invalidation_error, pending = await _await_owned(disposal)
            cancellation = cancellation or pending
            _report_cleanup_failure(cause, cancellation or kwargs.get("exc_val"), invalidation_error)
        if cancellation is not None:
            raise cancellation
