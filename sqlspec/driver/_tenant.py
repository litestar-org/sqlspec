"""Fail-closed tenant leases around adapter-owned connection hooks."""

import asyncio
import math
import sys
from collections.abc import Awaitable, Mapping
from typing import Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import AsyncTenantStrategy, SyncTenantStrategy, TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.tenant._context import TenantContext, TenantContextPayload

__all__ = (
    "DEFAULT_TENANT_CLEANUP_TIMEOUT",
    "AsyncTenantLease",
    "SyncTenantLease",
    "await_owned_disposal",
    "await_owned_release",
)

logger = get_logger("sqlspec.driver")
DEFAULT_TENANT_CLEANUP_TIMEOUT: Final = 5.0
_EAGER_TASKS: Final = sys.version_info >= (3, 12)


def _lease_state(payload: TenantContextPayload, state: TenantLeaseState | Mapping[str, str | None]) -> TenantLeaseState:
    if isinstance(state, TenantLeaseState):
        return state if state.payload is payload else TenantLeaseState(payload, state.baseline)
    return TenantLeaseState(payload, state)


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

    __slots__ = ("_connection", "_state", "_strategy", "tenant_key")

    def __init__(self, strategy: SyncTenantStrategy) -> None:
        self._strategy = strategy
        self._connection: Any = None
        self._state: TenantLeaseState | None = None
        self.tenant_key: Any = None

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
        payload = TenantContext.require(self.tenant_key)
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


class _Owned:
    """Owned work and the waiter its caller awaits instead of the work itself.

    Caller cancellation cancels only the waiter, never the work. The work resolves the waiter
    as it finishes, so the caller resumes one event-loop iteration later, as with a plain
    ``await``.
    """

    __slots__ = ("task", "timed_out", "waiter")

    def __init__(self) -> None:
        self.task: asyncio.Future[Any] | None = None
        self.waiter: asyncio.Future[None] | None = None
        self.timed_out = False

    def wake(self, _task: object = None) -> None:
        waiter = self.waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def expire(self) -> None:
        self.timed_out = True
        self.wake()


async def _run_owned(operation: Awaitable[Any], owned: _Owned) -> Any:
    try:
        return await operation
    finally:
        owned.wake()


def _start_owned(operation: Awaitable[Any]) -> _Owned:
    """Start owned work, eagerly where the runtime supports it."""
    owned = _Owned()
    if isinstance(operation, asyncio.Future):
        owned.task = operation
        operation.add_done_callback(owned.wake)
        return owned
    loop = asyncio.get_running_loop()
    runner = _run_owned(operation, owned)
    if _EAGER_TASKS:
        owned.task = cast("asyncio.Future[Any]", cast("Any", asyncio.Task)(runner, loop=loop, eager_start=True))
    else:
        owned.task = loop.create_task(runner)
    return owned


def _owned_outcome(task: "asyncio.Future[Any]") -> BaseException | None:
    try:
        task.result()
    except BaseException as exc:
        return exc
    return None


async def _await_owned(
    owned: _Owned, timeout: float | None = None
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Wait for owned work without letting caller cancellation abandon or interrupt it.

    Caller cancellation is recorded and the wait resumes on a fresh waiter. One timer handle
    bounds the wait without cancelling the work.

    Returns:
        The work's failure (``TimeoutError`` when the deadline passed first) and any observed
        caller cancellation.
    """
    task = cast("asyncio.Future[Any]", owned.task)
    if task.done():
        return _owned_outcome(task), None
    loop = asyncio.get_running_loop()
    cancellation: asyncio.CancelledError | None = None
    owned.timed_out = False
    owned.waiter = loop.create_future()
    handle = None if timeout is None else loop.call_later(timeout, owned.expire)
    try:
        while not task.done() and not owned.timed_out:
            try:
                await owned.waiter
            except asyncio.CancelledError as exc:
                cancellation = exc
                if owned.waiter.cancelled():
                    owned.waiter = loop.create_future()
    finally:
        owned.waiter = None
        if handle is not None:
            handle.cancel()
    if not task.done():
        return TimeoutError(), cancellation
    return _owned_outcome(task), cancellation


async def await_owned_release(operation: Awaitable[Any]) -> None:
    """Join adapter-owned release work before reporting its error or the caller's cancellation.

    Cancellation of the caller never abandons the operation. Its own failure
    takes precedence over a cancellation observed while waiting.
    """
    error, cancellation = await _await_owned(_start_owned(operation))
    if error is not None:
        raise error
    if cancellation is not None:
        raise cancellation


async def await_owned_disposal(operation: Awaitable[Any]) -> BaseException | None:
    """Join adapter-owned disposal on a failure path and return its error.

    Cancellation of the caller never abandons the disposal. The caller raises
    its own pending failure afterwards, so an observed cancellation is not
    re-raised.

    Returns:
        The disposal failure, or None when disposal completed.
    """
    error, _ = await _await_owned(_start_owned(operation))
    return error


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncTenantLease:
    """Capture tenant identity and shield adapter-owned disposal from cancellation.

    Adapters subclass the lease and implement the four connection hooks.
    """

    __slots__ = ("_cleanup_timeout", "_connection", "_state", "_strategy", "tenant_key")

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
        self.tenant_key: Any = None

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
        payload = await TenantContext.arequire(self.tenant_key)
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
        invalidation_error = await await_owned_disposal(self._invalidate(connection))
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
        cleanup = _start_owned(self._cleanup(connection, state, kwargs.get("exc_type")))
        cause, cancellation = await _await_owned(cleanup, self._cleanup_timeout)
        task = cast("asyncio.Future[Any]", cleanup.task)
        if not task.done():
            task.cancel()
            _, pending = await _await_owned(cleanup)
            cancellation = cancellation or pending
        if cause is not None:
            disposal = _start_owned(self._invalidate(connection))
            invalidation_error, pending = await _await_owned(disposal)
            cancellation = cancellation or pending
            _report_cleanup_failure(cause, cancellation or kwargs.get("exc_val"), invalidation_error)
        if cancellation is not None:
            raise cancellation
