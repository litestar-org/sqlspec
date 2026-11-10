"""Fail-closed tenant leases around adapter-owned connection callbacks."""

from collections.abc import Callable, Mapping
from typing import Any

from sqlspec.core.tenant.base import (
    AsyncTenantStrategy,
    SyncTenantStrategy,
    TenantLeaseState,
    TenantStrategy,
    TenantStrategyShorthand,
    require_session_pooler,
)
from sqlspec.exceptions import TenantCleanupError, TenantContextError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

__all__ = (
    "AsyncTenantStrategy",
    "SyncTenantLease",
    "SyncTenantStrategy",
    "TenantLeaseState",
    "TenantStrategy",
    "TenantStrategyShorthand",
    "require_session_pooler",
)

logger = get_logger("sqlspec.driver")


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
        raise error from cause


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
