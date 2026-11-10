from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.core.tenant import TenantLeaseState
from sqlspec.driver import AsyncTenantLease, SyncTenantLease
from sqlspec.exceptions import TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.core.tenant import AsyncTenantStrategy, SyncTenantStrategy


def make_recording_lease(
    calls: list[str],
    *,
    asynchronous: bool,
    failures: dict[str, BaseException] | None = None,
    hooks: dict[str, Callable[[], Awaitable[None]]] | None = None,
    timeout: float = 1,
) -> tuple[SyncTenantLease | AsyncTenantLease, object]:
    errors, actions = failures or {}, hooks or {}
    connection = object()

    def record(name: str) -> None:
        calls.append(name)
        if name in errors:
            raise errors[name]

    async def async_record(name: str) -> None:
        calls.append(name)
        if name in actions:
            await actions[name]()
        if name in errors:
            raise errors[name]

    def baseline(conn: object) -> dict[str, str]:
        assert conn is connection
        return {"original": "x"}

    def validate_state(conn: object, state: TenantLeaseState) -> None:
        assert conn is connection
        assert state.baseline == {"original": "x"}

    def acquire() -> object:
        record("acquire")
        return connection

    def checkout(conn: object, payload: TenantContextPayload) -> dict[str, str]:
        record(f"checkout:{payload.tenant_id}")
        return baseline(conn)

    def checkin(conn: object, state: TenantLeaseState) -> None:
        validate_state(conn, state)
        record(f"checkin:{state.payload.tenant_id}")

    async def async_acquire() -> object:
        await async_record("acquire")
        return connection

    async def async_checkout(conn: object, payload: TenantContextPayload) -> dict[str, str]:
        await async_record(f"checkout:{payload.tenant_id}")
        return baseline(conn)

    async def async_checkin(conn: object, state: TenantLeaseState) -> None:
        validate_state(conn, state)
        await async_record(f"checkin:{state.payload.tenant_id}")

    def finish_name(exc_type: type[BaseException] | None) -> str:
        return f"finish:{None if exc_type is None else exc_type.__name__}"

    if asynchronous:
        return AsyncTenantLease(
            cast("AsyncTenantStrategy", SimpleNamespace(checkout=async_checkout, checkin=async_checkin)),
            acquire=async_acquire,
            release=lambda conn: async_record("release"),
            invalidate=lambda conn: async_record("invalidate"),
            finish_transaction=lambda conn, exc_type: async_record(finish_name(exc_type)),
            cleanup_timeout=timeout,
        ), connection
    return SyncTenantLease(
        cast("SyncTenantStrategy", SimpleNamespace(checkout=checkout, checkin=checkin)),
        acquire=acquire,
        release=lambda conn: record("release"),
        invalidate=lambda conn: record("invalidate"),
        finish_transaction=lambda conn, exc_type: record(finish_name(exc_type)),
    ), connection


def make_sync_lease(
    calls: list[str], failures: dict[str, BaseException] | None = None
) -> tuple[SyncTenantLease, object]:
    lease, connection = make_recording_lease(calls, asynchronous=False, failures=failures)
    return cast("SyncTenantLease", lease), connection


def make_async_lease(
    calls: list[str],
    *,
    failures: dict[str, BaseException] | None = None,
    hooks: dict[str, Callable[[], Awaitable[None]]] | None = None,
    timeout: float = 1,
) -> tuple[AsyncTenantLease, object]:
    lease, connection = make_recording_lease(calls, asynchronous=True, failures=failures, hooks=hooks, timeout=timeout)
    return cast("AsyncTenantLease", lease), connection


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_lease_double_acquire_keeps_existing_connection(asynchronous: bool) -> None:
    calls: list[str] = []
    lease, _ = make_recording_lease(calls, asynchronous=asynchronous)
    with TenantContext.scope("a"):
        if isinstance(lease, AsyncTenantLease):
            await lease.acquire()
            with pytest.raises(TenantContextError, match="already owns"):
                await lease.acquire()
            await lease.release(exc_type=ValueError)
        else:
            lease.acquire()
            with pytest.raises(TenantContextError, match="already owns"):
                lease.acquire()
            lease.release(exc_type=ValueError)
    assert calls == ["acquire", "checkout:a", "finish:ValueError", "checkin:a", "release"]


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_lease_normalizes_strategy_state_to_captured_payload(asynchronous: bool) -> None:
    connection = object()
    checked_in: list[TenantLeaseState] = []
    returned = TenantLeaseState(TenantContextPayload("other"), {"key": "original"})

    async def async_value(value: Any) -> Any:
        return value

    async def async_checkin(conn: object, state: TenantLeaseState) -> None:
        checked_in.append(state)

    if asynchronous:
        lease: SyncTenantLease | AsyncTenantLease = AsyncTenantLease(
            cast(
                "AsyncTenantStrategy",
                SimpleNamespace(checkout=lambda conn, payload: async_value(returned), checkin=async_checkin),
            ),
            acquire=lambda: async_value(connection),
            release=lambda conn: async_value(None),
            invalidate=lambda conn: async_value(None),
            finish_transaction=lambda conn, exc_type: async_value(None),
        )
    else:
        lease = SyncTenantLease(
            cast(
                "SyncTenantStrategy",
                SimpleNamespace(
                    checkout=lambda conn, payload: returned, checkin=lambda conn, state: checked_in.append(state)
                ),
            ),
            acquire=lambda: connection,
            release=lambda conn: None,
            invalidate=lambda conn: None,
            finish_transaction=lambda conn, exc_type: None,
        )
    with TenantContext.scope("captured"):
        if isinstance(lease, AsyncTenantLease):
            await lease.acquire()
            await lease.release()
        else:
            lease.acquire()
            lease.release()
    assert [(state.payload.tenant_id, dict(state.baseline)) for state in checked_in] == [
        ("captured", {"key": "original"})
    ]
