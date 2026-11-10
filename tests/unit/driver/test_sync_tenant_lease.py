from typing import Any

import pytest

from sqlspec.driver import SyncTenantLease
from sqlspec.exceptions import TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext


def make_sync_lease(
    calls: list[str], failures: dict[str, BaseException] | None = None
) -> tuple[SyncTenantLease, object]:
    from types import SimpleNamespace

    errors = failures or {}
    connection = object()

    def record(name: str) -> None:
        calls.append(name)
        if name in errors:
            raise errors[name]

    def acquire() -> object:
        record("acquire")
        return connection

    def checkout(conn: object, payload: Any) -> dict[str, str]:
        assert conn is connection
        record(f"checkout:{payload.tenant_id}")
        return {"original": "x"}

    def checkin(conn: object, state: Any) -> None:
        assert conn is connection
        assert state.baseline == {"original": "x"}
        record(f"checkin:{state.payload.tenant_id}")

    lease = SyncTenantLease(
        SimpleNamespace(checkout=checkout, checkin=checkin),
        acquire=acquire,
        release=lambda conn: record("release"),
        invalidate=lambda conn: record("invalidate"),
        finish_transaction=lambda conn, exc_type: record(f"finish:{None if exc_type is None else exc_type.__name__}"),
    )
    return lease, connection


def test_sync_tenant_missing_before_acquisition() -> None:
    calls: list[str] = []
    lease, _ = make_sync_lease(calls)
    token = TenantContext.set(None)
    try:
        with pytest.raises(TenantContextError):
            lease.acquire()
    finally:
        TenantContext.reset(token)
    assert calls == []


@pytest.mark.parametrize("bypass", [False, True])
def test_sync_tenant_captured_scope_and_double_release(bypass: bool) -> None:
    calls: list[str] = []
    lease, connection = make_sync_lease(calls)
    with TenantContext.bypass() if bypass else TenantContext.scope("a"):
        assert lease.acquire() is connection
    with TenantContext.scope("b"):
        lease.release(connection)
        lease.release(connection)
    assert calls == (
        ["acquire", "finish:None", "release"]
        if bypass
        else ["acquire", "checkout:a", "finish:None", "checkin:a", "release"]
    )


@pytest.mark.parametrize("failure", ["finish:None", "checkin:a", "release"])
@pytest.mark.parametrize("body", [False, True])
def test_sync_tenant_cleanup_failure(failure: str, body: bool) -> None:
    calls: list[str] = []
    cause = RuntimeError("cleanup")
    lease, _ = make_sync_lease(calls, {failure: cause})
    with TenantContext.scope("a"):
        lease.acquire()
    body_error = ValueError("body") if body else None
    if body:
        lease.release(exc_val=body_error)
        assert body_error is not None
        assert len(body_error.__notes__) == 1
    else:
        with pytest.raises(TenantCleanupError) as raised:
            lease.release()
        assert raised.value.__cause__ is cause
    expected = ["acquire", "checkout:a", "finish:None"]
    if failure != "finish:None":
        expected.append("checkin:a")
    if failure == "release":
        expected.append("release")
    assert calls == [*expected, "invalidate"]
    lease.release()
    assert calls == [*expected, "invalidate"]


@pytest.mark.parametrize("error", [RuntimeError("setup"), KeyboardInterrupt()])
def test_sync_tenant_checkout_failure_preserves_error(error: BaseException) -> None:
    calls: list[str] = []
    lease, _ = make_sync_lease(calls, {"checkout:a": error, "invalidate": RuntimeError("drop")})
    with TenantContext.scope("a"), pytest.raises(type(error)) as raised:
        lease.acquire()
    assert raised.value is error
    assert "invalidation" in error.__notes__[0]
    assert calls == ["acquire", "checkout:a", "invalidate"]


def test_sync_tenant_invalidation_failure_is_not_released() -> None:
    calls: list[str] = []
    lease, _ = make_sync_lease(calls, {"checkin:a": ValueError("reset"), "invalidate": RuntimeError("drop")})
    with TenantContext.scope("a"):
        lease.acquire()
    with pytest.raises(TenantCleanupError) as raised:
        lease.release()
    assert "invalidation" in raised.value.__notes__[0]
    assert calls[-2:] == ["checkin:a", "invalidate"]


@pytest.mark.parametrize("failure", ["acquire", "finish:None"])
def test_sync_tenant_bypass_failures(failure: str) -> None:
    calls: list[str] = []
    cause = RuntimeError("failure")
    lease, _ = make_sync_lease(calls, {failure: cause})
    with TenantContext.bypass():
        if failure == "acquire":
            with pytest.raises(RuntimeError) as raised:
                lease.acquire()
            assert raised.value is cause
            assert calls == ["acquire"]
        else:
            lease.acquire()
            with pytest.raises(TenantCleanupError):
                lease.release()
            assert calls == ["acquire", "finish:None", "invalidate"]
