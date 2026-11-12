import pytest

from sqlspec.exceptions import TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext
from tests.unit.driver.test_tenant_lifecycle import make_sync_lease


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
