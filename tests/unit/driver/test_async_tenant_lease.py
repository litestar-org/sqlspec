import asyncio

import pytest

from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext
from tests.unit.driver.test_tenant_lifecycle import make_async_lease


async def test_async_missing_context_before_pool() -> None:
    calls: list[str] = []
    lease, _ = make_async_lease(calls)
    token = TenantContext.set(None)
    try:
        with pytest.raises(TenantContextError):
            await lease.acquire()
    finally:
        TenantContext.reset(token)
    assert calls == []


@pytest.mark.parametrize(("failure", "bypass"), [("acquire", False), ("acquire", True), ("checkout:a", False)])
async def test_async_acquisition_failure(failure: str, bypass: bool) -> None:
    calls: list[str] = []
    cause = ValueError("setup")
    lease, _ = make_async_lease(calls, failures={failure: cause, "invalidate": RuntimeError("drop")})
    with TenantContext.bypass() if bypass else TenantContext.scope("a"), pytest.raises(ValueError) as raised:
        await lease.acquire()
    assert raised.value is cause
    assert calls == (["acquire"] if failure == "acquire" else ["acquire", "checkout:a", "invalidate"])
    if failure != "acquire":
        assert "invalidation" in cause.__notes__[0]


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_async_invalid_cleanup_timeout(timeout: float) -> None:
    with pytest.raises(ImproperConfigurationError):
        make_async_lease([], timeout=timeout)


@pytest.mark.parametrize("phase", ["checkout:a", "checkin:a", "release"])
async def test_async_repeated_cancellation_waits_for_owned_cleanup(phase: str) -> None:
    calls: list[str] = []
    entered, complete, disposal_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    tasks_before = asyncio.all_tasks()

    async def block() -> None:
        entered.set()
        await complete.wait()

    async def dispose() -> None:
        disposal_entered.set()
        await complete.wait()

    lease, _ = make_async_lease(calls, hooks={phase: block, "invalidate": dispose})
    with TenantContext.scope("a"):
        if phase != "checkout:a":
            await lease.acquire()
        owner = asyncio.create_task(lease.acquire() if phase == "checkout:a" else lease.release())
    await entered.wait()
    owner.cancel()
    if phase == "checkout:a":
        await disposal_entered.wait()
    await asyncio.sleep(0)
    owner.cancel()
    complete.set()
    with pytest.raises(asyncio.CancelledError):
        await owner
    assert calls[-1] == ("invalidate" if phase == "checkout:a" else "release")
    assert asyncio.all_tasks() == tasks_before


@pytest.mark.parametrize("bypass", [False, True])
async def test_async_acquire_captures_payload_or_bypasses(bypass: bool) -> None:
    calls: list[str] = []
    lease, connection = make_async_lease(calls)
    with TenantContext.bypass() if bypass else TenantContext.scope("a"):
        assert await lease.acquire() is connection
    assert calls == (["acquire"] if bypass else ["acquire", "checkout:a"])


@pytest.mark.parametrize("bypass", [False, True])
async def test_async_captured_scope_and_double_release(bypass: bool) -> None:
    calls: list[str] = []
    lease, connection = make_async_lease(calls)
    with TenantContext.bypass() if bypass else TenantContext.scope("a"):
        assert await lease.acquire() is connection
    with TenantContext.scope("b"):
        await lease.release(connection)
        await lease.release(connection)
    assert calls == (
        ["acquire", "finish:None", "release"]
        if bypass
        else ["acquire", "checkout:a", "finish:None", "checkin:a", "release"]
    )


@pytest.mark.parametrize("failure", ["finish:None", "checkin:a", "release"])
@pytest.mark.parametrize("body", [False, True])
async def test_async_cleanup_failure(failure: str, body: bool) -> None:
    calls: list[str] = []
    cause = RuntimeError("cleanup")
    lease, _ = make_async_lease(calls, failures={failure: cause})
    with TenantContext.scope("a"):
        await lease.acquire()
    body_error = ValueError("body") if body else None
    if body_error is not None:
        await lease.release(exc_val=body_error)
        assert len(body_error.__notes__) == 1
    else:
        with pytest.raises(TenantCleanupError) as raised:
            await lease.release()
        assert raised.value.__cause__ is cause
    expected = ["acquire", "checkout:a", "finish:None"]
    if failure != "finish:None":
        expected.append("checkin:a")
    if failure == "release":
        expected.append("release")
    assert calls == [*expected, "invalidate"]


@pytest.mark.parametrize("cancel", [False, True])
async def test_async_deadline_disposes_without_stray_task(cancel: bool) -> None:
    calls: list[str] = []
    entered = asyncio.Event()
    tasks_before = asyncio.all_tasks()

    async def block() -> None:
        entered.set()
        await asyncio.Event().wait()

    lease, _ = make_async_lease(calls, hooks={"checkin:a": block}, timeout=0.01)
    with TenantContext.scope("a"):
        await lease.acquire()
        owner = asyncio.create_task(lease.release())
    await entered.wait()
    if cancel:
        owner.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else TenantCleanupError) as raised:
        await owner
    if not cancel:
        assert isinstance(raised.value.__cause__, TimeoutError)
    assert calls == ["acquire", "checkout:a", "finish:None", "checkin:a", "invalidate"]
    assert asyncio.all_tasks() == tasks_before


async def test_async_bypass_cancellation_finishes_native_release() -> None:
    calls: list[str] = []
    entered, complete = asyncio.Event(), asyncio.Event()

    async def block() -> None:
        entered.set()
        await complete.wait()

    lease, _ = make_async_lease(calls, hooks={"release": block})
    with TenantContext.bypass():
        await lease.acquire()
        owner = asyncio.create_task(lease.release())
    await entered.wait()
    owner.cancel()
    complete.set()
    with pytest.raises(asyncio.CancelledError):
        await owner
    assert calls == ["acquire", "finish:None", "release"]


@pytest.mark.parametrize("phase", ["finish:None", "checkin:a"])
async def test_async_deadline_cannot_release_after_callback_swallows_cancellation(phase: str) -> None:
    calls: list[str] = []

    async def swallow_cancel() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    lease, _ = make_async_lease(calls, hooks={phase: swallow_cancel}, timeout=0.01)
    with TenantContext.scope("a"):
        await lease.acquire()
    with pytest.raises(TenantCleanupError) as raised:
        await lease.release()
    assert isinstance(raised.value.__cause__, TimeoutError)
    assert calls == (
        ["acquire", "checkout:a", "finish:None", "invalidate"]
        if phase == "finish:None"
        else ["acquire", "checkout:a", "finish:None", "checkin:a", "invalidate"]
    )


async def test_async_cleanup_and_invalidation_failures_preserve_cause() -> None:
    calls: list[str] = []
    cause = ValueError("reset")
    lease, _ = make_async_lease(calls, failures={"checkin:a": cause, "invalidate": RuntimeError("drop")})
    with TenantContext.scope("a"):
        await lease.acquire()
    with pytest.raises(TenantCleanupError) as raised:
        await lease.release()
    assert raised.value.__cause__ is cause
    assert "invalidation" in raised.value.__notes__[0]
    assert calls[-1] == "invalidate"


async def test_async_bypass_finish_failure_disposes_once() -> None:
    calls: list[str] = []
    lease, _ = make_async_lease(calls, failures={"finish:None": RuntimeError("finish")})
    with TenantContext.bypass():
        await lease.acquire()
    with pytest.raises(TenantCleanupError):
        await lease.release()
    assert calls == ["acquire", "finish:None", "invalidate"]


async def test_owned_release_failure_takes_precedence_over_caller_cancellation() -> None:
    from sqlspec.driver._tenant import await_owned_release

    started = asyncio.Event()
    proceed = asyncio.Event()
    failure = RuntimeError("release failed")

    async def operation() -> None:
        started.set()
        await proceed.wait()
        raise failure

    caller = asyncio.create_task(await_owned_release(operation()))
    await started.wait()
    caller.cancel()
    await asyncio.sleep(0)
    assert not caller.done()
    proceed.set()
    with pytest.raises(RuntimeError) as caught:
        await caller
    assert caught.value is failure


async def test_owned_release_reraises_cancellation_only_after_the_work_finishes() -> None:
    from sqlspec.driver._tenant import await_owned_release

    started = asyncio.Event()
    proceed = asyncio.Event()
    finished: list[bool] = []

    async def operation() -> None:
        started.set()
        await proceed.wait()
        finished.append(True)

    caller = asyncio.create_task(await_owned_release(operation()))
    await started.wait()
    for _ in range(3):
        caller.cancel()
        await asyncio.sleep(0)
    assert not caller.done()
    proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert finished == [True]


async def test_owned_disposal_returns_failure_and_finishes_despite_cancellation() -> None:
    from sqlspec.driver._tenant import await_owned_disposal

    started = asyncio.Event()
    proceed = asyncio.Event()
    failure = RuntimeError("disposal failed")

    async def dispose() -> None:
        started.set()
        await proceed.wait()
        raise failure

    caller = asyncio.create_task(await_owned_disposal(dispose()))
    await started.wait()
    caller.cancel()
    await asyncio.sleep(0)
    proceed.set()
    assert await caller is failure


async def test_owned_wait_deadline_reports_timeout_without_cancelling_the_work() -> None:
    from sqlspec.driver._tenant import _await_owned, _start_owned

    proceed = asyncio.Event()
    finished: list[bool] = []

    async def slow() -> None:
        await proceed.wait()
        finished.append(True)

    owned = _start_owned(slow())
    error, cancellation = await _await_owned(owned, 0.01)
    assert isinstance(error, TimeoutError)
    assert cancellation is None
    assert finished == []
    proceed.set()
    assert await _await_owned(owned) == (None, None)
    assert finished == [True]
