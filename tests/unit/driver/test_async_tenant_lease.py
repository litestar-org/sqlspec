import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.driver._tenant import AsyncTenantLease
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.utils.tenant import TenantContext


def make_async_lease(
    calls: list[str],
    *,
    failures: dict[str, BaseException] | None = None,
    hooks: dict[str, Callable[[], Awaitable[None]]] | None = None,
    timeout: float = 1,
) -> tuple[AsyncTenantLease, object]:
    errors, actions = failures or {}, hooks or {}
    connection = object()

    async def record(name: str) -> None:
        calls.append(name)
        if name in actions:
            await actions[name]()
        if name in errors:
            raise errors[name]

    async def acquire() -> object:
        await record("acquire")
        return connection

    async def checkout(conn: object, payload: Any) -> dict[str, str]:
        assert conn is connection
        await record(f"checkout:{payload.tenant_id}")
        return {"original": "x"}

    async def checkin(conn: object, state: Any) -> None:
        assert conn is connection
        assert state.baseline == {"original": "x"}
        await record(f"checkin:{state.payload.tenant_id}")

    lease = AsyncTenantLease(
        SimpleNamespace(checkout=checkout, checkin=checkin),
        acquire=acquire,
        release=lambda conn: record("release"),
        invalidate=lambda conn: record("invalidate"),
        finish_transaction=lambda conn, exc_type: record(f"finish:{None if exc_type is None else exc_type.__name__}"),
        cleanup_timeout=timeout,
    )
    return lease, connection


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


@pytest.mark.parametrize("phase", ["checkout:a"])
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
