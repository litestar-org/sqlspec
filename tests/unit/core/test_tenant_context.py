"""Tenant identity validation and context lifetime contracts."""

import asyncio
from collections.abc import Mapping, MutableMapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import FrozenInstanceError
from threading import Thread
from typing import cast
from uuid import UUID

import pytest

from sqlspec.exceptions import SQLSpecError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload, async_tenant_bypass, async_tenant_scope


def test_payload_snapshots_variables() -> None:
    variables = {"region": "west"}
    payload = TenantContextPayload("tenant-a", variables)
    variables["region"] = "east"
    assert payload.tenant_id == "tenant-a"
    assert payload.variables == {"region": "west"}


@pytest.mark.parametrize("tenant_id", ["", " \t\n", "a\x00b", "x" * 257, "é" * 129, True, 123, None, b"tenant"])
def test_payload_rejects_invalid_identity(tenant_id: object) -> None:
    with pytest.raises(TenantContextError):
        TenantContextPayload(cast("str", tenant_id))


@pytest.mark.parametrize("tenant_id", ["x" * 256, "é" * 128, UUID("fd46b755-cdef-43d8-a0a1-32b97ee847fa")])
def test_payload_preserves_valid_identity(tenant_id: str | UUID) -> None:
    assert TenantContextPayload(tenant_id).tenant_id == str(tenant_id)


@pytest.mark.parametrize("variables", [{1: "value"}, {"key": 1}, {"a\x00b": "value"}, {"key": "a\x00b"}])
def test_payload_rejects_invalid_variables(variables: dict[object, object]) -> None:
    with pytest.raises(TenantContextError):
        TenantContextPayload("tenant", cast("Mapping[str, str]", variables))


def test_payload_is_immutable_and_hashable() -> None:
    payload = TenantContextPayload("tenant", {"region": "west"})
    with pytest.raises(TypeError):
        cast("MutableMapping[str, str]", payload.variables)["region"] = "east"
    with pytest.raises(FrozenInstanceError):
        setattr(payload, "tenant_id", "other")
    assert hash(payload) == hash(TenantContextPayload("tenant", {"region": "east"}))


@pytest.mark.parametrize("tenant_id,variables", [("tenant", {}), ("", {"region": "west"}), (True, {})])
def test_bypass_rejects_identity_and_variables(tenant_id: object, variables: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        TenantContextPayload(cast("str", tenant_id), variables, is_bypass=True)


def test_tenant_exception_hierarchy() -> None:
    error = TenantContextError("Invalid tenant")
    assert isinstance(error, SQLSpecError)
    assert isinstance(error, ValueError)
    assert str(error) == "Invalid tenant"
    assert isinstance(TenantCleanupError("Reset failed"), SQLSpecError)


def test_nested_scopes_and_explicit_clear_restore() -> None:
    assert TenantContext.get() is None
    with TenantContext.scope("outer") as outer:
        assert TenantContext.get() is outer
        with TenantContext.scope("inner"):
            assert TenantContext.get_tenant_id() == "inner"
            token = TenantContext.set(None)
            assert TenantContext.get() is None
            TenantContext.reset(token)
            assert TenantContext.get_tenant_id() == "inner"
        assert TenantContext.get() is outer
    assert TenantContext.get() is None


@pytest.mark.parametrize("bypass", [False, True])
def test_sync_exception_restores_context(bypass: bool) -> None:
    error = RuntimeError("body failed")
    with TenantContext.scope("outer") as outer:
        with pytest.raises(RuntimeError) as exc_info:
            with TenantContext.bypass() if bypass else TenantContext.scope("inner"):
                raise error
        assert exc_info.value is error
        assert TenantContext.get() is outer
    assert TenantContext.get() is None


def test_scope_accepts_uuid_payload_and_variable_overrides() -> None:
    tenant_id = UUID("fd46b755-cdef-43d8-a0a1-32b97ee847fa")
    with TenantContext.scope(tenant_id) as payload:
        assert payload.tenant_id == str(tenant_id)
    original = TenantContextPayload("tenant", {"region": "west", "role": "reader"})
    with TenantContext.scope(original) as unchanged:
        assert unchanged is original
    with TenantContext.scope(original, variables={"role": "writer"}) as merged:
        assert merged.variables == {"region": "west", "role": "writer"}
    assert original.variables == {"region": "west", "role": "reader"}


def test_bypass_scopes_and_payload() -> None:
    assert TenantContext.is_bypassed() is False
    with TenantContext.scope("outer") as outer:
        with TenantContext.bypass() as bypass:
            assert bypass == TenantContextPayload.bypass()
            assert bypass.tenant_id == ""
            assert bypass.variables == {}
            assert bypass.is_bypass is True
            assert TenantContext.is_bypassed() is True
            assert TenantContext.get_tenant_id() is None
            with TenantContext.scope(bypass, variables={}):
                assert TenantContext.is_bypassed() is True
            with pytest.raises(TenantContextError):
                with TenantContext.scope(bypass, variables={"role": "admin"}):
                    pytest.fail("Invalid bypass scope opened")
        assert TenantContext.get() is outer
        assert TenantContext.is_bypassed() is False


@pytest.mark.parametrize("invalid", ["tenant", True, 123, {}])
def test_set_rejects_invalid_payload_without_mutating_context(invalid: object) -> None:
    with TenantContext.scope("outer") as outer:
        with pytest.raises(TenantContextError):
            TenantContext.set(cast("TenantContextPayload", invalid))
        assert TenantContext.get() is outer


@pytest.mark.parametrize("invalid", ["", "x" * 257, True])
def test_invalid_scope_keeps_outer_context(invalid: object) -> None:
    with TenantContext.scope("outer") as outer:
        with pytest.raises(TenantContextError):
            with TenantContext.scope(cast("str", invalid)):
                pytest.fail("Invalid scope opened")
        assert TenantContext.get() is outer


def test_reset_rejects_token_from_another_context() -> None:
    token = copy_context().run(TenantContext.set, TenantContextPayload("other"))
    with pytest.raises(ValueError):
        TenantContext.reset(token)
    assert TenantContext.get() is None


def test_thread_propagation_requires_copied_context() -> None:
    observed: list[str | None] = []
    with TenantContext.scope("parent"):
        thread = Thread(target=lambda: observed.append(TenantContext.get_tenant_id()))
        thread.start()
        thread.join()
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(TenantContext.get_tenant_id).result() is None
            assert executor.submit(copy_context().run, TenantContext.get_tenant_id).result() == "parent"
            assert executor.submit(TenantContext.get_tenant_id).result() is None
        assert TenantContext.get_tenant_id() == "parent"
    assert observed == [None]


@pytest.mark.anyio
async def test_async_nested_scopes_restore_after_return() -> None:
    tenant_id = UUID("fd46b755-cdef-43d8-a0a1-32b97ee847fa")
    async with async_tenant_scope(tenant_id) as outer:
        assert TenantContext.get_tenant_id() == str(tenant_id)
        async with async_tenant_bypass() as bypass:
            assert bypass == TenantContextPayload.bypass()
            assert TenantContext.is_bypassed() is True
            assert TenantContext.get_tenant_id() is None
            async with async_tenant_scope("inner"):
                assert TenantContext.get_tenant_id() == "inner"
            assert TenantContext.get() is bypass
        assert TenantContext.get() is outer
    assert TenantContext.get() is None


@pytest.mark.anyio
async def test_async_tasks_are_isolated_and_to_thread_propagates() -> None:
    async def worker(tenant_id: str) -> tuple[str | None, str | None]:
        async with async_tenant_scope(tenant_id):
            await asyncio.sleep(0)
            return TenantContext.get_tenant_id(), await asyncio.to_thread(TenantContext.get_tenant_id)

    async with async_tenant_scope("parent"):
        first, second = await asyncio.gather(worker("a"), worker("b"))
        assert first == ("a", "a")
        assert second == ("b", "b")
        assert TenantContext.get_tenant_id() == "parent"
    assert TenantContext.get() is None


@pytest.mark.anyio
@pytest.mark.parametrize("bypass", [False, True])
async def test_async_exception_restores_context(bypass: bool) -> None:
    payload = TenantContextPayload(UUID("fd46b755-cdef-43d8-a0a1-32b97ee847fa"), {"role": "reader"})
    error = RuntimeError("body failed")
    async with async_tenant_scope(payload, variables={"role": "writer"}) as outer:
        assert outer.variables == {"role": "writer"}
        with pytest.raises(RuntimeError) as exc_info:
            async with async_tenant_bypass() if bypass else async_tenant_scope("inner"):
                assert TenantContext.is_bypassed() is bypass
                raise error
        assert exc_info.value is error
        assert TenantContext.get() is outer
    assert TenantContext.get() is None


@pytest.mark.anyio
@pytest.mark.parametrize("bypass", [False, True])
async def test_cancellation_restores_context_in_cancelled_task(bypass: bool) -> None:
    ready = asyncio.Event()
    restored: list[str | None] = []

    async def child() -> None:
        try:
            async with async_tenant_scope("outer"):
                try:
                    async with async_tenant_bypass() if bypass else async_tenant_scope("inner"):
                        assert TenantContext.is_bypassed() is bypass
                        ready.set()
                        await asyncio.Event().wait()
                finally:
                    restored.append(TenantContext.get_tenant_id())
        finally:
            restored.append(TenantContext.get_tenant_id())

    task = asyncio.create_task(child())
    async with async_tenant_scope("parent"):
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert TenantContext.get_tenant_id() == "parent"
    assert restored == ["outer", None]
