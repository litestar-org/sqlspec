"""Tenant identity validation and context lifetime contracts."""

import asyncio
import inspect
from collections.abc import Mapping, MutableMapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import FrozenInstanceError
from threading import Thread
from typing import Any, cast
from uuid import UUID

import pytest

from sqlspec.core.tenant import (
    TenantLeaseState,
    require_session_pooler,
    validate_session_setting_name,
    validate_tenant_session_baseline,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload


def test_lease_state_snapshots_baseline_and_captures_payload() -> None:
    baseline: dict[str, str | None] = {"app.tenant": "previous", "app.role": None}
    with TenantContext.scope("tenant-a") as payload:
        state = TenantLeaseState(payload, baseline)
    baseline["app.tenant"] = "changed"
    with TenantContext.scope("tenant-b"):
        assert state.payload is payload
        assert state.payload.tenant_id == "tenant-a"
        assert state.baseline == {"app.tenant": "previous", "app.role": None}
    with pytest.raises(TypeError):
        cast("MutableMapping[str, str | None]", state.baseline)["app.tenant"] = "changed"


def test_lease_states_have_independent_defaults_and_assignable_attributes() -> None:
    payload = TenantContextPayload("tenant")
    first, second = TenantLeaseState(payload), TenantLeaseState(payload)
    assert first is not second
    assert first.baseline == second.baseline == {}
    assert first.baseline is not second.baseline
    first.payload = TenantContextPayload("other")
    first.baseline = {"app.tenant": "restored"}
    assert first.payload.tenant_id == "other"
    assert first.baseline == {"app.tenant": "restored"}
    assert second.payload is payload
    assert second.baseline == {}


@pytest.mark.parametrize("name", ["app.current_tenant_id", "_tenant", "tenant9", "Tenant", "a.b_c.9"])
def test_session_setting_name_accepts_identifiers(name: str) -> None:
    assert validate_session_setting_name(name) == name


@pytest.mark.parametrize("name", ["app.x; DROP", "1app", "", "app\n", "app x", "é", "a\x00b", True, 123, None])
def test_session_setting_name_rejects_invalid_names(name: object) -> None:
    with pytest.raises(ImproperConfigurationError):
        validate_session_setting_name(name)


@pytest.mark.parametrize("server_settings", [None, {}, {"timezone": "UTC"}, {"application_name": "sqlspec"}])
def test_tenant_session_baseline_accepts_consistent_values(server_settings: dict[str, str] | None) -> None:
    baseline = {"timezone": "UTC"}
    validated = validate_tenant_session_baseline(baseline, server_settings)
    baseline["timezone"] = "CET"
    assert validated == {"timezone": "UTC"}
    assert validate_tenant_session_baseline({}, server_settings) == {}


@pytest.mark.parametrize(
    "baseline,server_settings",
    [
        ({"timezone": "UTC"}, {"timezone": "CET"}),
        ({"a": "x\x00"}, None),
        ({"a;b": "1"}, None),
        ({1: "value"}, None),
        ({"a": None}, None),
        ({"a": 123}, None),
        ("a=1", None),
        (None, None),
        ([("a", "1")], None),
    ],
)
def test_tenant_session_baseline_rejects_invalid_settings(
    baseline: object, server_settings: dict[str, str] | None
) -> None:
    with pytest.raises(ImproperConfigurationError):
        validate_tenant_session_baseline(baseline, server_settings)


@pytest.mark.parametrize("mode", [None, "session"])
def test_session_pooler_defaults_and_explicit_mode(mode: object) -> None:
    assert require_session_pooler(mode, "AsyncpgConfig") == "session"


@pytest.mark.parametrize("mode", ["transaction", "statement", "auto", "", 123, 0, True, False, [], {}])
def test_session_pooler_rejects_incompatible_modes(mode: object) -> None:
    with pytest.raises(ImproperConfigurationError, match="AsyncpgConfig"):
        require_session_pooler(mode, "AsyncpgConfig")


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


@pytest.mark.parametrize("tenant_id", ["\ud800", "\udc00"], ids=["high-surrogate", "low-surrogate"])
def test_payload_rejects_non_utf8_identity(tenant_id: str) -> None:
    with pytest.raises(TenantContextError) as exc_info:
        TenantContextPayload(tenant_id)
    assert isinstance(exc_info.value.__cause__, UnicodeEncodeError)
    assert exc_info.value.__cause__.object == tenant_id


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


@pytest.mark.parametrize("invalid", ["", "x" * 257, True, "\ud800", "\udc00"])
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
    async with TenantContext.scope(tenant_id) as outer:
        assert TenantContext.get_tenant_id() == str(tenant_id)
        async with TenantContext.bypass() as bypass:
            assert bypass == TenantContextPayload.bypass()
            assert TenantContext.is_bypassed() is True
            assert TenantContext.get_tenant_id() is None
            async with TenantContext.scope("inner"):
                assert TenantContext.get_tenant_id() == "inner"
            assert TenantContext.get() is bypass
        assert TenantContext.get() is outer
    assert TenantContext.get() is None


@pytest.mark.anyio
async def test_async_tasks_are_isolated_and_to_thread_propagates() -> None:
    async def worker(tenant_id: str) -> tuple[str | None, str | None]:
        async with TenantContext.scope(tenant_id):
            await asyncio.sleep(0)
            return TenantContext.get_tenant_id(), await asyncio.to_thread(TenantContext.get_tenant_id)

    async with TenantContext.scope("parent"):
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
    async with TenantContext.scope(payload, variables={"role": "writer"}) as outer:
        assert outer.variables == {"role": "writer"}
        with pytest.raises(RuntimeError) as exc_info:
            async with TenantContext.bypass() if bypass else TenantContext.scope("inner"):
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
            async with TenantContext.scope("outer"):
                try:
                    async with TenantContext.bypass() if bypass else TenantContext.scope("inner"):
                        assert TenantContext.is_bypassed() is bypass
                        ready.set()
                        await asyncio.Event().wait()
                finally:
                    restored.append(TenantContext.get_tenant_id())
        finally:
            restored.append(TenantContext.get_tenant_id())

    task = asyncio.create_task(child())
    async with TenantContext.scope("parent"):
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert TenantContext.get_tenant_id() == "parent"
    assert restored == ["outer", None]


async def test_scope_supports_sync_and_async_entry_and_sequential_reuse() -> None:
    scope = TenantContext.scope("tenant-a")
    with scope as payload:
        assert TenantContext.get() is payload
    assert TenantContext.get() is None
    async with scope as payload:
        assert TenantContext.get_tenant_id() == "tenant-a"
        with pytest.raises(TenantContextError, match="already active"):
            scope.__enter__()
        assert TenantContext.get() is payload
    assert TenantContext.get() is None


def test_bind_applies_resolved_payload_or_absence_and_restores() -> None:
    payload = TenantContextPayload("tenant-a")
    with TenantContext.scope("outer") as outer:
        with TenantContext.bind(payload):
            assert TenantContext.get() is payload
        assert TenantContext.get() is outer
        binding = TenantContext.bind(None)
        with binding:
            assert TenantContext.get() is None
            with pytest.raises(TenantContextError, match="already active"):
                binding.__enter__()
        assert TenantContext.get() is outer


def test_bind_restores_after_body_failure() -> None:
    with TenantContext.scope("outer") as outer:
        with pytest.raises(RuntimeError), TenantContext.bind(TenantContextPayload("inner")):
            raise RuntimeError
        assert TenantContext.get() is outer


def test_request_binding_resolves_lazily_once_per_key_with_ambient_fallback() -> None:
    from sqlspec.utils.tenant import TenantRequestBinding

    first, second = object(), object()
    calls: list[object] = []

    def resolve(key: object) -> str | None:
        calls.append(key)
        return "routed" if key is first else None

    binding = TenantRequestBinding(resolve, keys=(first, second), ambient=TenantContextPayload("ambient"))
    token = TenantContext.bind_request(binding)
    try:
        assert calls == []
        assert TenantContext.get_tenant_id() == "routed"
        assert TenantContext.require(second).tenant_id == "ambient"
        assert TenantContext.require(first).tenant_id == "routed"
        with TenantContext.scope("handler"):
            assert TenantContext.require(first).tenant_id == "handler"
        assert calls == [first, second]
    finally:
        TenantContext.reset(token)
    assert TenantContext.current_binding() is None


def test_request_binding_freeze_resolves_pending_keys_and_stops_reading_request_state() -> None:
    from sqlspec.utils.tenant import TenantRequestBinding

    key = object()
    outcomes: list[str | None] = ["handoff"]

    binding = TenantRequestBinding(lambda _key: outcomes.pop(0), keys=(key,))
    binding.freeze()
    assert binding.frozen
    assert binding.resolve(key) == TenantContextPayload("handoff")
    assert outcomes == []
    assert binding.resolve(object()) is None


def test_request_binding_missing_tenant_calls_framework_rejection_until_frozen() -> None:
    from sqlspec.utils.tenant import TenantRequestBinding

    class Rejected(Exception):
        pass

    def reject(_key: object) -> None:
        raise Rejected

    binding = TenantRequestBinding(lambda _key: None, keys=(object(),), missing=reject)
    with pytest.raises(Rejected):
        binding.require()
    binding.freeze()
    with pytest.raises(TenantContextError):
        binding.require()


def test_request_binding_rejects_synchronous_access_to_asynchronous_resolution() -> None:
    from sqlspec.utils.tenant import TenantRequestBinding

    async def resolve(_key: object) -> str:
        return "async"

    coroutines: list[Any] = []

    def resolver(key: object) -> Any:
        coroutines.append(resolve(key))
        return coroutines[-1]

    binding = TenantRequestBinding(resolver, keys=(object(),))
    with pytest.raises(ImproperConfigurationError, match="await TenantContext"):
        binding.resolve()
    assert inspect.getcoroutinestate(coroutines[0]) == inspect.CORO_CLOSED
    assert asyncio.run(binding.aresolve()) == TenantContextPayload("async")


def test_require_outside_a_request_raises_tenant_context_error() -> None:
    with pytest.raises(TenantContextError):
        TenantContext.require()
    with pytest.raises(TenantContextError):
        asyncio.run(TenantContext.arequire())
    assert asyncio.run(TenantContext.aget()) is None


def test_tenant_bypass_marks_route_handlers_and_wrapped_functions() -> None:
    from types import SimpleNamespace

    from sqlspec.utils.tenant import is_tenant_bypass_route, tenant_bypass

    @tenant_bypass
    def public() -> None:
        return None

    def private() -> None:
        return None

    assert is_tenant_bypass_route(public)
    assert is_tenant_bypass_route(SimpleNamespace(fn=public))
    assert not is_tenant_bypass_route(private)
    assert not is_tenant_bypass_route(None)
