"""Tenant session state is bound, verified, and restored through one SQL call."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.asyncpg import AsyncpgRLSStrategy
from sqlspec.adapters.asyncpg.tenant import apply_session_baseline, validate_asyncpg_tenant_config
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg import AsyncpgConnection


class FakeGucConnection:
    """Model bound session settings at the database I/O boundary."""

    def __init__(self, settings: dict[str, str | None] | None = None) -> None:
        self.settings = dict(settings or {})
        self.calls: list[tuple[str, tuple[str, ...]]] = []
        self.busy = False
        self.corrupt = False
        self.missing = False

    @property
    def native(self) -> "AsyncpgConnection":
        return cast("AsyncpgConnection", self)

    def is_in_transaction(self) -> bool:
        return self.busy

    async def fetchrow(self, sql: str, *params: str) -> tuple[str | None, ...] | None:
        self.calls.append((sql, params))
        if self.missing:
            return None
        values: list[str | None] = []
        checkout = "captured AS MATERIALIZED" in sql
        restoring = "applied AS MATERIALIZED" in sql
        for key, value in zip(params[::2], params[1::2], strict=True):
            baseline = self.settings.get(key)
            self.settings[key] = value
            if checkout:
                values.extend((baseline, "wrong" if self.corrupt else value))
            elif restoring:
                values.extend((value, "wrong" if self.corrupt else self.settings[key]))
            else:
                values.append(value)
        return tuple(values)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"variable_keys": {"region": "app.Current_Tenant_Id"}},
        {"variable_keys": {"a": "app.Region", "b": "APP.REGION"}},
        {"schema_resolver": lambda _: "tenant_a", "variable_keys": {"region": "SEARCH_PATH"}},
        {"schema_resolver": lambda _: "tenant_a", "tenant_key": "SEARCH_PATH"},
        {"tenant_key": "bad;key"},
    ],
)
def test_tenant_rejects_overlapping_or_invalid_targets(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        AsyncpgRLSStrategy(**kwargs)


async def test_tenant_captures_and_restores_owned_settings_once() -> None:
    connection = FakeGucConnection({"app.Tenant": "previous", "unrelated": "leave"})
    strategy = AsyncpgRLSStrategy(tenant_key="app.Tenant", variable_keys={"z": "app.Z", "a": "app.A"})
    payload = TenantContextPayload("a'; DROP", variables={"z": "2", "a": "1"})
    state = await strategy.checkout(connection.native, payload)
    assert dict(state.baseline) == {"app.Tenant": "previous", "app.A": None, "app.Z": None}
    assert connection.calls[0][1] == ("app.Tenant", "a'; DROP", "app.A", "1", "app.Z", "2")
    assert "a'; DROP" not in connection.calls[0][0]
    assert connection.settings["app.Tenant"] == "a'; DROP"
    await strategy.checkin(connection.native, state)
    assert len(connection.calls) == 2
    assert connection.settings == {"app.Tenant": "previous", "unrelated": "leave", "app.A": "", "app.Z": ""}


async def test_tenant_schema_restore_and_subset() -> None:
    connection = FakeGucConnection()
    strategy = AsyncpgRLSStrategy(variable_keys={"a": "app.a", "b": "app.b"}, schema_resolver=lambda _: "tenant_a")
    state = await strategy.checkout(connection.native, TenantContextPayload("a", variables={"b": "2"}))
    assert connection.settings["search_path"] == '"tenant_a"'
    assert connection.calls[0][1] == ("app.current_tenant_id", "a", "app.b", "2", "search_path", '"tenant_a"')
    await strategy.checkin(connection.native, state)
    assert connection.settings["search_path"] == '"$user", public'


@pytest.mark.parametrize("case", ["busy", "unmapped", "schema", "missing", "corrupt"])
async def test_tenant_checkout_fails_closed(case: str) -> None:
    connection = FakeGucConnection()
    connection.busy = case == "busy"
    connection.missing = case == "missing"
    connection.corrupt = case == "corrupt"
    strategy = AsyncpgRLSStrategy(schema_resolver=(lambda _: "bad;schema") if case == "schema" else None)
    payload = TenantContextPayload("a", variables={"region": "eu"} if case == "unmapped" else None)
    with pytest.raises(TenantContextError):
        await strategy.checkout(connection.native, payload)
    if case in {"busy", "unmapped", "schema"}:
        assert connection.calls == []


@pytest.mark.parametrize("missing", [True, False])
async def test_tenant_checkin_requires_verified_readback(missing: bool) -> None:
    connection = FakeGucConnection()
    strategy = AsyncpgRLSStrategy()
    state = await strategy.checkout(connection.native, TenantContextPayload("a"))
    connection.missing = missing
    connection.corrupt = not missing
    with pytest.raises(TenantCleanupError):
        await strategy.checkin(connection.native, state)


async def test_tenant_baseline_replay_is_batched() -> None:
    connection = FakeGucConnection()
    await apply_session_baseline(connection.native, {})
    assert connection.calls == []
    await apply_session_baseline(connection.native, {"timezone": "UTC", "statement_timeout": "5000"})
    assert len(connection.calls) == 1
    assert connection.settings == {"timezone": "UTC", "statement_timeout": "5000"}


@pytest.mark.parametrize("mode", ["transaction", "statement", "auto"])
def test_tenant_config_requires_session_affinity(mode: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="session"):
        validate_asyncpg_tenant_config({"pooler_mode": mode}, {})


@pytest.mark.parametrize("source", ["hook", "init", "setup"])
def test_tenant_callbacks_require_explicit_baseline(source: str) -> None:
    callback = object()
    features = {"on_connection_create": callback} if source == "hook" else {}
    connection = {source: callback} if source != "hook" else {}
    with pytest.raises(ImproperConfigurationError, match="tenant_session_baseline"):
        validate_asyncpg_tenant_config(features, connection)
    assert validate_asyncpg_tenant_config({**features, "tenant_session_baseline": {}}, connection) == {}


def test_tenant_baseline_validates_values_and_conflicts() -> None:
    assert validate_asyncpg_tenant_config({}, {}) == {}
    assert validate_asyncpg_tenant_config({"tenant_session_baseline": {"timezone": "UTC"}}, {}) == {"timezone": "UTC"}
    with pytest.raises(ImproperConfigurationError):
        validate_asyncpg_tenant_config(
            {"tenant_session_baseline": {"timezone": "UTC"}}, {"server_settings": {"timezone": "CET"}}
        )
    with pytest.raises(ImproperConfigurationError):
        validate_asyncpg_tenant_config({"tenant_session_baseline": {"bad;key": "x"}}, {})


async def test_tenant_preserves_empty_search_path_baseline() -> None:
    connection = FakeGucConnection({"search_path": ""})
    strategy = AsyncpgRLSStrategy(schema_resolver=lambda _: "tenant_a")
    state = await strategy.checkout(connection.native, TenantContextPayload("a"))
    await strategy.checkin(connection.native, state)
    assert connection.settings["search_path"] == ""


@pytest.mark.parametrize("key", ["", 1])
def test_tenant_rejects_invalid_variable_source_names(key: Any) -> None:
    with pytest.raises(ImproperConfigurationError):
        AsyncpgRLSStrategy(variable_keys={key: "app.region"})


@pytest.mark.parametrize("scenario", ["success", "error", "timeout", "native_cancel"])
async def test_owned_release_joins_sdk_shielded_child_after_repeated_cancellation(scenario: str) -> None:
    import asyncio

    from sqlspec.utils.tenant_asyncpg import await_owned_release

    started = asyncio.Event()
    complete = asyncio.Event()
    parent_finished = asyncio.Event()
    children: list[asyncio.Task[None]] = []

    async def reset() -> None:
        started.set()
        if scenario == "timeout":
            async with asyncio.timeout(0.02):
                await complete.wait()
        else:
            await complete.wait()
        if scenario == "error":
            msg = "SDK reset failed"
            raise RuntimeError(msg)
        if scenario == "native_cancel":
            raise asyncio.CancelledError

    async def native_release() -> None:
        child = asyncio.create_task(reset())
        children.append(child)
        try:
            await asyncio.shield(child)
        finally:
            parent_finished.set()

    operation = asyncio.create_task(await_owned_release(native_release()))
    await started.wait()
    operation.cancel()
    await asyncio.sleep(0)
    operation.cancel()
    if scenario != "timeout":
        complete.set()
    expected = (
        RuntimeError if scenario == "error" else TimeoutError if scenario == "timeout" else asyncio.CancelledError
    )
    with pytest.raises(expected):
        await operation
    assert parent_finished.is_set()
    assert len(children) == 1
    assert children[0].done()
