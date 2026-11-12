"""Psqlpy tenant identity and disposal use the native connection boundary."""

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.psqlpy.tenant import PsqlpyRLSStrategy, PsqlpyTenantLease
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy import PsqlpyConnection


class GucConnection:
    def __init__(self) -> None:
        self.settings: dict[str, str | None] = {"app.current_tenant_id": "before", "search_path": ""}
        self.calls: list[tuple[str, list[str]]] = []
        self.corrupt = False

    @property
    def native(self) -> "PsqlpyConnection":
        return cast("PsqlpyConnection", self)

    async def fetch_row(self, sql: str, params: list[str]) -> Any:
        self.calls.append((sql, params))
        values: list[str | None] = []
        for key, value in zip(params[::2], params[1::2], strict=True):
            old = self.settings.get(key)
            self.settings[key] = value
            values.extend(
                (old, value) if "captured AS MATERIALIZED" in sql else (value, "wrong" if self.corrupt else value)
            )
        return SimpleNamespace(result=lambda **_: tuple(values))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"variable_keys": {"a": "APP.Current_Tenant_Id"}},
        {"variable_keys": {"a": "app.x", "b": "APP.X"}},
        {"schema_resolver": lambda _: "public", "tenant_key": "SEARCH_PATH"},
    ],
)
def test_tenant_ownership_rejects_collisions(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        PsqlpyRLSStrategy(**kwargs)


async def test_tenant_binds_and_restores_a_snapshot_in_one_call() -> None:
    mapping = {"region": "app.region"}
    strategy = PsqlpyRLSStrategy(variable_keys=mapping, schema_resolver=lambda _: "public")
    mapping["region"] = "app.current_tenant_id"
    connection = GucConnection()
    state = await strategy.checkout(connection.native, TenantContextPayload("a'; DROP", variables={"region": "eu"}))
    assert state.baseline["app.current_tenant_id"] == "before"
    assert connection.settings["app.current_tenant_id"] == "a'; DROP"
    assert connection.settings["app.region"] == "eu"
    assert len(connection.calls) == 1
    assert "a'; DROP" not in connection.calls[0][0]
    assert isinstance(connection.calls[0][1], list)
    await strategy.checkin(connection.native, state)
    assert connection.settings == {"app.current_tenant_id": "before", "app.region": "", "search_path": ""}
    assert len(connection.calls) == 2
    connection.corrupt = True
    with pytest.raises(TenantCleanupError):
        await strategy.checkin(connection.native, state)
    with pytest.raises(TenantContextError):
        await PsqlpyRLSStrategy().checkout(connection.native, TenantContextPayload("a", variables={"region": "eu"}))


async def test_tenant_checkin_preserves_driver_error(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = GucConnection()
    strategy = PsqlpyRLSStrategy()
    state = await strategy.checkout(connection.native, TenantContextPayload("a"))
    error = RuntimeError("native restore failed")
    monkeypatch.setattr(connection, "fetch_row", AsyncMock(side_effect=error))

    with pytest.raises(TenantCleanupError, match="Tenant session restoration failed") as caught:
        await strategy.checkin(connection.native, state)

    assert caught.value.__cause__ is error
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("disposal", ["closed", "live", "timeout", "aborted"])
async def test_tenant_disposal_is_verified_and_bounded(disposal: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlspec.adapters.psqlpy.tenant as tenant_module

    payload = TenantContextPayload("a")
    strategy = AsyncMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    strategy.checkin.side_effect = RuntimeError("restore failed")
    events: list[str] = []
    connection = AsyncMock()
    connection.is_closed = Mock(return_value=disposal == "closed")

    async def execute(sql: str) -> None:
        events.append(sql)
        if "pg_terminate_backend" in sql and disposal == "timeout":
            await asyncio.Event().wait()
        if sql == "SELECT 1" and disposal in {"closed", "aborted"}:
            raise RuntimeError("probe error")

    connection.execute.side_effect = execute
    context = AsyncMock()
    context.__aenter__.return_value = connection
    pool = SimpleNamespace(acquire=lambda: context)
    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=pool),
        _ensure_connection=AsyncMock(),
        _close_pool=AsyncMock(),
        driver_features={"tenant_strategy": strategy},
    )
    monkeypatch.setattr(tenant_module, "PSQLPY_DISPOSAL_TIMEOUT_S", 0.01)
    lease = PsqlpyTenantLease(config, config.driver_features["tenant_strategy"])
    with TenantContext.scope(payload):
        await lease.acquire()
    with pytest.raises(TenantCleanupError):
        await lease.release()
    assert events[0] == "ROLLBACK"
    assert "pg_terminate_backend" in events[1]
    context.__aexit__.assert_awaited_once()
    if disposal == "closed":
        config._close_pool.assert_not_awaited()
    else:
        config._close_pool.assert_awaited_once()


async def test_tenant_acquire_probe_failure_exits_once() -> None:
    context = AsyncMock()
    error = RuntimeError("initialization failed")
    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=SimpleNamespace(acquire=lambda: context)),
        _ensure_connection=AsyncMock(side_effect=error),
        _close_pool=AsyncMock(),
        driver_features={"tenant_strategy": AsyncMock()},
    )
    lease = PsqlpyTenantLease(config, config.driver_features["tenant_strategy"])
    with TenantContext.scope("a"), pytest.raises(RuntimeError) as caught:
        await lease.acquire()
    assert caught.value is error
    context.__aexit__.assert_awaited_once()
    config.driver_features["tenant_strategy"].checkout.assert_not_awaited()
    config._close_pool.assert_awaited_once()
    context.__aenter__.return_value.execute.assert_any_await("SELECT pg_terminate_backend(pg_backend_pid())")


async def test_tenant_failed_acquire_waits_for_disposal_despite_cancellation() -> None:
    entered_disposal = asyncio.Event()
    finish_disposal = asyncio.Event()
    context = AsyncMock()
    failure = RuntimeError("partial setup")

    async def close_pool() -> None:
        entered_disposal.set()
        await finish_disposal.wait()

    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=SimpleNamespace(acquire=lambda: context)),
        _ensure_connection=AsyncMock(side_effect=failure),
        _close_pool=close_pool,
        driver_features={"tenant_strategy": AsyncMock()},
    )
    lease = PsqlpyTenantLease(config, config.driver_features["tenant_strategy"])
    with TenantContext.scope("a"):
        task = asyncio.create_task(lease.acquire())
    await entered_disposal.wait()
    task.cancel()
    finish_disposal.set()
    with pytest.raises(RuntimeError) as caught:
        await task
    assert caught.value is failure
    context.__aexit__.assert_awaited_once()


async def test_tenant_failed_pool_disposal_never_returns_dirty_context() -> None:
    context = AsyncMock()
    failure = RuntimeError("partial setup")
    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=SimpleNamespace(acquire=lambda: context)),
        _ensure_connection=AsyncMock(side_effect=failure),
        _close_pool=AsyncMock(side_effect=RuntimeError("pool close failed")),
        driver_features={"tenant_strategy": AsyncMock()},
    )
    lease = PsqlpyTenantLease(config, config.driver_features["tenant_strategy"])
    with TenantContext.scope("a"), pytest.raises(RuntimeError) as caught:
        await lease.acquire()
    assert caught.value is failure
    assert any("invalidation failed" in note for note in caught.value.__notes__)
    context.__aexit__.assert_not_awaited()


@pytest.mark.parametrize(("probe", "rolls_back"), [(True, False), (False, True), (RuntimeError("aborted"), True)])
async def test_tenant_rolls_back_only_open_or_aborted_transactions(probe: object, rolls_back: bool) -> None:
    payload = TenantContextPayload("a")
    strategy = AsyncMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    connection = AsyncMock()
    if isinstance(probe, BaseException):
        connection.fetch_val.side_effect = probe
    else:
        connection.fetch_val.return_value = probe
    context = AsyncMock()
    context.__aenter__.return_value = connection
    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=SimpleNamespace(acquire=lambda: context)),
        _ensure_connection=AsyncMock(),
        _close_pool=AsyncMock(),
        driver_features={"tenant_strategy": strategy},
    )
    lease = PsqlpyTenantLease(config, strategy)
    with TenantContext.scope(payload):
        await lease.acquire()
    await lease.release()
    executed = [call.args[0] for call in connection.execute.await_args_list]
    assert executed == (["ROLLBACK"] if rolls_back else [])
    context.__aexit__.assert_awaited_once()
