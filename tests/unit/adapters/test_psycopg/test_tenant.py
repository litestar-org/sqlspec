"""Sync and async psycopg tenant management obey identical session contracts."""

from inspect import iscoroutinefunction
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Self, cast
from unittest.mock import Mock

import pytest
from psycopg import pq

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncRLSStrategy, CockroachPsycopgSyncRLSStrategy
from sqlspec.adapters.cockroach_psycopg.tenant import CockroachPsycopgAsyncTenantLease, CockroachPsycopgSyncTenantLease
from sqlspec.adapters.psycopg import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy
from sqlspec.adapters.psycopg.tenant import PsycopgAsyncTenantLease, PsycopgSyncTenantLease
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg import PsycopgAsyncConnection, PsycopgSyncConnection


class GucCursorBase:
    def __init__(self, connection: "GucConnection") -> None:
        self.connection = connection
        self.row: tuple[str | None, ...] = ()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass

    def apply(self, sql: str | bytes, params: dict[str, str]) -> None:
        sql = sql.decode("ascii") if isinstance(sql, bytes) else sql
        self.connection.calls.append((sql, params))
        values: list[str | None] = []
        for index in range(1, len(params), 2):
            key, value = params[f"p{index}"], params[f"p{index + 1}"]
            old = self.connection.settings.get(key)
            self.connection.settings[key] = value
            if "captured AS MATERIALIZED" in sql:
                values.extend((old, value))
            else:
                values.extend((value, "wrong" if self.connection.corrupt else value))
        self.row = tuple(values)
        self.connection.info.transaction_status = pq.TransactionStatus.INTRANS


class GucCursor(GucCursorBase):
    def execute(self, sql: str | bytes, params: dict[str, str]) -> None:
        self.apply(sql, params)

    def fetchone(self) -> tuple[str | None, ...]:
        return self.row


class AsyncGucCursor(GucCursorBase):
    async def execute(self, sql: str | bytes, params: dict[str, str]) -> None:
        self.apply(sql, params)

    async def fetchone(self) -> tuple[str | None, ...]:
        return self.row


class GucConnection:
    def __init__(self, asynchronous: bool, autocommit: bool = False) -> None:
        self.asynchronous = asynchronous
        self.autocommit = autocommit
        self.info = SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE)
        self.settings: dict[str, str | None] = {"app.current_tenant_id": "before"}
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.corrupt = False

    def cursor(self, **_: Any) -> GucCursor | AsyncGucCursor:
        return AsyncGucCursor(self) if self.asynchronous else GucCursor(self)

    def commit(self) -> Any:
        self.commits += 1
        self.info.transaction_status = pq.TransactionStatus.IDLE
        return self._done() if self.asynchronous else None

    def rollback(self) -> Any:
        self.rollbacks += 1
        self.info.transaction_status = pq.TransactionStatus.IDLE
        return self._done() if self.asynchronous else None

    async def _done(self) -> None:
        pass


@pytest.fixture(
    params=[
        SimpleNamespace(
            sync_strategy=PsycopgSyncRLSStrategy,
            async_strategy=PsycopgAsyncRLSStrategy,
            sync_lease=PsycopgSyncTenantLease,
            async_lease=PsycopgAsyncTenantLease,
        ),
        SimpleNamespace(
            sync_strategy=CockroachPsycopgSyncRLSStrategy,
            async_strategy=CockroachPsycopgAsyncRLSStrategy,
            sync_lease=CockroachPsycopgSyncTenantLease,
            async_lease=CockroachPsycopgAsyncTenantLease,
        ),
    ],
    ids=["psycopg", "cockroach_psycopg"],
)
def family(request: pytest.FixtureRequest) -> Any:
    """Return one psycopg-protocol adapter's sync and async strategies and leases."""
    return request.param


async def checkout(strategy: Any, connection: GucConnection, payload: TenantContextPayload) -> TenantLeaseState:
    if iscoroutinefunction(strategy.checkout):
        return cast("TenantLeaseState", await strategy.checkout(cast("PsycopgAsyncConnection", connection), payload))
    return cast("TenantLeaseState", strategy.checkout(cast("PsycopgSyncConnection", connection), payload))


async def checkin(strategy: Any, connection: GucConnection, state: TenantLeaseState) -> None:
    if iscoroutinefunction(strategy.checkin):
        await strategy.checkin(cast("PsycopgAsyncConnection", connection), state)
    else:
        strategy.checkin(cast("PsycopgSyncConnection", connection), state)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("autocommit", [False, True])
async def test_tenant_manages_bound_identity_and_restores(asynchronous: bool, autocommit: bool, family: Any) -> None:
    connection = GucConnection(asynchronous, autocommit)
    strategy = (family.async_strategy if asynchronous else family.sync_strategy)()
    payload = TenantContextPayload("a'; DROP")
    state = await checkout(strategy, connection, payload)
    assert state.baseline["app.current_tenant_id"] == "before"
    assert connection.settings["app.current_tenant_id"] == "a'; DROP"
    assert len(connection.calls) == 1
    assert "a'; DROP" not in connection.calls[0][0]
    assert connection.commits == int(not autocommit)
    connection.info.transaction_status = pq.TransactionStatus.INTRANS
    await checkin(strategy, connection, state)
    assert connection.settings["app.current_tenant_id"] == "before"
    assert connection.rollbacks == 1
    assert connection.commits == 2 * int(not autocommit)
    assert len(connection.calls) == 2


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_rejects_active_checkout_and_failed_restore(asynchronous: bool, family: Any) -> None:
    connection = GucConnection(asynchronous)
    strategy = (family.async_strategy if asynchronous else family.sync_strategy)()
    payload = TenantContextPayload("a")
    connection.info.transaction_status = pq.TransactionStatus.INTRANS
    with pytest.raises(TenantContextError):
        await checkout(strategy, connection, payload)
    assert connection.calls == []
    connection.info.transaction_status = pq.TransactionStatus.IDLE
    state = await checkout(strategy, connection, payload)
    connection.corrupt = True
    with pytest.raises(TenantCleanupError):
        await checkin(strategy, connection, state)


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_checkin_preserves_driver_error(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch, family: Any
) -> None:
    connection = GucConnection(asynchronous)
    strategy = (family.async_strategy if asynchronous else family.sync_strategy)()
    state = await checkout(strategy, connection, TenantContextPayload("a"))
    error = RuntimeError("native restore failed")
    monkeypatch.setattr(connection, "cursor", Mock(side_effect=error))

    with pytest.raises(TenantCleanupError, match="Tenant session restoration failed") as caught:
        await checkin(strategy, connection, state)

    assert caught.value.__cause__ is error
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_close_failure_never_returns_unverified_connection(asynchronous: bool, family: Any) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.utils.tenant import TenantContext

    payload = TenantContextPayload("a")
    strategy = AsyncMock() if asynchronous else MagicMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    strategy.checkin.side_effect = RuntimeError("unverified restore")
    connection = AsyncMock() if asynchronous else MagicMock()
    connection.close.side_effect = RuntimeError("physical close failed")
    context = AsyncMock() if asynchronous else MagicMock()
    if asynchronous:
        context.__aenter__.return_value = connection
    else:
        context.__enter__.return_value = connection
    pool = SimpleNamespace(
        connection=lambda: context,
        close=AsyncMock(side_effect=RuntimeError("pool close failed"))
        if asynchronous
        else MagicMock(side_effect=RuntimeError("pool close failed")),
    )
    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=pool) if asynchronous else lambda: pool,
        driver_features={"tenant_strategy": strategy},
    )
    lease_type: Any = family.async_lease if asynchronous else family.sync_lease
    lease = lease_type(config, strategy)
    with TenantContext.scope(payload):
        if asynchronous:
            await lease.acquire()
        else:
            lease.acquire()
    with pytest.raises(TenantCleanupError) as caught:
        if asynchronous:
            await lease.release()
        else:
            lease.release()
    assert caught.value.__cause__ is strategy.checkin.side_effect
    assert any("physical close failed" in note for note in caught.value.__notes__)
    assert any("pool quarantine failed" in note for note in caught.value.__notes__)
    if asynchronous:
        context.__aexit__.assert_not_awaited()
    else:
        context.__exit__.assert_not_called()


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_failed_close_quarantines_native_pool_before_context_gc(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch, family: Any
) -> None:
    import gc
    from unittest.mock import AsyncMock, MagicMock

    from psycopg_pool import AsyncConnectionPool, ConnectionPool

    from sqlspec.utils.tenant import TenantContext

    payload = TenantContextPayload("a")
    connection = AsyncMock() if asynchronous else MagicMock()
    connection.close.side_effect = RuntimeError("physical close failed")
    strategy = AsyncMock() if asynchronous else MagicMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    strategy.checkin.side_effect = RuntimeError("unverified reset")
    reusable_returns: list[bool] = []
    pool: Any = (
        AsyncConnectionPool(open=False, min_size=0, max_size=1)
        if asynchronous
        else ConnectionPool(open=False, min_size=0, max_size=1)
    )
    monkeypatch.setattr(
        pool, "getconn", AsyncMock(return_value=connection) if asynchronous else MagicMock(return_value=connection)
    )

    async def async_putconn(_connection: Any) -> None:
        reusable_returns.append(not pool.closed)

    def sync_putconn(_connection: Any) -> None:
        reusable_returns.append(not pool.closed)

    monkeypatch.setattr(pool, "putconn", async_putconn if asynchronous else sync_putconn)
    config = SimpleNamespace(
        provide_pool=AsyncMock(return_value=pool) if asynchronous else lambda: pool,
        driver_features={"tenant_strategy": strategy},
    )
    lease_type: Any = family.async_lease if asynchronous else family.sync_lease
    if asynchronous:
        await pool.open()
    else:
        pool.open()
    try:
        lease = lease_type(config, strategy)
        with TenantContext.scope(payload):
            if asynchronous:
                await lease.acquire()
            else:
                lease.acquire()
        with pytest.raises(TenantCleanupError):
            if asynchronous:
                await lease.release()
            else:
                lease.release()
        connection.close.side_effect.__traceback__ = None
        strategy.checkin.side_effect.__traceback__ = None
        del lease
        gc.collect()
        if asynchronous:
            import asyncio

            await asyncio.sleep(0)
            await asyncio.sleep(0)
        assert reusable_returns == [False]
        assert pool.closed
    finally:
        if asynchronous:
            await pool.close()
        else:
            pool.close()
