"""Sync and async psycopg tenant management obey identical session contracts."""

from typing import Any, Self

import pytest
from psycopg import pq

from sqlspec.adapters.psycopg import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy
from sqlspec.exceptions import TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContextPayload


class GucCursor:
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

    def apply(self, sql: str, params: dict[str, str]) -> None:
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

    def execute(self, sql: str, params: dict[str, str]) -> None:
        self.apply(sql, params)

    def fetchone(self) -> tuple[str | None, ...]:
        return self.row


class AsyncGucCursor(GucCursor):
    async def execute(self, sql: str, params: dict[str, str]) -> None:
        self.apply(sql, params)

    async def fetchone(self) -> tuple[str | None, ...]:
        return self.row


class GucConnection:
    def __init__(self, asynchronous: bool, autocommit: bool = False) -> None:
        self.asynchronous = asynchronous
        self.autocommit = autocommit
        self.info = type("ConnectionInfo", (), {"transaction_status": pq.TransactionStatus.IDLE})()
        self.settings: dict[str, str | None] = {"app.current_tenant_id": "before"}
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.corrupt = False

    def cursor(self, **_: Any) -> GucCursor:
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


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("autocommit", [False, True])
async def test_tenant_manages_bound_identity_and_restores(asynchronous: bool, autocommit: bool) -> None:
    connection = GucConnection(asynchronous, autocommit)
    strategy = PsycopgAsyncRLSStrategy() if asynchronous else PsycopgSyncRLSStrategy()
    payload = TenantContextPayload("a'; DROP")
    state = await strategy.checkout(connection, payload) if asynchronous else strategy.checkout(connection, payload)
    assert state.baseline["app.current_tenant_id"] == "before"
    assert connection.settings["app.current_tenant_id"] == "a'; DROP"
    assert len(connection.calls) == 1
    assert "a'; DROP" not in connection.calls[0][0]
    assert connection.commits == int(not autocommit)
    connection.info.transaction_status = pq.TransactionStatus.INTRANS
    if asynchronous:
        await strategy.checkin(connection, state)
    else:
        strategy.checkin(connection, state)
    assert connection.settings["app.current_tenant_id"] == "before"
    assert connection.rollbacks == 1
    assert connection.commits == 2 * int(not autocommit)
    assert len(connection.calls) == 2


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_rejects_active_checkout_and_failed_restore(asynchronous: bool) -> None:
    connection = GucConnection(asynchronous)
    strategy = PsycopgAsyncRLSStrategy() if asynchronous else PsycopgSyncRLSStrategy()
    payload = TenantContextPayload("a")
    connection.info.transaction_status = pq.TransactionStatus.INTRANS
    with pytest.raises(TenantContextError):
        if asynchronous:
            await strategy.checkout(connection, payload)
        else:
            strategy.checkout(connection, payload)
    assert connection.calls == []
    connection.info.transaction_status = pq.TransactionStatus.IDLE
    state = await strategy.checkout(connection, payload) if asynchronous else strategy.checkout(connection, payload)
    connection.corrupt = True
    with pytest.raises(TenantCleanupError):
        if asynchronous:
            await strategy.checkin(connection, state)
        else:
            strategy.checkin(connection, state)
