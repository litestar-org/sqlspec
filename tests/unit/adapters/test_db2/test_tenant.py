"""Db2 tenant strategies set, verify, and restore session global variables."""

from inspect import isawaitable
from typing import Any

import pytest

from sqlspec.adapters.db2 import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.tenant import (
    Db2AsyncGlobalVariableStrategy,
    Db2AsyncTenantLease,
    Db2SyncGlobalVariableStrategy,
    Db2SyncTenantLease,
    validate_global_variable,
)
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

pytestmark = pytest.mark.anyio


class GlobalVariableSession:
    """Model Db2 session global variables at the cursor boundary."""

    def __init__(self, asynchronous: bool, values: dict[str, str | None] | None = None) -> None:
        self.asynchronous = asynchronous
        self.values: dict[str, str | None] = dict(values or {})
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.corrupt = False
        self.fail = False
        self.closed = 0
        self.rollbacks = 0
        self._row: tuple[str | None, ...] | None = None

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.statements.append((sql, tuple(params)))
        if self.fail:
            msg = "driver failure"
            raise RuntimeError(msg)
        if sql.startswith("SET "):
            target = sql[4:].split(" = ")[0].strip("()")
            for name, value in zip([part.strip() for part in target.split(",")], params, strict=True):
                self.values[name] = "corrupted" if self.corrupt else value
            return
        columns = sql[len("SELECT ") : sql.index(" FROM SYSIBM.SYSDUMMY1")].split(", ")
        self._row = tuple(self.values.get(column) for column in columns)

    def cursor(self) -> Any:
        session = self

        class SyncCursor:
            def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
                session._execute(sql, params)

            def fetchone(self) -> Any:
                return session._row

            def close(self) -> None:
                return None

        class AsyncCursor:
            async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
                session._execute(sql, params)

            async def fetchone(self) -> Any:
                return session._row

            async def close(self) -> None:
                return None

        if self.asynchronous:

            async def open_cursor() -> AsyncCursor:
                return AsyncCursor()

            return open_cursor()
        return SyncCursor()


async def _invoke(function: Any, *args: Any) -> Any:
    result = function(*args)
    return await result if isawaitable(result) else result


def _strategy(asynchronous: bool, **kwargs: Any) -> Any:
    return (Db2AsyncGlobalVariableStrategy if asynchronous else Db2SyncGlobalVariableStrategy)(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tenant_variable": "TENANT_ID"},
        {"tenant_variable": "app.tenant; DROP"},
        {"tenant_variable": 1},
        {"variable_keys": {"region": "APP.CURRENT_TENANT_ID"}},
        {"variable_keys": {"": "APP.REGION"}},
        {"variable_keys": {"region": "app.region", "zone": "APP.REGION"}},
    ],
)
def test_global_variable_names_are_validated(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        Db2SyncGlobalVariableStrategy(**kwargs)


def test_global_variable_names_fold_to_upper_case() -> None:
    assert validate_global_variable("app.current_tenant_id") == "APP.CURRENT_TENANT_ID"


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_checkout_binds_verifies_and_checkin_restores(asynchronous: bool) -> None:
    session = GlobalVariableSession(asynchronous, {"APP.CURRENT_TENANT_ID": "previous"})
    strategy = _strategy(asynchronous, variable_keys={"region": "APP.REGION"})
    payload = TenantContextPayload("a'; DROP", variables={"region": "eu"})

    state = await _invoke(strategy.checkout, session, payload)

    assert dict(state.baseline) == {"APP.CURRENT_TENANT_ID": "previous", "APP.REGION": None}
    assert session.values == {"APP.CURRENT_TENANT_ID": "a'; DROP", "APP.REGION": "eu"}
    assignment = next(statement for statement in session.statements if statement[0].startswith("SET "))
    assert assignment == ("SET (APP.CURRENT_TENANT_ID, APP.REGION) = (?, ?)", ("a'; DROP", "eu"))
    assert all("DROP" not in sql for sql, _ in session.statements)

    await _invoke(strategy.checkin, session, state)
    assert session.values == {"APP.CURRENT_TENANT_ID": "previous", "APP.REGION": None}


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_single_variable_uses_scalar_assignment(asynchronous: bool) -> None:
    session = GlobalVariableSession(asynchronous)
    await _invoke(_strategy(asynchronous).checkout, session, TenantContextPayload("a"))
    assert ("SET APP.CURRENT_TENANT_ID = ?", ("a",)) in session.statements


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("case", ["unmapped", "corrupt", "driver"])
async def test_checkout_fails_closed(asynchronous: bool, case: str) -> None:
    session = GlobalVariableSession(asynchronous)
    session.corrupt = case == "corrupt"
    session.fail = case == "driver"
    payload = TenantContextPayload("a", variables={"region": "eu"} if case == "unmapped" else None)
    with pytest.raises(TenantContextError) as caught:
        await _invoke(_strategy(asynchronous).checkout, session, payload)
    if case == "unmapped":
        assert session.statements == []
    if case == "driver":
        assert isinstance(caught.value.__cause__, RuntimeError)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("case", ["corrupt", "driver"])
async def test_checkin_requires_verified_restore(asynchronous: bool, case: str) -> None:
    session = GlobalVariableSession(asynchronous)
    strategy = _strategy(asynchronous)
    state = await _invoke(strategy.checkout, session, TenantContextPayload("a"))
    session.corrupt = case == "corrupt"
    session.fail = case == "driver"
    with pytest.raises(TenantCleanupError) as caught:
        await _invoke(strategy.checkin, session, state)
    if case == "driver":
        assert isinstance(caught.value.__cause__, RuntimeError)


@pytest.mark.parametrize("config_type", [Db2SyncConfig, Db2AsyncConfig])
@pytest.mark.parametrize("shorthand", [True, "default", "rcac", "global_variable"])
def test_config_shorthands_select_the_global_variable_strategy(config_type: Any, shorthand: Any) -> None:
    config = config_type(driver_features={"tenant_strategy": shorthand})
    expected = Db2AsyncGlobalVariableStrategy if config_type is Db2AsyncConfig else Db2SyncGlobalVariableStrategy
    assert type(config.driver_features["tenant_strategy"]) is expected


@pytest.mark.parametrize("config_type", [Db2SyncConfig, Db2AsyncConfig])
async def test_tenant_config_rejects_standalone_connections(config_type: Any) -> None:
    config = config_type(driver_features={"tenant_strategy": "rcac"})
    with pytest.raises(ImproperConfigurationError, match="leases"):
        await _invoke(config.create_connection)


class _Connection:
    def __init__(self) -> None:
        self.events: list[str] = []

    def close(self) -> None:
        self.events.append("close")

    def rollback(self) -> None:
        self.events.append("rollback")


@pytest.mark.parametrize("checkin_fails", [False, True])
def test_sync_lease_uses_one_standalone_connection_and_always_closes_it(checkin_fails: bool) -> None:
    connection = _Connection()

    class Pool:
        def new_connection(self) -> _Connection:
            return connection

    class Strategy:
        def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
            return TenantLeaseState(payload)

        def checkin(self, connection: Any, state: TenantLeaseState) -> None:
            if checkin_fails:
                msg = "restore failed"
                raise RuntimeError(msg)

    config = type("Config", (), {"provide_pool": lambda self: Pool()})()
    lease = Db2SyncTenantLease(config, Strategy())
    with TenantContext.scope("a"):
        assert lease.acquire() is connection
    if checkin_fails:
        with pytest.raises(TenantCleanupError):
            lease.release()
        assert connection.events == ["rollback", "close"]
    else:
        lease.release()
        assert connection.events == ["rollback", "close"]


@pytest.mark.parametrize("checkin_fails", [False, True])
async def test_async_lease_returns_verified_connections_and_discards_the_rest(checkin_fails: bool) -> None:
    events: list[str] = []

    class Connection:
        async def rollback(self) -> None:
            events.append("rollback")

    connection = Connection()

    class Pool:
        async def acquire(self) -> Connection:
            return connection

        async def release(self, conn: Any) -> None:
            events.append("release")

        async def discard(self, conn: Any) -> None:
            events.append("discard")

    pool = Pool()

    class Strategy:
        async def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
            return TenantLeaseState(payload)

        async def checkin(self, connection: Any, state: TenantLeaseState) -> None:
            if checkin_fails:
                msg = "restore failed"
                raise RuntimeError(msg)

    async def provide_pool() -> Pool:
        return pool

    config = type("Config", (), {"provide_pool": staticmethod(provide_pool)})()
    lease = Db2AsyncTenantLease(config, Strategy())
    with TenantContext.scope("a"):
        assert await lease.acquire() is connection
    if checkin_fails:
        with pytest.raises(TenantCleanupError):
            await lease.release()
        assert events == ["rollback", "discard"]
    else:
        await lease.release()
        assert events == ["rollback", "release"]
