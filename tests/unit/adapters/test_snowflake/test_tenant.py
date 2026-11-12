"""Snowflake tenant strategies bind immutable session attributes on one connection per lease."""

import re
import threading
from typing import Any, cast

import pytest

from sqlspec.adapters.snowflake import SnowflakeConfig
from sqlspec.adapters.snowflake.tenant import (
    SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE,
    SnowflakeSessionAttributeStrategy,
    SnowflakeTenantLease,
    validate_session_attribute,
)
from sqlspec.config import LifecycleConfig
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import (
    DatabaseConnectionError,
    ImproperConfigurationError,
    TenantCleanupError,
    TenantContextError,
)
from sqlspec.observability import ObservabilityConfig
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

_SET_CALL = re.compile(r"CALL SET_SYS_CONTEXT\((\?|:1|%s), (\?|:2|%s), (\?|:3|%s)\)")


class SessionAttributeCursor:
    """Model SET_SYS_CONTEXT and SYS_CONTEXT at the connector cursor boundary."""

    def __init__(self, connection: "SessionAttributeConnection") -> None:
        self.connection = connection
        self._row: tuple[str | None, ...] | None = None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        connection = self.connection
        connection.statements.append((sql, tuple(params)))
        if connection.fail:
            msg = "driver failure"
            raise RuntimeError(msg)
        if _SET_CALL.fullmatch(sql):
            namespace, key, value = params
            assert namespace == SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE
            if key in connection.attributes:
                msg = f"Cannot overwrite context value: {key}"
                raise RuntimeError(msg)
            connection.attributes[key] = "corrupted" if connection.corrupt else value
            return
        assert sql.startswith("SELECT SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', ")
        self._row = tuple(connection.attributes.get(key) for key in params)

    def fetchone(self) -> tuple[str | None, ...] | None:
        row, self._row = self._row, None
        return row

    def close(self) -> None:
        self.connection.cursor_closes += 1


class SessionAttributeConnection:
    """Snowflake session whose custom attributes cannot be overwritten or cleared."""

    def __init__(self, paramstyle: str = "qmark") -> None:
        self.paramstyle = paramstyle
        self.attributes: dict[str, str] = {}
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.events: list[str] = []
        self.fail = False
        self.corrupt = False
        self.close_leaves_open = False
        self.cursor_closes = 0
        self._closed = False

    def cursor(self) -> SessionAttributeCursor:
        return SessionAttributeCursor(self)

    def rollback(self) -> None:
        self.events.append("rollback")

    def close(self) -> None:
        self.events.append("close")
        self._closed = not self.close_leaves_open

    def is_closed(self) -> bool:
        return self._closed


@pytest.mark.parametrize("name", ["", "1tenant", "tenant-id", "x" * 256, None, 7])
def test_session_attribute_names_are_validated(name: Any) -> None:
    with pytest.raises(ImproperConfigurationError, match="session attributes"):
        validate_session_attribute(name)


def test_session_attribute_names_keep_their_case() -> None:
    assert validate_session_attribute("Tenant_Id") == "Tenant_Id"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tenant_attribute": "bad name"},
        {"variable_keys": {"region": "tenant_id"}},
        {"variable_keys": {"": "region"}},
        {"variable_keys": {"region": "bad-name"}},
    ],
)
def test_strategy_rejects_invalid_or_colliding_attributes(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        SnowflakeSessionAttributeStrategy(**kwargs)


@pytest.mark.parametrize(
    ("paramstyle", "set_sql"),
    [
        ("qmark", "CALL SET_SYS_CONTEXT(?, ?, ?)"),
        ("numeric", "CALL SET_SYS_CONTEXT(:1, :2, :3)"),
        ("pyformat", "CALL SET_SYS_CONTEXT(%s, %s, %s)"),
    ],
)
def test_checkout_binds_values_and_verifies_the_session(paramstyle: str, set_sql: str) -> None:
    connection = SessionAttributeConnection(paramstyle)
    strategy = SnowflakeSessionAttributeStrategy(variable_keys={"region": "tenant_region"})
    payload = TenantContextPayload("acme", {"region": "west"})

    state = strategy.checkout(connection, payload)

    assert state.payload is payload
    assert dict(state.baseline) == {}
    assert connection.attributes == {"tenant_id": "acme", "tenant_region": "west"}
    assert [statement for statement, _ in connection.statements[1:3]] == [set_sql, set_sql]
    assert connection.statements[1][1] == (SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE, "tenant_id", "acme")
    assert connection.statements[-1][1] == ("tenant_id", "tenant_region")
    assert all("acme" not in statement and "west" not in statement for statement, _ in connection.statements)
    assert connection.cursor_closes == 1


def test_checkout_rejects_unmapped_payload_variables() -> None:
    connection = SessionAttributeConnection()
    with pytest.raises(TenantContextError, match="mapping"):
        SnowflakeSessionAttributeStrategy().checkout(connection, TenantContextPayload("acme", {"region": "west"}))
    assert connection.statements == []


@pytest.mark.parametrize("case", ["already_bound", "driver", "corrupt"])
def test_checkout_fails_closed(case: str) -> None:
    connection = SessionAttributeConnection()
    if case == "already_bound":
        connection.attributes["tenant_id"] = "other"
    connection.fail = case == "driver"
    connection.corrupt = case == "corrupt"

    with pytest.raises(TenantContextError) as caught:
        SnowflakeSessionAttributeStrategy().checkout(connection, TenantContextPayload("acme"))

    if case == "already_bound":
        assert not any(statement.startswith("CALL") for statement, _ in connection.statements)
        assert connection.attributes == {"tenant_id": "other"}
    if case == "driver":
        assert isinstance(caught.value.__cause__, RuntimeError)
    assert connection.cursor_closes == 1


def test_bound_session_cannot_be_checked_out_again() -> None:
    connection = SessionAttributeConnection()
    strategy = SnowflakeSessionAttributeStrategy()
    strategy.checkout(connection, TenantContextPayload("acme"))

    with pytest.raises(TenantContextError, match="already carries"):
        strategy.checkout(connection, TenantContextPayload("globex"))
    assert connection.attributes == {"tenant_id": "acme"}


@pytest.mark.parametrize("shorthand", [True, "default", "rls", "session_attributes"])
def test_config_shorthands_select_the_session_attribute_strategy(shorthand: Any) -> None:
    config = SnowflakeConfig(connection_config={"account": "acct"}, driver_features={"tenant_strategy": shorthand})
    assert type(config.driver_features["tenant_strategy"]) is SnowflakeSessionAttributeStrategy
    assert config.tenant_lease_type is SnowflakeTenantLease


def test_tenant_config_rejects_standalone_connections() -> None:
    config = SnowflakeConfig(connection_config={"account": "acct"}, driver_features={"tenant_strategy": "rls"})
    with pytest.raises(ImproperConfigurationError, match="leases"):
        config.create_connection()


class _Pool:
    def __init__(self, connection: SessionAttributeConnection) -> None:
        self.connection = connection
        self.opened = 0

    def new_connection(self) -> SessionAttributeConnection:
        self.opened += 1
        return self.connection

    def discard_connection(self, connection: SessionAttributeConnection) -> None:
        connection.close()


def _lease(connection: SessionAttributeConnection, strategy: Any = None) -> tuple[SnowflakeTenantLease, _Pool]:
    pool = _Pool(connection)
    config = type("Config", (), {"provide_pool": lambda self: pool})()
    return SnowflakeTenantLease(config, strategy or SnowflakeSessionAttributeStrategy()), pool


@pytest.mark.parametrize("tenant", ["acme", TenantContextPayload.bypass()])
def test_lease_binds_a_standalone_connection_and_always_closes_it(tenant: Any) -> None:
    connection = SessionAttributeConnection()
    lease, pool = _lease(connection)

    with TenantContext.scope(tenant):
        assert lease.acquire() is connection
    assert connection.attributes == ({} if isinstance(tenant, TenantContextPayload) else {"tenant_id": "acme"})
    lease.release()

    assert pool.opened == 1
    assert connection.events == ["rollback", "close"]
    assert connection.is_closed()


def test_lease_reports_a_connection_that_does_not_close() -> None:
    connection = SessionAttributeConnection()
    connection.close_leaves_open = True
    lease, _ = _lease(connection)
    with TenantContext.scope("acme"):
        lease.acquire()

    with pytest.raises(TenantCleanupError):
        lease.release()
    assert connection.events == ["rollback", "close", "close"]


def test_lease_disposes_of_a_connection_whose_checkout_fails() -> None:
    connection = SessionAttributeConnection()
    connection.corrupt = True
    lease, _ = _lease(connection)

    with TenantContext.scope("acme"), pytest.raises(TenantContextError):
        lease.acquire()
    assert connection.events == ["close"]


def test_lease_checkin_failure_still_closes_the_connection() -> None:
    class FailingCheckin(SnowflakeSessionAttributeStrategy):
        def checkin(self, connection: Any, state: TenantLeaseState) -> None:
            msg = "checkin failed"
            raise RuntimeError(msg)

    connection = SessionAttributeConnection()
    lease, _ = _lease(connection, FailingCheckin())
    with TenantContext.scope("acme"):
        lease.acquire()

    with pytest.raises(TenantCleanupError):
        lease.release()
    assert connection.events == ["rollback", "close"]


def test_tenant_connections_bypass_the_pool_and_close_after_each_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    """provide_connection() leases open a fresh session per tenant and never return it to the pool."""
    connector = pytest.importorskip("snowflake.connector")
    opened: list[SessionAttributeConnection] = []

    def connect(**_: Any) -> SessionAttributeConnection:
        connection = SessionAttributeConnection()
        opened.append(connection)
        return connection

    monkeypatch.setattr(connector, "connect", connect)
    config = SnowflakeConfig(
        connection_config={"account": "acct", "user": "u", "password": "p"},
        driver_features={"tenant_strategy": "session_attributes"},
    )
    try:
        for tenant in ("acme", "globex"):
            with TenantContext.scope(tenant), config.provide_connection() as connection:
                assert cast("SessionAttributeConnection", connection).attributes == {"tenant_id": tenant}
        assert [connection.attributes for connection in opened] == [{"tenant_id": "acme"}, {"tenant_id": "globex"}]
        assert all(connection.is_closed() for connection in opened)
        assert config.provide_pool().size() == 0
    finally:
        config.close_pool()


def _tenant_config(
    monkeypatch: pytest.MonkeyPatch, observability_config: ObservabilityConfig | None = None, **pool: Any
) -> tuple[SnowflakeConfig, list[SessionAttributeConnection]]:
    connector = pytest.importorskip("snowflake.connector")
    opened: list[SessionAttributeConnection] = []

    def connect(**_: Any) -> SessionAttributeConnection:
        connection = SessionAttributeConnection()
        opened.append(connection)
        return connection

    monkeypatch.setattr(connector, "connect", connect)
    config = SnowflakeConfig(
        connection_config={"account": "acct", "user": "u", "password": "p", **pool},
        driver_features={"tenant_strategy": "session_attributes"},
        observability_config=observability_config,
    )
    return config, opened


def test_tenant_leases_beyond_pool_max_size_wait_for_a_free_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    config, opened = _tenant_config(monkeypatch, pool_max_size=1, pool_timeout=5.0)
    waiting_lease: list[SessionAttributeConnection] = []

    def second_lease() -> None:
        with TenantContext.scope("globex"), config.provide_connection() as connection:
            waiting_lease.append(cast("SessionAttributeConnection", connection))

    try:
        with TenantContext.scope("acme"), config.provide_connection():
            waiter = threading.Thread(target=second_lease)
            waiter.start()
            waiter.join(timeout=0.2)
            assert waiter.is_alive()
            assert len(opened) == 1
        waiter.join(timeout=5.0)
        assert not waiter.is_alive()
        assert waiting_lease == [opened[1]]
        assert all(connection.is_closed() for connection in opened)
    finally:
        config.close_pool()


def test_tenant_lease_slot_wait_times_out_and_failed_opens_free_their_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    config, opened = _tenant_config(monkeypatch, pool_max_size=1, pool_timeout=0.05)
    try:
        with TenantContext.scope("acme"), config.provide_connection():
            with pytest.raises(DatabaseConnectionError, match="Timed out"), config.provide_connection():
                pass
            assert len(opened) == 1

        connector = pytest.importorskip("snowflake.connector")
        succeed = connector.connect

        def fail(**_: Any) -> SessionAttributeConnection:
            msg = "login failed"
            raise OSError(msg)

        monkeypatch.setattr(connector, "connect", fail)
        with TenantContext.scope("acme"), pytest.raises(OSError, match="login failed"), config.provide_connection():
            pass
        monkeypatch.setattr(connector, "connect", succeed)
        with TenantContext.scope("globex"), config.provide_connection() as connection:
            assert cast("SessionAttributeConnection", connection).attributes == {"tenant_id": "globex"}
        assert len(opened) == 2
    finally:
        config.close_pool()


@pytest.mark.parametrize("checkout_fails", [False, True])
def test_tenant_lease_close_emits_destroy_paired_with_create(
    monkeypatch: pytest.MonkeyPatch, checkout_fails: bool
) -> None:
    events: list[tuple[str, Any]] = []
    lifecycle = cast(
        "LifecycleConfig",
        {
            "on_connection_create": [lambda context: events.append(("create", context["connection"]))],
            "on_connection_destroy": [lambda context: events.append(("destroy", context["connection"]))],
        },
    )
    config, opened = _tenant_config(
        monkeypatch, ObservabilityConfig(lifecycle=lifecycle), pool_max_size=1, pool_timeout=0.05
    )
    try:
        if checkout_fails:
            with (
                TenantContext.scope(TenantContextPayload("acme", {"region": "west"})),
                pytest.raises(TenantContextError),
                config.provide_connection(),
            ):
                pass
        else:
            with TenantContext.scope("acme"), config.provide_connection():
                pass
        assert events == [("create", opened[0]), ("destroy", opened[0])]
        assert opened[0].is_closed()
        with TenantContext.scope("globex"), config.provide_connection():
            pass
        assert len(opened) == 2
    finally:
        config.close_pool()
