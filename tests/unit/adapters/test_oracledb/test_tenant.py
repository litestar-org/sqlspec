"""Oracle tenant strategies preserve hooks and verify database state."""

from inspect import isawaitable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.core import OracleSessionContextConfig, apply_oracle_session_trace_attributes
from sqlspec.adapters.oracledb.tenant import (
    OracleAsyncIdentifierStrategy,
    OracleAsyncVPDStrategy,
    OracleSyncIdentifierStrategy,
    OracleSyncVPDStrategy,
    compose_tenant_session_hook,
    tenant_client_identifier,
)
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.tenant import TenantContext, TenantContextPayload


async def _invoke(function: Any, *args: Any) -> Any:
    result = function(*args)
    return await result if isawaitable(result) else result


def _connection(mode: str, connection_type: Any = SimpleNamespace) -> Any:
    connection = connection_type(client_identifier=None, calls=[], dirty=False, ignore_identifier=False)
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor

    def execute(statement: str, binds: dict[str, Any] | None = None) -> None:
        connection.calls.append((statement, binds))
        if connection.ignore_identifier:
            connection.client_identifier = None
        if "CLEAR_IDENTIFIER" in statement:
            connection.client_identifier = "dirty" if connection.dirty else None
        if "SYS_CONTEXT" in statement:
            cursor.fetchone.return_value = (connection.client_identifier or None,)

    cursor.execute = AsyncMock(side_effect=execute) if mode == "async" else MagicMock(side_effect=execute)
    cursor.fetchone = AsyncMock() if mode == "async" else MagicMock()
    connection.cursor = MagicMock(return_value=cursor)
    return connection


@pytest.mark.parametrize("hook", [None, {"module": "billing"}, OracleSessionContextConfig(edition="E1")])
def test_tenant_hook_composes_without_mutating_source(hook: Any) -> None:
    composed = compose_tenant_session_hook(hook)
    assert composed.client_identifier is tenant_client_identifier
    assert composed.clear_identifier is True
    assert composed.reset_on_release is True
    if isinstance(hook, dict):
        assert composed.module == "billing"
        assert hook == {"module": "billing"}
    elif hook is not None:
        assert composed.edition == "E1"
        assert hook.client_identifier is None


def test_tenant_hook_keeps_every_session_option() -> None:
    hook = OracleSessionContextConfig(
        module="m",
        action="a",
        edition="E1",
        time_zone="UTC",
        nls_parameters={"NLS_DATE_FORMAT": "YYYY-MM-DD"},
        ecid="e",
        reset_package=True,
        clear_context_namespaces=("app",),
        apex_detach=True,
        contexts={"app": {"tenant": "acme"}},
        ebs_apps_initialize=(101, 202, 303),
        mo_global_policy=("S", 84),
        apex_session=(100, 1, 999),
        reset_on_release=False,
    )
    composed = compose_tenant_session_hook(hook)
    assert composed.module == "m"
    assert composed.action == "a"
    assert composed.edition == "E1"
    assert composed.time_zone == "UTC"
    assert composed.nls_parameters == {"NLS_DATE_FORMAT": "YYYY-MM-DD"}
    assert composed.ecid == "e"
    assert composed.contexts == {"app": {"tenant": "acme"}}
    assert composed.context_attributes == {"app": {"tenant": "acme"}}
    assert composed.ebs_apps_initialize == (101, 202, 303)
    assert composed.mo_global_policy == ("S", 84)
    assert composed.apex_session == (100, 1, 999)
    assert composed.reset_package is True
    assert composed.clear_context_namespaces == ("app",)
    assert composed.apex_detach is True
    assert composed.client_identifier is tenant_client_identifier
    assert composed.clear_identifier is True
    assert composed.reset_on_release is True


def test_tenant_hook_rejects_conflicting_client_identifier() -> None:
    with pytest.raises(ImproperConfigurationError, match="client_identifier"):
        compose_tenant_session_hook({"client_identifier": "trace"})
    assert compose_tenant_session_hook({"client_identifier": tenant_client_identifier}).clear_identifier is True


@pytest.mark.parametrize("tenant_id", ["a", "x" * 64, "é" * 32])
def test_tenant_identifier_accepts_bounded_values(tenant_id: str) -> None:
    with TenantContext.scope(tenant_id):
        assert tenant_client_identifier() == tenant_id


@pytest.mark.parametrize("tenant_id", ["x" * 65, "é" * 33])
def test_tenant_identifier_rejects_oversize_values(tenant_id: str) -> None:
    with TenantContext.scope(tenant_id), pytest.raises(TenantContextError, match="64"):
        tenant_client_identifier()


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_identifier_strategy_applies_captured_payload_and_clears(mode: str) -> None:
    connection = _connection(mode)
    strategy = OracleSyncIdentifierStrategy() if mode == "sync" else OracleAsyncIdentifierStrategy()
    payload = TenantContextPayload("a")
    with TenantContext.scope("outer"):
        state = await _invoke(strategy.checkout, connection, payload)
        assert TenantContext.get_tenant_id() == "outer"
        assert state.payload is payload
        assert connection.client_identifier == "a"
        await _invoke(strategy.checkin, connection, state)
    assert connection.client_identifier is None
    assert sum("SET_IDENTIFIER" in statement for statement, _ in connection.calls) == 1
    assert sum("CLEAR_IDENTIFIER" in statement for statement, _ in connection.calls) == 1
    assert sum("SYS_CONTEXT" in statement for statement, _ in connection.calls) == 2


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_identifier_strategy_detects_unapplied_identity(mode: str) -> None:
    connection = _connection(mode)
    connection.ignore_identifier = True
    strategy = OracleSyncIdentifierStrategy() if mode == "sync" else OracleAsyncIdentifierStrategy()
    with pytest.raises(TenantContextError, match="verif"):
        await _invoke(strategy.checkout, connection, TenantContextPayload("a"))


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_identifier_strategy_detects_dirty_release(mode: str) -> None:
    connection = _connection(mode)
    strategy = OracleSyncIdentifierStrategy() if mode == "sync" else OracleAsyncIdentifierStrategy()
    state = await _invoke(strategy.checkout, connection, TenantContextPayload("a"))
    connection.dirty = True
    with pytest.raises(TenantCleanupError, match="verif"):
        await _invoke(strategy.checkin, connection, state)


@pytest.mark.parametrize("preserve", [False, True])
def test_oracle_tracing_preserves_tenant_identifier(preserve: bool) -> None:
    connection = SimpleNamespace(client_identifier="tenant-a")
    with CorrelationContext.context("header"):
        apply_oracle_session_trace_attributes(connection, module="billing", preserve_client_identifier=preserve)
    assert connection.module == "billing"
    assert connection.client_identifier == ("tenant-a" if preserve else "header")


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("strategy", [None, object()])
async def test_driver_tracing_respects_resolved_strategy(mode: str, strategy: Any) -> None:
    from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver

    connection = _connection(mode)
    connection.transaction_in_progress = False
    cursor = connection.cursor.return_value
    cursor.description = [("VAL", "NUMBER")]
    cursor.rowcount = 1
    cursor.fetchall = AsyncMock(return_value=[(1,)]) if mode == "async" else MagicMock(return_value=[(1,)])
    driver_cls = OracleSyncDriver if mode == "sync" else OracleAsyncDriver
    driver = driver_cls(connection, driver_features={"enable_session_tracing": True, "tenant_strategy": strategy})
    connection.client_identifier = "tenant-a"
    with CorrelationContext.context("hdr"):
        await _invoke(driver.execute, "SELECT 1 FROM DUAL")
        await _invoke(driver.execute, "SELECT 1 FROM DUAL")
    assert connection.client_identifier == ("hdr" if strategy is None else "tenant-a")
    assert connection.module == "sqlspec"


def _vpd_connection(mode: str) -> Any:
    connection = _connection(mode)
    connection.context_value = None
    connection.ignore_context_set = False
    connection.ignore_context_clear = False
    connection.fail_context_set = False
    connection.fail_context_clear = False
    cursor = connection.cursor.return_value
    identifier_execute = cursor.execute.side_effect

    def execute(statement: str, binds: dict[str, Any] | None = None) -> None:
        identifier_execute(statement, binds)
        if statement.startswith("BEGIN app.ctx.set_tenant"):
            if connection.fail_context_set:
                raise RuntimeError("setter failed")
            if not connection.ignore_context_set:
                connection.context_value = (binds or {})["tenant_id"]
        elif statement.startswith("BEGIN app.ctx.clear_tenant"):
            if connection.fail_context_clear:
                raise RuntimeError("clearer failed")
            if not connection.ignore_context_clear:
                connection.context_value = None
        elif "SYS_CONTEXT(:namespace" in statement:
            cursor.fetchone.return_value = (connection.client_identifier or None, connection.context_value)

    cursor.execute.side_effect = execute
    return connection


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_vpd_strategy_binds_and_verifies_context(mode: str) -> None:
    from sqlspec.adapters.oracledb.tenant import OracleAsyncVPDStrategy, OracleSyncVPDStrategy

    connection = _vpd_connection(mode)
    strategy_cls = OracleSyncVPDStrategy if mode == "sync" else OracleAsyncVPDStrategy
    strategy = strategy_cls(
        set_procedure="app.ctx.set_tenant",
        clear_procedure="app.ctx.clear_tenant",
        namespace="tenant_ctx",
        attribute="tenant_id",
    )
    payload = TenantContextPayload("a'; DROP TABLE users;")
    state = await _invoke(strategy.checkout, connection, payload)
    assert connection.context_value == payload.tenant_id
    await _invoke(strategy.checkin, connection, state)
    assert connection.context_value is None
    assert connection.client_identifier is None
    statements = [statement for statement, _ in connection.calls]
    assert all(payload.tenant_id not in statement for statement in statements)
    assert ("BEGIN app.ctx.set_tenant(:tenant_id); END;", {"tenant_id": payload.tenant_id}) in connection.calls
    assert statements.index("BEGIN app.ctx.clear_tenant; END;") < next(
        i for i, statement in enumerate(statements) if "CLEAR_IDENTIFIER" in statement
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    "failure", ["ignore_context_set", "ignore_context_clear", "fail_context_set", "fail_context_clear"]
)
async def test_vpd_strategy_detects_setter_and_clear_failures(mode: str, failure: str) -> None:
    from sqlspec.adapters.oracledb.tenant import OracleAsyncVPDStrategy, OracleSyncVPDStrategy

    connection = _vpd_connection(mode)
    strategy_cls = OracleSyncVPDStrategy if mode == "sync" else OracleAsyncVPDStrategy
    strategy = strategy_cls(
        set_procedure="app.ctx.set_tenant",
        clear_procedure="app.ctx.clear_tenant",
        namespace="tenant_ctx",
        attribute="tenant_id",
    )
    if "set" in failure:
        setattr(connection, failure, True)
        with pytest.raises(TenantContextError) as checkout_error:
            await _invoke(strategy.checkout, connection, TenantContextPayload("a"))
        if failure == "fail_context_set":
            assert isinstance(checkout_error.value.__cause__, RuntimeError)
    else:
        state = await _invoke(strategy.checkout, connection, TenantContextPayload("a"))
        setattr(connection, failure, True)
        with pytest.raises(TenantCleanupError) as checkin_error:
            await _invoke(strategy.checkin, connection, state)
        if failure == "fail_context_clear":
            assert isinstance(checkin_error.value.__cause__, RuntimeError)


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_vpd_strategy_verifies_identifier_with_context(mode: str) -> None:
    from sqlspec.adapters.oracledb.tenant import OracleAsyncVPDStrategy, OracleSyncVPDStrategy

    strategy_cls = OracleSyncVPDStrategy if mode == "sync" else OracleAsyncVPDStrategy
    strategy = strategy_cls(
        set_procedure="app.ctx.set_tenant",
        clear_procedure="app.ctx.clear_tenant",
        namespace="tenant_ctx",
        attribute="tenant_id",
    )
    unapplied = _vpd_connection(mode)
    unapplied.ignore_identifier = True
    with pytest.raises(TenantContextError, match="identifier verification"):
        await _invoke(strategy.checkout, unapplied, TenantContextPayload("a"))

    connection = _vpd_connection(mode)
    state = await _invoke(strategy.checkout, connection, TenantContextPayload("a"))
    verification_reads = sum("SYS_CONTEXT" in statement for statement, _ in connection.calls)
    assert verification_reads == 1
    connection.dirty = True
    with pytest.raises(TenantCleanupError, match="identifier cleanup verification"):
        await _invoke(strategy.checkin, connection, state)
    assert sum("SYS_CONTEXT" in statement for statement, _ in connection.calls) == 2


@pytest.mark.parametrize("strategy_cls_name", ["OracleSyncVPDStrategy", "OracleAsyncVPDStrategy"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("set_procedure", "app; DROP"),
        ("set_procedure", 1),
        ("clear_procedure", "app..clear"),
        ("clear_procedure", 1),
        ("namespace", "a' OR 1=1"),
        ("attribute", ""),
    ],
)
def test_vpd_strategy_rejects_untrusted_identifiers(strategy_cls_name: str, field: str, value: object) -> None:
    from sqlspec.adapters.oracledb import tenant

    args: dict[str, object] = {
        "set_procedure": "app.ctx.set_tenant",
        "clear_procedure": "app.ctx.clear_tenant",
        "namespace": "tenant_ctx",
        "attribute": "tenant_id",
    }
    args[field] = value
    with pytest.raises(ImproperConfigurationError):
        getattr(tenant, strategy_cls_name)(**args)


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_identifier_cleanup_uses_nullable_sdk_identifier(mode: str) -> None:
    class NullableIdentifierConnection(SimpleNamespace):
        def __setattr__(self, name: str, value: Any) -> None:
            if name == "client_identifier" and value == "":
                raise RuntimeError("SDK rejects empty-string client identifier")
            super().__setattr__(name, value)

    connection = _connection(mode, NullableIdentifierConnection)
    strategy = OracleSyncIdentifierStrategy() if mode == "sync" else OracleAsyncIdentifierStrategy()
    state = await _invoke(strategy.checkout, connection, TenantContextPayload("a"))
    await _invoke(strategy.checkin, connection, state)
    assert connection.client_identifier is None


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("release_error", [False, True])
async def test_identifier_cleanup_resolves_hook_with_captured_tenant(mode: str, release_error: bool) -> None:
    connection = _connection(mode)
    cursor = connection.cursor.return_value
    native_execute = cursor.execute.side_effect
    contexts: dict[str, dict[str, str]] = {"ctx_b": {"tenant": "b"}}
    resolved: list[str] = []

    def tenant_contexts() -> dict[str, dict[str, str]]:
        payload = TenantContext.get()
        if payload is None:
            raise AssertionError("tenant hook requires a captured payload")
        resolved.append(payload.tenant_id)
        return {f"ctx_{payload.tenant_id}": {"tenant": payload.tenant_id}}

    def execute(statement: str, binds: dict[str, Any] | None = None) -> None:
        native_execute(statement, binds)
        if "DBMS_SESSION.SET_CONTEXT" in statement and binds is not None:
            contexts[binds["ctx_ns_0"]] = {binds["ctx_attr_0"]: binds["ctx_val_0"]}
        if "DBMS_SESSION.CLEAR_CONTEXT" in statement:
            if release_error:
                raise RuntimeError("context clear failed")
            for namespace in tuple(contexts):
                if f"DBMS_SESSION.CLEAR_CONTEXT('{namespace}')" in statement:
                    contexts.pop(namespace)

    cursor.execute.side_effect = execute
    strategy_cls = OracleSyncIdentifierStrategy if mode == "sync" else OracleAsyncIdentifierStrategy
    strategy = strategy_cls(OracleSessionContextConfig(contexts=tenant_contexts))
    state = await _invoke(strategy.checkout, connection, TenantContextPayload("a"))
    assert contexts == {"ctx_b": {"tenant": "b"}, "ctx_a": {"tenant": "a"}}
    with TenantContext.scope("b") as caller:
        if release_error:
            with pytest.raises(RuntimeError, match="context clear failed"):
                await _invoke(strategy.checkin, connection, state)
        else:
            await _invoke(strategy.checkin, connection, state)
        assert TenantContext.get() is caller
    assert resolved == ["a", "a"]
    release_sql = next(statement for statement, _ in connection.calls if "DBMS_SESSION.CLEAR_CONTEXT" in statement)
    assert "DBMS_SESSION.CLEAR_CONTEXT('ctx_a')" in release_sql
    assert contexts == (
        {"ctx_b": {"tenant": "b"}, "ctx_a": {"tenant": "a"}} if release_error else {"ctx_b": {"tenant": "b"}}
    )


def test_tenant_hook_keeps_deferred_options_and_context_alias() -> None:
    def deferred_text() -> str:
        raise AssertionError("composing a hook must not evaluate deferred text")

    def deferred_nls() -> dict[str, str]:
        raise AssertionError("composing a hook must not evaluate deferred NLS settings")

    def deferred_contexts() -> dict[str, dict[str, str]]:
        raise AssertionError("composing a hook must not evaluate deferred contexts")

    hook = OracleSessionContextConfig(
        module=deferred_text,
        action=deferred_text,
        ecid=deferred_text,
        edition=deferred_text,
        time_zone=deferred_text,
        nls_parameters=deferred_nls,
        context_attributes=deferred_contexts,
    )
    composed = compose_tenant_session_hook(hook)
    assert composed.module is deferred_text
    assert composed.action is deferred_text
    assert composed.ecid is deferred_text
    assert composed.edition is deferred_text
    assert composed.time_zone is deferred_text
    assert composed.nls_parameters is deferred_nls
    assert composed.contexts is deferred_contexts
    assert composed.context_attributes is deferred_contexts


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("kind", ["identifier", "vpd"])
async def test_oracle_config_auto_attaches_session_context_hook_to_tenant_strategy(mode: str, kind: str) -> None:
    if mode == "sync":
        base: Any = (
            OracleSyncVPDStrategy(
                set_procedure="app.ctx.set_tenant",
                clear_procedure="app.ctx.clear_tenant",
                namespace="tenant_ctx",
                attribute="tenant_id",
            )
            if kind == "vpd"
            else OracleSyncIdentifierStrategy()
        )
        cfg_cls: Any = OracleSyncConfig
    else:
        base = (
            OracleAsyncVPDStrategy(
                set_procedure="app.ctx.set_tenant",
                clear_procedure="app.ctx.clear_tenant",
                namespace="tenant_ctx",
                attribute="tenant_id",
            )
            if kind == "vpd"
            else OracleAsyncIdentifierStrategy()
        )
        cfg_cls = OracleAsyncConfig

    cfg_one = cfg_cls(driver_features={"tenant_strategy": base, "session_context_hook": {"module": "billing"}})
    cfg_two = cfg_cls(driver_features={"tenant_strategy": base, "session_context_hook": {"module": "audit"}})

    assert base.source_hook is None
    resolved_one = cfg_one.driver_features["tenant_strategy"]
    resolved_two = cfg_two.driver_features["tenant_strategy"]
    assert resolved_one is not base
    assert resolved_two is not base
    assert cfg_one.driver_features["session_context_hook"].module == "billing"
    assert cfg_two.driver_features["session_context_hook"].module == "audit"
    assert resolved_one.source_hook == cfg_one.driver_features["session_context_hook"]
    assert resolved_one._hook.client_identifier is tenant_client_identifier

    conn = _vpd_connection(mode) if kind == "vpd" else _connection(mode)
    state = await _invoke(resolved_one.checkout, conn, TenantContextPayload("acme"))
    assert conn.module == "billing"
    await _invoke(resolved_one.checkin, conn, state)
    assert conn.module == ""

    explicit = (
        OracleSyncVPDStrategy(
            set_procedure="app.ctx.set_tenant",
            clear_procedure="app.ctx.clear_tenant",
            namespace="tenant_ctx",
            attribute="tenant_id",
            hook={"module": "billing"},
        )
        if mode == "sync"
        else OracleAsyncVPDStrategy(
            set_procedure="app.ctx.set_tenant",
            clear_procedure="app.ctx.clear_tenant",
            namespace="tenant_ctx",
            attribute="tenant_id",
            hook={"module": "billing"},
        )
    )
    with pytest.raises(ImproperConfigurationError, match="session_context_hook"):
        cfg_cls(driver_features={"tenant_strategy": explicit, "session_context_hook": {"module": "other"}})


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("hook_fails", [False, True])
async def test_bypass_lease_applies_and_releases_session_context_hook(mode: str, hook_fails: bool) -> None:
    connection = _connection(mode)
    events: list[str] = []

    class HookConnection(SimpleNamespace):
        def __setattr__(self, name: str, value: Any) -> None:
            if name == "module":
                if hook_fails and value == "billing":
                    raise RuntimeError("module rejected")
                events.append(f"module={value}")
            super().__setattr__(name, value)

    connection = HookConnection(**vars(connection))

    def record(name: str) -> Any:
        def call(conn: Any) -> None:
            assert conn is connection
            events.append(name)

        return AsyncMock(side_effect=call) if mode == "async" else MagicMock(side_effect=call)

    pool = SimpleNamespace(
        acquire=(AsyncMock if mode == "async" else MagicMock)(return_value=connection),
        release=record("release"),
        drop=record("drop"),
    )
    cfg_cls: Any = OracleSyncConfig if mode == "sync" else OracleAsyncConfig
    config = cfg_cls(driver_features={"tenant_strategy": "identifier", "session_context_hook": {"module": "billing"}})
    config.connection_instance = pool
    connection.rollback = (AsyncMock if mode == "async" else MagicMock)()

    async def use_connection() -> None:
        if mode == "sync":
            with config.provide_connection():
                events.append("body")
        else:
            async with config.provide_connection():
                events.append("body")

    with TenantContext.bypass():
        if hook_fails:
            with pytest.raises(RuntimeError, match="module rejected"):
                await use_connection()
        else:
            await use_connection()

    assert events == (["drop"] if hook_fails else ["module=billing", "body", "module=", "release"])


def test_sync_session_builds_one_session_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    config = OracleSyncConfig(driver_features={"tenant_strategy": "identifier"})
    created: list[Any] = []
    build = OracleSyncConfig._create_tenant_lease

    def record(self: OracleSyncConfig) -> Any:
        lease = build(self)
        created.append(lease)
        return lease

    monkeypatch.setattr(OracleSyncConfig, "_create_tenant_lease", record)
    config.provide_session()
    assert len(created) == 1
