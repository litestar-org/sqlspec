"""OracleDB configuration tests covering driver kwargs and typed options."""

import io
import zipfile
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from pathlib import Path
from ssl import TLSVersion
from typing import Any, NotRequired, cast, get_args, get_origin, get_type_hints
from unittest.mock import AsyncMock, MagicMock, Mock, call

import pytest
from oracledb import AuthMode, PoolGetMode, Purity

from sqlspec.adapters.oracledb import (
    OracleEndpoint,
    OracleSessionContextConfig,
    OracleTopologyConfig,
    build_connection_config,
    build_session_checkout_plsql,
    build_session_release_plsql,
)
from sqlspec.adapters.oracledb import config as oracle_config_module
from sqlspec.adapters.oracledb.config import (
    OracleAsyncConfig,
    OracleConnectionParams,
    OracleDriverFeatures,
    OraclePoolParams,
    OracleSyncConfig,
)
from sqlspec.exceptions import ImproperConfigurationError


class _StubConnection:
    version = "23.5.0.0.0"


def _unwrap_not_required(annotation: object) -> object:
    assert get_origin(annotation) is NotRequired
    return get_args(annotation)[0]


def _oracle_config_hints(typeddict: type[object]) -> dict[str, object]:
    globalns = dict(vars(oracle_config_module))
    globalns.update({
        "AuthMode": AuthMode,
        "Awaitable": Awaitable,
        "Callable": Callable,
        "PoolGetMode": PoolGetMode,
        "Purity": Purity,
        "TLSVersion": TLSVersion,
    })
    return get_type_hints(typeddict, globalns=globalns, localns=globalns, include_extras=True)


def _stub_sync_connection_setup(monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> None:
    monkeypatch.setattr(oracle_config_module, "register_numpy_handlers", lambda _connection: calls.append("numpy"))
    monkeypatch.setattr(oracle_config_module, "register_json_handlers", lambda _connection: calls.append("json"))
    monkeypatch.setattr(oracle_config_module, "register_uuid_handlers", lambda _connection: calls.append("uuid"))


def test_oracle_connection_params_expose_current_driver_options() -> None:
    """Connection params should mirror current python-oracledb connection knobs."""
    annotations = _oracle_config_hints(OracleConnectionParams)

    expected_options = {
        "access_token",
        "appcontext",
        "cclass",
        "connection_id_prefix",
        "debug_jdwp",
        "disable_oob",
        "driver_name",
        "events",
        "expire_time",
        "extra",
        "extra_auth_params",
        "externalauth",
        "handle",
        "https_proxy",
        "https_proxy_port",
        "instance_name",
        "machine",
        "matchanytag",
        "mode",
        "newpassword",
        "on_connect_callback",
        "osuser",
        "pool_boundary",
        "pool_name",
        "program",
        "protocol",
        "proxy_user",
        "purity",
        "sdu",
        "server_type",
        "shardingkey",
        "ssl_context",
        "ssl_server_cert_dn",
        "ssl_server_dn_match",
        "ssl_version",
        "stmtcachesize",
        "supershardingkey",
        "tag",
        "terminal",
        "thick_mode_dsn_passthrough",
        "use_sni",
        "use_tcp_fast_open",
        "wallet_password",
    }

    assert expected_options <= annotations.keys()


def test_oracle_pool_params_expose_current_pool_options_and_remove_threaded() -> None:
    """Pool params should include current pool options without stale ``threaded``."""
    annotations = _oracle_config_hints(OraclePoolParams)

    assert {
        "connectiontype",
        "getmode",
        "homogeneous",
        "max_lifetime_session",
        "max_sessions_per_shard",
        "on_connect_callback",
        "ping_timeout",
        "pool_alias",
        "pool_class",
        "soda_metadata_cache",
        "wait_timeout",
    } <= annotations.keys()
    assert "threaded" not in annotations


def test_oracle_config_finite_options_use_literals_and_driver_enums() -> None:
    """Finite Oracle settings should be typed more narrowly than plain ``str`` or ``Any``."""
    connection_hints = _oracle_config_hints(OracleConnectionParams)
    pool_hints = _oracle_config_hints(OraclePoolParams)
    driver_feature_hints = _oracle_config_hints(OracleDriverFeatures)

    assert set(get_args(_unwrap_not_required(connection_hints["protocol"]))) == {"tcp", "tcps"}
    assert set(get_args(_unwrap_not_required(connection_hints["server_type"]))) == {"dedicated", "pooled", "shared"}
    assert _unwrap_not_required(connection_hints["mode"]) is AuthMode
    assert _unwrap_not_required(connection_hints["purity"]) is Purity
    assert _unwrap_not_required(pool_hints["getmode"]) is PoolGetMode
    assert set(get_args(_unwrap_not_required(driver_feature_hints["vector_return_format"]))) == {
        "array",
        "list",
        "numpy",
    }
    assert set(get_args(_unwrap_not_required(driver_feature_hints["events_backend"]))) == {
        "aq",
        "poll_queue",
        "txeventq",
    }


def test_oracle_sync_create_pool_merges_extra_and_drops_stale_threaded(monkeypatch: pytest.MonkeyPatch) -> None:
    """``extra`` should merge as kwargs, while stale ``threaded`` should not reach python-oracledb."""
    seen_kwargs: dict[str, object] = {}

    def fake_create_pool(**kwargs: object) -> object:
        seen_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(oracle_config_module.oracledb, "create_pool", fake_create_pool)
    config = OracleSyncConfig(
        connection_config={"threaded": True, "user": "scott", "extra": {"pool_alias": "sqlspec-main", "use_sni": True}}
    )

    config._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert seen_kwargs["user"] == "scott"
    assert seen_kwargs["pool_alias"] == "sqlspec-main"
    assert seen_kwargs["use_sni"] is True
    assert "extra" not in seen_kwargs
    assert "threaded" not in seen_kwargs


def test_oracle_sync_minimal_pool_omits_tuning_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal pool config should not inject statement-cache or fetch tuning keys."""
    seen_kwargs: dict[str, object] = {}

    def fake_create_pool(**kwargs: object) -> object:
        seen_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(oracle_config_module.oracledb, "create_pool", fake_create_pool)

    OracleSyncConfig(connection_config={"user": "scott"})._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert seen_kwargs["user"] == "scott"
    assert callable(seen_kwargs["session_callback"])
    assert "stmtcachesize" not in seen_kwargs
    assert "arraysize" not in seen_kwargs
    assert "prefetchrows" not in seen_kwargs


def test_oracle_sync_connection_config_session_callback_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native pool ``session_callback`` should run in addition to SQLSpec setup."""
    calls: list[str] = []
    _stub_sync_connection_setup(monkeypatch, calls)

    def session_callback(_connection: object, _tag: str) -> None:
        calls.append("session_callback")

    def on_connection_create(_connection: object, _tag: str) -> None:
        calls.append("on_connection_create")

    config = OracleSyncConfig(
        connection_config={"session_callback": session_callback},
        driver_features={"on_connection_create": on_connection_create},
    )

    config._init_connection(cast(Any, _StubConnection()), "analytics")  # pyright: ignore[reportPrivateUsage]

    assert calls == ["numpy", "json", "uuid", "session_callback", "on_connection_create"]


@pytest.mark.anyio
async def test_oracle_async_connection_config_session_callback_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async native pool ``session_callback`` should be awaited when it returns an awaitable."""
    calls: list[str] = []
    _stub_sync_connection_setup(monkeypatch, calls)

    async def session_callback(_connection: object, _tag: str) -> None:
        calls.append("session_callback")

    async def on_connection_create(_connection: object, _tag: str) -> None:
        calls.append("on_connection_create")

    config = OracleAsyncConfig(
        connection_config={"session_callback": session_callback},
        driver_features={"on_connection_create": on_connection_create},
    )

    await config._init_connection(cast(Any, _StubConnection()), "analytics")  # pyright: ignore[reportPrivateUsage]

    assert calls == ["numpy", "json", "uuid", "session_callback", "on_connection_create"]


def test_build_connection_config_normalizes_aliases() -> None:
    """build_connection_config should normalize url, connection_string, and username."""
    from_url = build_connection_config({"url": "localhost/orclpdb1", "username": "scott"})
    assert from_url == {"dsn": "localhost/orclpdb1", "user": "scott"}

    from_conn_str = build_connection_config({"connection_string": "localhost/orclpdb1"})
    assert from_conn_str == {"dsn": "localhost/orclpdb1"}


def test_oracle_config_normalizes_aliases() -> None:
    """Oracle configs should normalize url, connection_string, and username to dsn and user."""
    sync_config = OracleSyncConfig(connection_config={"url": "localhost/orclpdb1", "username": "scott"})
    assert sync_config.connection_config["dsn"] == "localhost/orclpdb1"
    assert sync_config.connection_config["user"] == "scott"
    assert "url" not in sync_config.connection_config
    assert "username" not in sync_config.connection_config

    async_config = OracleAsyncConfig(connection_config={"connection_string": "localhost/orclpdb1", "username": "scott"})
    assert async_config.connection_config["dsn"] == "localhost/orclpdb1"
    assert async_config.connection_config["user"] == "scott"
    assert "connection_string" not in async_config.connection_config
    assert "username" not in async_config.connection_config


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_pool_close_preserves_native_borrowed_connection_guard(asynchronous: bool) -> None:
    pool = Mock()
    close = AsyncMock if asynchronous else Mock
    pool.close = close(side_effect=RuntimeError("connections remain checked out"))
    config = OracleAsyncConfig(connection_instance=pool) if asynchronous else OracleSyncConfig(connection_instance=pool)

    with pytest.raises(RuntimeError, match="checked out"):
        result = config._close_pool()
        if isawaitable(result):
            await result

    pool.close.assert_called_once_with()
    assert config.connection_instance is pool


@pytest.mark.parametrize("options", [{"thick_mode": True}, {"lib_dir": "/oracle/lib"}, {"soda_metadata_cache": True}])
@pytest.mark.parametrize("thin_mode", [False, True])
def test_sync_pool_initializes_requested_thick_mode(
    options: dict[str, Any], thin_mode: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize = Mock()
    create_pool = Mock()
    monkeypatch.setattr(oracle_config_module.oracledb, "init_oracle_client", initialize)
    monkeypatch.setattr(oracle_config_module.oracledb, "is_thin_mode", lambda: thin_mode)
    monkeypatch.setattr(oracle_config_module.oracledb, "create_pool", create_pool)
    config = OracleSyncConfig(connection_config={**options, "config_dir": "/oracle/config"})

    assert config._create_pool() is create_pool.return_value

    expected = {"config_dir": "/oracle/config"}
    if "lib_dir" in options:
        expected["lib_dir"] = options["lib_dir"]
    assert initialize.call_args_list == ([call(**expected)] if thin_mode else [])
    assert create_pool.call_args.kwargs == {
        **{key: value for key, value in options.items() if key not in {"thick_mode", "lib_dir"}},
        "config_dir": "/oracle/config",
        "session_callback": config._init_connection,
    }


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_explicit_thin_mode_is_consumed(asynchronous: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = Mock()
    create_pool = Mock()
    monkeypatch.setattr(oracle_config_module.oracledb, "init_oracle_client", initialize)
    monkeypatch.setattr(oracle_config_module.oracledb, "is_thin_mode", lambda: True)
    monkeypatch.setattr(
        oracle_config_module.oracledb, "create_pool_async" if asynchronous else "create_pool", create_pool
    )
    config_type = OracleAsyncConfig if asynchronous else OracleSyncConfig
    config = config_type(connection_config={"thick_mode": False})
    result = config._create_pool()
    result = await result if isawaitable(result) else result

    assert result is create_pool.return_value
    initialize.assert_not_called()
    assert create_pool.call_args.kwargs == {"session_callback": config._init_connection}


@pytest.mark.parametrize("options", [{"thick_mode": True}, {"lib_dir": "/oracle/lib"}])
async def test_async_pool_rejects_thick_mode(options: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = Mock()
    create_pool = Mock()
    monkeypatch.setattr(oracle_config_module.oracledb, "init_oracle_client", initialize)
    monkeypatch.setattr(oracle_config_module.oracledb, "create_pool_async", create_pool)
    config = OracleAsyncConfig(connection_config=options)

    with pytest.raises(ImproperConfigurationError, match="only supports Thin mode"):
        await config._create_pool()

    initialize.assert_not_called()
    create_pool.assert_not_called()


async def test_async_soda_option_does_not_initialize_thick_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = Mock()
    create_pool = Mock()
    monkeypatch.setattr(oracle_config_module.oracledb, "init_oracle_client", initialize)
    monkeypatch.setattr(oracle_config_module.oracledb, "create_pool_async", create_pool)
    config = OracleAsyncConfig(connection_config={"soda_metadata_cache": True})

    assert await config._create_pool() is create_pool.return_value

    initialize.assert_not_called()
    assert create_pool.call_args.kwargs["soda_metadata_cache"] is True


def test_oracle_config_wallet_zip_extracted_and_cleaned_up_on_close_pool() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ewallet.pem", b"pem-bytes")
    pool_mock = Mock()
    config = OracleSyncConfig(
        connection_instance=pool_mock,
        connection_config={
            "user": "admin",
            "password": "pw",
            "wallet_zip": buf.getvalue(),
            "topology": OracleTopologyConfig(
                endpoints=(OracleEndpoint(host="adb.oraclecloud.com", port=1522, protocol="tcps"),),
                service_name="adb_high.adb.oraclecloud.com",
            ),
        },
    )
    wallet_dir = config._extracted_wallet_dir
    assert wallet_dir is not None
    assert Path(wallet_dir).exists()
    assert config.connection_config["wallet_location"] == str(wallet_dir)
    assert config.connection_config["config_dir"] == str(wallet_dir)
    assert "(SERVICE_NAME=adb_high.adb.oraclecloud.com)" in config.connection_config["dsn"]

    config._close_pool()
    assert not Path(wallet_dir).exists()


def test_oracle_derive_pool_preserves_popped_callbacks_and_applies_overrides() -> None:
    def _session_cb(conn: Any, tag: str) -> None:
        return None

    def _conn_hook(conn: Any, tag: str) -> None:
        return None

    session_cb: Any = _session_cb
    conn_hook: Any = _conn_hook

    base_sync = OracleSyncConfig(
        connection_config={"dsn": "tp", "session_callback": session_cb},
        driver_features={"on_connection_create": conn_hook, "arraysize": 100},
        extension_config={"litestar": {"commit_mode": "manual"}},
    )
    derived_sync = base_sync.derive_pool(
        arraysize=500, cclass="BATCH", extension_config={"adk": {"session_table": "tenant_sessions"}}
    )
    assert derived_sync.connection_instance is None
    assert derived_sync._pool_session_callback is session_cb
    assert derived_sync._user_connection_hook is conn_hook
    assert derived_sync.driver_features["arraysize"] == 500
    assert derived_sync.connection_config["cclass"] == "BATCH"
    assert base_sync.driver_features["arraysize"] == 100
    assert "cclass" not in base_sync.connection_config
    assert "adk" in derived_sync.extension_config
    assert "adk" not in base_sync.extension_config

    base_async = OracleAsyncConfig(
        connection_config={"dsn": "tp", "session_callback": session_cb},
        driver_features={"on_connection_create": conn_hook, "fetch_lobs": False, "arraysize": 100},
    )
    batch_cfg = base_async.derive_pool(fetch_lobs=True, arraysize=250, min=1, max=4, stmtcachesize=0, cclass="BATCH")
    assert batch_cfg.connection_instance is None
    assert batch_cfg._pool_session_callback is session_cb
    assert batch_cfg._user_connection_hook is conn_hook
    assert batch_cfg.driver_features["fetch_lobs"] is True
    assert batch_cfg.driver_features["arraysize"] == 250
    assert batch_cfg.connection_config["cclass"] == "BATCH"
    assert batch_cfg.connection_config["min"] == 1
    assert batch_cfg.connection_config["max"] == 4
    assert batch_cfg.connection_config["stmtcachesize"] == 0
    assert base_async.driver_features["fetch_lobs"] is False
    assert base_async.driver_features["arraysize"] == 100


def test_oracle_session_context_hook_sync_checkout_and_release() -> None:
    executed_sql: list[tuple[str, dict[str, Any]]] = []
    cursor_mock = MagicMock()
    cursor_mock.__enter__.return_value = cursor_mock
    cursor_mock.__exit__.return_value = False
    cursor_mock.execute.side_effect = lambda sql, params=None: executed_sql.append((sql, dict(params or {})))

    conn_mock = MagicMock()
    conn_mock.__enter__.return_value = conn_mock
    conn_mock.__exit__.return_value = False
    conn_mock.cursor.return_value = cursor_mock

    pool_mock = MagicMock()
    pool_mock.acquire.return_value = conn_mock

    hook = OracleSessionContextConfig(
        module="billing",
        action=lambda: "invoice",
        client_identifier="tenant_42",
        edition="ORA$BASE",
        time_zone="UTC",
        nls_parameters={"NLS_DATE_FORMAT": "YYYY-MM-DD"},
        contexts={"APP_CTX": {"TENANT_ID": "42"}},
        reset_on_release=True,
    )
    config = OracleSyncConfig(connection_instance=pool_mock, driver_features={"session_context_hook": hook})
    with config.provide_connection():
        assert conn_mock.module == "billing"
        assert conn_mock.action == "invoice"
        assert conn_mock.client_identifier == "tenant_42"

    assert len(executed_sql) == 2
    checkout_sql, checkout_binds = executed_sql[0]
    release_sql, _ = executed_sql[1]
    assert "EDITION = ORA$BASE" in checkout_sql
    assert "TIME_ZONE = ''UTC''" in checkout_sql
    assert "DBMS_SESSION.SET_CONTEXT(:ctx_ns_0, :ctx_attr_0, :ctx_val_0);" in checkout_sql
    assert checkout_binds == {"ctx_ns_0": "APP_CTX", "ctx_attr_0": "TENANT_ID", "ctx_val_0": "42"}
    assert "DBMS_SESSION.CLEAR_CONTEXT('APP_CTX');" in release_sql
    assert conn_mock.module == ""
    assert conn_mock.action == ""
    assert conn_mock.client_identifier == ""


@pytest.mark.anyio
async def test_oracle_session_context_hook_async_checkout_and_release() -> None:
    executed_sql: list[tuple[str, dict[str, Any]]] = []
    cursor_mock = MagicMock()
    cursor_mock.__enter__.return_value = cursor_mock
    cursor_mock.__exit__.return_value = False
    cursor_mock.__aenter__ = AsyncMock(return_value=cursor_mock)
    cursor_mock.__aexit__ = AsyncMock(return_value=False)

    async def record_execute(sql: str, params: dict[str, Any] | None = None) -> None:
        executed_sql.append((sql, dict(params or {})))

    cursor_mock.execute = AsyncMock(side_effect=record_execute)

    conn_mock = MagicMock()
    conn_mock.cursor.return_value = cursor_mock

    pool_mock = MagicMock()
    pool_mock.acquire = AsyncMock(return_value=conn_mock)
    pool_mock.release = AsyncMock()

    hook = OracleSessionContextConfig(
        module="search", client_identifier="user_99", contexts={"CTX_NS": {"ORG_ID": "org_1"}}, reset_on_release=True
    )
    config = OracleAsyncConfig(connection_instance=pool_mock, driver_features={"session_context_hook": hook})
    async with config.provide_connection():
        assert conn_mock.module == "search"
        assert conn_mock.client_identifier == "user_99"

    assert len(executed_sql) == 2
    assert "DBMS_SESSION.SET_CONTEXT" in executed_sql[0][0]
    assert "DBMS_SESSION.CLEAR_CONTEXT('CTX_NS');" in executed_sql[1][0]
    pool_mock.release.assert_awaited_once_with(conn_mock)


def test_build_session_checkout_and_release_plsql() -> None:
    cfg = OracleSessionContextConfig(
        client_identifier=lambda: "tenant_user_1",
        context_attributes={"APP_CTX": {"TENANT_ID": "t1"}},
        ebs_apps_initialize=(1010, 20420, 1),
        mo_global_policy=("S", 84),
        apex_session=(100, 1, 999999),
        reset_package=True,
        clear_identifier=True,
        clear_context_namespaces=("APP_CTX",),
        apex_detach=True,
    )
    checkout_sql, checkout_binds = build_session_checkout_plsql(cfg)
    assert checkout_sql is not None
    assert "DBMS_SESSION.SET_IDENTIFIER(:p_client_id);" in checkout_sql
    assert "DBMS_SESSION.SET_CONTEXT(:ctx_ns_0, :ctx_attr_0, :ctx_val_0);" in checkout_sql
    assert (
        "FND_GLOBAL.APPS_INITIALIZE(user_id => :p_ebs_uid, resp_id => :p_ebs_rid, resp_appl_id => :p_ebs_aid);"
        in checkout_sql
    )
    assert "MO_GLOBAL.SET_POLICY_CONTEXT(:p_mo_mode, :p_mo_org);" in checkout_sql
    assert (
        "APEX_SESSION.ATTACH(p_app_id => :p_apex_app, p_page_id => :p_apex_page, p_session_id => :p_apex_sess);"
        in checkout_sql
    )
    assert checkout_binds["p_client_id"] == "tenant_user_1"
    assert checkout_binds["p_ebs_uid"] == 1010
    assert checkout_binds["p_mo_mode"] == "S"
    assert checkout_binds["p_apex_app"] == 100

    release_sql, release_binds = build_session_release_plsql(cfg)
    assert release_sql is not None
    assert "APEX_SESSION.DETACH;" in release_sql
    assert "DBMS_SESSION.CLEAR_IDENTIFIER;" in release_sql
    assert "DBMS_SESSION.CLEAR_ALL_CONTEXT(:p_clr_ns_0);" in release_sql
    assert "DBMS_SESSION.RESET_PACKAGE;" in release_sql
    assert release_binds == {"p_clr_ns_0": "APP_CTX"}


@pytest.mark.anyio
async def test_oracle_sync_and_async_provide_connection_and_session_run_checkout_and_release_hooks_on_exception() -> (
    None
):
    executed_sync: list[str] = []
    cursor_sync = MagicMock()
    cursor_sync.__enter__.return_value = cursor_sync
    cursor_sync.__exit__.return_value = False
    cursor_sync.execute.side_effect = lambda sql, params=None: executed_sync.append(sql)

    conn_sync = MagicMock()
    conn_sync.cursor.return_value = cursor_sync
    pool_sync = MagicMock()
    pool_sync.acquire.return_value = conn_sync

    hook = OracleSessionContextConfig(client_identifier="u1", clear_identifier=True, reset_package=True)
    sync_cfg = OracleSyncConfig(connection_instance=pool_sync, driver_features={"session_context_hook": hook})
    with pytest.raises(RuntimeError, match="boom_sync"), sync_cfg.provide_session():
        raise RuntimeError("boom_sync")

    assert len(executed_sync) == 2
    assert "DBMS_SESSION.SET_IDENTIFIER" in executed_sync[0]
    assert "DBMS_SESSION.RESET_PACKAGE;" in executed_sync[1]
    pool_sync.release.assert_called_once_with(conn_sync)

    executed_async: list[str] = []
    cursor_async = MagicMock()
    cursor_async.__enter__.return_value = cursor_async
    cursor_async.__exit__.return_value = False

    async def rec_async(sql: str, params: Any = None) -> None:
        executed_async.append(sql)

    cursor_async.execute = AsyncMock(side_effect=rec_async)
    conn_async = MagicMock()
    conn_async.cursor.return_value = cursor_async
    pool_async = MagicMock()
    pool_async.acquire = AsyncMock(return_value=conn_async)
    pool_async.release = AsyncMock()

    async_cfg = OracleAsyncConfig(connection_instance=pool_async, driver_features={"session_context_hook": hook})
    with pytest.raises(RuntimeError, match="boom_async"):
        async with async_cfg.provide_session():
            raise RuntimeError("boom_async")

    assert len(executed_async) == 2
    assert "DBMS_SESSION.SET_IDENTIFIER" in executed_async[0]
    assert "DBMS_SESSION.RESET_PACKAGE;" in executed_async[1]
    pool_async.release.assert_awaited_once_with(conn_async)
