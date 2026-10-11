"""OracleDB configuration tests covering driver kwargs and typed options."""

from collections.abc import Awaitable, Callable
from inspect import isawaitable
from ssl import TLSVersion
from typing import Any, NotRequired, cast, get_args, get_origin, get_type_hints
from unittest.mock import AsyncMock, Mock, call

import pytest
from oracledb import AuthMode, PoolGetMode, Purity

from sqlspec.adapters.oracledb import build_connection_config
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
