"""OracleDB configuration tests covering driver kwargs and typed options."""

from collections.abc import Awaitable, Callable
from ssl import TLSVersion
from typing import Any, cast, get_args, get_origin, get_type_hints

import pytest
from oracledb import AuthMode, PoolGetMode, Purity
from typing_extensions import NotRequired

from sqlspec.adapters.oracledb import config as oracle_config_module
from sqlspec.adapters.oracledb.config import (
    OracleAsyncConfig,
    OracleConnectionParams,
    OracleDriverFeatures,
    OraclePoolParams,
    OracleSyncConfig,
)


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
    monkeypatch.setattr(oracle_config_module, "_extract_oracle_major", lambda _connection: 23)


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
        "table_queue",
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
