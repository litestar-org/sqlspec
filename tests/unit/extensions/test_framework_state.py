"""Tests for framework-owned configuration state."""

from dataclasses import fields
from typing import Any, cast

import pytest

from sqlspec.extensions.flask._state import FlaskConfigState
from sqlspec.extensions.sanic._state import SanicConfigState
from sqlspec.extensions.starlette._state import SQLSpecConfigState

_STATE_TYPES = (FlaskConfigState, SanicConfigState, SQLSpecConfigState)
FrameworkState = FlaskConfigState | SanicConfigState | SQLSpecConfigState


def _state(
    commit_mode: str, extra_commit: "set[int] | None", extra_rollback: "set[int] | None"
) -> "list[FrameworkState]":
    states: list[FrameworkState] = []
    for state_type in _STATE_TYPES:
        state = cast("FrameworkState", object.__new__(state_type))
        state.commit_mode = cast("Any", commit_mode)
        state.extra_commit_statuses = extra_commit
        state.extra_rollback_statuses = extra_rollback
        states.append(state)
    return states


def _should_commit(
    status_code: int, commit_mode: str, extra_commit: "set[int] | None", extra_rollback: "set[int] | None"
) -> bool:
    results = {state.should_commit(status_code) for state in _state(commit_mode, extra_commit, extra_rollback)}
    assert len(results) == 1
    return results.pop()


def _should_rollback(
    status_code: int, commit_mode: str, extra_commit: "set[int] | None", extra_rollback: "set[int] | None"
) -> bool:
    results = {state.should_rollback(status_code) for state in _state(commit_mode, extra_commit, extra_rollback)}
    assert len(results) == 1
    return results.pop()


_COMMON_FIELDS = [
    "config",
    "connection_key",
    "pool_key",
    "session_key",
    "commit_mode",
    "extra_commit_statuses",
    "extra_rollback_statuses",
    "disable_di",
    "enable_correlation_middleware",
    "correlation_header",
    "correlation_headers",
    "auto_trace_headers",
    "enable_sqlcommenter_middleware",
]

_ALL_24_ADAPTER_SPECS: list[tuple[str, str, dict[str, Any], bool, bool]] = [
    ("adbc", "AdbcConfig", {}, True, False),
    ("aiomysql", "AiomysqlConfig", {}, False, False),
    ("aiosqlite", "AiosqliteConfig", {"connection_config": {"database": ":memory:"}}, False, False),
    ("arrow_odbc", "ArrowOdbcConfig", {"connection_config": {"connection_string": "Driver=test"}}, True, False),
    ("asyncmy", "AsyncmyConfig", {}, False, False),
    ("asyncpg", "AsyncpgConfig", {}, True, False),
    ("bigquery", "BigQueryConfig", {}, False, True),
    ("cockroach_asyncpg", "CockroachAsyncpgConfig", {}, True, False),
    ("cockroach_psycopg", "CockroachPsycopgSyncConfig", {}, False, False),
    ("db2", "Db2SyncConfig", {}, True, False),
    ("duckdb", "DuckDBConfig", {}, True, False),
    ("mssql_python", "MssqlPythonConfig", {}, False, False),
    ("mysqlconnector", "MysqlConnectorSyncConfig", {}, False, False),
    ("oracledb", "OracleSyncConfig", {}, False, False),
    ("psqlpy", "PsqlpyConfig", {}, True, False),
    ("psycopg", "PsycopgSyncConfig", {}, False, False),
    ("pymssql", "PymssqlConfig", {}, False, False),
    ("pymysql", "PyMysqlConfig", {}, False, False),
    ("snowflake", "SnowflakeConfig", {}, True, False),
    ("spanner", "SpannerSyncConfig", {"connection_config": {"instance_id": "i", "database_id": "d"}}, False, True),
    ("sqlanywhere", "SqlanywhereConfig", {}, False, False),
    ("sqlite", "SqliteConfig", {"connection_config": {"database": ":memory:"}}, False, False),
    ("sybase", "SybaseConfig", {}, True, False),
    ("teradata", "TeradataConfig", {}, True, False),
]


def test_starlette_config_state_field_order() -> None:
    """SQLSpecConfigState preserves its positional fields and includes tenant extraction."""
    assert [f.name for f in fields(SQLSpecConfigState)] == [
        *_COMMON_FIELDS,
        "sqlcommenter_framework",
        "tenant_extractor",
        "tenant_receipt_keys",
        "read_session_key",
        "read_connection_key",
        "read_pool_key",
        "routing_session_key",
        "default_routing_mode",
        "enable_routing_middleware",
        "routing_header",
        "write_pin_cookie",
        "routing_secret",
        "routing_header_bytes",
    ]


def test_sanic_config_state_field_order() -> None:
    """SanicConfigState exposes its request tenant extractor after core fields."""
    assert [f.name for f in fields(SanicConfigState)] == [
        *_COMMON_FIELDS[:8],
        "tenant_extractor",
        "tenant_receipt_keys",
        *_COMMON_FIELDS[8:],
        "sqlcommenter_framework",
        "read_session_key",
        "read_connection_key",
        "read_pool_key",
        "routing_session_key",
        "default_routing_mode",
        "enable_routing_middleware",
        "routing_header",
        "write_pin_cookie",
        "routing_secret",
    ]


def test_flask_config_state_field_order() -> None:
    """FlaskConfigState exposes its request tenant extractor after core fields."""
    assert [f.name for f in fields(FlaskConfigState)] == [
        "config",
        "connection_key",
        "session_key",
        "commit_mode",
        "extra_commit_statuses",
        "extra_rollback_statuses",
        "is_async",
        "disable_di",
        "tenant_extractor",
        "tenant_receipt_keys",
        "extractor_takes_request",
        "enable_correlation_middleware",
        "correlation_header",
        "correlation_headers",
        "auto_trace_headers",
        "enable_sqlcommenter_middleware",
        "read_session_key",
        "read_connection_key",
        "routing_session_key",
        "default_routing_mode",
        "enable_routing_middleware",
        "routing_header",
        "write_pin_cookie",
        "routing_secret",
    ]


def test_sqlcommenter_framework_defaults() -> None:
    """Each subclass keeps its framework-specific sqlcommenter default."""
    starlette_default = next(f for f in fields(SQLSpecConfigState) if f.name == "sqlcommenter_framework").default
    sanic_default = next(f for f in fields(SanicConfigState) if f.name == "sqlcommenter_framework").default
    assert starlette_default == "starlette"
    assert sanic_default == "sanic"


@pytest.mark.parametrize("status_code", [200, 204, 299, 300, 302, 399, 400, 500])
def test_should_commit_manual_mode_never_commits(status_code: int) -> None:
    """Manual mode never commits regardless of status code."""
    assert not _should_commit(status_code, "manual", None, None)


def test_should_commit_autocommit_mode_commits_2xx_only() -> None:
    """Autocommit mode commits on 2xx and rolls back everything else."""
    assert _should_commit(200, "autocommit", None, None)
    assert _should_commit(201, "autocommit", None, None)
    assert _should_commit(299, "autocommit", None, None)

    assert not _should_commit(300, "autocommit", None, None)
    assert not _should_commit(302, "autocommit", None, None)
    assert not _should_commit(400, "autocommit", None, None)
    assert not _should_commit(500, "autocommit", None, None)


def test_should_commit_autocommit_include_redirect_mode_commits_2xx_3xx() -> None:
    """Redirect-inclusive autocommit commits on 2xx-3xx."""
    assert _should_commit(200, "autocommit_include_redirect", None, None)
    assert _should_commit(300, "autocommit_include_redirect", None, None)
    assert _should_commit(302, "autocommit_include_redirect", None, None)
    assert _should_commit(399, "autocommit_include_redirect", None, None)

    assert not _should_commit(400, "autocommit_include_redirect", None, None)
    assert not _should_commit(500, "autocommit_include_redirect", None, None)


@pytest.mark.parametrize("commit_mode", ["autocommit", "autocommit_include_redirect"])
def test_should_commit_extra_commit_statuses_override(commit_mode: str) -> None:
    """Extra commit statuses force a commit in autocommit modes only."""
    assert _should_commit(404, commit_mode, {404}, None)
    assert _should_commit(500, commit_mode, {500}, None)


@pytest.mark.parametrize("status_code", [200, 302, 404, 500])
def test_manual_mode_ignores_extra_commit_and_rollback_statuses(status_code: int) -> None:
    """Manual mode never applies extra_commit_statuses or extra_rollback_statuses."""
    assert not _should_commit(status_code, "manual", {200, 302, 404, 500}, None)
    assert _should_rollback(status_code, "manual", {200, 302, 404, 500}, None)
    assert _should_rollback(status_code, "manual", None, {status_code})


@pytest.mark.parametrize("commit_mode", ["autocommit", "autocommit_include_redirect"])
def test_should_commit_extra_rollback_statuses_override(commit_mode: str) -> None:
    """Extra rollback statuses force a rollback in autocommit modes."""
    assert not _should_commit(200, commit_mode, None, {200})
    assert not _should_commit(201, commit_mode, None, {201})


def test_should_commit_extra_commit_takes_precedence_over_extra_rollback() -> None:
    """Extra commit statuses are checked before extra rollback statuses."""
    assert _should_commit(404, "autocommit", {404}, {418})
    assert not _should_commit(418, "autocommit", {404}, {418})


@pytest.mark.parametrize("status_code", [200, 300, 400, 500])
def test_should_rollback_manual_mode_always_rolls_back(status_code: int) -> None:
    """Manual mode rolls back uncommitted work regardless of status code."""
    assert _should_rollback(status_code, "manual", None, None)


def test_should_rollback_autocommit_mode_inverts_commit() -> None:
    """Autocommit mode rolls back any status that does not commit."""
    assert not _should_rollback(200, "autocommit", None, None)
    assert not _should_rollback(299, "autocommit", None, None)

    assert _should_rollback(300, "autocommit", None, None)
    assert _should_rollback(400, "autocommit", None, None)
    assert _should_rollback(500, "autocommit", None, None)


def test_should_rollback_autocommit_include_redirect_mode_inverts_commit() -> None:
    """Redirect-inclusive autocommit rolls back only 4xx and above."""
    assert not _should_rollback(200, "autocommit_include_redirect", None, None)
    assert not _should_rollback(302, "autocommit_include_redirect", None, None)

    assert _should_rollback(400, "autocommit_include_redirect", None, None)
    assert _should_rollback(500, "autocommit_include_redirect", None, None)


def test_should_rollback_respects_extra_statuses() -> None:
    """Extra status overrides feed through to the rollback decision."""
    assert not _should_rollback(404, "autocommit", {404}, None)
    assert _should_rollback(200, "autocommit", None, {200})


@pytest.mark.parametrize("state_type", _STATE_TYPES)
def test_read_and_routing_key_defaults(state_type: Any) -> None:
    defaults = {item.name: item.default for item in fields(state_type)}
    assert defaults["read_session_key"] == "read_session"
    assert defaults["read_connection_key"] == "read_connection"
    assert defaults["routing_session_key"] is None
    assert defaults["default_routing_mode"] == "auto"


@pytest.mark.parametrize("framework", ["litestar", "starlette", "fastapi", "flask", "sanic"])
def test_all_frameworks_default_to_autocommit(framework: str) -> None:
    """Verify every web framework extension defaults commit_mode to 'autocommit'."""
    import importlib

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    mod_name = "plugin" if framework == "litestar" else "extension"
    ext = importlib.import_module(f"sqlspec.extensions.{framework}.{mod_name}")
    plugin_cls = ext.SQLSpecPlugin
    default_mode = getattr(ext, "DEFAULT_COMMIT_MODE", None)
    if default_mode is not None:
        assert default_mode == "autocommit"

    spec = SQLSpec()
    spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    plugin = plugin_cls(spec)
    states = getattr(plugin, "_plugin_configs", None) or plugin._config_states
    assert len(states) == 1
    assert states[0].commit_mode == "autocommit"


@pytest.mark.parametrize(
    "module_path",
    [
        "sqlspec.extensions.litestar.handlers",
        "sqlspec.extensions.starlette._utils",
        "sqlspec.extensions.flask._utils",
        "sqlspec.extensions.sanic._utils",
    ],
)
def test_lazy_begin_adapters_parity(module_path: str) -> None:
    """Verify _LAZY_BEGIN_ADAPTERS matches across all framework registries."""
    import importlib

    mod = importlib.import_module(module_path)
    expected = frozenset({
        "adbc",
        "arrow_odbc",
        "asyncpg",
        "cockroach_asyncpg",
        "db2",
        "duckdb",
        "psqlpy",
        "snowflake",
        "sybase",
        "teradata",
    })
    assert mod._LAZY_BEGIN_ADAPTERS == expected


@pytest.mark.parametrize(
    ("adapter", "cfg_name", "cfg_kwargs", "expect_lazy_begin", "is_client_tx_exception"),
    _ALL_24_ADAPTER_SPECS,
    ids=[spec[0] for spec in _ALL_24_ADAPTER_SPECS],
)
@pytest.mark.parametrize("commit_mode", ["manual", "autocommit", "autocommit_include_redirect"])
def test_all_24_adapters_lazy_begin_arming_across_commit_modes(
    adapter: str,
    cfg_name: str,
    cfg_kwargs: dict[str, Any],
    expect_lazy_begin: bool,
    is_client_tx_exception: bool,
    commit_mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify lazy-begin arming across all 24 adapters in Litestar, Starlette, Flask, and Sanic."""
    import importlib
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from sqlspec.extensions.flask._state import FlaskConfigState
    from sqlspec.extensions.flask._utils import get_or_create_session as flask_get_session
    from sqlspec.extensions.litestar.handlers import build_request_session as litestar_build_session
    from sqlspec.extensions.sanic._state import SanicConfigState
    from sqlspec.extensions.sanic._utils import get_or_create_session as sanic_get_session
    from sqlspec.extensions.sanic._utils import set_context_value as sanic_set_ctx
    from sqlspec.extensions.starlette._utils import get_or_create_driver_session as starlette_get_session

    _ = is_client_tx_exception
    cfg_cls = getattr(importlib.import_module(f"sqlspec.adapters.{adapter}.config"), cfg_name)
    monkeypatch.setattr(cfg_cls, "_prepare_driver", lambda _self, drv: drv)
    config = cfg_cls(**cfg_kwargs)

    litestar_session = litestar_build_session(config, MagicMock(), cast("Any", commit_mode))
    assert litestar_session._auto_begin_on_write is expect_lazy_begin

    starlette_state = SQLSpecConfigState(
        config=config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode=cast("Any", commit_mode),
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )
    starlette_req = SimpleNamespace(state=SimpleNamespace(db_connection=MagicMock()))
    starlette_session = starlette_get_session(cast("Any", starlette_req), starlette_state)
    assert starlette_session._auto_begin_on_write is expect_lazy_begin

    sanic_state = SanicConfigState(
        config=config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode=cast("Any", commit_mode),
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )
    sanic_req = SimpleNamespace(ctx=SimpleNamespace())
    sanic_set_ctx(sanic_req.ctx, "db_connection", MagicMock())
    sanic_session = sanic_get_session(sanic_req, sanic_state)
    assert sanic_session._auto_begin_on_write is expect_lazy_begin

    from flask import Flask, g

    flask_state = FlaskConfigState(
        config=config,
        connection_key="db_connection",
        session_key="db_session",
        commit_mode=cast("Any", commit_mode),
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )
    flask_app = Flask(__name__)
    with flask_app.app_context():
        g.db_connection = MagicMock()
        flask_session = flask_get_session(flask_state, portal=None)
        assert flask_session._auto_begin_on_write is expect_lazy_begin


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter", "cfg_name", "cfg_kwargs", "expect_lazy_begin", "is_client_tx_exception"),
    _ALL_24_ADAPTER_SPECS,
    ids=[spec[0] for spec in _ALL_24_ADAPTER_SPECS],
)
async def test_all_24_adapters_manual_mode_rollback_and_explicit_commit_contract(
    adapter: str,
    cfg_name: str,
    cfg_kwargs: dict[str, Any],
    expect_lazy_begin: bool,
    is_client_tx_exception: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contract across all 24 adapters: manual mode discards uncommitted writes and preserves explicit commit()."""
    import importlib
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from sqlspec.extensions.litestar.handlers import _finalize_request_session, build_request_session
    from sqlspec.extensions.starlette._utils import set_state_value
    from sqlspec.extensions.starlette.middleware import _finalize_request_transaction

    cfg_cls = getattr(importlib.import_module(f"sqlspec.adapters.{adapter}.config"), cfg_name)
    monkeypatch.setattr(cfg_cls, "_prepare_driver", lambda _self, drv: drv)
    config = cfg_cls(**cfg_kwargs)

    if is_client_tx_exception:
        conn = MagicMock()
        if adapter == "bigquery":
            raw_session = build_request_session(config, conn, "manual")
            assert raw_session._auto_begin_on_write is False
            raw_session.rollback()
            assert raw_session._connection_in_transaction() is False
        else:
            raw_session = config.driver_type(
                connection=conn, statement_config=config.statement_config, driver_features=config.driver_features
            )
            raw_session._owns_transaction = False
            raw_session.begin()
            raw_session.rollback()
            conn.rollback.assert_not_called()

            tx_session, tx_counts = _build_contract_driver(config, monkeypatch)
            tx_session._owns_transaction = True
            tx_session.begin()
            await _finalize_request_session(tx_session, tx_session.connection, commit=False)
            assert tx_counts["rollback"] == 1
        return

    session, counts = _build_contract_driver(config, monkeypatch)
    session._auto_begin_on_write = expect_lazy_begin
    if expect_lazy_begin:
        await _finalize_request_session(session, session.connection, commit=False)
        assert counts["rollback"] == 0

    if config.is_async:
        await session.begin()
    else:
        session.begin()
    await _finalize_request_session(session, session.connection, commit=False)
    assert counts["rollback"] == 1
    assert counts["commit"] == 0

    starlette_state = SQLSpecConfigState(
        config=config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )
    req = SimpleNamespace(state=SimpleNamespace())
    set_state_value(req.state, "db_session_instance", session)

    if config.is_async:
        await session.begin()
        await session.commit()
    else:
        session.begin()
        session.commit()
    assert counts["commit"] == 1
    await _finalize_request_transaction(cast("Any", req), starlette_state, session.connection, commit=False)
    if expect_lazy_begin:
        assert counts["rollback"] == 1
    else:
        assert counts["rollback"] == 2
    assert counts["commit"] == 1


def _build_contract_driver(config: Any, monkeypatch: pytest.MonkeyPatch) -> tuple[Any, dict[str, int]]:
    from unittest.mock import AsyncMock, MagicMock

    counts = {"begin": 0, "commit": 0, "rollback": 0}
    conn = AsyncMock() if config.is_async else MagicMock()
    session = config._prepare_driver(
        config.driver_type(
            connection=conn, statement_config=config.statement_config, driver_features=config.driver_features
        )
    )
    tx_active = False

    def _begin_impl(_self: Any = None) -> None:
        nonlocal tx_active
        counts["begin"] += 1
        tx_active = True

    def _commit_impl(_self: Any = None) -> None:
        nonlocal tx_active
        counts["commit"] += 1
        tx_active = False

    def _rollback_impl(_self: Any = None) -> None:
        nonlocal tx_active
        counts["rollback"] += 1
        tx_active = False

    driver_cls = type(session)
    if config.is_async:

        async def _abegin(_self: Any = None) -> None:
            _begin_impl()

        async def _acommit(_self: Any = None) -> None:
            _commit_impl()

        async def _arollback(_self: Any = None) -> None:
            _rollback_impl()

        monkeypatch.setattr(driver_cls, "begin", _abegin)
        monkeypatch.setattr(driver_cls, "commit", _acommit)
        monkeypatch.setattr(driver_cls, "rollback", _arollback)
    else:
        monkeypatch.setattr(driver_cls, "begin", _begin_impl)
        monkeypatch.setattr(driver_cls, "commit", _commit_impl)
        monkeypatch.setattr(driver_cls, "rollback", _rollback_impl)
    monkeypatch.setattr(driver_cls, "_connection_in_transaction", lambda _self: tx_active)
    return session, counts
