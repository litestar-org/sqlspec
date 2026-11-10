# pyright: reportArgumentType=false
"""Tests for Flask SQLSpec plugin lifecycle and configuration."""

import tempfile
from typing import TYPE_CHECKING, Any, cast

import pytest
from flask import Flask, g

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.flask import FlaskConfigState, SQLSpecPlugin
from sqlspec.extensions.flask._utils import get_or_create_session
from sqlspec.extensions.flask.extension import DEFAULT_SESSION_KEY

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

pytest.importorskip("flask")


def test_shutdown_closes_sync_pools(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown should dispose sync pools exactly once."""
    sqlspec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)
    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)
    close_calls = 0
    original_close_pool = SqliteConfig.close_pool

    def tracking_close_pool(self: SqliteConfig) -> None:
        nonlocal close_calls
        close_calls += 1
        return original_close_pool(self)

    monkeypatch.setattr(SqliteConfig, "close_pool", tracking_close_pool)
    plugin.shutdown()
    plugin.shutdown()
    assert close_calls == 1


def test_shutdown_closes_async_pools_and_stops_portal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown should dispose async pools and stop portal."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sqlspec.add_config(config)
        app = Flask(__name__)
        plugin = SQLSpecPlugin(sqlspec, app)
        close_calls = 0
        original_close_pool = AiosqliteConfig.close_pool

        async def tracking_close_pool(self: AiosqliteConfig) -> None:
            nonlocal close_calls
            close_calls += 1
            await original_close_pool(self)

        monkeypatch.setattr(AiosqliteConfig, "close_pool", tracking_close_pool)
        plugin.shutdown()
        assert close_calls == 1
        assert plugin._portal is None


def test_default_session_key_is_db_session() -> None:
    """Flask should default to 'db_session' for consistency with other frameworks."""
    assert DEFAULT_SESSION_KEY == "db_session"


def test_uses_default_session_key_when_not_configured() -> None:
    """Plugin should use DEFAULT_SESSION_KEY when no extension_config provided."""
    sqlspec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    assert len(plugin._config_states) == 1
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key in extension_config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"flask": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    assert len(plugin._config_states) == 1
    assert plugin._config_states[0].session_key == custom_key


pytest.importorskip("flask")


class _Driver:
    def __init__(self, *, connection: Any, statement_config: Any, driver_features: dict[str, Any]) -> None:
        self.connection = connection
        self.statement_config = statement_config
        self.driver_features = driver_features


class _Config:
    driver_type = _Driver
    driver_features = {"returning_support": True}
    statement_config = object()


def _make_state() -> FlaskConfigState:
    return FlaskConfigState(
        config=cast("DatabaseConfigProtocol[Any, Any, Any]", _Config()),
        connection_key="sqlspec_connection",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )


def test_utils_get_or_create_session_passes_driver_features() -> None:
    app = Flask(__name__)
    state = _make_state()
    connection = object()
    with app.app_context():
        setattr(g, state.connection_key, connection)
        session = get_or_create_session(state, portal=None)
    assert session.connection is connection
    assert session.statement_config is _Config.statement_config
    assert session.driver_features == _Config.driver_features


def test_utils_get_or_create_session_returns_cached_session() -> None:
    app = Flask(__name__)
    state = _make_state()
    with app.app_context():
        setattr(g, state.connection_key, object())
        first = get_or_create_session(state, portal=None)
        second = get_or_create_session(state, portal=None)
    assert second is first


def test_duplicate_state_keys_raise_improper_configuration_error() -> None:
    """Duplicate state keys should raise ImproperConfigurationError with the Flask message."""
    plugin = SQLSpecPlugin(SQLSpec())
    plugin._config_states = [_make_state(), _make_state()]
    with pytest.raises(ImproperConfigurationError, match="Use unique session_key values"):
        plugin._ensure_unique_keys()


def test_config_state_by_key_unknown_key_raises_improper_configuration_error() -> None:
    """Unknown session keys should raise ImproperConfigurationError with the Flask message."""
    plugin = SQLSpecPlugin(SQLSpec())
    with pytest.raises(ImproperConfigurationError, match="No configuration found for key: missing"):
        plugin._config_state_by_key("missing")


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("identity", ["a", "uuid", "bypass", "none"])
def test_tenant_extractor_binds_before_flask_acquisition(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool, identity: str
) -> None:
    from contextlib import asynccontextmanager, contextmanager
    from uuid import UUID

    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    raw = {"a": "a", "uuid": UUID(int=1), "bypass": TenantContextPayload.bypass(), "none": None}[identity]
    seen = []

    def extractor() -> Any:
        return raw

    @contextmanager
    def connection_context(self: Any, pool: Any = None) -> Any:
        seen.append(TenantContext.get())
        yield object()

    @asynccontextmanager
    async def async_connection_context(self: Any, pool: Any = None) -> Any:
        seen.append(TenantContext.get())
        yield object()

    config_type = AiosqliteConfig if asynchronous else SqliteConfig
    monkeypatch.setattr(
        config_type, "provide_connection", async_connection_context if asynchronous else connection_context
    )
    config = config_type(
        connection_config={"database": ":memory:"}, extension_config={"flask": {"tenant_extractor": extractor}}
    )
    spec = SQLSpec()
    spec.add_config(config)
    app = Flask(__name__)
    plugin = SQLSpecPlugin(spec, app)
    app.add_url_rule("/", view_func=lambda: "ok")
    try:
        with TenantContext.scope("outer"):
            result = app.test_client().get("/")
            assert result.status_code == 200
            assert TenantContext.get_tenant_id() == "outer"
        assert seen == [
            None if raw is None else (raw if isinstance(raw, TenantContextPayload) else TenantContextPayload(raw))
        ]
    finally:
        plugin.shutdown()


@pytest.mark.parametrize("invalid", ["missing", "async", "conflict"])
def test_flask_rejects_invalid_tenant_extractors(monkeypatch: pytest.MonkeyPatch, invalid: str) -> None:
    from types import SimpleNamespace

    monkeypatch.setattr(SqliteConfig, "supports_tenant_strategy", True)

    async def async_extractor() -> str:
        return "a"

    spec = SQLSpec()
    first_extractor = (lambda: "a") if invalid == "conflict" else async_extractor if invalid == "async" else None
    cfg = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"flask": {"tenant_extractor": first_extractor}}
    )
    cfg.driver_features["tenant_strategy"] = SimpleNamespace(checkout=lambda *args: None, checkin=lambda *args: None)
    spec.add_config(cfg)
    if invalid == "conflict":
        spec.add_config(
            SqliteConfig(
                connection_config={"database": ":memory:"},
                extension_config={"flask": {"session_key": "other", "tenant_extractor": lambda: "b"}},
            )
        )
    with pytest.raises(ImproperConfigurationError, match="tenant_extractor"):
        SQLSpecPlugin(spec, Flask(__name__))


def test_flask_tenant_resets_after_handler_error_and_short_circuit() -> None:
    from sqlspec.utils.tenant import TenantContext

    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"tenant_extractor": lambda req: "a"}},
        )
    )
    app = Flask(__name__)
    plugin = SQLSpecPlugin(spec, app)

    seen = []

    @app.get("/")
    def error_handler() -> str:
        seen.append(TenantContext.get_tenant_id())
        raise ValueError("handler")

    try:
        with TenantContext.scope("outer"):
            assert app.test_client().get("/").status_code == 500
            assert TenantContext.get_tenant_id() == "outer"
            assert seen == ["a"]
    finally:
        plugin.shutdown()
    early = Flask("early")
    early.before_request(lambda: ("denied", 401))
    plugin = SQLSpecPlugin(spec, early)
    try:
        with TenantContext.scope("outer"):
            assert early.test_client().get("/").status_code == 401
            assert TenantContext.get_tenant_id() == "outer"
    finally:
        plugin.shutdown()


def test_flask_extractor_failure_leaves_ambient_context_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.utils.tenant import TenantContext

    def extractor() -> str:
        raise KeyError("authentication failed")

    def unexpected_acquire(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("No connection may be acquired after extractor failure")

    monkeypatch.setattr(SqliteConfig, "provide_connection", unexpected_acquire)
    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"flask": {"tenant_extractor": extractor}}
        )
    )
    app = Flask(__name__)
    plugin = SQLSpecPlugin(spec, app)
    app.add_url_rule("/", view_func=lambda: "ok")
    try:
        with TenantContext.scope("outer"):
            assert app.test_client().get("/").status_code == 500
            assert TenantContext.get_tenant_id() == "outer"
    finally:
        plugin.shutdown()
