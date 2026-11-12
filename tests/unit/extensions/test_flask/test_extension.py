# pyright: reportArgumentType=false
"""Tests for Flask SQLSpec plugin lifecycle and configuration."""

import tempfile
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest
from flask import Flask, g

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.flask import FlaskConfigState, SQLSpecPlugin, set_request_tenant
from sqlspec.extensions.flask._utils import get_or_create_session
from sqlspec.extensions.flask.extension import DEFAULT_SESSION_KEY
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

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

    identities: dict[str, str | UUID | TenantContextPayload | None] = {
        "a": "a",
        "uuid": UUID(int=1),
        "bypass": TenantContextPayload.bypass(),
        "none": None,
    }
    raw = identities[identity]
    seen = []

    def extractor() -> Any:
        return raw

    @contextmanager
    def connection_context(self: Any, pool: Any = None) -> "Iterator[object]":
        seen.append(TenantContext.get())
        yield object()

    @asynccontextmanager
    async def async_connection_context(self: Any, pool: Any = None) -> "AsyncIterator[object]":
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


class _AsyncExtractorCallable:
    async def __call__(self) -> str:
        return "a"


@pytest.mark.parametrize("invalid", ["async_fn", "async_callable", "non_callable"])
def test_flask_rejects_invalid_tenant_extractors(invalid: str) -> None:
    async def async_extractor() -> str:
        return "a"

    extractor: Any = (
        async_extractor
        if invalid == "async_fn"
        else _AsyncExtractorCallable()
        if invalid == "async_callable"
        else "untrusted"
    )
    cfg = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"flask": {"tenant_extractor": extractor}}
    )
    with pytest.raises(ImproperConfigurationError, match="tenant_extractor"):
        SQLSpecPlugin(cfg, Flask(__name__))


@pytest.mark.parametrize("asynchronous", [False, True])
def test_flask_defers_tenant_connection_and_supports_set_request_tenant(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    seen: list[tuple[str, TenantContextPayload | None]] = []

    @contextmanager
    def sync_conn(self: Any, pool: Any = None) -> "Iterator[object]":
        seen.append(("enter", TenantContext.get()))
        try:
            yield self.create_connection()
        finally:
            seen.append(("exit", TenantContext.get()))

    @asynccontextmanager
    async def async_conn(self: Any, pool: Any = None) -> "AsyncIterator[object]":
        seen.append(("enter", TenantContext.get()))
        conn = await self.create_connection()
        try:
            yield conn
        finally:
            seen.append(("exit", TenantContext.get()))
            await conn.close()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config: Any = (
            AiosqliteConfig(connection_config={"database": tmp.name})
            if asynchronous
            else SqliteConfig(connection_config={"database": ":memory:"})
        )
        config.driver_features["tenant_strategy"] = object()
        monkeypatch.setattr(type(config), "provide_connection", async_conn if asynchronous else sync_conn)
        app = Flask(__name__)
        plugin = SQLSpecPlugin(config, app)
        assert plugin.config == [config]
        assert plugin.get_config() is config

        @app.get("/health")
        def health() -> str:
            return "ok"

        @app.get("/projects/<org_id>")
        def projects(org_id: str) -> str:
            set_request_tenant(org_id)
            session = plugin.get_session()
            if asynchronous:
                assert plugin._portal is not None
                value = plugin._portal.portal.call(session.select_value, "SELECT 1")
            else:
                value = session.select_value("SELECT 1")
            return f"{org_id}:{value}"

        try:
            client = app.test_client()
            assert client.get("/health").status_code == 200
            assert seen == []
            resp = client.get("/projects/acme")
            assert resp.status_code == 200
            assert resp.get_data(as_text=True) == "acme:1"
            assert seen == [("enter", TenantContextPayload("acme")), ("exit", TenantContextPayload("acme"))]
        finally:
            plugin.shutdown()


def test_flask_supports_multi_config_auth_and_deferred_tenant_db(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_tenant: list[str | None] = []
    auth_cfg = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"session_key": "auth_db", "commit_mode": "autocommit"}},
    )
    tenant_cfg = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"session_key": "tenant_db", "commit_mode": "autocommit"}},
    )
    tenant_cfg.driver_features["tenant_strategy"] = object()  # type: ignore[typeddict-unknown-key]
    original_provide = SqliteConfig.provide_connection

    @contextmanager
    def tracked_provide(self: SqliteConfig, *args: Any, **kwargs: Any) -> "Iterator[Any]":
        if self is tenant_cfg:
            seen_tenant.append(TenantContext.get_tenant_id())
            conn = self.create_connection()
            try:
                yield conn
            finally:
                conn.close()
            return
        with original_provide(self, *args, **kwargs) as conn:
            yield conn

    monkeypatch.setattr(SqliteConfig, "provide_connection", tracked_provide)
    app = Flask(__name__)
    plugin = SQLSpecPlugin([auth_cfg, tenant_cfg], app)
    assert plugin.get_config("auth_db") is auth_cfg
    assert plugin.get_config("tenant_db") is tenant_cfg

    @app.get("/login")
    def login() -> str:
        val = plugin.get_session("auth_db").select_value("SELECT 1")
        return f"auth:{val}"

    @app.get("/tenant")
    def tenant_route() -> str:
        auth_session = plugin.get_session("auth_db")
        assert auth_session.select_value("SELECT 1") == 1
        set_request_tenant("globex", key="tenant_db")
        tenant_session = plugin.get_session(tenant_cfg)
        return f"tenant:{tenant_session.select_value('SELECT 2')}"

    try:
        client = app.test_client()
        assert client.get("/login").get_data(as_text=True) == "auth:1"
        assert seen_tenant == []
        assert client.get("/tenant").get_data(as_text=True) == "tenant:2"
        assert seen_tenant == ["globex"]
    finally:
        plugin.shutdown()


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


def test_flask_rejects_extractor_with_invalid_request_signature() -> None:
    def extractor(first: object, second: object) -> str:
        return "a"

    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"flask": {"tenant_extractor": extractor}}
        )
    )
    with pytest.raises(ImproperConfigurationError, match="zero arguments or one request"):
        SQLSpecPlugin(spec, Flask(__name__))


def test_flask_disabled_injection_needs_no_tenant_extractor() -> None:
    spec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"flask": {"disable_di": True}})
    config.driver_features["tenant_strategy"] = object()
    spec.add_config(config)
    app = Flask(__name__)
    plugin = SQLSpecPlugin(spec, app)
    app.add_url_rule("/", view_func=lambda: "ok")
    try:
        assert app.test_client().get("/").status_code == 200
    finally:
        plugin.shutdown()


@pytest.mark.parametrize("status", [200, 500])
def test_flask_autocommit_finishes_connection_only_requests(status: int) -> None:
    import sqlite3

    with tempfile.TemporaryDirectory() as directory:
        database = f"{directory}/autocommit.db"
        setup = sqlite3.connect(database)
        setup.execute("CREATE TABLE items (value INTEGER)")
        setup.commit()
        setup.close()
        config = SqliteConfig(
            connection_config={"database": database}, extension_config={"flask": {"commit_mode": "autocommit"}}
        )
        app = Flask(__name__)
        plugin = SQLSpecPlugin(config, app)

        @app.post("/items")
        def create_item() -> tuple[str, int]:
            connection = plugin.get_connection()
            connection.execute("BEGIN")
            connection.execute("INSERT INTO items VALUES (1)")
            return "", status

        try:
            assert app.test_client().post("/items").status_code == status
        finally:
            plugin.shutdown()
            config.close_pool()
        reader = sqlite3.connect(database)
        try:
            assert reader.execute("SELECT COUNT(*) FROM items").fetchone()[0] == int(status == 200)
        finally:
            reader.close()
