"""Tests for FastAPI SQLSpec plugin."""

import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, cast

import pytest

pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.extensions.fastapi import SQLSpecPlugin, set_request_tenant
from sqlspec.extensions.starlette._utils import get_state_value
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY
from sqlspec.extensions.starlette.middleware import _response_owner_key, _ResponseCompletion
from sqlspec.observability import ObservabilityConfig
from sqlspec.utils.tenant import TenantContext

if TYPE_CHECKING:
    from sqlspec.config import LifecycleConfig


def test_provide_session_method_exists() -> None:
    """Verify provide_session() method exists without legacy session_dependency()."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    plugin = SQLSpecPlugin(config)

    assert hasattr(plugin, "provide_session")
    assert callable(plugin.provide_session)
    assert not hasattr(plugin, "session_dependency")


def test_provide_connection_method_exists() -> None:
    """Verify provide_connection() method exists without legacy connection_dependency()."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    plugin = SQLSpecPlugin(config)

    assert hasattr(plugin, "provide_connection")
    assert callable(plugin.provide_connection)
    assert not hasattr(plugin, "connection_dependency")


def test_plugin_direct_config_sequences_and_get_config() -> None:
    """SQLSpecPlugin accepts SQLSpec, single configs, and config sequences cleanly."""
    auth_cfg = AiosqliteConfig(
        bind_key="auth",
        connection_config={"database": ":memory:"},
        extension_config={
            "fastapi": {"session_key": "auth_session", "connection_key": "auth_conn", "pool_key": "auth_pool"}
        },
    )
    sync_cfg = SqliteConfig(
        bind_key="reporting",
        connection_config={"database": ":memory:"},
        extension_config={
            "fastapi": {"session_key": "report_session", "connection_key": "report_conn", "pool_key": "report_pool"}
        },
    )

    single = SQLSpecPlugin(auth_cfg)
    assert isinstance(single.sqlspec, SQLSpec)
    assert single.config == [auth_cfg]
    assert single.get_config() is auth_cfg
    assert single.get_config(AiosqliteConfig) is auth_cfg
    assert single.get_config(auth_cfg) is auth_cfg
    assert single.get_config("auth") is auth_cfg
    assert single.get_config("auth_session") is auth_cfg

    multi = SQLSpecPlugin([auth_cfg, sync_cfg])
    assert multi.config == [auth_cfg, sync_cfg]
    assert multi.get_config(AiosqliteConfig) is auth_cfg
    assert multi.get_config(SqliteConfig) is sync_cfg
    assert multi.get_config("reporting") is sync_cfg
    with pytest.raises(KeyError, match="Multiple database configurations"):
        multi.get_config()

    empty = SQLSpecPlugin(SQLSpec())
    with pytest.raises(KeyError, match="No database configurations"):
        empty.get_config()


def test_uses_default_session_key() -> None:
    """FastAPI should default to DEFAULT_SESSION_KEY."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY  # pyright: ignore[reportPrivateUsage]
    assert DEFAULT_SESSION_KEY == "db_session"


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key via fastapi config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"fastapi": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == custom_key  # pyright: ignore[reportPrivateUsage]


def test_respects_fastapi_extension_config_key() -> None:
    """Plugin should respect FastAPI-specific extension_config."""
    custom_key = "fastapi_db"
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"fastapi": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == custom_key  # pyright: ignore[reportPrivateUsage]


def test_ignores_starlette_extension_config_key() -> None:
    """FastAPI plugin should not read Starlette extension config."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"session_key": "starlette_db"}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY  # pyright: ignore[reportPrivateUsage]


def test_fastapi_extension_config_takes_fastapi_config() -> None:
    """FastAPI-specific config should own FastAPI settings."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"starlette": {"session_key": "starlette_db"}, "fastapi": {"session_key": "fastapi_db"}},
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == "fastapi_db"  # pyright: ignore[reportPrivateUsage]


def test_fastapi_sqlcommenter_framework_defaults_to_fastapi() -> None:
    """FastAPI plugin should report FastAPI to SQLCommenter by default."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].sqlcommenter_framework == "fastapi"  # pyright: ignore[reportPrivateUsage]


def test_provide_session_works_in_route() -> None:
    """Test that provide_session() works correctly in FastAPI routes."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"fastapi": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.get("/test")
    async def test_route(db=Depends(plugin.provide_session())):
        result = await db.execute("SELECT 1 as value")
        return {"value": result.scalar()}

    with TestClient(app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


class _TenantConn:
    def __init__(self, tenant_id: str | None, variables: dict[str, str]) -> None:
        self.tenant_id = tenant_id
        self.variables = variables
        self.commits: list[str | None] = []
        self.rollbacks: list[str | None] = []

    async def commit(self) -> None:
        self.commits.append(self.tenant_id)

    async def rollback(self) -> None:
        self.rollbacks.append(self.tenant_id)


class _TenantDriver:
    _cache_holder = None

    def __init__(self, connection: _TenantConn, statement_config: Any = None, driver_features: Any = None) -> None:
        self.connection: Any = connection
        self.statement_config = statement_config
        self.driver_features = driver_features or {}
        self.observability = _IdleObservability()

    def attach_observability(self, observability: Any) -> None:
        self.observability = observability

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()

    def _detach_connection(self) -> None:
        self.connection = None  # type: ignore[assignment]


class _IdleObservability:
    is_idle = True


def _make_deferred_tenant_config(checkouts: list[_TenantConn]) -> AiosqliteConfig:
    class _DeferredTenantConfig(AiosqliteConfig):
        driver_type: ClassVar[type[Any]] = _TenantDriver

        async def create_pool(self) -> Any:
            return object()

        async def close_pool(self) -> None:
            return None

        @asynccontextmanager
        async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
            payload = TenantContext.get()
            if payload is None:
                raise TenantContextError("Tenant context required")
            conn = _TenantConn(payload.tenant_id, dict(payload.variables))
            checkouts.append(conn)
            yield conn

    cfg = _DeferredTenantConfig(
        bind_key="tenant_db",
        connection_config={"database": ":memory:"},
        extension_config={
            "fastapi": {
                "session_key": "tenant_session",
                "connection_key": "tenant_conn",
                "pool_key": "tenant_pool",
                "commit_mode": "autocommit",
                "enable_correlation_middleware": False,
                "enable_sqlcommenter_middleware": False,
            }
        },
    )
    cfg.driver_features["tenant_strategy"] = object()  # type: ignore[typeddict-unknown-key]
    return cfg


def test_fastapi_deferred_tenant_depends_skips_public_routes_and_handles_transactions() -> None:
    """Deferred tenant leases acquire lazily after auth Depends() without blocking /docs or /health."""
    checkouts: list[_TenantConn] = []
    auth_cfg = AiosqliteConfig(
        bind_key="auth",
        connection_config={"database": ":memory:"},
        extension_config={
            "fastapi": {
                "session_key": "auth_session",
                "connection_key": "auth_conn",
                "pool_key": "auth_pool",
                "enable_correlation_middleware": False,
                "enable_sqlcommenter_middleware": False,
            }
        },
    )
    tenant_cfg = _make_deferred_tenant_config(checkouts)
    app = FastAPI()
    plugin = SQLSpecPlugin([auth_cfg, tenant_cfg], app)

    async def bind_tenant(
        request: Request, org_id: str, auth_db: Annotated[Any, Depends(plugin.provide_session(auth_cfg))]
    ) -> str:
        probe = await auth_db.execute("SELECT 1 AS ok")
        assert probe.scalar() == 1
        token = request.headers.get("authorization")
        if token != "Bearer valid":
            raise HTTPException(status_code=401, detail="Authentication required")
        if org_id not in {"acme", "globex"}:
            raise HTTPException(status_code=403, detail="Forbidden tenant")
        set_request_tenant(request, org_id, variables={"region": "eu"})
        return org_id

    @app.get("/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/orgs/{org_id}/projects")
    async def list_projects(
        _org_id: Annotated[str, Depends(bind_tenant)],
        db: Annotated[_TenantDriver, Depends(plugin.provide_session(tenant_cfg))],
        conn: Annotated[_TenantConn, Depends(plugin.provide_connection("tenant_session"))],
    ) -> dict[str, Any]:
        assert db.connection is conn
        return {"tenant": db.connection.tenant_id, "variables": db.connection.variables}

    @app.post("/orgs/{org_id}/fail")
    async def fail_write(
        _org_id: Annotated[str, Depends(bind_tenant)],
        _db: Annotated[_TenantDriver, Depends(plugin.provide_session(tenant_cfg))],
    ) -> None:
        raise RuntimeError("write failure")

    @app.get("/unguarded")
    async def unguarded(_db: Annotated[_TenantDriver, Depends(plugin.provide_session(tenant_cfg))]) -> dict[str, str]:
        return {"status": "unexpected"}

    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/health").json() == {"ok": True}
        assert client.get("/openapi.json").status_code == 200
        assert client.get("/docs").status_code == 200
        assert client.get("/orgs/acme/projects").status_code == 401
        assert client.get("/orgs/evil/projects", headers={"authorization": "Bearer valid"}).status_code == 403
        assert checkouts == []

        ok = client.get("/orgs/globex/projects", headers={"authorization": "Bearer valid"})
        assert ok.status_code == 200
        assert ok.json() == {"tenant": "globex", "variables": {"region": "eu"}}
        assert len(checkouts) == 1
        assert checkouts[0].commits == ["globex"]
        assert checkouts[0].rollbacks == []

        failed = client.post("/orgs/acme/fail", headers={"authorization": "Bearer valid"})
        assert failed.status_code == 500
        assert len(checkouts) == 2
        assert checkouts[1].commits == []
        assert checkouts[1].rollbacks == ["acme"]

    with TestClient(app, raise_server_exceptions=True) as strict_client:
        assert strict_client.get("/unguarded").status_code == 401
    assert len(checkouts) == 2
    assert TenantContext.get() is None


@pytest.mark.parametrize(("status_code", "expected_rows"), [(200, [(1,), (2,)]), (500, [])])
def test_fastapi_prepared_session_streaming_completion_and_detachment(
    tmp_path: Any, status_code: int, expected_rows: list[tuple[int]]
) -> None:
    """FastAPI prepares sessions once, emits start/end, stores _ResponseCompletion, keeps StreamingResponse connected through exhaustion, and detaches."""
    database = str(tmp_path / f"fastapi_stream_{status_code}.db")
    setup = sqlite3.connect(database)
    setup.execute("CREATE TABLE stream_items (value INTEGER)")
    setup.commit()
    setup.close()

    events: list[str] = []
    config = AiosqliteConfig(
        connection_config={"database": database},
        extension_config={"fastapi": {"commit_mode": "autocommit"}},
        observability_config=ObservabilityConfig(
            lifecycle=cast(
                "LifecycleConfig",
                {
                    "on_session_start": [lambda _ctx: events.append("start")],
                    "on_session_end": [lambda _ctx: events.append("end")],
                },
            )
        ),
    )
    prepare_count = 0
    original_prepare = config._prepare_driver

    def _prepare(driver: Any) -> Any:
        nonlocal prepare_count
        prepare_count += 1
        return original_prepare(driver)

    config._prepare_driver = _prepare  # type: ignore[method-assign]
    app = FastAPI()
    plugin = SQLSpecPlugin(config, app)
    retained: list[Any] = []
    owners: list[Any] = []

    @app.get("/stream")
    async def stream_items(
        request: Request, db: Annotated[Any, Depends(plugin.provide_session())]
    ) -> StreamingResponse:
        retained.append(db)
        owners.append(get_state_value(request.state, _response_owner_key("db_connection")))
        await db.begin()
        await db.execute("INSERT INTO stream_items VALUES (1)")
        val = await db.select_value("SELECT 1")

        async def body() -> AsyncIterator[bytes]:
            await db.execute("INSERT INTO stream_items VALUES (2)")
            second = await db.select_value("SELECT 2")
            yield f"{val}:{second}".encode()

        return StreamingResponse(body(), status_code=status_code)

    with TestClient(app) as client:
        response = client.get("/stream")
        assert response.status_code == status_code
        assert response.text == "1:2"

    assert prepare_count == 1
    assert events == ["start", "end"]
    assert len(owners) == 1
    assert isinstance(owners[0], _ResponseCompletion)
    assert not hasattr(owners[0], "__dict__")
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        _ = retained[0].connection.cursor

    reader = sqlite3.connect(database)
    try:
        assert reader.execute("SELECT value FROM stream_items ORDER BY value").fetchall() == expected_rows
    finally:
        reader.close()
