"""Flask portal session over asynchronous configurations."""

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.driver._common import _CLOSED_CONNECTION
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.flask._portal import PortalSession
from sqlspec.utils.portal import PortalProvider
from tests.unit.extensions._tenant_configs import AsyncRecordingStrategy, TenantAiosqliteConfig


def _items_database(tmp_path: Path) -> str:
    database = str(tmp_path / "items.db")
    setup = sqlite3.connect(database)
    setup.execute("CREATE TABLE items (status INTEGER)")
    setup.commit()
    setup.close()
    return database


def _stored(database: str) -> list[int]:
    reader = sqlite3.connect(database)
    try:
        return [row[0] for row in reader.execute("SELECT status FROM items ORDER BY rowid")]
    finally:
        reader.close()


def _counting_config(database: str, framework: str, checkouts: list[int]) -> AiosqliteConfig:
    class CountingConfig(AiosqliteConfig):
        def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
            checkouts.append(1)
            return super().provide_connection(*args, **kwargs)

    return CountingConfig(
        connection_config={"database": database}, extension_config={framework: {"commit_mode": "autocommit"}}
    )


def test_flask_session_on_async_configuration_blocks_through_the_portal(tmp_path: Path) -> None:
    from flask import Flask

    from sqlspec.extensions.flask import SQLSpecPlugin, set_request_tenant

    database = _items_database(tmp_path)
    checkouts: list[int] = []
    config = _counting_config(database, "flask", checkouts)
    strategy = AsyncRecordingStrategy()
    tenant_config = TenantAiosqliteConfig(
        connection_config={"database": ":memory:"},
        driver_features={"tenant_strategy": strategy},
        extension_config={"flask": {"session_key": "tenant_db", "connection_key": "tenant_conn", "pool_key": "tp"}},
    )
    app = Flask(__name__)
    plugin = SQLSpecPlugin([config, tenant_config], app)

    @app.get("/nodb")
    def nodb() -> Any:
        return {"checkouts": len(checkouts)}

    @app.get("/write/<int:status>")
    def write(status: int) -> Any:
        session = plugin.get_session()
        session.begin()
        session.execute("INSERT INTO items VALUES (:status)", {"status": status})
        set_request_tenant("acme")
        tenant_value = plugin.get_session("tenant_db").select_value("SELECT 1")
        return {"same": plugin.get_session() is session, "tenant": tenant_value}, status

    @app.get("/stream")
    def stream() -> Any:
        session = plugin.get_session()
        with session.transaction():
            session.execute("INSERT INTO items VALUES (:status)", {"status": 7})
        return {"rows": [row["status"] for row in session.select_stream("SELECT status FROM items")]}

    try:
        client = app.test_client()
        assert client.get("/nodb").json == {"checkouts": 0}
        ok = client.get("/write/201")
        assert ok.status_code == 201
        assert ok.json == {"same": True, "tenant": 1}
        assert client.get("/write/500").status_code == 500
        assert client.get("/stream").json == {"rows": [201, 7]}
    finally:
        plugin.shutdown()
    assert checkouts == [1, 1, 1]
    assert strategy.checkouts == ["acme", "acme"]
    assert _stored(database) == [201, 7]


def test_portal_session_closes_without_portal_hop(tmp_path: Path) -> None:
    provider = PortalProvider()
    provider.start()
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "portal_close.db")})
    session_ctx = config.provide_session()
    real_session: Any = None
    try:
        real_session = provider.portal.call(session_ctx.__aenter__)
        live_view = PortalSession(real_session, provider.portal)
        unstarted_view = PortalSession(real_session, PortalProvider().portal)

        assert live_view.select_value("SELECT 1") == 1
        unstarted_view._mark_closed()
        assert real_session.connection is _CLOSED_CONNECTION

        with pytest.raises(SQLSpecError, match="already closed"):
            live_view.select_value("SELECT 1")
    finally:
        if real_session is not None:
            provider.portal.call(session_ctx.__aexit__, None, None, None)
        provider.portal.call(config.close_pool)
        provider.stop()
