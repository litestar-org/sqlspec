"""FastAPI dependency factories preserve routing, mode, and schema contracts."""

import inspect
from pathlib import Path
from typing import Annotated, Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.fastapi import SQLSpecPlugin
from tests.unit.driver.test_sync_routing_session import seed_routing_databases


@pytest.mark.parametrize("async_config", [False, True])
@pytest.mark.parametrize("key_kind", ["none", "instance", "type", "bind", "read_key"])
def test_read_and_routing_depends_support_keys_and_openapi(tmp_path: Path, async_config: bool, key_kind: str) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config_type = AiosqliteConfig if async_config else SqliteConfig
    config = config_type(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={"fastapi": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    app = FastAPI()
    plugin = SQLSpecPlugin(config, app)
    key = {"none": None, "instance": config, "type": config_type, "bind": "shop", "read_key": "read_session"}[key_kind]
    read_dependency = plugin.provide_read_session(key)
    routing_dependency = plugin.provide_routing_session(key)
    assert inspect.iscoroutinefunction(read_dependency) is async_config
    assert inspect.signature(read_dependency).return_annotation is Any

    @app.get("/read")
    async def read(session: Annotated[Any, Depends(read_dependency)]) -> dict[str, str]:
        if async_config:
            return {"origin": await session.select_value("SELECT origin FROM notes WHERE id=1")}
        return {"origin": session.select_value("SELECT origin FROM notes WHERE id=1")}

    @app.post("/route")
    async def route(session: Annotated[Any, Depends(routing_dependency)]) -> dict[str, str]:
        if async_config:
            before = await session.select_value("SELECT origin FROM notes WHERE id=1")
            await session.execute("INSERT INTO notes VALUES (4,'new')")
            after = await session.select_value("SELECT origin FROM notes WHERE id=1")
        else:
            before = session.select_value("SELECT origin FROM notes WHERE id=1")
            session.execute("INSERT INTO notes VALUES (4,'new')")
            after = session.select_value("SELECT origin FROM notes WHERE id=1")
        return {"before": before, "after": after}

    assert app.openapi()["paths"]["/read"]["get"]["responses"]
    with TestClient(app) as client:
        assert client.get("/read").json() == {"origin": "replica"}
        assert client.post("/route").json() == {"before": "replica", "after": "primary"}


def test_narrowed_read_dependencies_validate_modes_and_unknown_keys() -> None:
    sync = SQLSpecPlugin(SqliteConfig())
    asynchronous = SQLSpecPlugin(AiosqliteConfig())
    assert not inspect.iscoroutinefunction(sync.provide_sync_read_session())
    assert not inspect.iscoroutinefunction(sync.provide_sync_read_connection())
    assert inspect.iscoroutinefunction(asynchronous.provide_async_read_session())
    assert inspect.iscoroutinefunction(asynchronous.provide_async_read_connection())
    with pytest.raises(ImproperConfigurationError, match="mode"):
        sync.provide_async_read_session()
    with pytest.raises(ImproperConfigurationError, match="mode"):
        asynchronous.provide_sync_read_connection()
    with pytest.raises(ImproperConfigurationError, match="resolve"):
        sync.provide_read_session("missing")
