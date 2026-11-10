"""Sanic request tenant scopes against PostgreSQL policies."""

from typing import Any
from uuid import uuid4

import pytest
from sanic import Request, Sanic, response

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.extensions.sanic import SQLSpecPlugin
from tests.integration.fixtures.tenant import tenant_policy_database as tenant_policy_database

pytestmark = pytest.mark.xdist_group("postgres")
ReusableClient = pytest.importorskip("sanic_testing.reusable").ReusableClient


@pytest.mark.parametrize("adapter", ["asyncpg", "psycopg"])
def test_sanic_tenant_isolation_and_failed_request_cleanup(
    tenant_policy_database: tuple[dict[str, Any], str], adapter: str
) -> None:
    credentials, table = tenant_policy_database
    app = Sanic("TenantAcceptance" + uuid4().hex)
    app.config.USE_UVLOOP = False
    app.ctx.test_identity = "a"
    options: dict[str, Any] = {**credentials, "min_size": 1, "max_size": 1}
    if adapter == "asyncpg":
        options["database"] = options.pop("dbname")
    config_type = AsyncpgConfig if adapter == "asyncpg" else PsycopgAsyncConfig
    config = config_type(
        connection_config=options,
        driver_features={"tenant_strategy": "rls"},
        extension_config={
            "sanic": {"tenant_extractor": lambda request: app.ctx.test_identity, "commit_mode": "manual"}
        },
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(spec, app)

    @app.get("/")
    async def rows(request: Request) -> Any:
        session = plugin.get_session(request)
        values = await session.select(f'SELECT value, pg_backend_pid() AS pid FROM "{table}" ORDER BY value')
        return response.json({"rows": values})

    @app.post("/fail")
    async def fail(request: Request) -> Any:
        session = plugin.get_session(request)
        await session.execute(f'INSERT INTO "{table}" VALUES (:tenant, :value)', {"tenant": "a", "value": 99})
        raise ValueError("handler failed")

    client: Any = ReusableClient(app)
    try:
        with client:
            pid = None
            for identity, expected in [("a", 1), ("b", 2)]:
                app.ctx.test_identity = identity
                _, result = client.get("/")
                assert result.status == 200
                assert [row["value"] for row in result.json["rows"]] == [expected]
                current_pid = result.json["rows"][0]["pid"]
                if pid is not None:
                    assert current_pid == pid
                pid = current_pid
            app.ctx.test_identity = None
            _, result = client.get("/", headers={"X-Tenant-ID": "evil"})
            assert result.status == 500
            app.ctx.test_identity = "a"
            if adapter == "psycopg":
                _, result = client.post("/fail")
                assert result.status == 500
            _, result = client.get("/")
            assert result.status == 200
            assert [row["value"] for row in result.json["rows"]] == [1]
    finally:
        Sanic.unregister_app(app)
