"""Flask request tenant scopes against PostgreSQL policies."""

from typing import Any

import pytest
from flask import Flask, current_app

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.extensions.flask import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.parametrize("asynchronous", [False, True])
def test_flask_tenant_policy_isolation_and_missing_identity(
    tenant_policy_database: tuple[dict[str, Any], str], asynchronous: bool
) -> None:
    credentials, table = tenant_policy_database
    app = Flask(__name__)
    app.config["TEST_IDENTITY"] = "a"

    def tenant_extractor() -> Any:
        return current_app.config["TEST_IDENTITY"]

    options: dict[str, Any] = {**credentials, "min_size": 1, "max_size": 1}
    if asynchronous:
        options["database"] = options.pop("dbname")
    config_type = AsyncpgConfig if asynchronous else PsycopgSyncConfig
    config = config_type(
        connection_config=options,
        driver_features={"tenant_strategy": "rls"},
        extension_config={"flask": {"tenant_extractor": tenant_extractor}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(spec, app)

    @app.get("/")
    def rows() -> dict[str, Any]:
        session = plugin.get_session()
        query = f'SELECT value, pg_backend_pid() AS pid FROM "{table}"'
        if asynchronous:
            assert plugin._portal is not None
            values = plugin._portal.portal.call(session.select, query)
        else:
            values = session.select(query)
        return {"rows": values}

    client = app.test_client()
    try:
        pid = None
        for identity, expected in [("a", 1), ("b", 2), ("a", 1)]:
            app.config["TEST_IDENTITY"] = identity
            response = client.get("/")
            assert response.status_code == 200
            assert [row["value"] for row in response.json["rows"]] == [expected]
            current_pid = response.json["rows"][0]["pid"]
            if pid is not None:
                assert current_pid == pid
            pid = current_pid
        app.config["TEST_IDENTITY"] = None
        assert client.get("/", headers={"X-Tenant-ID": "evil"}).status_code == 500
        app.config["TEST_IDENTITY"] = "b"
        response = client.get("/")
        assert response.status_code == 200
        assert [row["value"] for row in response.json["rows"]] == [2]
    finally:
        plugin.shutdown()
