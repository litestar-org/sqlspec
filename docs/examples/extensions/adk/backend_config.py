from __future__ import annotations

import pytest

__all__ = ("test_adk_backend_config",)

pytestmark = [pytest.mark.adbc, pytest.mark.xdist_group("gizmosql")]


def test_adk_backend_config(request: pytest.FixtureRequest) -> None:
    pytest.importorskip("adbc_driver_manager")
    # start-example
    from sqlspec.adapters.adbc import AdbcConfig

    adk_config = {
        "session_table": "adk_sessions",
        "events_table": "adk_events",
        "memory_table": "adk_memory_entries",
        "memory_use_fts": True,
    }

    connection_config = {"driver_name": "gizmosql", "gizmosql_backend": "duckdb"}
    try:
        service = request.getfixturevalue("gizmosql_service")
    except pytest.FixtureLookupError:
        service = None

    if service is not None:
        connection_config.update({
            "uri": service.uri,
            "username": service.username,
            "password": service.password,
            "tls_skip_verify": True,
        })

    gizmo = AdbcConfig(connection_config=connection_config, extension_config={"adk": adk_config})
    # end-example

    assert gizmo.extension_config["adk"]["memory_use_fts"] is True
