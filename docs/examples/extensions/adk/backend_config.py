from __future__ import annotations

import pytest

__all__ = ("test_adk_backend_config",)


def test_adk_backend_config() -> None:
    pytest.importorskip("adbc_driver_manager")
    # start-example
    from sqlspec.adapters.adbc import AdbcConfig

    adk_config = {
        "session_table": "adk_sessions",
        "events_table": "adk_events",
        "memory_table": "adk_memory_entries",
        "memory_use_fts": True,
    }

    gizmo = AdbcConfig(
        connection_config={"driver_name": "gizmosql", "gizmosql_backend": "duckdb"},
        extension_config={"adk": adk_config},
    )
    # end-example

    assert gizmo.extension_config["adk"]["memory_use_fts"] is True
