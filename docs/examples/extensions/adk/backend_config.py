"""Configure Google ADK session, event, and memory table names on an ADBC adapter."""

from sqlspec.adapters.adbc import AdbcConfig

__all__ = ("main",)


def main() -> AdbcConfig:
    """Attach ADK store table settings via extension_config on AdbcConfig."""
    # --8<-- [start:example]
    adk_config = {
        "session_table": "adk_session",
        "events_table": "adk_event",
        "memory_table": "adk_memory",
        "memory_use_fts": True,
    }
    config = AdbcConfig(
        connection_config={"driver_name": "gizmosql", "gizmosql_backend": "duckdb"},
        extension_config={"adk": adk_config},
    )
    # --8<-- [end:example]
    adk_settings = config.extension_config.get("adk")
    assert isinstance(adk_settings, dict)
    assert adk_settings.get("memory_use_fts") is True
    return config


if __name__ == "__main__":
    print(main())
