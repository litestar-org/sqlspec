"""Configure Google ADK session, event, and memory table names on an ADBC adapter."""

# --8<-- [start:example]
from sqlspec.adapters.adbc import AdbcConfig

config = AdbcConfig(
    connection_config={"driver_name": "gizmosql", "gizmosql_backend": "duckdb"},
    extension_config={
        "adk": {
            "session_table": "adk_session",
            "events_table": "adk_event",
            "memory_table": "adk_memory",
            "memory_use_fts": True,
        }
    },
)
# --8<-- [end:example]

__all__ = ("config", "main")


def main() -> AdbcConfig:
    """Attach ADK store table settings via extension_config on AdbcConfig."""
    adk_settings = config.extension_config.get("adk")
    assert isinstance(adk_settings, dict)
    assert adk_settings.get("memory_use_fts") is True
    return config


if __name__ == "__main__":
    print(main())
