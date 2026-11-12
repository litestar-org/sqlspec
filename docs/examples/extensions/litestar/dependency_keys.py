"""Register distinct Litestar dependency injection keys for multiple database configurations."""

# --8<-- [start:example]
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

plugin = SQLSpecPlugin([
    SqliteConfig(
        bind_key="primary",
        connection_config={"database": ":memory:"},
        extension_config={
            "litestar": {"session_key": "primary", "connection_key": "primary_conn", "pool_key": "primary_pool"}
        },
    ),
    SqliteConfig(
        bind_key="analytics",
        connection_config={"database": ":memory:"},
        extension_config={
            "litestar": {"session_key": "analytics", "connection_key": "analytics_conn", "pool_key": "analytics_pool"}
        },
    ),
])
# --8<-- [end:example]

__all__ = ("main", "plugin")


def main() -> SQLSpecPlugin:
    """Configure primary and analytics SQLite binds with explicit Litestar session_key values."""
    assert plugin.get_config("analytics").bind_key == "analytics"
    assert plugin.get_config("primary").bind_key == "primary"
    return plugin


if __name__ == "__main__":
    print(main())
