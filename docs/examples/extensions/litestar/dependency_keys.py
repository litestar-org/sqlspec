"""Register distinct Litestar dependency injection keys for multiple database configurations."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("main",)


def main() -> SQLSpecPlugin:
    """Configure primary and analytics SQLite binds with explicit Litestar session_key values."""
    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            bind_key="analytics",
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"session_key": "analytics"}},
        )
    )
    spec.add_config(
        SqliteConfig(
            bind_key="primary",
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"session_key": "primary"}},
        )
    )
    plugin = SQLSpecPlugin(sqlspec=spec)
    assert plugin.get_config("analytics").bind_key == "analytics"
    assert plugin.get_config("primary").bind_key == "primary"
    return plugin


if __name__ == "__main__":
    print(main())
