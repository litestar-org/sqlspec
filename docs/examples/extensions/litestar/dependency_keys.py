"""Register distinct Litestar dependency injection keys for multiple database configurations."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

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
# --8<-- [end:example]

__all__ = ("main", "plugin", "spec")


def main() -> SQLSpecPlugin:
    """Configure primary and analytics SQLite binds with explicit Litestar session_key values."""
    assert plugin.get_config("analytics").bind_key == "analytics"
    assert plugin.get_config("primary").bind_key == "primary"
    return plugin


if __name__ == "__main__":
    print(main())
