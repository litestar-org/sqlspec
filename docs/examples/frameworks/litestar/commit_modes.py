"""Configure autocommit and manual transaction commit modes on Litestar database binds."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

spec = SQLSpec()
spec.add_config(
    SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"litestar": {"commit_mode": "autocommit"}}
    )
)
spec.add_config(
    SqliteConfig(
        bind_key="manual",
        connection_config={"database": ":memory:"},
        extension_config={"litestar": {"commit_mode": "manual"}},
    )
)
plugin = SQLSpecPlugin(sqlspec=spec)
# --8<-- [end:example]

__all__ = ("main", "plugin", "spec")


def main() -> SQLSpecPlugin:
    """Create a Litestar SQLSpecPlugin with autocommit and manual binds."""
    return plugin


if __name__ == "__main__":
    print(main())
