"""Configure autocommit and manual transaction commit modes on Litestar database binds."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("main",)


def main() -> SQLSpecPlugin:
    """Create a Litestar SQLSpecPlugin with autocommit and manual binds."""
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
    return SQLSpecPlugin(sqlspec=spec)


if __name__ == "__main__":
    print(main())
