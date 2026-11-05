"""Construct an asynchronous SQLSpec server-side session store for Litestar."""

# --8<-- [start:example]
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore

config = AiosqliteConfig(connection_config={"database": ":memory:"})
store = AiosqliteStore(config)
# --8<-- [end:example]

__all__ = ("config", "main", "store")


def main() -> AiosqliteStore:
    """Return the configured AiosqliteStore instance."""
    return store


if __name__ == "__main__":
    print(main())
