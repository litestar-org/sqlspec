"""Construct an asynchronous SQLSpec server-side session store for Litestar."""

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore

__all__ = ("main",)


def main() -> AiosqliteStore:
    """Create an AiosqliteStore backed by an in-memory SQLite database."""
    # --8<-- [start:example]
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    return AiosqliteStore(config)
    # --8<-- [end:example]


if __name__ == "__main__":
    print(main())
