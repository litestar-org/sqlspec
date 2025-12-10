"""Async Oracle connection powered by SQLSpec."""

import asyncio
import os

from sqlspec.adapters.oracledb import OracleAsyncConfig

__all__ = ("main",)


USER = os.getenv("SQLSPEC_ORACLE_USER", "system")
PASSWORD = os.getenv("SQLSPEC_ORACLE_PASSWORD", "oracle")
DSN = os.getenv("SQLSPEC_ORACLE_DSN", "localhost/FREE")
config = OracleAsyncConfig(
    bind_key="docs_oracle_async", connection_config={"user": USER, "password": PASSWORD, "dsn": DSN, "min": 1, "max": 4}
)


async def main() -> None:
    """Connect to Oracle and print the current timestamp."""
    async with config.provide_session() as session:
        result = await session.select_one_or_none("SELECT systimestamp AS ts FROM dual")
        if result:
            print({"adapter": "oracledb", "timestamp": str(result["ts"])})
        else:
            print({"adapter": "oracledb", "timestamp": "unknown"})


if __name__ == "__main__":
    asyncio.run(main())
