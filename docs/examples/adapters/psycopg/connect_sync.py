"""Psycopg configuration for synchronous SQLSpec sessions."""

import os

from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.core.statement import SQL

__all__ = ("main",)


DSN = os.getenv("SQLSPEC_PSYCOPG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
config = PsycopgSyncConfig(bind_key="docs_psycopg", pool_config={"conninfo": DSN, "min_size": 1, "max_size": 4})


def main() -> None:
    """Open a Psycopg session and fetch version metadata."""
    with config.provide_session() as session:
        result = session.execute(SQL("SELECT version() AS version"))
        row = result.one_or_none()
        if row:
            print({"adapter": "psycopg", "version": row["version"]})
        else:
            print({"adapter": "psycopg", "version": "unknown"})


if __name__ == "__main__":
    main()
