from __future__ import annotations

import pytest

__all__ = ("test_cockroach_asyncpg_connection",)


def test_cockroach_asyncpg_connection() -> None:
    pytest.importorskip("asyncpg")
    # start-example
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

    config = CockroachAsyncpgConfig(connection_config={"dsn": "postgresql://user:pass@localhost:26257/defaultdb"})
    # end-example

    assert "dsn" in config.connection_config
