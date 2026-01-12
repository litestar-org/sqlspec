from __future__ import annotations

import pytest

__all__ = ("test_cockroach_psycopg_connection",)


def test_cockroach_psycopg_connection() -> None:
    pytest.importorskip("psycopg")
    # start-example
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig

    config = CockroachPsycopgSyncConfig(connection_config={"dsn": "postgresql://user:pass@localhost:26257/defaultdb"})
    # end-example

    assert "dsn" in config.connection_config
