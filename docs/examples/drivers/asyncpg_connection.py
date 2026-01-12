from __future__ import annotations

import pytest

__all__ = ("test_asyncpg_connection",)


def test_asyncpg_connection() -> None:
    pytest.importorskip("asyncpg")
    # start-example
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://user:pass@localhost:5432/app"},
        pool_config={"min_size": 1, "max_size": 5},
    )
    # end-example

    assert "dsn" in config.connection_config
