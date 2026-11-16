from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementConfig


@pytest.mark.parametrize("config_type", [SqliteConfig, AiosqliteConfig, DuckDBConfig])
def test_derive_pool_clones_without_sharing_runtime_state(config_type: Any) -> None:
    primary = config_type(connection_config={"database": ":memory:"}, bind_key="app")
    replica = primary.derive_pool(statement_config=StatementConfig(dialect="sqlite"))
    assert type(replica) is type(primary)
    assert replica.connection_instance is None
    assert replica._tenant_key is primary
    assert replica.driver_features["_statement_cache_holder"] is not primary.driver_features["_statement_cache_holder"]
    replica.connection_config["timeout"] = 12
    assert primary.connection_config.get("timeout") != 12
    chained = replica.derive_pool()
    assert chained._tenant_key is primary
    independent = primary.derive_pool(bind_key="analytics")
    assert independent._tenant_key is independent
