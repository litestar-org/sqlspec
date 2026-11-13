"""Concrete ADK provider selection retains primary mutation consistency."""

import pytest

from tests.unit.extensions.test_adk.test_read_offload_ch16_t01 import exercise_concrete_store


@pytest.mark.parametrize(
    ("adapter", "store_name", "config_name", "artifact"),
    [
        ("oracledb", "OracleAsyncADKStore", "OracleAsyncConfig", False),
        ("oracledb", "OracleSyncADKStore", "OracleSyncConfig", False),
        ("oracledb", "OracleAsyncADKMemoryStore", "OracleAsyncConfig", False),
        ("oracledb", "OracleSyncADKMemoryStore", "OracleSyncConfig", False),
        ("db2", "Db2SyncADKStore", "Db2SyncConfig", False),
        ("db2", "Db2AsyncADKStore", "Db2AsyncConfig", False),
        ("db2", "Db2SyncADKMemoryStore", "Db2SyncConfig", False),
        ("db2", "Db2AsyncADKMemoryStore", "Db2AsyncConfig", False),
        ("sqlanywhere", "SqlanywhereADKStore", "SqlanywhereConfig", False),
        ("sqlanywhere", "SqlanywhereADKMemoryStore", "SqlanywhereConfig", False),
    ],
)
async def test_concrete_store_offloads_only_pure_reads(
    monkeypatch: pytest.MonkeyPatch, adapter: str, store_name: str, config_name: str, artifact: bool
) -> None:
    await exercise_concrete_store(monkeypatch, adapter, store_name, config_name, artifact)
