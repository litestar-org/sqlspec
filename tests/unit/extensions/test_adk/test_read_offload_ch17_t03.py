"""Concrete ADK provider selection retains primary mutation consistency."""

import pytest

from tests.unit.extensions.test_adk.test_read_offload_ch16_t01 import exercise_concrete_store


@pytest.mark.parametrize(
    ("adapter", "store_name", "config_name", "artifact"),
    [
        ("duckdb", "DuckDBADKStore", "DuckDBConfig", False),
        ("duckdb", "DuckDBADKMemoryStore", "DuckDBConfig", False),
        ("arrow_odbc", "ArrowOdbcADKStore", "ArrowOdbcConfig", False),
        ("arrow_odbc", "ArrowOdbcADKMemoryStore", "ArrowOdbcConfig", False),
        ("sybase", "SybaseADKStore", "SybaseConfig", False),
        ("sybase", "SybaseADKMemoryStore", "SybaseConfig", False),
    ],
)
async def test_concrete_store_offloads_only_pure_reads(
    monkeypatch: pytest.MonkeyPatch, adapter: str, store_name: str, config_name: str, artifact: bool
) -> None:
    await exercise_concrete_store(monkeypatch, adapter, store_name, config_name, artifact)
