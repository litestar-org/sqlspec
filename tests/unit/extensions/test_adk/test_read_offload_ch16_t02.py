"""Concrete ADK provider selection retains primary mutation consistency."""

import pytest

from tests.unit.extensions.test_adk.test_read_offload_ch16_t01 import exercise_concrete_store


@pytest.mark.parametrize(
    ("adapter", "store_name", "config_name", "artifact"),
    [
        ("cockroach_asyncpg", "CockroachAsyncpgADKStore", "CockroachAsyncpgConfig", False),
        ("cockroach_asyncpg", "CockroachAsyncpgADKMemoryStore", "CockroachAsyncpgConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgAsyncADKStore", "CockroachPsycopgAsyncConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgSyncADKStore", "CockroachPsycopgSyncConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgAsyncADKMemoryStore", "CockroachPsycopgAsyncConfig", False),
        ("cockroach_psycopg", "CockroachPsycopgSyncADKMemoryStore", "CockroachPsycopgSyncConfig", False),
        ("adbc", "AdbcADKStore", "AdbcConfig", False),
        ("adbc", "AdbcADKMemoryStore", "AdbcConfig", False),
    ],
)
async def test_concrete_store_offloads_only_pure_reads(
    monkeypatch: pytest.MonkeyPatch, adapter: str, store_name: str, config_name: str, artifact: bool
) -> None:
    await exercise_concrete_store(monkeypatch, adapter, store_name, config_name, artifact)
