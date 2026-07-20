# pyright: reportPrivateUsage=false
"""Cross-backend extension storage-option contract tests."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.events import AiosqliteEventQueueStore
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.events import AsyncpgEventQueueStore
from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
from sqlspec.adapters.bigquery.events import BigQueryEventQueueStore
from sqlspec.adapters.bigquery.litestar import BigQueryStore
from sqlspec.adapters.cockroach_asyncpg.litestar import CockroachAsyncpgStore
from sqlspec.adapters.cockroach_psycopg.litestar import CockroachPsycopgAsyncStore, CockroachPsycopgSyncStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar import SQLiteStore
from sqlspec.exceptions import ImproperConfigurationError


class RecordingConnection:
    """Record synchronous SQLite PRAGMA execution."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


class AsyncRecordingConnection:
    """Record asynchronous SQLite PRAGMA execution."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_litestar_config_rejects_unhonored_adapter_option() -> None:
    """A declared storage option must not be silently ignored by another backend."""
    with pytest.raises(ImproperConfigurationError, match=r"asyncpg.*shard_count"):
        AsyncpgStore(_mock_config("litestar", {"shard_count": 8}))


def test_events_config_rejects_unknown_option() -> None:
    """Events stores reject keys outside their backend-specific accepted set."""
    with pytest.raises(ImproperConfigurationError, match=r"asyncpg.*unknown_storage_option"):
        AsyncpgEventQueueStore(_mock_config("events", {"unknown_storage_option": True}))


def test_adk_config_rejects_unknown_option() -> None:
    """ADK validates settings against the selected adapter's typed config."""
    with pytest.raises(ImproperConfigurationError, match=r"asyncpg.*unknown_storage_option"):
        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/test"},
            extension_config={"adk": {"unknown_storage_option": True}},
        )
        AsyncpgADKStore(config)


def test_bigquery_litestar_partition_expiration() -> None:
    """BigQuery session DDL retains clustering and adds configured partition options."""
    store = BigQueryStore(
        _mock_config(
            "litestar", {"partitioning": True, "partition_expiration_days": 7, "require_partition_filter": True}
        )
    )

    ddl = store._table_ddl()

    assert "PARTITION BY DATE(expires_at)" in ddl
    assert "CLUSTER BY session_id" in ddl
    assert "require_partition_filter = TRUE" in ddl
    assert "partition_expiration_days = 7" in ddl


def test_bigquery_events_partition_expiration() -> None:
    """BigQuery queue DDL partitions available work while retaining clustering."""
    store = BigQueryEventQueueStore(
        _mock_config("events", {"partitioning": True, "partition_expiration_days": 3, "require_partition_filter": True})
    )

    ddl = store.create_statements()[0]

    assert "PARTITION BY DATE(available_at)" in ddl
    assert "CLUSTER BY channel, status, available_at" in ddl
    assert "require_partition_filter = TRUE" in ddl
    assert "partition_expiration_days = 3" in ddl


@pytest.mark.parametrize("store_type", [CockroachAsyncpgStore, CockroachPsycopgAsyncStore, CockroachPsycopgSyncStore])
def test_cockroach_litestar_hash_shard_and_row_level_ttl(store_type: type[Any]) -> None:
    """Cockroach session DDL supports opt-in hash sharding and row-level TTL."""
    store = store_type(
        _mock_config(
            "litestar",
            {
                "enable_hash_sharded_indexes": True,
                "hash_shard_bucket_count": 8,
                "ttl_expiration_expression": "expires_at",
            },
        )
    )

    ddl = store._table_ddl()

    assert "WITH (ttl_expiration_expression = 'expires_at')" in ddl
    assert "USING HASH WITH (bucket_count = 8)" in ddl


def test_asyncpg_events_autovacuum_options() -> None:
    """PostgreSQL queue DDL honors explicit fillfactor and autovacuum tuning."""
    store = AsyncpgEventQueueStore(
        _mock_config(
            "events", {"fillfactor": 70, "autovacuum_vacuum_scale_factor": 0.1, "autovacuum_analyze_scale_factor": 0.2}
        )
    )

    ddl = store.create_statements()[0]

    assert "fillfactor = 70" in ddl
    assert "autovacuum_vacuum_scale_factor = 0.1" in ddl
    assert "autovacuum_analyze_scale_factor = 0.2" in ddl


def test_sqlite_litestar_pragma_profile_applied() -> None:
    """SQLite Litestar setup applies its opt-in profile and validated overrides."""
    config = SqliteConfig(
        extension_config={
            "litestar": {"pragma_profile": True, "pragma_overrides": {"cache_size": -32000, "journal_mode": "WAL"}}
        }
    )
    store = SQLiteStore(config)
    connection = RecordingConnection()

    store.prepare_schema_sync(SimpleNamespace(connection=connection))

    assert connection.statements[:4] == _pragma_profile()
    assert connection.statements[-2:] == ["PRAGMA cache_size = -32000", "PRAGMA journal_mode = WAL"]


def test_sqlite_events_pragma_profile_applied() -> None:
    """SQLite events setup uses the same opt-in profile."""
    store = SqliteEventQueueStore(SqliteConfig(extension_config={"events": {"pragma_profile": True}}))
    connection = RecordingConnection()

    store.prepare_schema_sync(SimpleNamespace(connection=connection))

    assert connection.statements == _pragma_profile()


async def test_aiosqlite_litestar_pragma_profile_applied() -> None:
    """AioSQLite Litestar setup mirrors synchronous PRAGMA behavior."""
    store = AiosqliteStore(AiosqliteConfig(extension_config={"litestar": {"pragma_profile": True}}))
    connection = AsyncRecordingConnection()

    await store.prepare_schema_async(SimpleNamespace(connection=connection))

    assert connection.statements == _pragma_profile()


async def test_aiosqlite_events_pragma_profile_applied() -> None:
    """AioSQLite events setup mirrors synchronous PRAGMA behavior."""
    store = AiosqliteEventQueueStore(AiosqliteConfig(extension_config={"events": {"pragma_profile": True}}))
    connection = AsyncRecordingConnection()

    await store.prepare_schema_async(SimpleNamespace(connection=connection))

    assert connection.statements == _pragma_profile()


def _mock_config(extension_name: str, settings: dict[str, object]) -> MagicMock:
    config = MagicMock()
    config.extension_config = {extension_name: settings}
    return config


def _pragma_profile() -> list[str]:
    return [
        "PRAGMA foreign_keys = ON",
        "PRAGMA cache_size = -64000",
        "PRAGMA mmap_size = 30000000",
        "PRAGMA journal_size_limit = 67108864",
    ]
