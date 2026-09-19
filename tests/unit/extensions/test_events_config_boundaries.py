"""Events settings are validated for the selected adapter and transport."""

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.events import AsyncpgEventQueueStore, AsyncpgEventsConfig
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.events import BigQueryEventQueueStore, BigQueryEventsConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.events import OracleEventsConfig, OracleEventsPartitionConfig, OracleSyncEventQueueStore
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psqlpy.events import PsqlpyEventQueueStore, PsqlpyEventsConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.events import PsycopgEventsConfig, PsycopgSyncEventQueueStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.events import SqliteEventQueueStore
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import AsyncEventChannel, SyncEventChannel


@pytest.mark.parametrize("key", ["listener_queue_capacity", "run_migrations", "fillfactor"])
def test_sqlite_events_rejects_unsupported_settings(key: str) -> None:
    config = SqliteConfig(extension_config={"events": {key: 4}})
    with pytest.raises(ImproperConfigurationError, match=f"Unsupported events configuration.*{key}"):
        SqliteEventQueueStore(config)
    with pytest.raises(ImproperConfigurationError, match=f"Unsupported events configuration.*{key}"):
        SyncEventChannel(config)


@pytest.mark.parametrize("backend", ["notify", "notify_queue"])
@pytest.mark.parametrize("key", ["run_migrations", "pragma_profile"])
def test_native_postgres_events_rejects_unsupported_settings(backend: str, key: str) -> None:
    config = AsyncpgConfig(extension_config={"events": {"backend": backend, key: True}})
    with pytest.raises(ImproperConfigurationError, match=f"Unsupported events configuration.*{key}"):
        AsyncEventChannel(config)


@pytest.mark.parametrize("backend", ["notify", "notify_queue", "poll_queue"])
def test_postgres_typed_events_preserve_capacity_and_storage(backend: str) -> None:
    settings = AsyncpgEventsConfig(fillfactor=75, listener_queue_capacity=4)
    config = AsyncpgConfig(extension_config={"events": {**settings, "backend": backend}})
    assert AsyncEventChannel(config).backend_name == backend
    assert "fillfactor = 75" in AsyncpgEventQueueStore(config).create_statements()[0]

    psycopg_settings = PsycopgEventsConfig(fillfactor=75, listener_queue_capacity=4)
    for config_type in (PsycopgSyncConfig, PsycopgAsyncConfig):
        psycopg_config = config_type(extension_config={"events": {**psycopg_settings, "backend": backend}})
        channel = (
            AsyncEventChannel(psycopg_config)
            if isinstance(psycopg_config, PsycopgAsyncConfig)
            else SyncEventChannel(psycopg_config)
        )
        assert channel.backend_name == backend
    assert (
        "fillfactor = 75"
        in PsycopgSyncEventQueueStore(
            PsycopgSyncConfig(extension_config={"events": psycopg_settings})
        ).create_statements()[0]
    )


def test_psqlpy_typed_storage_and_unsupported_capacity() -> None:
    settings = PsqlpyEventsConfig(fillfactor=80)
    assert (
        "fillfactor = 80"
        in PsqlpyEventQueueStore(PsqlpyConfig(extension_config={"events": settings})).create_statements()[0]
    )
    config = PsqlpyConfig(extension_config={"events": {"listener_queue_capacity": 4}})
    with pytest.raises(ImproperConfigurationError, match=r"Unsupported events configuration.*listener_queue_capacity"):
        AsyncEventChannel(config)


def test_bigquery_typed_partitioning_preserves_queue_ddl() -> None:
    settings = BigQueryEventsConfig(partitioning=True, partition_expiration_days=7, require_partition_filter=True)
    ddl = BigQueryEventQueueStore(BigQueryConfig(extension_config={"events": settings})).create_statements()[0]
    assert "PARTITION BY DATE(available_at)" in ddl
    assert "partition_expiration_days = 7" in ddl
    assert "require_partition_filter = TRUE" in ddl


@pytest.mark.parametrize("backend", ["aq", "txeventq"])
def test_oracle_typed_events_preserve_native_transport_options(backend: str) -> None:
    settings = OracleEventsConfig(aq_queue="APP_EVENTS", aq_visibility=1, aq_wait_seconds=2)
    for config_type in (OracleSyncConfig, OracleAsyncConfig):
        config = config_type(extension_config={"events": {**settings, "backend": backend}})
        channel = AsyncEventChannel(config) if isinstance(config, OracleAsyncConfig) else SyncEventChannel(config)
        assert channel.backend_name == backend
        bad_config = config_type(extension_config={"events": {**settings, "backend": backend, "fillfactor": 80}})
        with pytest.raises(ImproperConfigurationError, match=r"Unsupported events configuration.*fillfactor"):
            if isinstance(bad_config, OracleAsyncConfig):
                AsyncEventChannel(bad_config)
            else:
                SyncEventChannel(bad_config)


def test_oracle_typed_events_preserve_storage_options() -> None:
    settings = OracleEventsConfig(
        in_memory=True,
        compression={"enabled": True, "algorithm": "basic"},
        partitioning=OracleEventsPartitionConfig(
            strategy="hash", partition_count=8, partition_key="channel", queue_partition_key="event_id"
        ),
    )
    ddl = OracleSyncEventQueueStore(OracleSyncConfig(extension_config={"events": settings})).create_statements()[0]
    assert "INMEMORY" in ddl
    assert "COMPRESS BASIC" in ddl
    assert "PARTITION BY HASH (event_id) PARTITIONS 8" in ddl


def test_custom_config_events_preserve_application_settings() -> None:
    class CustomSqliteConfig(SqliteConfig):
        pass

    config = CustomSqliteConfig(extension_config={"events": {"application_option": True}})
    assert SyncEventChannel(config).backend_name == "poll_queue"
