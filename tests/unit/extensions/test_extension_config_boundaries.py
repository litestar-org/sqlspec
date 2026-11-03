"""Runtime boundaries for shared and adapter-specific extension settings."""

import importlib

import pytest

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.migrations.context import MigrationContext


@pytest.mark.parametrize("extension", ["litestar", "events", "adk"])
def test_extension_store_rejects_unused_migration_switch(extension: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="run_migrations"):
        config = SqliteConfig(extension_config={extension: {"run_migrations": True}})
        if extension == "litestar":
            SqliteStore(config)
        elif extension == "events":
            SqliteEventQueueStore(config)
        else:
            SqliteADKStore(config)


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("vector_index_type", "hnsw"),
        ("vector_dimensions", 256),
        ("enable_bm25", True),
        ("scann_num_leaves", 32),
        ("scann_quantizer", "SQ8"),
    ],
)
def test_sqlite_adk_rejects_postgres_memory_tuning(option: str, value: object) -> None:
    with pytest.raises(ImproperConfigurationError, match=option):
        config = SqliteConfig(extension_config={"adk": {option: value}})
        SqliteADKMemoryStore(config)


@pytest.mark.parametrize(
    "option", ["session_table", "events_table", "app_state_table", "user_state_table", "metadata_table", "memory_table"]
)
def test_sqlite_adk_rejects_qualified_table_names(option: str) -> None:
    """Unsupported qualified names fail before any schema creation can partially succeed."""
    config = SqliteConfig(extension_config={"adk": {option: "main.custom_table"}})
    store_type = SqliteADKMemoryStore if option == "memory_table" else SqliteADKStore

    with pytest.raises(ValueError, match="Invalid table name"):
        store_type(config)


@pytest.mark.anyio
async def test_litestar_migration_resolves_external_spanner_and_subclass_configs() -> None:
    """Litestar session migration resolves external configs via adapter_name, MRO, and litestar_store_class."""
    litestar_migration = importlib.import_module("sqlspec.extensions.litestar.migrations.0001_create_session_table")

    class ExternalSpannerConfig(SpannerSyncConfig):
        adapter_name = "spanner"

    ExternalSpannerConfig.__module__ = "myapp.database.config"
    spanner_config = ExternalSpannerConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    spanner_up = await litestar_migration.up(MigrationContext(config=spanner_config, dialect="spanner"))
    assert any("CREATE TABLE IF NOT EXISTS litestar_session" in stmt for stmt in spanner_up)

    class SubclassedSqliteConfig(SqliteConfig):
        pass

    SubclassedSqliteConfig.__module__ = "myapp.database.config"
    sqlite_config = SubclassedSqliteConfig(connection_config={"database": ":memory:"})
    sqlite_up = await litestar_migration.up(MigrationContext(config=sqlite_config, dialect="sqlite"))
    assert any("CREATE TABLE IF NOT EXISTS litestar_session" in stmt for stmt in sqlite_up)


def test_custom_stores_in_short_module_paths_report_adapter_name_on_invalid_config() -> None:
    """Custom stores outside sqlspec.adapters report resolved adapter name instead of raising IndexError."""

    class CustomEventStore(SqliteEventQueueStore):
        pass

    class CustomLitestarStore(SqliteStore):
        pass

    CustomEventStore.__module__ = "myapp"
    CustomLitestarStore.__module__ = "myapp"

    bad_events_config = SqliteConfig(extension_config={"events": {"unknown_opt": True}})
    with pytest.raises(ImproperConfigurationError, match="Unsupported events configuration key\\(s\\) for sqlite"):
        CustomEventStore(bad_events_config)

    bad_litestar_config = SqliteConfig(extension_config={"litestar": {"unknown_opt": True}})
    with pytest.raises(ImproperConfigurationError, match="Unsupported Litestar configuration key\\(s\\) for sqlite"):
        CustomLitestarStore(bad_litestar_config)
