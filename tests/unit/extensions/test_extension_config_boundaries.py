"""Runtime boundaries for shared and adapter-specific extension settings."""

import pytest

from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.exceptions import ImproperConfigurationError


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
