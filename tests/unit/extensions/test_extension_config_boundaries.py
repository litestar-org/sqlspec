"""Runtime boundaries for shared and adapter-specific extension settings."""

import pytest

from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SQLiteStore
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize("extension", ["litestar", "events", "adk"])
def test_extension_store_rejects_unused_migration_switch(extension: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="run_migrations"):
        config = SqliteConfig(extension_config={extension: {"run_migrations": True}})
        if extension == "litestar":
            SQLiteStore(config)
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
