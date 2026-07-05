"""SQLite ADK adapter-local configuration tests."""

from typing import get_type_hints

import pytest

from sqlspec.adapters.sqlite.adk import SqliteADKConfig, SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError


class RecordingConnection:
    """Record SQL executed by ADK PRAGMA setup."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_sqlite_adk_config_extends_shared_config_and_exports_sqlite_knobs() -> None:
    """SqliteADKConfig should inherit shared ADK settings and add only SQLite-specific knobs."""
    annotations = get_type_hints(SqliteADKConfig, include_extras=True)

    assert set(ADKConfig.__annotations__) <= set(annotations)
    assert {"pragma_overrides", "fts_tokenize", "fts_detail"} <= set(annotations)


def test_sqlite_adk_defaults_preserve_existing_pragma_profile_and_ignore_driver_features() -> None:
    """Default ADK PRAGMAs should stay fixed and not read global driver_features."""
    config = SqliteConfig(driver_features={"pragma_overrides": {"cache_size": -32000}})
    store = SqliteADKStore(config)
    connection = RecordingConnection()

    store._apply_pragmas(connection)

    assert connection.statements == [
        "PRAGMA foreign_keys = ON",
        "PRAGMA cache_size = -64000",
        "PRAGMA mmap_size = 30000000",
        "PRAGMA journal_size_limit = 67108864",
    ]


def test_sqlite_adk_pragma_overrides_apply_after_defaults() -> None:
    """Adapter-local ADK PRAGMA overrides should render through SQLite validation and apply last."""
    config = SqliteConfig(
        extension_config={
            "adk": {"pragma_overrides": {"cache_size": -32000, "foreign_keys": False, "journal_mode": "WAL"}}
        }
    )
    store = SqliteADKStore(config)
    connection = RecordingConnection()

    store._apply_pragmas(connection)

    assert connection.statements[-3:] == [
        "PRAGMA cache_size = -32000",
        "PRAGMA foreign_keys = 0",
        "PRAGMA journal_mode = WAL",
    ]


def test_sqlite_adk_pragma_overrides_reuse_sqlite_validation() -> None:
    """ADK PRAGMA overrides should reject unsafe names before SQL rendering."""
    with pytest.raises(ImproperConfigurationError, match="PRAGMA name"):
        SqliteADKStore(
            SqliteConfig(extension_config={"adk": {"pragma_overrides": {"cache_size; DROP TABLE x": -32000}}})
        )


def test_sqlite_adk_fts5_options_render_only_when_fts_enabled() -> None:
    """FTS5 tokenizer/detail options should be emitted only for opt-in FTS DDL."""
    default_store = SqliteADKMemoryStore(SqliteConfig())
    configured_store = SqliteADKMemoryStore(
        SqliteConfig(
            extension_config={
                "adk": {"memory_use_fts": True, "fts_tokenize": "porter unicode61", "fts_detail": "column"}
            }
        )
    )

    assert "tokenize" not in default_store._memory_table_ddl()
    sql = configured_store._memory_table_ddl()
    assert "tokenize = 'porter unicode61'" in sql
    assert "detail = column" in sql
