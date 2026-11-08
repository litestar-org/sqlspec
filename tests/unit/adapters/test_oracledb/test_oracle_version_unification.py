# pyright: reportPrivateUsage=false
"""Unification of Oracle version / JSON-storage detection onto the data dictionary.

Covers the C1 contract:
- version resolved once per config/pool lifetime (config-scoped cache),
- one JSON-threshold source shared by the connection handlers and OracleVersionInfo,
- the full storage ladder (JSON_NATIVE / BLOB_JSON / BLOB_PLAIN) with every rung
  version-stubbed for selection and serialization round-trip,
- no ``_extract_oracle_major`` string parser,
- stores read the shared cache instead of instantiating fresh dictionaries,
- a single ``JSONStorageType`` object across the events/adk surfaces.
"""

import importlib
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.oracledb import config as oracle_config_module
from sqlspec.adapters.oracledb.adk import store as adk_store
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.data_dictionary import (
    JSONStorageType,
    OracledbAsyncDataDictionary,
    OracledbSyncDataDictionary,
    OracleVersionCache,
    OracleVersionInfo,
    storage_type_from_version,
)
from sqlspec.adapters.oracledb.events import store as events_store
from sqlspec.migrations.context import MigrationContext

adk_migration = importlib.import_module("sqlspec.extensions.adk.migrations.0001_create_adk_tables")


def _version_for_rung(rung: JSONStorageType) -> OracleVersionInfo:
    if rung is JSONStorageType.JSON_NATIVE:
        return OracleVersionInfo(21, 3, 0, compatible="21.0.0")
    if rung is JSONStorageType.BLOB_JSON:
        return OracleVersionInfo(19, 0, 0, compatible="19.0.0")
    return OracleVersionInfo(11, 2, 0, compatible="11.2.0")


def _config_with_version(version: "OracleVersionInfo | None") -> Any:
    config = MagicMock()
    cache = OracleVersionCache()
    cache.resolved = True
    cache.version = version
    config._oracle_version_cache = cache
    return config


def _mock_version_driver(cache: OracleVersionCache) -> MagicMock:
    driver = MagicMock()
    driver._oracle_version_cache = cache
    driver.select_one_or_none = MagicMock(return_value="Oracle Database 21c 21.3.0.0.0")
    driver.select_value = MagicMock(return_value="21.0.0")
    driver.select_value_or_none = MagicMock(return_value=None)
    return driver


def test_oracle_version_resolved_once_per_pool() -> None:
    """Two sessions from one config resolve the server version with a single query."""
    config = OracleSyncConfig(connection_config={"user": "u", "password": "p", "dsn": "d"})
    cache = config._oracle_version_cache
    assert isinstance(cache, OracleVersionCache)

    dictionary = OracledbSyncDataDictionary()
    first_driver = _mock_version_driver(cache)
    second_driver = _mock_version_driver(cache)

    first = dictionary.get_version(first_driver)
    second = dictionary.get_version(second_driver)

    assert first is not None and first.major == 21
    assert second is not None and second.major == 21
    assert first_driver.select_one_or_none.call_count == 1
    assert second_driver.select_one_or_none.call_count == 0


def test_oracle_storage_ladder_native_requires_compatible() -> None:
    """A 21c server with a low COMPATIBLE degrades to the BLOB_JSON rung."""
    version = OracleVersionInfo(21, 0, 0, compatible="19.0.0")
    assert storage_type_from_version(version) is JSONStorageType.BLOB_JSON


def test_oracle_no_extract_major_parser() -> None:
    """The hand-rolled major-version string parser is gone; the major reads the cache."""
    assert not hasattr(oracle_config_module, "_extract_oracle_major")

    config = OracleSyncConfig(connection_config={"user": "u", "password": "p", "dsn": "d"})
    config._oracle_version_cache.resolved = True
    config._oracle_version_cache.version = OracleVersionInfo(21, 0, 0, compatible="21.0.0")

    connection = MagicMock()
    connection.version = "19.3.0.0.0"
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    config._init_connection(connection, "tag")

    assert connection._sqlspec_oracle_major == 21


def test_oracle_connection_major_parsed_from_shared_parser() -> None:
    """Without a resolved cache the major comes from the shared version parser."""
    config = OracleSyncConfig(connection_config={"user": "u", "password": "p", "dsn": "d"})

    connection = MagicMock()
    connection.version = "19.3.0.0.0"
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    config._init_connection(connection, "tag")

    assert connection._sqlspec_oracle_major == 19


_STORE_CLASSES = (
    events_store.OracleSyncEventQueueStore,
    events_store.OracleAsyncEventQueueStore,
    adk_store.OracleSyncADKStore,
    adk_store.OracleAsyncADKStore,
    adk_store.OracleSyncADKMemoryStore,
    adk_store.OracleAsyncADKMemoryStore,
)


def test_oracle_stores_use_shared_version_cache() -> None:
    for store_class in _STORE_CLASSES:
        slots = set(store_class.__slots__)
        assert "_oracle_version_info" not in slots


async def test_events_store_reads_config_scoped_cache() -> None:
    """Events storage-type resolution reads the config cache without a new session."""
    config = _config_with_version(OracleVersionInfo(21, 3, 0, compatible="21.0.0"))
    store = events_store.OracleAsyncEventQueueStore.__new__(events_store.OracleAsyncEventQueueStore)
    store._config = config
    store._json_storage_override = None

    storage_type = await store._detect_json_storage_type()

    assert storage_type is JSONStorageType.JSON_NATIVE
    config.provide_session.assert_not_called()


def test_oracle_single_storage_type_enum() -> None:
    """A single JSONStorageType object with all three rungs backs every store surface."""
    from sqlspec.adapters.oracledb.adk import JSONStorageType as adk_init_enum
    from sqlspec.adapters.oracledb.adk.store import JSONStorageType as adk_store_enum
    from sqlspec.adapters.oracledb.data_dictionary import JSONStorageType as dd_enum
    from sqlspec.adapters.oracledb.events.store import JSONStorageType as events_enum

    assert adk_store_enum is dd_enum
    assert events_enum is dd_enum
    assert adk_init_enum is dd_enum
    assert {member.name for member in dd_enum} == {"JSON_NATIVE", "BLOB_JSON", "BLOB_PLAIN"}


@pytest.mark.parametrize("rung", [JSONStorageType.JSON_NATIVE, JSONStorageType.BLOB_JSON, JSONStorageType.BLOB_PLAIN])
async def test_adk_async_state_roundtrip_per_rung(rung: JSONStorageType) -> None:
    """ADK async state serialization round-trips a JSON payload for every rung."""
    store = adk_store.OracleAsyncADKStore.__new__(adk_store.OracleAsyncADKStore)
    store._config = _config_with_version(_version_for_rung(rung))

    payload = {"count": 3, "nested": {"values": [1, 2, 3]}, "label": "state"}
    serialized = await store._serialize_state(payload)

    if rung is JSONStorageType.JSON_NATIVE:
        assert isinstance(serialized, str)
    else:
        assert isinstance(serialized, bytes)

    restored = await store._deserialize_state(serialized)
    assert restored == payload


@pytest.mark.parametrize("rung", [JSONStorageType.JSON_NATIVE, JSONStorageType.BLOB_JSON, JSONStorageType.BLOB_PLAIN])
def test_adk_sync_state_roundtrip_per_rung(rung: JSONStorageType) -> None:
    """ADK sync state serialization round-trips a JSON payload for every rung."""
    store = adk_store.OracleSyncADKStore.__new__(adk_store.OracleSyncADKStore)
    store._config = _config_with_version(_version_for_rung(rung))

    payload = {"count": 7, "nested": {"values": ["a", "b"]}, "flag": True}
    serialized = store._serialize_state(payload)

    if rung is JSONStorageType.JSON_NATIVE:
        assert isinstance(serialized, str)
    else:
        assert isinstance(serialized, bytes)

    restored = store._deserialize_state(serialized)
    assert restored == payload


@pytest.mark.parametrize("rung", [JSONStorageType.JSON_NATIVE, JSONStorageType.BLOB_JSON, JSONStorageType.BLOB_PLAIN])
async def test_adk_async_event_data_roundtrip_per_rung(rung: JSONStorageType) -> None:
    """ADK async event_data serialization round-trips for every rung."""
    store = adk_store.OracleAsyncADKStore.__new__(adk_store.OracleAsyncADKStore)
    store._config = _config_with_version(_version_for_rung(rung))

    payload = {"parts": [{"text": "hi"}], "role": "user"}
    serialized = await store._serialize_event_data(payload)

    if rung is JSONStorageType.JSON_NATIVE:
        assert isinstance(serialized, str)
    else:
        assert isinstance(serialized, bytes)

    restored = await store._read_event_data(serialized)
    assert isinstance(restored, str)


@pytest.mark.parametrize(
    ("rung", "expected_column"),
    [(JSONStorageType.JSON_NATIVE, "JSON"), (JSONStorageType.BLOB_JSON, "BLOB"), (JSONStorageType.BLOB_PLAIN, "BLOB")],
)
def test_events_store_column_types_per_rung(rung: JSONStorageType, expected_column: str) -> None:
    """Events column DDL selection maps each rung to its Oracle column type."""
    config = _config_with_version(_version_for_rung(rung))
    config.extension_config = {"events": {"json_storage": rung.value}}
    store = events_store.OracleSyncEventQueueStore(config)

    payload_col, metadata_col, _timestamp = store._column_types()

    assert payload_col == expected_column
    assert metadata_col == expected_column


def test_oracle_events_store_compression_partitioning_config() -> None:
    """Events DDL applies the shared Oracle storage-option shape."""
    config = _config_with_version(_version_for_rung(JSONStorageType.JSON_NATIVE))
    config.extension_config = {
        "events": {
            "compression": {"enabled": True, "algorithm": "oltp"},
            "partitioning": {"strategy": "range", "interval": "day"},
            "table_options": "TABLESPACE event_data",
        }
    }
    store = events_store.OracleSyncEventQueueStore(config)

    sql = store.create_statements()[0]

    assert "ROW STORE COMPRESS ADVANCED" in sql
    assert "TABLESPACE event_data" in sql
    assert "PARTITION BY RANGE (available_at)" in sql
    assert "NUMTODSINTERVAL(1, ''DAY'')" in sql


def test_resolve_oracle_connection_major_from_core() -> None:
    """resolve_oracle_connection_major and cache are importable and callable directly from core."""
    from sqlspec.adapters.oracledb.core import (
        OracleVersionCache,
        resolve_oracle_connection_compatible_major,
        resolve_oracle_connection_major,
    )

    conn = MagicMock()
    conn._sqlspec_oracle_major = 23
    conn._sqlspec_oracle_compatible_major = 23
    cache = OracleVersionCache()
    assert resolve_oracle_connection_major(conn, cache) == 23
    assert resolve_oracle_connection_compatible_major(conn, cache) == 23


def _migration_driver(
    cache: OracleVersionCache, version_text: str | None, compatible: str | None, *, is_async: bool
) -> MagicMock:
    query_mock = AsyncMock if is_async else MagicMock
    driver = MagicMock()
    driver._oracle_version_cache = cache
    driver.select_one_or_none = query_mock(return_value=version_text)
    driver.select_value = query_mock(return_value=compatible)
    driver.select_value_or_none = query_mock(return_value=None)
    driver.data_dictionary = OracledbAsyncDataDictionary() if is_async else OracledbSyncDataDictionary()
    return driver


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_sessions_only_native(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cold-cache Oracle sessions migration creates native JSON tables without second session."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": True, "enable_memory": False}},
    )
    driver = _migration_driver(
        config._oracle_version_cache, "Oracle Database 21c 21.3.0.0.0", "21.0.0", is_async=is_async
    )
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    statements = await adk_migration.up(context)

    assert len(statements) == 5
    assert not any("BLOB CHECK" in s for s in statements)
    assert config._oracle_version_cache.resolved is True
    assert config._oracle_version_cache.version is not None
    assert config._oracle_version_cache.version.major == 21


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_sessions_only_blob(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cold-cache Oracle sessions migration creates BLOB tables on 19c without second session."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": True, "enable_memory": False}},
    )
    driver = _migration_driver(
        config._oracle_version_cache, "Oracle Database 19c 19.3.0.0.0", "19.0.0", is_async=is_async
    )
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    statements = await adk_migration.up(context)

    assert len(statements) == 5
    assert any("state BLOB CHECK" in s for s in statements)


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_memory_only_native(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cold-cache Oracle memory migration creates native JSON table without second session."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": False, "enable_memory": True}},
    )
    driver = _migration_driver(
        config._oracle_version_cache, "Oracle Database 21c 21.3.0.0.0", "21.0.0", is_async=is_async
    )
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    statements = await adk_migration.up(context)

    assert len(statements) == 1
    assert "content_json JSON" in statements[0]
    assert "BLOB CHECK" not in statements[0]


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_memory_only_blob(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cold-cache Oracle memory migration creates BLOB table on 19c without second session."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": False, "enable_memory": True}},
    )
    driver = _migration_driver(
        config._oracle_version_cache, "Oracle Database 19c 19.3.0.0.0", "19.0.0", is_async=is_async
    )
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    statements = await adk_migration.up(context)

    assert len(statements) == 1
    assert "content_json BLOB CHECK" in statements[0]


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_combined_blob(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Combined sessions and memory query version exactly once on cold cache without second session."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": True, "enable_memory": True}},
    )
    driver = _migration_driver(
        config._oracle_version_cache, "Oracle Database 19c 19.3.0.0.0", "19.0.0", is_async=is_async
    )
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    statements = await adk_migration.up(context)

    assert len(statements) == 6
    assert any("state BLOB CHECK" in s for s in statements)
    assert any("content_json BLOB CHECK" in s for s in statements)
    assert driver.select_one_or_none.call_count == 1


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
@pytest.mark.parametrize(
    "adk_config",
    [
        {"enable_sessions": True, "enable_memory": False},
        {"enable_sessions": False, "enable_memory": True},
        {"enable_sessions": True, "enable_memory": True},
    ],
)
async def test_oracle_adk_migration_warns_when_version_unknown(
    config_cls: type[Any],
    is_async: bool,
    adk_config: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When Oracle version cannot be detected, warning is emitted exactly once and BLOB is used."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"}, extension_config={"adk": adk_config}
    )
    driver = _migration_driver(config._oracle_version_cache, None, None, is_async=is_async)
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    with caplog.at_level(logging.WARNING, logger="sqlspec.adapters.oracledb.adk.store"):
        statements = await adk_migration.up(context)

    warning_records = [
        r
        for r in caplog.records
        if "Could not detect Oracle version, defaulting to BLOB_JSON storage" in r.getMessage()
    ]
    assert len(warning_records) == 1
    assert config._oracle_version_cache.resolved is True
    assert config._oracle_version_cache.version is None
    assert any("BLOB CHECK" in s for s in statements)


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_skips_version_query_when_cache_resolved(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Pre-resolved version cache avoids database queries and warnings."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": True, "enable_memory": False}},
    )
    config._oracle_version_cache.resolved = True
    config._oracle_version_cache.version = None
    driver = _migration_driver(config._oracle_version_cache, None, None, is_async=is_async)
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    with caplog.at_level(logging.WARNING, logger="sqlspec.adapters.oracledb.adk.store"):
        statements = await adk_migration.up(context)

    assert driver.select_one_or_none.call_count == 0
    warning_records = [
        r
        for r in caplog.records
        if "Could not detect Oracle version, defaulting to BLOB_JSON storage" in r.getMessage()
    ]
    assert len(warning_records) == 0
    assert len(statements) == 5


@pytest.mark.parametrize(("config_cls", "is_async"), [(OracleSyncConfig, False), (OracleAsyncConfig, True)])
async def test_oracle_adk_migration_propagates_dictionary_errors(
    config_cls: type[Any], is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catalog errors during preparation propagate without masking."""
    config = config_cls(
        connection_config={"user": "u", "password": "p", "dsn": "d"},
        extension_config={"adk": {"enable_sessions": True, "enable_memory": False}},
    )
    driver = _migration_driver(config._oracle_version_cache, None, None, is_async=is_async)
    driver.select_one_or_none.side_effect = RuntimeError("catalog")
    monkeypatch.setattr(config_cls, "provide_session", MagicMock(side_effect=AssertionError("second session")))
    context = MigrationContext(config=config, dialect="oracle", driver=driver)

    with pytest.raises(RuntimeError, match="catalog"):
        await adk_migration.up(context)
