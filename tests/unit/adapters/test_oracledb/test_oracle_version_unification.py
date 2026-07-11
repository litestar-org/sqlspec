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

from typing import Any
from unittest.mock import MagicMock

import oracledb
import pytest

from sqlspec.adapters.oracledb import config as oracle_config_module
from sqlspec.adapters.oracledb._json_handlers import json_input_type_handler
from sqlspec.adapters.oracledb._typing import DB_TYPE_BLOB, DB_TYPE_CLOB
from sqlspec.adapters.oracledb.adk import store as adk_store
from sqlspec.adapters.oracledb.config import OracleSyncConfig
from sqlspec.adapters.oracledb.data_dictionary import (
    JSONStorageType,
    OracledbSyncDataDictionary,
    OracleVersionCache,
    OracleVersionInfo,
    storage_type_from_version,
)
from sqlspec.adapters.oracledb.events import store as events_store


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


@pytest.mark.parametrize("major", [11, 12, 19, 21, 23])
def test_oracle_json_thresholds_single_source(major: int) -> None:
    """The connection JSON handler agrees with OracleVersionInfo for every major."""
    cursor = MagicMock()
    cursor.connection._sqlspec_oracle_major = major
    sentinel = object()
    cursor.var = MagicMock(return_value=sentinel)

    json_input_type_handler(cursor, {"key": "value"}, 1)
    bound_type = cursor.var.call_args.args[0]

    version = OracleVersionInfo(major, 0, 0, compatible=str(major))
    if version.supports_native_json():
        expected = oracledb.DB_TYPE_JSON
    elif version.supports_json_blob():
        expected = DB_TYPE_BLOB
    else:
        expected = DB_TYPE_CLOB

    assert bound_type is expected


@pytest.mark.parametrize(
    ("major", "compatible", "expected"),
    [
        (11, "11.2.0", JSONStorageType.BLOB_PLAIN),
        (12, "12.1.0", JSONStorageType.BLOB_JSON),
        (19, "19.0.0", JSONStorageType.BLOB_JSON),
        (21, "21.0.0", JSONStorageType.JSON_NATIVE),
        (23, "23.0.0", JSONStorageType.JSON_NATIVE),
    ],
)
def test_oracle_storage_ladder_selection_all_rungs(major: int, compatible: str, expected: JSONStorageType) -> None:
    """Storage-type selection lands on the adjudicated rung for every supported major."""
    version = OracleVersionInfo(major, 0, 0, compatible=compatible)
    assert storage_type_from_version(version) is expected


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
    """Stores neither instantiate fresh dictionaries nor keep private version caches."""
    import inspect

    for module in (events_store, adk_store):
        source = inspect.getsource(module)
        assert "OracledbSyncDataDictionary()" not in source
        assert "OracledbAsyncDataDictionary()" not in source

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
