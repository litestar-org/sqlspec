"""Tests for ADK flat-config resolution."""

from collections.abc import Callable
from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.adapters.oracledb.adk.store import OracleSyncADKMemoryStore, OracleSyncADKStore
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.adk.store import SpannerSyncADKMemoryStore, SpannerSyncADKStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.extensions.adk import _config_utils
from sqlspec.extensions.adk._config_utils import (
    _adk_artifact_store_config,
    _adk_memory_migration_enabled,
    _adk_memory_store_config,
    _adk_session_store_config,
    _adk_sessions_migration_enabled,
    _ADKSessionStoreConfig,
    _apply_owner_id,
    _ensure_adk_store_registration,
)


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: dict[str, Any]) -> None:
        self.extension_config = {"adk": adk_config}


class _DeclaringADKStore(SqliteADKStore):
    extension_config_options = frozenset({"session_opt"})


class _DeclaringADKMemoryStore(SqliteADKMemoryStore):
    extension_config_options = frozenset({"memory_opt"})


class _ExplicitBuiltinConfig(SqliteConfig):
    adk_store_class = SqliteADKStore
    adk_memory_store_class = SqliteADKMemoryStore


class _DeclaringBuiltinConfig(SqliteConfig):
    adk_store_class = _DeclaringADKStore
    adk_memory_store_class = _DeclaringADKMemoryStore


class _ExternalConfig(SqliteConfig):
    adapter_name = "third_party"
    adk_store_class = _DeclaringADKStore
    adk_memory_store_class = _DeclaringADKMemoryStore


class _ExternalNoStoreConfig(SqliteConfig):
    adapter_name = "third_party"


class _PlainSubclassConfig(SqliteConfig):
    pass


class _ExplicitOracleConfig(OracleSyncConfig):
    adk_store_class = OracleSyncADKStore
    adk_memory_store_class = OracleSyncADKMemoryStore


class _BareExternalConfig(_Config):
    adapter_name = "third_party"


class _BareExternalSessionStoreConfig(_Config):
    adapter_name = "third_party"
    adk_store_class = _DeclaringADKStore


class _BareSqliteStoreConfig(_Config):
    adk_store_class = SqliteADKStore


class Duck:
    pass


Duck.__module__ = "myapp"


class _BareDuckStoreConfig(_Config):
    adk_store_class = Duck


class _InstanceStoreConfig(_Config):
    adk_store_class: "type[Any] | None" = None

    def __init__(self, adk_config: dict[str, Any], store_class: "type[Any]") -> None:
        super().__init__(adk_config)
        self.adk_store_class = store_class


def _sqlite_config(config_class: type[SqliteConfig], adk_settings: dict[str, Any]) -> SqliteConfig:
    return config_class(connection_config={"database": ":memory:"}, extension_config={"adk": adk_settings})


def _oracle_config(adk_settings: dict[str, Any]) -> OracleSyncConfig:
    return _ExplicitOracleConfig(connection_config={"dsn": "x"}, extension_config={"adk": adk_settings})


def test_apply_owner_id_adds_only_configured_values() -> None:
    configured = cast("_ADKSessionStoreConfig", {})
    unconfigured = cast("_ADKSessionStoreConfig", {})

    _apply_owner_id(configured, {"owner_id_column": "tenant_id UUID"})
    _apply_owner_id(unconfigured, {})

    assert dict(configured) == {"owner_id_column": "tenant_id UUID"}
    assert not unconfigured


def test_adk_config_uses_flat_keys() -> None:
    """ADKConfig is a flat TypedDict; no per-adapter or nested negotiation blocks."""
    annotations = set(ADKConfig.__annotations__)
    expected_flat = {
        "session_table",
        "events_table",
        "app_state_table",
        "user_state_table",
        "metadata_table",
        "memory_table",
        "artifact_table",
        "owner_id_column",
    }
    forbidden_backend_settings = {
        "schema",
        "lifecycle",
        "capabilities",
        "optimizations",
        "oracle",
        "spanner",
        "adbc",
        "bigquery",
        "asyncpg",
        "in_memory",
        "shard_count",
        "session_table_options",
        "events_table_options",
        "memory_table_options",
        "expires_index_options",
        "fts_language",
        "schema_version",
        "partitioning",
        "retention",
        "compression",
        "sqlite_optimization",
        "enable_event_generated_columns",
        "enable_covering_indexes",
    }
    assert expected_flat <= annotations
    assert annotations.isdisjoint(forbidden_backend_settings)


def test_flat_schema_config_resolves_all_adk_table_names() -> None:
    config = _Config({
        "session_table": "agent_sessions",
        "events_table": "agent_events",
        "app_state_table": "agent_app_states",
        "user_state_table": "agent_user_states",
        "metadata_table": "agent_metadata",
        "owner_id_column": "tenant_id UUID",
    })

    resolved = _adk_session_store_config(config)

    assert resolved == {
        "session_table": "agent_sessions",
        "events_table": "agent_events",
        "app_state_table": "agent_app_states",
        "user_state_table": "agent_user_states",
        "metadata_table": "agent_metadata",
        "owner_id_column": "tenant_id UUID",
    }


def test_flat_memory_config_resolves_memory_store_settings() -> None:
    config = _Config({
        "enable_memory": False,
        "memory_table": "agent_memories",
        "memory_use_fts": True,
        "memory_max_results": 50,
    })

    resolved = _adk_memory_store_config(config)

    assert resolved["enable_memory"] is False
    assert resolved["memory_table"] == "agent_memories"
    assert resolved["use_fts"] is True
    assert resolved["max_results"] == 50
    assert resolved["vector_index_type"] == "hnsw"
    assert resolved["vector_dimensions"] == 768
    assert resolved["enable_bm25"] is False
    assert resolved["scann_num_leaves"] == 100
    assert resolved["scann_quantizer"] == "SQ8"


def test_flat_artifact_config_resolves_store_owned_table_only() -> None:
    config = _Config({"artifact_table": "agent_artifacts", "artifact_storage_uri": "s3://bucket/adk"})

    resolved = _adk_artifact_store_config(config)

    assert resolved == {"artifact_table": "agent_artifacts"}


def test_adk_config_has_no_separate_migration_include_flags() -> None:
    """Migration inclusion is not configurable per feature."""
    annotations = set(ADKConfig.__annotations__)

    assert "include_sessions_migration" not in annotations
    assert "include_memory_migration" not in annotations


def test_memory_migration_gate_follows_enable_memory() -> None:
    enabled = _Config({"enable_memory": True})
    disabled = _Config({"enable_memory": False})
    defaulted = _Config({})

    assert _adk_memory_migration_enabled(enabled) is True
    assert _adk_memory_migration_enabled(disabled) is False
    assert _adk_memory_migration_enabled(defaulted) is True


def test_sessions_migration_gate_follows_enable_sessions() -> None:
    enabled = _Config({"enable_sessions": True})
    disabled = _Config({"enable_sessions": False})
    defaulted = _Config({})

    assert _adk_sessions_migration_enabled(enabled) is True
    assert _adk_sessions_migration_enabled(disabled) is False
    assert _adk_sessions_migration_enabled(defaulted) is True


def test_retired_migration_include_keys_are_rejected() -> None:
    """Adapter configs reject the removed include flags through normal validation."""
    from sqlspec.adapters.sqlite import SqliteConfig

    for retired_key in ("include_sessions_migration", "include_memory_migration"):
        with pytest.raises(ImproperConfigurationError, match=retired_key):
            SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {retired_key: False}})


def test_store_registration_skips_disabled_feature_store_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disabled features do not require their adapter store class to exist."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"enable_sessions": False, "enable_memory": True}},
    )
    resolved: list[str] = []

    def record_store_class(config: Any, store_suffix: str) -> object:
        resolved.append(store_suffix)
        return object

    monkeypatch.setattr(_config_utils, "_adk_adapter_store_class", record_store_class)

    _ensure_adk_store_registration(config)

    assert resolved == ["ADKMemoryStore"]


def test_adk_store_resolution_supports_external_subclass_and_explicit_classvars() -> None:
    """ADK store registration and validation resolve external subclasses via MRO, adapter_name, and ClassVars."""

    class ExternalSpannerConfig(SpannerSyncConfig):
        adapter_name = "spanner"

    ExternalSpannerConfig.__module__ = "myapp.database.config"
    spanner_config = ExternalSpannerConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        extension_config={"adk": {"shard_count": 4}},
    )
    assert _config_utils._adk_adapter_store_class(spanner_config, "ADKStore") is SpannerSyncADKStore
    assert _config_utils._adk_memory_migration_store_class(spanner_config) is SpannerSyncADKMemoryStore

    class SubclassedSqliteConfig(SqliteConfig):
        pass

    SubclassedSqliteConfig.__module__ = "myapp.database.config"
    sqlite_config = SubclassedSqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})
    assert _config_utils._adk_adapter_store_class(sqlite_config, "ADKStore") is SqliteADKStore
    assert _config_utils._adk_memory_migration_store_class(sqlite_config) is SqliteADKMemoryStore

    class CustomThirdPartyConfig:
        adapter_name = "third_party"
        adk_store_class = SqliteADKStore
        adk_memory_store_class = SqliteADKMemoryStore
        extension_config: dict[str, dict[str, Any]] = {"adk": {}}

    third_party_config = CustomThirdPartyConfig()
    assert _config_utils._adk_adapter_store_class(third_party_config, "ADKStore") is SqliteADKStore
    assert _config_utils._adk_memory_migration_store_class(third_party_config) is SqliteADKMemoryStore


@pytest.mark.parametrize("config_class", [_DeclaringBuiltinConfig, _ExternalConfig])
def test_adk_declared_store_options_union_across_config_paths(config_class: type[SqliteConfig]) -> None:
    """Options from both explicit stores are accepted by session, memory, and artifact config paths."""
    config = _sqlite_config(
        config_class, {"session_opt": 1, "memory_opt": 2, "session_table": "s_t", "memory_table": "m_t"}
    )
    artifact_config = _sqlite_config(config_class, {"session_opt": 1, "memory_opt": 2, "artifact_table": "a_t"})

    assert _DeclaringADKStore(config).session_table == "s_t"
    assert _DeclaringADKMemoryStore(config).memory_table == "m_t"
    assert _adk_artifact_store_config(artifact_config) == {"artifact_table": "a_t"}


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: _sqlite_config(_ExternalConfig, {"unknown_option": 1}), "for third_party: 'unknown_option'"),
        (lambda: _sqlite_config(_ExternalConfig, {"include_memory_migration": False}), "include_memory_migration"),
        (lambda: _sqlite_config(_ExternalConfig, {"fts_detail": "full"}), "fts_detail"),
        (lambda: _sqlite_config(_ExternalConfig, {"vector_dimensions": 512}), "vector_dimensions"),
        (lambda: _adk_session_store_config(_BareExternalSessionStoreConfig({"memory_opt": 1})), "memory_opt"),
    ],
    ids=["typo", "retired-key", "builtin-adapter-key", "base-memory-key", "session-store-only"],
)
def test_adk_external_adapter_explicit_stores_reject_undeclared_keys(build: Callable[[], object], match: str) -> None:
    with pytest.raises(ImproperConfigurationError, match=match):
        build()


def test_adk_external_adapter_explicit_stores_accept_shared_keys() -> None:
    config = _sqlite_config(
        _ExternalConfig, {"session_table": "s_t", "events_table": "e_t", "memory_table": "m_t", "enable_memory": False}
    )

    session_settings = _adk_session_store_config(config)
    memory_settings = _adk_memory_store_config(config)

    assert session_settings["session_table"] == "s_t"
    assert session_settings["events_table"] == "e_t"
    assert memory_settings["memory_table"] == "m_t"
    assert memory_settings["enable_memory"] is False


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda: _sqlite_config(_ExplicitBuiltinConfig, {"fts_detail": "full"}), {"fts_detail": "full"}),
        (lambda: _oracle_config({"in_memory": True}), {"in_memory": True}),
        (
            lambda: _sqlite_config(_DeclaringBuiltinConfig, {"session_opt": 1, "fts_detail": "full"}),
            {"session_opt": 1, "fts_detail": "full"},
        ),
    ],
    ids=["sqlite-adapter-key", "oracle-adapter-key", "declared-and-adapter-key"],
)
def test_adk_builtin_explicit_stores_accept_adapter_and_declared_keys(
    build: Callable[[], Any], expected: dict[str, Any]
) -> None:
    assert build().extension_config["adk"] == expected


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: _sqlite_config(_ExplicitBuiltinConfig, {"custom_opt": 1}), "for sqlite: 'custom_opt'"),
        (lambda: _oracle_config({"custom_opt": 1}), "custom_opt"),
        (lambda: _sqlite_config(_PlainSubclassConfig, {"custom_opt": 1}), "custom_opt"),
        (lambda: _sqlite_config(_PlainSubclassConfig, {"include_memory_migration": False}), "include_memory_migration"),
        (lambda: _sqlite_config(_PlainSubclassConfig, {"in_memory": True}), "in_memory"),
    ],
    ids=["explicit-builtin", "explicit-oracle", "plain-subclass", "plain-retired-key", "oracle-key-on-sqlite"],
)
def test_adk_strict_configs_reject_undeclared_keys(build: Callable[[], object], match: str) -> None:
    with pytest.raises(ImproperConfigurationError, match=match):
        build()


def test_adk_external_adapter_without_explicit_store_keeps_skip() -> None:
    """Store resolution, not option validation, fails for an enabled feature with no store."""
    with pytest.raises(SQLSpecError, match="Failed to import ADK store class") as excinfo:
        _sqlite_config(_ExternalNoStoreConfig, {"unknown_option": 1})
    assert not isinstance(excinfo.value, ImproperConfigurationError)

    disabled = _sqlite_config(
        _ExternalNoStoreConfig, {"unknown_option": 1, "enable_sessions": False, "enable_memory": False}
    )
    assert cast("dict[str, Any]", disabled.extension_config["adk"])["unknown_option"] == 1

    assert _adk_session_store_config(_BareExternalConfig({"unknown_option": 1}))["session_table"] == "adk_session"


@pytest.mark.parametrize(
    ("config_class", "match"),
    [(_BareSqliteStoreConfig, "for sqlite: 'unknown_option'"), (_BareDuckStoreConfig, "for Duck: 'unknown_option'")],
    ids=["builtin-store", "duck-store"],
)
def test_adk_explicit_store_without_adapter_name_labels_error(config_class: type[_Config], match: str) -> None:
    with pytest.raises(ImproperConfigurationError, match=match):
        _adk_session_store_config(config_class({"unknown_option": 1}))


def test_adk_instance_store_attribute_supplies_declared_options() -> None:
    """A store class set on the instance contributes options like a class attribute."""
    accepted = _InstanceStoreConfig({"session_opt": 1}, _DeclaringADKStore)
    rejected = _InstanceStoreConfig({"unknown_option": 1}, _DeclaringADKStore)

    assert _adk_session_store_config(accepted)["session_table"] == "adk_session"
    with pytest.raises(ImproperConfigurationError, match="unknown_option"):
        _adk_session_store_config(rejected)
