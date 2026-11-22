"""Tests for ADK flat-config resolution."""

import importlib
import sys
from collections.abc import Callable
from pathlib import Path
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

import sqlspec.extensions.adk as adk_pkg
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.adapters.oracledb.adk.store import OracleSyncADKMemoryStore, OracleSyncADKStore
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.adk.artifact_store import SpannerAsyncADKArtifactStore, SpannerSyncADKArtifactStore
from sqlspec.adapters.spanner.adk.store import SpannerSyncADKMemoryStore, SpannerSyncADKStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.extensions.adk import (
    _config_utils,
    adk_memory_migration_enabled,
    adk_sessions_migration_enabled,
    resolve_adk_artifact_store_class,
    resolve_adk_config,
    resolve_adk_memory_store_class,
    resolve_adk_store_class,
)
from sqlspec.extensions.adk._config_utils import (
    _adk_artifact_store_config,
    _adk_memory_store_config,
    _adk_session_store_config,
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

    assert adk_memory_migration_enabled(enabled) is True
    assert adk_memory_migration_enabled(disabled) is False
    assert adk_memory_migration_enabled(defaulted) is True


def test_sessions_migration_gate_follows_enable_sessions() -> None:
    enabled = _Config({"enable_sessions": True})
    disabled = _Config({"enable_sessions": False})
    defaulted = _Config({})

    assert adk_sessions_migration_enabled(enabled) is True
    assert adk_sessions_migration_enabled(disabled) is False
    assert adk_sessions_migration_enabled(defaulted) is True


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

    monkeypatch.setattr(_config_utils, "resolve_adk_store_class", record_store_class)

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
    assert resolve_adk_store_class(spanner_config, "ADKStore") is SpannerSyncADKStore
    assert resolve_adk_memory_store_class(spanner_config) is SpannerSyncADKMemoryStore

    class SubclassedSqliteConfig(SqliteConfig):
        pass

    SubclassedSqliteConfig.__module__ = "myapp.database.config"
    sqlite_config = SubclassedSqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})
    assert resolve_adk_store_class(sqlite_config, "ADKStore") is SqliteADKStore
    assert resolve_adk_memory_store_class(sqlite_config) is SqliteADKMemoryStore

    class CustomThirdPartyConfig:
        adapter_name = "third_party"
        adk_store_class = SqliteADKStore
        adk_memory_store_class = SqliteADKMemoryStore
        extension_config: dict[str, dict[str, Any]] = {"adk": {}}

    third_party_config = CustomThirdPartyConfig()
    assert resolve_adk_store_class(third_party_config, "ADKStore") is SqliteADKStore
    assert resolve_adk_memory_store_class(third_party_config) is SqliteADKMemoryStore


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


@pytest.mark.parametrize("config_class", [_DeclaringBuiltinConfig, _ExternalConfig])
def test_adk_explicit_custom_store_validates_migration_mode(config_class: type[SqliteConfig]) -> None:
    """Explicit custom ADK stores accept migration_mode without declaring it and reject invalid values."""
    valid_config = _sqlite_config(config_class, {"session_opt": 1, "migration_mode": "reconcile"})
    assert _DeclaringADKStore(valid_config).session_table == "adk_session"

    with pytest.raises(MigrationError, match="migration_mode"):
        _sqlite_config(config_class, {"session_opt": 1, "migration_mode": "bogus"})

    with pytest.raises(ImproperConfigurationError, match="unknown_opt"):
        _sqlite_config(config_class, {"session_opt": 1, "unknown_opt": 2})


@pytest.mark.parametrize("invalid_mode", ["bogus", None], ids=["bogus-str", "none-value"])
def test_adk_unresolved_external_config_rejects_invalid_migration_mode(invalid_mode: object) -> None:
    """Unresolved external ADK configs reject invalid migration_mode before the early return."""
    with pytest.raises(MigrationError, match="migration_mode"):
        _adk_session_store_config(_BareExternalConfig({"migration_mode": invalid_mode}))


class _RawExtensionConfig:
    extension_config: Any

    def __init__(self, extension_config: Any) -> None:
        self.extension_config = extension_config


class _BareObject:
    pass


class _ExplicitArtifactStore:
    pass


class _ClassArtifactOverrideConfig:
    adk_artifact_store_class = _ExplicitArtifactStore


class _InstanceArtifactOverrideConfig:
    adk_artifact_store_class: type[Any] | None = None

    def __init__(self, store_class: type[Any]) -> None:
        self.adk_artifact_store_class = store_class


class _CustomSingleArtifactStore:
    pass


class _CustomSyncADKArtifactStore:
    pass


class _CustomAsyncADKArtifactStore:
    pass


class _CustomNoSuffixAdapter:
    adapter_name = "custom_dual"
    is_async = False


class _CustomAsyncNoSuffixAdapter:
    adapter_name = "custom_dual"
    is_async = True


class _AbsentArtifactAdapter:
    adapter_name = "custom_absent"


class _BrokenArtifactAdapter:
    adapter_name = "custom_broken"


class _SubclassSpannerSyncConfig(SpannerSyncConfig):
    pass


class _SubclassSpannerAsyncConfig(SpannerAsyncConfig):
    pass


def test_resolve_adk_config_returns_isolated_mutable_copy() -> None:
    """Mutating the dict returned by resolve_adk_config does not mutate config.extension_config."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {"session_table": "s"}})

    resolved = resolve_adk_config(config)
    assert resolved == {"session_table": "s"}

    resolved["session_table"] = "mutated"
    resolved["extra_key"] = "added"
    assert config.extension_config["adk"] == {"session_table": "s"}


def test_resolve_adk_config_copies_readonly_mapping() -> None:
    """Read-only Mapping instances are copied into a mutable dict."""
    proxy = MappingProxyType({"adk": MappingProxyType({"session_table": "readonly_sessions"})})
    config = _RawExtensionConfig(proxy)

    resolved = resolve_adk_config(config)
    assert resolved == {"session_table": "readonly_sessions"}
    assert isinstance(resolved, dict)
    resolved["session_table"] = "updated"
    assert proxy["adk"]["session_table"] == "readonly_sessions"


@pytest.mark.parametrize(
    "config",
    [
        _RawExtensionConfig({}),
        _RawExtensionConfig({"adk": None}),
        _RawExtensionConfig(None),
        _RawExtensionConfig("not-a-mapping"),
        _BareObject(),
        None,
        SqliteConfig,
    ],
    ids=[
        "empty-extension-dict",
        "adk-none",
        "extension-config-none",
        "extension-config-non-mapping",
        "bare-object-no-attr",
        "none-config",
        "config-class",
    ],
)
def test_resolve_adk_config_missing_or_none_returns_empty_dict(config: Any) -> None:
    """Missing, None, or non-mapping extension_config inputs resolve to an empty dict."""
    assert resolve_adk_config(config) == {}


@pytest.mark.parametrize("bad_value", ["invalid", 42, ["session_table"]], ids=["str", "int", "list"])
def test_resolve_adk_config_rejects_non_mapping_adk_value(bad_value: Any) -> None:
    """Non-mapping extension_config['adk'] values raise ImproperConfigurationError."""
    config = _RawExtensionConfig({"adk": bad_value})
    with pytest.raises(ImproperConfigurationError, match="expected a mapping"):
        resolve_adk_config(config)


@pytest.mark.parametrize(
    ("config", "expected_error", "match"),
    [
        (_BareSqliteStoreConfig({"unknown_key": 1}), ImproperConfigurationError, "unknown_key"),
        (_BareExternalConfig({"migration_mode": "bogus"}), MigrationError, "migration_mode"),
    ],
    ids=["unknown-key", "invalid-migration-mode"],
)
def test_resolve_adk_config_validates_keys_and_migration_mode(
    config: Any, expected_error: type[Exception], match: str
) -> None:
    """resolve_adk_config runs key and migration_mode validation."""
    with pytest.raises(expected_error, match=match):
        resolve_adk_config(config)


@pytest.mark.parametrize(
    ("target", "store_suffix", "expected"),
    [
        (SqliteConfig(connection_config={"database": ":memory:"}), "ADKStore", SqliteADKStore),
        (SqliteConfig, "ADKStore", SqliteADKStore),
        (SqliteConfig(connection_config={"database": ":memory:"}), "ADKMemoryStore", SqliteADKMemoryStore),
        (SqliteConfig, "ADKMemoryStore", SqliteADKMemoryStore),
        (
            SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"}),
            "ADKStore",
            SpannerSyncADKStore,
        ),
        (SpannerSyncConfig, "ADKStore", SpannerSyncADKStore),
        (_PlainSubclassConfig, "ADKStore", SqliteADKStore),
        (_PlainSubclassConfig(connection_config={"database": ":memory:"}), "ADKStore", SqliteADKStore),
        (_ExternalConfig, "ADKStore", _DeclaringADKStore),
        (_ExternalConfig, "ADKMemoryStore", _DeclaringADKMemoryStore),
    ],
    ids=[
        "sqlite-instance-session",
        "sqlite-class-session",
        "sqlite-instance-memory",
        "sqlite-class-memory",
        "spanner-instance-session",
        "spanner-class-session",
        "subclass-class-session",
        "subclass-instance-session",
        "external-classvar-session",
        "external-classvar-memory",
    ],
)
def test_resolve_adk_store_class_builtin_subclass_and_classvar(
    target: Any, store_suffix: str, expected: type[Any]
) -> None:
    """resolve_adk_store_class handles instances, classes, subclasses, and explicit ClassVars."""
    assert resolve_adk_store_class(target, store_suffix) is expected


@pytest.mark.parametrize(
    "target",
    [_BareObject(), _BareObject, None, _ExternalNoStoreConfig],
    ids=["bare-instance", "bare-class", "none", "external-missing-module"],
)
def test_resolve_adk_store_class_unknown_or_missing_raises(target: Any) -> None:
    """Unknown or missing adapter store configs raise SQLSpecError."""
    with pytest.raises(SQLSpecError):
        resolve_adk_store_class(target, "ADKStore")


@pytest.mark.parametrize(
    "target",
    [
        BigQueryConfig(connection_config={"project": "p"}),
        BigQueryConfig,
        _ExternalNoStoreConfig,
        _BareExternalConfig({}),
    ],
    ids=["bigquery-instance", "bigquery-class", "external-no-store-class", "bare-external-instance"],
)
def test_resolve_adk_memory_store_class_missing_returns_none(target: Any) -> None:
    """Adapters without an ADK memory store return None instead of raising ImportError."""
    assert resolve_adk_memory_store_class(target) is None


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (SqliteConfig(connection_config={"database": ":memory:"}), SqliteADKMemoryStore),
        (SqliteConfig, SqliteADKMemoryStore),
        (SpannerSyncConfig, SpannerSyncADKMemoryStore),
        (_ExternalConfig, _DeclaringADKMemoryStore),
    ],
    ids=["sqlite-instance", "sqlite-class", "spanner-class", "external-classvar"],
)
def test_resolve_adk_memory_store_class_resolves_builtin_and_explicit(target: Any, expected: type[Any]) -> None:
    """resolve_adk_memory_store_class resolves built-in stores and explicit ClassVar overrides."""
    assert resolve_adk_memory_store_class(target) is expected


@pytest.mark.parametrize("target", [_BareObject(), _BareObject, None], ids=["bare-instance", "bare-class", "none"])
def test_resolve_adk_memory_store_class_unknown_raises(target: Any) -> None:
    """Bare configs with no adapter_name and no explicit memory store raise SQLSpecError."""
    with pytest.raises(SQLSpecError):
        resolve_adk_memory_store_class(target)


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (
            SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"}),
            SpannerSyncADKArtifactStore,
        ),
        (SpannerSyncConfig, SpannerSyncADKArtifactStore),
        (_SubclassSpannerSyncConfig, SpannerSyncADKArtifactStore),
        (
            SpannerAsyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"}),
            SpannerAsyncADKArtifactStore,
        ),
        (SpannerAsyncConfig, SpannerAsyncADKArtifactStore),
        (_SubclassSpannerAsyncConfig, SpannerAsyncADKArtifactStore),
    ],
    ids=[
        "spanner-sync-instance",
        "spanner-sync-class",
        "spanner-sync-subclass",
        "spanner-async-instance",
        "spanner-async-class",
        "spanner-async-subclass",
    ],
)
def test_resolve_adk_artifact_store_class_spanner_sync_and_async(target: Any, expected: type[Any]) -> None:
    """resolve_adk_artifact_store_class resolves Spanner sync and async stores for instances and classes."""
    assert resolve_adk_artifact_store_class(target) is expected


@pytest.mark.parametrize(
    "target",
    [
        _ClassArtifactOverrideConfig,
        _ClassArtifactOverrideConfig(),
        _InstanceArtifactOverrideConfig(_ExplicitArtifactStore),
    ],
    ids=["class-override-class", "class-override-instance", "instance-override"],
)
def test_resolve_adk_artifact_store_class_explicit_override(target: Any) -> None:
    """Explicit adk_artifact_store_class overrides take precedence on classes and instances."""
    assert resolve_adk_artifact_store_class(target) is _ExplicitArtifactStore


@pytest.mark.parametrize(
    "target",
    [
        SqliteConfig(connection_config={"database": ":memory:"}),
        SqliteConfig,
        None,
        _BareObject(),
        _BareObject,
        _BareExternalConfig({}),
    ],
    ids=["sqlite-instance", "sqlite-class", "none", "bare-instance", "bare-class", "unknown-external"],
)
def test_resolve_adk_artifact_store_class_absent_or_unknown_returns_none(target: Any) -> None:
    """Configs without an artifact store or adapter_name return None."""
    assert resolve_adk_artifact_store_class(target) is None


@pytest.mark.parametrize(
    ("module_source", "missing_name"),
    [
        ("import third_party_sdk\n", "third_party_sdk"),
        (
            "import sqlspec.adapters.custom_broken.missing_dependency\n",
            "sqlspec.adapters.custom_broken.missing_dependency",
        ),
        ('raise ModuleNotFoundError("No module named None")\n', None),
    ],
    ids=["third-party-sdk", "missing-internal-sub-dep", "none-module-name"],
)
def test_resolve_adk_artifact_store_class_reraises_broken_dependency(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_source: str, missing_name: str | None
) -> None:
    """Missing transitive dependencies inside artifact_store modules are re-raised.

    The adapter and ``adk`` layers are registered in ``sys.modules`` as packages so the import
    system executes a real ``artifact_store`` module from ``tmp_path``.
    """
    (tmp_path / "artifact_store.py").write_text(module_source, encoding="utf-8")
    adapter_package = ModuleType("sqlspec.adapters.custom_broken")
    adapter_package.__path__ = []
    adk_package = ModuleType("sqlspec.adapters.custom_broken.adk")
    adk_package.__path__ = [str(tmp_path)]
    monkeypatch.setitem(sys.modules, adapter_package.__name__, adapter_package)
    monkeypatch.setitem(sys.modules, adk_package.__name__, adk_package)

    with pytest.raises(ModuleNotFoundError) as exc_info:
        resolve_adk_artifact_store_class(_BrokenArtifactAdapter)
    assert exc_info.value.name == missing_name


@pytest.mark.parametrize(
    "missing_name",
    [
        "sqlspec.adapters",
        "sqlspec.adapters.custom_absent",
        "sqlspec.adapters.custom_absent.adk",
        "sqlspec.adapters.custom_absent.adk.artifact_store",
    ],
    ids=["adapters-pkg", "adapter-pkg", "adk-pkg", "artifact-store-mod"],
)
def test_resolve_adk_artifact_store_class_ignores_absent_module_chain(
    monkeypatch: pytest.MonkeyPatch, missing_name: str
) -> None:
    """Absent adapter artifact_store package layers return None without raising.

    A ``None`` entry in ``sys.modules`` for the parent of ``missing_name`` makes the import
    system report ``missing_name`` itself as the absent module.
    """
    monkeypatch.delitem(sys.modules, missing_name, raising=False)
    monkeypatch.setitem(sys.modules, missing_name.rpartition(".")[0], None)

    with pytest.raises(ModuleNotFoundError) as exc_info:
        importlib.import_module("sqlspec.adapters.custom_absent.adk.artifact_store")
    assert exc_info.value.name == missing_name
    assert resolve_adk_artifact_store_class(_AbsentArtifactAdapter) is None


def test_resolve_adk_artifact_store_class_single_and_mode_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Falls back to single exported store or Async/Sync mode matching when MRO prefix does not match."""
    module_name = "sqlspec.adapters.custom_dual.adk.artifact_store"
    single_module = SimpleNamespace(
        __all__=("OnlyADKArtifactStore", "ignored_value"),
        OnlyADKArtifactStore=_CustomSingleArtifactStore,
        ignored_value="not_a_type",
    )
    monkeypatch.setitem(sys.modules, module_name, single_module)
    assert resolve_adk_artifact_store_class(_CustomNoSuffixAdapter()) is _CustomSingleArtifactStore

    dual_module = SimpleNamespace(
        __all__=("DualSyncADKArtifactStore", "DualAsyncADKArtifactStore"),
        DualSyncADKArtifactStore=_CustomSyncADKArtifactStore,
        DualAsyncADKArtifactStore=_CustomAsyncADKArtifactStore,
    )
    monkeypatch.setitem(sys.modules, module_name, dual_module)
    assert resolve_adk_artifact_store_class(_CustomNoSuffixAdapter()) is _CustomSyncADKArtifactStore
    assert resolve_adk_artifact_store_class(_CustomAsyncNoSuffixAdapter()) is _CustomAsyncADKArtifactStore
    assert resolve_adk_artifact_store_class(_CustomNoSuffixAdapter) is _CustomSyncADKArtifactStore
    assert resolve_adk_artifact_store_class(_CustomAsyncNoSuffixAdapter) is _CustomAsyncADKArtifactStore


@pytest.mark.parametrize(
    ("settings", "expected"),
    [({"enable_sessions": True}, True), ({"enable_sessions": False}, False), ({}, True)],
    ids=["enabled", "disabled", "default-enabled"],
)
def test_adk_sessions_migration_enabled_matrix(settings: dict[str, Any], expected: bool) -> None:
    """adk_sessions_migration_enabled follows enable_sessions and defaults to True."""
    assert adk_sessions_migration_enabled(_Config(settings)) is expected


@pytest.mark.parametrize(
    ("settings", "expected"),
    [({"enable_memory": True}, True), ({"enable_memory": False}, False), ({}, True)],
    ids=["enabled", "disabled", "default-enabled"],
)
def test_adk_memory_migration_enabled_matrix(settings: dict[str, Any], expected: bool) -> None:
    """adk_memory_migration_enabled follows enable_memory and defaults to True."""
    assert adk_memory_migration_enabled(_Config(settings)) is expected


@pytest.mark.parametrize(
    "export_name",
    [
        "adk_memory_migration_enabled",
        "adk_sessions_migration_enabled",
        "resolve_adk_artifact_store_class",
        "resolve_adk_config",
        "resolve_adk_memory_store_class",
        "resolve_adk_store_class",
    ],
)
def test_adk_public_config_helpers_exported_lazily_and_identical(export_name: str) -> None:
    """All six helpers appear in __all__ and dir() and match _config_utils attributes."""
    assert export_name in adk_pkg.__all__
    assert export_name in dir(adk_pkg)
    assert getattr(adk_pkg, export_name) is getattr(_config_utils, export_name)
