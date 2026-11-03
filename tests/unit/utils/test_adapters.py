"""Unit tests for sqlspec.utils.adapters resolution helpers."""

from typing import Any, ClassVar

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.adk.store import (
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
)
from sqlspec.adapters.spanner.events.store import SpannerAsyncEventQueueStore, SpannerSyncEventQueueStore
from sqlspec.adapters.spanner.litestar.store import SpannerAsyncStore, SpannerSyncStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.config import AsyncDatabaseConfig, DatabaseConfigProtocol, SyncDatabaseConfig
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore
from sqlspec.extensions.events._store import BaseEventQueueStore
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class


@pytest.mark.parametrize(
    ("config_cls", "expected_name"),
    [
        (AdbcConfig, "adbc"),
        (AiosqliteConfig, "aiosqlite"),
        (AsyncpgConfig, "asyncpg"),
        (BigQueryConfig, "bigquery"),
        (CockroachAsyncpgConfig, "cockroach_asyncpg"),
        (CockroachPsycopgSyncConfig, "cockroach_psycopg"),
        (CockroachPsycopgAsyncConfig, "cockroach_psycopg"),
        (DuckDBConfig, "duckdb"),
        (MssqlPythonConfig, "mssql_python"),
        (OracleSyncConfig, "oracledb"),
        (OracleAsyncConfig, "oracledb"),
        (PsqlpyConfig, "psqlpy"),
        (PsycopgSyncConfig, "psycopg"),
        (PsycopgAsyncConfig, "psycopg"),
        (SpannerSyncConfig, "spanner"),
        (SpannerAsyncConfig, "spanner"),
        (SqliteConfig, "sqlite"),
    ],
)
def test_resolve_adapter_name_for_builtin_configs(config_cls: type[Any], expected_name: str) -> None:
    """Built-in adapter config classes and instances resolve their adapter package name."""
    assert resolve_adapter_name(config_cls) == expected_name


def test_resolve_adapter_name_for_none_and_unassociated_objects() -> None:
    """None and classes outside sqlspec.adapters without adapter_name return None."""

    class PlainConfig:
        pass

    assert resolve_adapter_name(None) is None
    assert resolve_adapter_name(PlainConfig) is None
    assert resolve_adapter_name(PlainConfig()) is None


def test_resolve_adapter_name_prefers_explicit_classvar_over_mro() -> None:
    """Explicit adapter_name ClassVar takes precedence over module path and MRO."""

    class ExternalSpannerConfig:
        adapter_name: ClassVar[str | None] = "spanner"

    class OverriddenSqliteConfig(SqliteConfig):
        adapter_name: ClassVar[str | None] = "custom_sqlite"

    assert resolve_adapter_name(ExternalSpannerConfig) == "spanner"
    assert resolve_adapter_name(ExternalSpannerConfig()) == "spanner"
    assert resolve_adapter_name(OverriddenSqliteConfig) == "custom_sqlite"
    assert resolve_adapter_name(OverriddenSqliteConfig(connection_config={"database": ":memory:"})) == "custom_sqlite"


def test_resolve_adapter_name_walks_mro_for_subclass_without_adapter_name() -> None:
    """Subclasses outside sqlspec.adapters without adapter_name resolve through their base class in MRO."""

    class SubclassedSqliteConfig(SqliteConfig):
        pass

    class DeepSubclassedSpannerConfig(SpannerSyncConfig):
        pass

    assert resolve_adapter_name(SubclassedSqliteConfig) == "sqlite"
    assert resolve_adapter_name(SubclassedSqliteConfig(connection_config={"database": ":memory:"})) == "sqlite"
    assert resolve_adapter_name(DeepSubclassedSpannerConfig) == "spanner"


def test_resolve_adapter_name_for_store_classes_and_instances() -> None:
    """Store classes and instances resolve via explicit adapter_name, MRO, or bound _config."""

    class ExternalStore(BaseEventQueueStore[Any]):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    class NamedExternalStore(ExternalStore):
        adapter_name: ClassVar[str | None] = "custom_engine"

    sqlite_config = SqliteConfig(connection_config={"database": ":memory:"})
    external_store = ExternalStore(sqlite_config)
    named_store = NamedExternalStore(sqlite_config)

    assert resolve_adapter_name(SqliteEventQueueStore) == "sqlite"
    assert resolve_adapter_name(SqliteStore) == "sqlite"
    assert resolve_adapter_name(SqliteADKStore) == "sqlite"
    assert resolve_adapter_name(SqliteADKMemoryStore) == "sqlite"
    assert resolve_adapter_name(ExternalStore) is None
    assert resolve_adapter_name(external_store) == "sqlite"
    assert resolve_adapter_name(NamedExternalStore) == "custom_engine"
    assert resolve_adapter_name(named_store) == "custom_engine"


@pytest.mark.parametrize(
    ("is_async", "expected_events", "expected_adk", "expected_memory", "expected_litestar"),
    [
        (
            False,
            SpannerSyncEventQueueStore,
            SpannerSyncADKStore,
            SpannerSyncADKMemoryStore,
            SpannerSyncStore,
        ),
        (
            True,
            SpannerAsyncEventQueueStore,
            SpannerAsyncADKStore,
            SpannerAsyncADKMemoryStore,
            SpannerAsyncStore,
        ),
    ],
)
def test_resolve_extension_store_class_for_external_config_with_spanner_adapter_name(
    is_async: bool,
    expected_events: type[Any],
    expected_adk: type[Any],
    expected_memory: type[Any],
    expected_litestar: type[Any],
) -> None:
    """External config class setting adapter_name = 'spanner' resolves Spanner sync/async stores."""

    class ExternalSpannerConfig:
        adapter_name: ClassVar[str | None] = "spanner"

        def __init__(self, async_mode: bool) -> None:
            self.is_async = async_mode

    config = ExternalSpannerConfig(is_async)

    assert (
        resolve_extension_store_class(
            config, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
        is expected_events
    )
    assert (
        resolve_extension_store_class(config, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is expected_adk
    )
    assert (
        resolve_extension_store_class(
            config, extension="adk", store_attr="adk_memory_store_class", store_suffix="ADKMemoryStore"
        )
        is expected_memory
    )
    assert (
        resolve_extension_store_class(
            config, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
        is expected_litestar
    )


def test_resolve_extension_store_class_for_subclass_without_adapter_name() -> None:
    """Config subclass without adapter_name resolves stores through its built-in base class."""

    class SubclassedSqliteConfig(SqliteConfig):
        pass

    config = SubclassedSqliteConfig(connection_config={"database": ":memory:"})

    assert (
        resolve_extension_store_class(
            config, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
        is SqliteEventQueueStore
    )
    assert (
        resolve_extension_store_class(config, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is SqliteADKStore
    )
    assert (
        resolve_extension_store_class(
            config, extension="adk", store_attr="adk_memory_store_class", store_suffix="ADKMemoryStore"
        )
        is SqliteADKMemoryStore
    )
    assert (
        resolve_extension_store_class(
            config, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
        is SqliteStore
    )


def test_resolve_extension_store_class_prefers_explicit_store_classvars() -> None:
    """Explicit *_store_class ClassVars on a config take precedence over module lookup."""

    class CustomEventStore(BaseEventQueueStore[Any]):
        pass

    class CustomADKStore(BaseSyncADKStore[Any]):
        pass

    class CustomADKMemoryStore(BaseSyncADKMemoryStore[Any]):
        pass

    class CustomLitestarStore(BaseSQLSpecStore[Any]):
        pass

    class ThirdPartyConfig:
        adapter_name: ClassVar[str | None] = "third_party"
        events_store_class: ClassVar[type[Any] | None] = CustomEventStore
        adk_store_class: ClassVar[type[Any] | None] = CustomADKStore
        adk_memory_store_class: ClassVar[type[Any] | None] = CustomADKMemoryStore
        litestar_store_class: ClassVar[type[Any] | None] = CustomLitestarStore

    config = ThirdPartyConfig()

    assert (
        resolve_extension_store_class(
            config, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
        is CustomEventStore
    )
    assert (
        resolve_extension_store_class(config, extension="adk", store_attr="adk_store_class", store_suffix="ADKStore")
        is CustomADKStore
    )
    assert (
        resolve_extension_store_class(
            config, extension="adk", store_attr="adk_memory_store_class", store_suffix="ADKMemoryStore"
        )
        is CustomADKMemoryStore
    )
    assert (
        resolve_extension_store_class(
            config, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
        is CustomLitestarStore
    )


def test_config_and_store_base_classes_declare_adapter_and_store_classvars() -> None:
    """DatabaseConfigProtocol and extension base stores expose default None ClassVar attributes."""
    assert DatabaseConfigProtocol.adapter_name is None
    assert DatabaseConfigProtocol.events_store_class is None
    assert DatabaseConfigProtocol.adk_store_class is None
    assert DatabaseConfigProtocol.adk_memory_store_class is None
    assert DatabaseConfigProtocol.litestar_store_class is None
    assert SyncDatabaseConfig.adapter_name is None
    assert AsyncDatabaseConfig.adapter_name is None
    assert BaseEventQueueStore.adapter_name is None
    assert BaseSQLSpecStore.adapter_name is None
    assert BaseSyncADKStore.adapter_name is None
    assert BaseAsyncADKStore.adapter_name is None
    assert BaseSyncADKMemoryStore.adapter_name is None
    assert BaseAsyncADKMemoryStore.adapter_name is None
