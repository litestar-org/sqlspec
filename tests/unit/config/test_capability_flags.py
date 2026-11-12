"""Tests for adapter configuration capability flags."""

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.tenant import TenantContextPayload

CONFIG_CLASSES: tuple[tuple[str, str], ...] = (
    ("sqlspec.adapters.adbc.config", "AdbcConfig"),
    ("sqlspec.adapters.aiomysql.config", "AiomysqlConfig"),
    ("sqlspec.adapters.aiosqlite.config", "AiosqliteConfig"),
    ("sqlspec.adapters.arrow_odbc.config", "ArrowOdbcConfig"),
    ("sqlspec.adapters.asyncmy.config", "AsyncmyConfig"),
    ("sqlspec.adapters.asyncpg.config", "AsyncpgConfig"),
    ("sqlspec.adapters.bigquery.config", "BigQueryConfig"),
    ("sqlspec.adapters.cockroach_asyncpg.config", "CockroachAsyncpgConfig"),
    ("sqlspec.adapters.cockroach_psycopg.config", "CockroachPsycopgAsyncConfig"),
    ("sqlspec.adapters.cockroach_psycopg.config", "CockroachPsycopgSyncConfig"),
    ("sqlspec.adapters.duckdb.config", "DuckDBConfig"),
    ("sqlspec.adapters.mssql_python.config", "MssqlPythonConfig"),
    ("sqlspec.adapters.mysqlconnector.config", "MysqlConnectorAsyncConfig"),
    ("sqlspec.adapters.mysqlconnector.config", "MysqlConnectorSyncConfig"),
    ("sqlspec.adapters.oracledb.config", "OracleAsyncConfig"),
    ("sqlspec.adapters.oracledb.config", "OracleSyncConfig"),
    ("sqlspec.adapters.psqlpy.config", "PsqlpyConfig"),
    ("sqlspec.adapters.psycopg.config", "PsycopgAsyncConfig"),
    ("sqlspec.adapters.psycopg.config", "PsycopgSyncConfig"),
    ("sqlspec.adapters.pymssql.config", "PymssqlConfig"),
    ("sqlspec.adapters.pymysql.config", "PyMysqlConfig"),
    ("sqlspec.adapters.spanner.config", "SpannerAsyncConfig"),
    ("sqlspec.adapters.spanner.config", "SpannerSyncConfig"),
    ("sqlspec.adapters.sqlite.config", "SqliteConfig"),
)

UNRELIABLE_ROWCOUNT_CONFIGS: frozenset[str] = frozenset({"ArrowOdbcConfig", "AdbcConfig"})


def test_reliable_rowcount_defaults() -> None:
    """Verify the reliable rowcount default on the base configuration."""
    from sqlspec.config import DatabaseConfigProtocol

    assert DatabaseConfigProtocol.supports_reliable_rowcount is True


@pytest.mark.parametrize(("module_path", "class_name"), CONFIG_CLASSES)
def test_each_adapter_reliable_rowcount_flag(module_path: str, class_name: str) -> None:
    """Verify supports_reliable_rowcount value for each individual adapter config."""
    module = pytest.importorskip(module_path)
    config_cls = getattr(module, class_name)
    expected = class_name not in UNRELIABLE_ROWCOUNT_CONFIGS
    assert config_cls.supports_reliable_rowcount is expected


@dataclass(slots=True)
class _SyncTenantStrategy:
    def checkout(self, connection: object, payload: TenantContextPayload) -> TenantLeaseState:
        return TenantLeaseState(payload)

    def checkin(self, connection: object, state: TenantLeaseState) -> None:
        pass


class _AsyncTenantStrategy:
    __slots__ = ()

    async def checkout(self, connection: object, payload: TenantContextPayload) -> TenantLeaseState:
        return TenantLeaseState(payload)

    async def checkin(self, connection: object, state: TenantLeaseState) -> None:
        pass


@pytest.mark.parametrize("strategy", [_SyncTenantStrategy(), "default", "rls", "session_context", "identifier", True])
def test_unsupported_tenant_strategy_rejected(strategy: object) -> None:
    with pytest.raises(ImproperConfigurationError, match=r"SqliteConfig.*tenant_strategy"):
        SqliteConfig(driver_features={"tenant_strategy": strategy})


@pytest.mark.parametrize("strategy", [None, False])
def test_disabled_tenant_strategy_normalized_without_mutating_input(strategy: object) -> None:
    features = {"tenant_strategy": strategy}
    config = SqliteConfig(driver_features=features)

    assert config.driver_features["tenant_strategy"] is None
    assert features["tenant_strategy"] is strategy


@pytest.mark.parametrize("strategy", [object(), {}, {"checkout": "invalid"}, 1])
def test_invalid_tenant_strategy_rejected(strategy: object, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SqliteConfig, "supports_tenant_strategy", True, raising=False)

    with pytest.raises(ImproperConfigurationError, match="invalid tenant_strategy"):
        SqliteConfig(driver_features={"tenant_strategy": strategy})


@pytest.mark.parametrize(
    ("config_type", "strategy", "accepted"),
    [
        (SqliteConfig, _SyncTenantStrategy(), True),
        (AiosqliteConfig, _AsyncTenantStrategy(), True),
        (SqliteConfig, _AsyncTenantStrategy(), False),
        (AiosqliteConfig, _SyncTenantStrategy(), False),
    ],
)
def test_tenant_strategy_execution_mode(
    config_type: type[SqliteConfig] | type[AiosqliteConfig],
    strategy: object,
    accepted: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config_type, "supports_tenant_strategy", True, raising=False)

    if accepted:
        assert config_type(driver_features={"tenant_strategy": strategy}).driver_features["tenant_strategy"] is strategy
    else:
        with pytest.raises(ImproperConfigurationError, match=r"tenant_strategy.*async"):
            config_type(driver_features={"tenant_strategy": strategy})


@pytest.mark.parametrize("shorthand", ["default", True])
def test_supporting_config_resolves_tenant_shorthand(shorthand: object) -> None:
    resolved_strategy = _SyncTenantStrategy()

    class TenantConfig(SqliteConfig):
        __slots__ = ()
        supports_tenant_strategy = True

        def _resolve_tenant_strategy(self, strategy: object) -> _SyncTenantStrategy:
            assert strategy is shorthand
            return resolved_strategy

    assert (
        TenantConfig(driver_features={"tenant_strategy": shorthand}).driver_features["tenant_strategy"]
        is resolved_strategy
    )


def test_unsupported_tenant_baseline_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SqliteConfig, "supports_tenant_strategy", True, raising=False)

    with pytest.raises(ImproperConfigurationError, match="tenant_session_baseline"):
        SqliteConfig(driver_features={"tenant_strategy": _SyncTenantStrategy(), "tenant_session_baseline": {}})


def test_tenant_strategy_without_lease_implementation_refuses_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SqliteConfig, "supports_tenant_strategy", True, raising=False)
    strategy = SimpleNamespace(checkout=lambda connection, payload: None, checkin=lambda connection, state: None)
    config = SqliteConfig(driver_features={"tenant_strategy": strategy})

    with pytest.raises(ImproperConfigurationError, match="no tenant lease implementation"):
        config.provide_connection()
    with pytest.raises(ImproperConfigurationError, match="no tenant lease implementation"), config.provide_session():
        pytest.fail("Session opened without a tenant lease")
