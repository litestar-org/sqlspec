"""Tests for adapter configuration capability flags."""

import pytest

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
