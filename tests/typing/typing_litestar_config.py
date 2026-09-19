"""The public Litestar config accepts plugin options and boolean session setup."""

from sqlspec.adapters.aiomysql.litestar import AiomysqlLitestarConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteLitestarConfig
from sqlspec.adapters.asyncmy.litestar import AsyncmyLitestarConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.litestar import AsyncpgLitestarConfig
from sqlspec.adapters.bigquery.litestar import BigQueryLitestarConfig
from sqlspec.adapters.cockroach_asyncpg.litestar import CockroachAsyncpgLitestarConfig
from sqlspec.adapters.cockroach_psycopg.litestar import CockroachPsycopgLitestarConfig
from sqlspec.adapters.mysqlconnector.litestar import MysqlConnectorLitestarConfig
from sqlspec.adapters.oracledb.litestar import OracleLitestarConfig
from sqlspec.adapters.psqlpy.litestar import PsqlpyLitestarConfig
from sqlspec.adapters.psycopg.litestar import PsycopgLitestarConfig
from sqlspec.adapters.pymysql.litestar import PyMysqlLitestarConfig
from sqlspec.adapters.spanner.litestar import SpannerLitestarConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.litestar import SqliteLitestarConfig
from sqlspec.config import ExtensionConfigs
from sqlspec.extensions.litestar import LitestarConfig


def litestar_extension_config() -> ExtensionConfigs:
    config: LitestarConfig = {
        "session_table": True,
        "auto_trace_headers": True,
        "commit_mode": "autocommit",
        "connection_key": "connection",
        "correlation_header": "X-Request-ID",
        "correlation_headers": ["X-Correlation-ID"],
        "disable_di": False,
        "enable_correlation_middleware": True,
        "enable_sqlcommenter_middleware": True,
        "extra_commit_statuses": {201},
        "extra_rollback_statuses": {409},
        "manage_lifespan": True,
        "migrations_path": "migrations",
        "pool_key": "pool",
        "session_key": "session",
    }
    return {"litestar": config}


def aiomysql_litestar_extension_config() -> ExtensionConfigs:
    config: AiomysqlLitestarConfig = {"table_options": "ENGINE=InnoDB", "index_options": "USING BTREE"}
    return {"litestar": config}


def asyncmy_litestar_extension_config() -> ExtensionConfigs:
    config: AsyncmyLitestarConfig = {"table_options": "ENGINE=InnoDB"}
    return {"litestar": config}


def mysqlconnector_litestar_extension_config() -> ExtensionConfigs:
    config: MysqlConnectorLitestarConfig = {"index_options": "USING BTREE"}
    return {"litestar": config}


def pymysql_litestar_extension_config() -> ExtensionConfigs:
    config: PyMysqlLitestarConfig = {"table_options": "ENGINE=InnoDB"}
    return {"litestar": config}


def spanner_litestar_extension_config() -> ExtensionConfigs:
    config: SpannerLitestarConfig = {"shard_count": 8, "table_options": "locality_group = 'hot'"}
    return {"litestar": config}


def bigquery_litestar_extension_config() -> ExtensionConfigs:
    config: BigQueryLitestarConfig = {
        "partitioning": True,
        "partition_expiration_days": 7,
        "require_partition_filter": True,
    }
    return {"litestar": config}


def oracledb_litestar_extension_config() -> ExtensionConfigs:
    config: OracleLitestarConfig = {
        "compression": {"enabled": True, "algorithm": "basic"},
        "partitioning": {"strategy": "hash", "partition_count": 8, "session_partition_key": "session_id"},
    }
    return {"litestar": config}


def asyncpg_litestar_extension_config() -> ExtensionConfigs:
    config: AsyncpgLitestarConfig = {
        "fillfactor": 80,
        "autovacuum_vacuum_scale_factor": 0.1,
        "session_table": "sessions",
    }
    return {"litestar": config}


def psycopg_litestar_extension_config() -> ExtensionConfigs:
    config: PsycopgLitestarConfig = {"fillfactor": 80}
    return {"litestar": config}


def psqlpy_litestar_extension_config() -> ExtensionConfigs:
    config: PsqlpyLitestarConfig = {"autovacuum_analyze_scale_factor": 0.2}
    return {"litestar": config}


def cockroach_asyncpg_litestar_extension_config() -> ExtensionConfigs:
    config: CockroachAsyncpgLitestarConfig = {
        "enable_hash_sharded_indexes": True,
        "hash_shard_bucket_count": 8,
        "ttl_expiration_expression": False,
    }
    return {"litestar": config}


def cockroach_psycopg_litestar_extension_config() -> ExtensionConfigs:
    config: CockroachPsycopgLitestarConfig = {"ttl_expiration_expression": "expires_at"}
    return {"litestar": config}


def sqlite_litestar_extension_config() -> ExtensionConfigs:
    config: SqliteLitestarConfig = {"pragma_profile": True, "pragma_overrides": {"busy_timeout": 10000}}
    return {"litestar": config}


def aiosqlite_litestar_extension_config() -> ExtensionConfigs:
    config: AiosqliteLitestarConfig = {"pragma_profile": True}
    return {"litestar": config}


def typed_adapter_constructors() -> tuple[AsyncpgConfig, SqliteConfig]:
    postgres_settings: AsyncpgLitestarConfig = {"fillfactor": 80, "session_table": "sessions"}
    sqlite_settings: SqliteLitestarConfig = {"pragma_profile": True}
    return (
        AsyncpgConfig(extension_config={"litestar": postgres_settings}),
        SqliteConfig(extension_config={"litestar": sqlite_settings}),
    )


def plain_dictionary_constructor() -> SqliteConfig:
    return SqliteConfig(extension_config={"litestar": {"pragma_profile": True, "session_key": "db"}})
