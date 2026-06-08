"""Unit tests for data dictionary functionality."""

from unittest.mock import AsyncMock, Mock

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.adapters.aiomysql.data_dictionary import AiomysqlDataDictionary
from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.asyncmy.data_dictionary import AsyncmyDataDictionary
from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.duckdb.data_dictionary import DuckDBDataDictionary
from sqlspec.adapters.mysqlconnector.data_dictionary import (
    MysqlConnectorAsyncDataDictionary,
    MysqlConnectorSyncDataDictionary,
)
from sqlspec.adapters.oracledb.data_dictionary import OracledbAsyncDataDictionary, OracledbSyncDataDictionary
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.adapters.psycopg.data_dictionary import PsycopgAsyncDataDictionary, PsycopgSyncDataDictionary
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.data_dictionary import VersionInfo
from sqlspec.driver import SyncDriverAdapterBase
from tests.conftest import requires_interpreted

pytestmark = requires_interpreted


def test_version_info_creation() -> None:
    """Test VersionInfo object creation."""
    version = VersionInfo(1, 2, 3)
    assert version.major == 1
    assert version.minor == 2
    assert version.patch == 3


def test_version_info_defaults() -> None:
    """Test VersionInfo defaults."""
    version = VersionInfo(5)
    assert version.major == 5
    assert version.minor == 0
    assert version.patch == 0


def test_version_info_comparison() -> None:
    """Test VersionInfo comparison operators."""
    v1 = VersionInfo(1, 2, 3)
    v2 = VersionInfo(1, 2, 3)
    v3 = VersionInfo(1, 2, 4)
    v4 = VersionInfo(2, 0, 0)

    assert v1 == v2
    assert v1 < v3
    assert v1 < v4
    assert v3 > v1
    assert v4 > v1


def test_version_info_string_representation() -> None:
    """Test VersionInfo string representation."""
    version = VersionInfo(1, 2, 3)
    assert str(version) == "1.2.3"


def test_version_tuple() -> None:
    """Test version_tuple property."""
    version = VersionInfo(1, 2, 3)
    assert version.version_tuple == (1, 2, 3)


def test_public_data_dictionary_classes_remain_constructible() -> None:
    """All adapter data-dictionary public classes should remain directly constructible."""
    data_dictionary_types: tuple[type[object], ...] = (
        AdbcDataDictionary,
        AiomysqlDataDictionary,
        AiosqliteDataDictionary,
        AsyncmyDataDictionary,
        AsyncpgDataDictionary,
        BigQueryDataDictionary,
        CockroachAsyncpgDataDictionary,
        CockroachPsycopgAsyncDataDictionary,
        CockroachPsycopgSyncDataDictionary,
        DuckDBDataDictionary,
        MysqlConnectorAsyncDataDictionary,
        MysqlConnectorSyncDataDictionary,
        OracledbAsyncDataDictionary,
        OracledbSyncDataDictionary,
        PsqlpyDataDictionary,
        PsycopgAsyncDataDictionary,
        PsycopgSyncDataDictionary,
        PyMysqlDataDictionary,
        SpannerDataDictionary,
        SqliteDataDictionary,
    )

    for dictionary_type in data_dictionary_types:
        assert dictionary_type().__class__ is dictionary_type


def test_postgres_data_dictionary_normalizes_identifier_binds() -> None:
    """PostgreSQL metadata lookups should bind normalized schema and table identifiers."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select.return_value = []

    data_dict = PsycopgSyncDataDictionary()
    data_dict.get_columns(mock_driver, table="Widgets", schema="Tenant")

    _, kwargs = mock_driver.select.call_args
    assert kwargs["schema_name"] == "tenant"
    assert kwargs["table_name"] == "widgets"


def test_oracle_data_dictionary_normalizes_lowercase_schema_and_preserves_mixed_case_table() -> None:
    """Oracle metadata lookups should normalize lowercase users without flattening mixed-case names."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select.return_value = []

    data_dict = OracledbSyncDataDictionary()
    data_dict.get_columns(mock_driver, table="MixedCase", schema="myapp")

    _, kwargs = mock_driver.select.call_args
    assert kwargs["schema_name"] == "MYAPP"
    assert kwargs["table_name"] == "MixedCase"


def test_sqlite_get_version_success() -> None:
    """Test successful version detection for SQLite."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = "3.42.0"

    data_dict = SqliteDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is not None
    assert version.major == 3
    assert version.minor == 42
    assert version.patch == 0


def test_sqlite_get_version_failure() -> None:
    """Test version detection failure for SQLite."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = None

    data_dict = SqliteDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is None


def test_sqlite_get_version_parse_error() -> None:
    """Test version parsing error for SQLite."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = "invalid-version"

    data_dict = SqliteDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is None


def test_sqlite_feature_flags_with_version() -> None:
    """Test feature flags based on version for SQLite."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = "3.42.0"

    data_dict = SqliteDataDictionary()

    assert data_dict.get_feature_flag(mock_driver, "supports_json") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_returning") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_upsert") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_window_functions") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_cte") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_arrays") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_uuid") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_schemas") is False


def test_sqlite_feature_flags_old_version() -> None:
    """Test feature flags for older SQLite version."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = "3.20.0"

    data_dict = SqliteDataDictionary()

    assert data_dict.get_feature_flag(mock_driver, "supports_json") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_returning") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_upsert") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_transactions") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_prepared_statements") is True


def test_sqlite_feature_flags_no_version() -> None:
    """Test feature flags when version detection fails."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = None

    data_dict = SqliteDataDictionary()

    assert data_dict.get_feature_flag(mock_driver, "supports_json") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_returning") is False


def test_sqlite_get_optimal_type_with_json_support() -> None:
    """Test optimal type selection with JSON support."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = "3.42.0"

    data_dict = SqliteDataDictionary()

    assert data_dict.get_optimal_type(mock_driver, "json") == "JSON"
    assert data_dict.get_optimal_type(mock_driver, "uuid") == "TEXT"
    assert data_dict.get_optimal_type(mock_driver, "boolean") == "INTEGER"
    assert data_dict.get_optimal_type(mock_driver, "text") == "TEXT"
    assert data_dict.get_optimal_type(mock_driver, "blob") == "BLOB"


def test_sqlite_get_optimal_type_without_json_support() -> None:
    """Test optimal type selection without JSON support."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none.return_value = "3.20.0"

    data_dict = SqliteDataDictionary()

    assert data_dict.get_optimal_type(mock_driver, "json") == "TEXT"


def test_sqlite_dialect_resolves_json_type_by_version() -> None:
    """SQLite dialect helper should resolve JSON type boundaries."""
    from sqlspec.data_dictionary.dialects.sqlite import resolve_sqlite_json_type

    assert resolve_sqlite_json_type(VersionInfo(3, 42, 0)) == "JSON"
    assert resolve_sqlite_json_type(VersionInfo(3, 20, 0)) == "TEXT"
    assert resolve_sqlite_json_type(None) == "TEXT"


async def test_sqlite_family_json_type_selection_matches_adbc() -> None:
    """SQLite, aiosqlite, and ADBC-as-SQLite should agree on JSON type selection."""
    sqlite_driver = Mock(spec=SyncDriverAdapterBase)
    sqlite_driver.select_value_or_none.return_value = "3.42.0"

    aiosqlite_driver = Mock()
    aiosqlite_driver.select_value_or_none = AsyncMock(return_value="3.42.0")

    adbc_driver = Mock()
    adbc_driver.dialect = "sqlite"
    adbc_driver.select_value_or_none.return_value = "3.42.0"

    assert SqliteDataDictionary().get_optimal_type(sqlite_driver, "json") == "JSON"
    assert await AiosqliteDataDictionary().get_optimal_type(aiosqlite_driver, "json") == "JSON"
    assert AdbcDataDictionary().get_optimal_type(adbc_driver, "json") == "JSON"


def test_sqlite_list_available_features() -> None:
    """Test listing available features."""
    data_dict = SqliteDataDictionary()
    features = data_dict.list_available_features()

    expected_features = [
        "supports_json",
        "supports_returning",
        "supports_upsert",
        "supports_window_functions",
        "supports_cte",
        "supports_transactions",
        "supports_prepared_statements",
        "supports_schemas",
        "supports_arrays",
        "supports_uuid",
    ]

    assert all(feature in features for feature in expected_features)


def test_mysql_dialect_resolves_json_type_by_version() -> None:
    """MySQL dialect helper should resolve JSON type boundaries."""
    from sqlspec.data_dictionary.dialects.mysql import resolve_mysql_json_type

    assert resolve_mysql_json_type(VersionInfo(8, 0, 0)) == "JSON"
    assert resolve_mysql_json_type(VersionInfo(5, 7, 7)) == "TEXT"
    assert resolve_mysql_json_type(None) == "TEXT"


async def test_mysql_family_json_type_selection_matches_adbc() -> None:
    """MySQL-family adapters and ADBC-as-MySQL should agree on JSON type selection."""
    sync_driver = Mock(spec=SyncDriverAdapterBase)
    sync_driver.select_value_or_none.return_value = "5.7.8"

    async_driver = Mock()
    async_driver.select_value_or_none = AsyncMock(return_value="5.7.8")

    adbc_driver = Mock()
    adbc_driver.dialect = "mariadb"
    adbc_driver.select_value_or_none.return_value = "10.6.2-MariaDB"

    assert MysqlConnectorSyncDataDictionary().get_optimal_type(sync_driver, "json") == "JSON"
    assert PyMysqlDataDictionary().get_optimal_type(sync_driver, "json") == "JSON"
    assert await MysqlConnectorAsyncDataDictionary().get_optimal_type(async_driver, "json") == "JSON"
    assert await AiomysqlDataDictionary().get_optimal_type(async_driver, "json") == "JSON"
    assert await AsyncmyDataDictionary().get_optimal_type(async_driver, "json") == "JSON"
    assert AdbcDataDictionary().get_optimal_type(adbc_driver, "json") == "JSON"


def test_adbc_get_dialect() -> None:
    """Test dialect retrieval from ADBC driver."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"

    dialect = str(mock_driver.dialect)

    assert dialect == "postgres"


def test_adbc_get_version_postgres() -> None:
    """Test version detection for PostgreSQL via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.select_value_or_none.return_value = "PostgreSQL 15.3 on x86_64-pc-linux-gnu"

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is not None
    assert version.major == 15
    assert version.minor == 3
    assert version.patch == 0


def test_adbc_get_version_postgresql_alias() -> None:
    """Test version detection for PostgreSQL alias via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "postgresql"
    mock_driver.select_value_or_none.return_value = "PostgreSQL 16.1 on x86_64-pc-linux-gnu"

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is not None
    assert version.major == 16
    assert version.minor == 1
    assert version.patch == 0


def test_adbc_get_version_sqlite() -> None:
    """Test version detection for SQLite via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "sqlite"
    mock_driver.select_value_or_none.return_value = "3.42.0"

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is not None
    assert version.major == 3
    assert version.minor == 42
    assert version.patch == 0


def test_adbc_get_version_mariadb_alias() -> None:
    """Test version detection for MariaDB alias via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "mariadb"
    mock_driver.select_value_or_none.return_value = "10.6.2-MariaDB"

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is not None
    assert version.major == 10
    assert version.minor == 6
    assert version.patch == 2


def test_adbc_get_version_duckdb() -> None:
    """Test version detection for DuckDB via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "duckdb"
    mock_driver.select_value_or_none.return_value = "v0.9.2"

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is not None
    assert version.major == 0
    assert version.minor == 9
    assert version.patch == 2


def test_adbc_get_version_bigquery() -> None:
    """Test version detection for BigQuery via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "bigquery"

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is None


def test_adbc_get_version_exception_handling() -> None:
    """Test exception handling in version detection."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.select_value_or_none.side_effect = Exception("Database error")

    data_dict = AdbcDataDictionary()
    version = data_dict.get_version(mock_driver)

    assert version is None


def test_adbc_postgres_feature_flags() -> None:
    """Test PostgreSQL feature flags via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.select_value_or_none.return_value = "PostgreSQL 15.3 on x86_64-pc-linux-gnu"

    data_dict = AdbcDataDictionary()

    assert data_dict.get_feature_flag(mock_driver, "supports_json") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_jsonb") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_uuid") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_arrays") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_returning") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_upsert") is True


def test_adbc_postgres_optimal_types() -> None:
    """Test PostgreSQL optimal type selection via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.select_value_or_none.return_value = "PostgreSQL 15.3 on x86_64-pc-linux-gnu"

    data_dict = AdbcDataDictionary()

    assert data_dict.get_optimal_type(mock_driver, "json") == "JSONB"
    assert data_dict.get_optimal_type(mock_driver, "uuid") == "UUID"
    assert data_dict.get_optimal_type(mock_driver, "boolean") == "BOOLEAN"
    assert data_dict.get_optimal_type(mock_driver, "timestamp") == "TIMESTAMP WITH TIME ZONE"
    assert data_dict.get_optimal_type(mock_driver, "text") == "TEXT"
    assert data_dict.get_optimal_type(mock_driver, "blob") == "BYTEA"


def test_postgres_dialect_resolves_json_type_by_version() -> None:
    """Postgres dialect helper should resolve JSON type boundaries."""
    from sqlspec.data_dictionary.dialects.postgres import resolve_postgres_json_type

    assert resolve_postgres_json_type(VersionInfo(15, 3, 0)) == "JSONB"
    assert resolve_postgres_json_type(VersionInfo(9, 3, 0)) == "JSON"
    assert resolve_postgres_json_type(VersionInfo(9, 1, 0)) == "TEXT"
    assert resolve_postgres_json_type(None) == "TEXT"


def test_postgres_sync_json_type_selection_matches_adbc_before_jsonb() -> None:
    """ADBC-as-Postgres and psycopg should agree before JSONB is available."""
    adbc_driver = Mock()
    adbc_driver.dialect = "postgres"
    adbc_driver.select_value_or_none.return_value = "PostgreSQL 9.3 on x86_64-pc-linux-gnu"

    psycopg_driver = Mock(spec=SyncDriverAdapterBase)
    psycopg_driver.select_value_or_none.return_value = "PostgreSQL 9.3 on x86_64-pc-linux-gnu"

    assert AdbcDataDictionary().get_optimal_type(adbc_driver, "json") == "JSON"
    assert PsycopgSyncDataDictionary().get_optimal_type(psycopg_driver, "json") == "JSON"


async def test_postgres_async_json_type_selection_matches_before_jsonb() -> None:
    """Async Postgres adapters should agree before JSONB is available."""
    asyncpg_driver = Mock()
    asyncpg_driver.select_value_or_none = AsyncMock(return_value="PostgreSQL 9.3 on x86_64-pc-linux-gnu")

    psycopg_driver = Mock()
    psycopg_driver.select_value_or_none = AsyncMock(return_value="PostgreSQL 9.3 on x86_64-pc-linux-gnu")

    psqlpy_driver = Mock()
    psqlpy_driver.select_value = AsyncMock(return_value="PostgreSQL 9.3 on x86_64-pc-linux-gnu")

    assert await AsyncpgDataDictionary().get_optimal_type(asyncpg_driver, "json") == "JSON"
    assert await PsycopgAsyncDataDictionary().get_optimal_type(psycopg_driver, "json") == "JSON"
    assert await PsqlpyDataDictionary().get_optimal_type(psqlpy_driver, "json") == "JSON"


def test_cockroach_dialect_resolves_json_type_by_version() -> None:
    """CockroachDB dialect helper should resolve JSON type boundaries."""
    from sqlspec.data_dictionary.dialects.cockroachdb import resolve_cockroachdb_json_type

    assert resolve_cockroachdb_json_type(VersionInfo(23, 1, 0)) == "JSONB"
    assert resolve_cockroachdb_json_type(VersionInfo(19, 2, 0)) == "TEXT"
    assert resolve_cockroachdb_json_type(None) == "TEXT"


async def test_cockroach_family_json_type_selection_matches_adbc() -> None:
    """CockroachDB adapters and ADBC-as-Cockroach should agree on JSON type selection."""
    sync_driver = Mock(spec=SyncDriverAdapterBase)
    sync_driver.select_value_or_none.return_value = "CockroachDB CCL v19.2.0"

    async_driver = Mock()
    async_driver.select_value_or_none = AsyncMock(return_value="CockroachDB CCL v19.2.0")

    adbc_driver = Mock()
    adbc_driver.dialect = "cockroach"
    adbc_driver.select_value_or_none.return_value = "CockroachDB CCL v19.2.0"

    assert CockroachPsycopgSyncDataDictionary().get_optimal_type(sync_driver, "json") == "TEXT"
    assert await CockroachPsycopgAsyncDataDictionary().get_optimal_type(async_driver, "json") == "TEXT"
    assert await CockroachAsyncpgDataDictionary().get_optimal_type(async_driver, "json") == "TEXT"
    assert AdbcDataDictionary().get_optimal_type(adbc_driver, "json") == "TEXT"


def test_adbc_bigquery_feature_flags() -> None:
    """Test BigQuery feature flags via ADBC."""
    mock_driver = Mock()
    mock_driver.dialect = "bigquery"

    data_dict = AdbcDataDictionary()

    assert data_dict.get_feature_flag(mock_driver, "supports_json") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_arrays") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_structs") is True
    assert data_dict.get_feature_flag(mock_driver, "supports_returning") is False
    assert data_dict.get_feature_flag(mock_driver, "supports_transactions") is True


def test_bigquery_dialect_formats_information_schema_identifiers() -> None:
    """BigQuery dialect helper should format shared INFORMATION_SCHEMA identifiers."""
    from sqlspec.data_dictionary.dialects.bigquery import (
        format_bigquery_information_schema_tables,
        format_bigquery_schema_prefix,
    )

    tables_table, kcu_table, rc_table = format_bigquery_information_schema_tables("project.dataset")

    assert tables_table == "`project.dataset.INFORMATION_SCHEMA.TABLES`"
    assert kcu_table == "`project.dataset.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
    assert rc_table == "`project.dataset.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
    assert format_bigquery_schema_prefix("project.dataset") == "`project.dataset`."


def test_bigquery_dialect_formats_unqualified_information_schema_identifiers() -> None:
    """BigQuery dialect helper should leave INFORMATION_SCHEMA unqualified without schema."""
    from sqlspec.data_dictionary.dialects.bigquery import (
        format_bigquery_information_schema_tables,
        format_bigquery_schema_prefix,
    )

    tables_table, kcu_table, rc_table = format_bigquery_information_schema_tables(None)

    assert tables_table == "INFORMATION_SCHEMA.TABLES"
    assert kcu_table == "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
    assert rc_table == "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"
    assert format_bigquery_schema_prefix(None) == ""


def test_bigquery_family_uses_dialect_information_schema_helpers(monkeypatch) -> None:
    """BigQuery and ADBC-as-BigQuery should delegate shared INFORMATION_SCHEMA formatting."""
    from sqlspec.adapters.adbc import data_dictionary as adbc_data_dictionary_module
    from sqlspec.adapters.bigquery import data_dictionary as bigquery_data_dictionary_module

    def fake_tables(schema_name: "str | None") -> "tuple[str, str, str]":
        assert schema_name == "project.dataset"
        return ("BIGQUERY_TABLES_SENTINEL", "BIGQUERY_KCU_SENTINEL", "BIGQUERY_RC_SENTINEL")

    def fake_prefix(schema_name: "str | None") -> str:
        assert schema_name == "project.dataset"
        return "BIGQUERY_PREFIX_SENTINEL."

    monkeypatch.setattr(bigquery_data_dictionary_module, "format_bigquery_information_schema_tables", fake_tables)
    monkeypatch.setattr(adbc_data_dictionary_module, "format_bigquery_information_schema_tables", fake_tables)
    monkeypatch.setattr(bigquery_data_dictionary_module, "format_bigquery_schema_prefix", fake_prefix)
    monkeypatch.setattr(adbc_data_dictionary_module, "format_bigquery_schema_prefix", fake_prefix)

    native_driver = Mock()
    native_driver.select.return_value = []
    adbc_driver = Mock()
    adbc_driver.dialect = "bigquery"
    adbc_driver.select.return_value = []

    BigQueryDataDictionary().get_tables(native_driver, schema="project.dataset")
    AdbcDataDictionary().get_tables(adbc_driver, schema="project.dataset")
    BigQueryDataDictionary().get_columns(native_driver, schema="project.dataset")
    AdbcDataDictionary().get_columns(adbc_driver, schema="project.dataset")

    native_tables_query = native_driver.select.call_args_list[0].args[0]
    adbc_tables_query = adbc_driver.select.call_args_list[0].args[0]
    native_columns_query = native_driver.select.call_args_list[1].args[0]
    adbc_columns_query = adbc_driver.select.call_args_list[1].args[0]

    assert "BIGQUERY_TABLES_SENTINEL" in native_tables_query
    assert "BIGQUERY_KCU_SENTINEL" in native_tables_query
    assert "BIGQUERY_RC_SENTINEL" in native_tables_query
    assert "BIGQUERY_TABLES_SENTINEL" in adbc_tables_query
    assert "BIGQUERY_KCU_SENTINEL" in adbc_tables_query
    assert "BIGQUERY_RC_SENTINEL" in adbc_tables_query
    assert "BIGQUERY_PREFIX_SENTINEL.INFORMATION_SCHEMA.COLUMNS" in native_columns_query
    assert "BIGQUERY_PREFIX_SENTINEL.INFORMATION_SCHEMA.COLUMNS" in adbc_columns_query


def test_adbc_unknown_feature_flag() -> None:
    """Test unknown feature flag."""
    mock_driver = Mock()
    mock_driver.dialect = "postgres"
    mock_driver.select_value_or_none.return_value = "PostgreSQL 15.3 on x86_64-pc-linux-gnu"

    data_dict = AdbcDataDictionary()

    assert data_dict.get_feature_flag(mock_driver, "unknown_feature") is False


def test_adbc_list_available_features() -> None:
    """Test listing available features for ADBC."""
    data_dict = AdbcDataDictionary()
    features = data_dict.list_available_features()

    expected_features = [
        "supports_json",
        "supports_jsonb",
        "supports_uuid",
        "supports_arrays",
        "supports_structs",
        "supports_returning",
        "supports_upsert",
        "supports_window_functions",
        "supports_cte",
        "supports_transactions",
        "supports_prepared_statements",
        "supports_schemas",
    ]

    assert all(feature in features for feature in expected_features)
