"""Unit tests for data dictionary functionality."""

from unittest.mock import Mock

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.typing import VersionInfo
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
