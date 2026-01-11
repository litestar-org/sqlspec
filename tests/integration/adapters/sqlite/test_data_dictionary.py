"""Integration tests for SQLite data dictionary."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import VersionInfo

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.driver import SqliteDriver
pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_data_dictionary_version_detection(sqlite_driver: "SqliteDriver") -> None:
    """Test SQLite version detection with real database."""
    data_dict = sqlite_driver.data_dictionary

    version = data_dict.get_version(sqlite_driver)
    assert version is not None
    assert isinstance(version, VersionInfo)
    assert version.major >= 3
    assert version.minor >= 0
    assert version.patch >= 0


def test_sqlite_data_dictionary_feature_flags(sqlite_driver: "SqliteDriver") -> None:
    """Test SQLite feature flags with real database."""
    data_dict = sqlite_driver.data_dictionary

    # Test always supported features
    assert data_dict.get_feature_flag(sqlite_driver, "supports_transactions") is True
    assert data_dict.get_feature_flag(sqlite_driver, "supports_prepared_statements") is True
    assert data_dict.get_feature_flag(sqlite_driver, "supports_cte") is True
    assert data_dict.get_feature_flag(sqlite_driver, "supports_window_functions") is True

    # Test never supported features
    assert data_dict.get_feature_flag(sqlite_driver, "supports_arrays") is False
    assert data_dict.get_feature_flag(sqlite_driver, "supports_uuid") is False
    assert data_dict.get_feature_flag(sqlite_driver, "supports_schemas") is False

    # Test version-dependent features (these depend on actual SQLite version)
    version = data_dict.get_version(sqlite_driver)
    if version and version >= VersionInfo(3, 38, 0):
        assert data_dict.get_feature_flag(sqlite_driver, "supports_json") is True
    else:
        assert data_dict.get_feature_flag(sqlite_driver, "supports_json") is False

    if version and version >= VersionInfo(3, 35, 0):
        assert data_dict.get_feature_flag(sqlite_driver, "supports_returning") is True
    else:
        assert data_dict.get_feature_flag(sqlite_driver, "supports_returning") is False


def test_sqlite_data_dictionary_optimal_types(sqlite_driver: "SqliteDriver") -> None:
    """Test SQLite optimal type selection with real database."""
    data_dict = sqlite_driver.data_dictionary

    # Test basic types
    assert data_dict.get_optimal_type(sqlite_driver, "uuid") == "TEXT"
    assert data_dict.get_optimal_type(sqlite_driver, "boolean") == "INTEGER"
    assert data_dict.get_optimal_type(sqlite_driver, "timestamp") == "TIMESTAMP"
    assert data_dict.get_optimal_type(sqlite_driver, "text") == "TEXT"
    assert data_dict.get_optimal_type(sqlite_driver, "blob") == "BLOB"

    # Test JSON type based on version
    version = data_dict.get_version(sqlite_driver)
    if version and version >= VersionInfo(3, 38, 0):
        assert data_dict.get_optimal_type(sqlite_driver, "json") == "JSON"
    else:
        assert data_dict.get_optimal_type(sqlite_driver, "json") == "TEXT"

    # Test unknown type defaults to TEXT
    assert data_dict.get_optimal_type(sqlite_driver, "unknown_type") == "TEXT"


def test_sqlite_data_dictionary_available_features(sqlite_driver: "SqliteDriver") -> None:
    """Test listing available features for SQLite."""
    data_dict = sqlite_driver.data_dictionary

    features = data_dict.list_available_features()
    assert isinstance(features, list)
    assert len(features) > 0

    expected_features = [
        "supports_json",
        "supports_returning",
        "supports_upsert",
        "supports_window_functions",
        "supports_cte",
        "supports_transactions",
        "supports_prepared_statements",
    ]

    for feature in expected_features:
        assert feature in features
