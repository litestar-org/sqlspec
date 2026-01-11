"""Integration tests for ADBC data dictionary."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import VersionInfo

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.driver import AdbcDriver

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.mark.adbc
def test_adbc_data_dictionary_version_detection(adbc_sync_driver: "AdbcDriver") -> None:
    """Test version detection with real database via ADBC."""
    data_dict = adbc_sync_driver.data_dictionary

    version = data_dict.get_version(adbc_sync_driver)
    assert version is not None
    assert isinstance(version, VersionInfo)
    assert version.major >= 0
    assert version.minor >= 0
    assert version.patch >= 0


@pytest.mark.adbc
def test_adbc_data_dictionary_dialect_detection(adbc_sync_driver: "AdbcDriver") -> None:
    """Test dialect detection from ADBC driver."""
    # Test dialect via the driver's public interface
    dialect = str(adbc_sync_driver.dialect)
    assert isinstance(dialect, str)
    assert len(dialect) > 0
    # Should be one of the supported dialects
    supported_dialects = ["postgres", "sqlite", "duckdb", "mysql", "bigquery"]
    assert dialect in supported_dialects


@pytest.mark.adbc
def test_adbc_data_dictionary_feature_flags(adbc_sync_driver: "AdbcDriver") -> None:
    """Test feature flags with real database via ADBC."""
    data_dict = adbc_sync_driver.data_dictionary
    dialect = str(adbc_sync_driver.dialect)

    # Test at least some basic features are reported
    features_to_test = [
        "supports_transactions",
        "supports_prepared_statements",
        "supports_cte",
        "supports_window_functions",
    ]

    for feature in features_to_test:
        result = data_dict.get_feature_flag(adbc_sync_driver, feature)
        assert isinstance(result, bool)

    # Test dialect-specific features
    if dialect == "postgres":
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_uuid") is True
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_arrays") is True
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_schemas") is True
    elif dialect == "sqlite":
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_uuid") is False
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_arrays") is False
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_schemas") is False
    elif dialect == "bigquery":
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_arrays") is True
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_structs") is True
        assert data_dict.get_feature_flag(adbc_sync_driver, "supports_transactions") is False


@pytest.mark.adbc
def test_adbc_data_dictionary_optimal_types(adbc_sync_driver: "AdbcDriver") -> None:
    """Test optimal type selection with real database via ADBC."""
    data_dict = adbc_sync_driver.data_dictionary
    dialect = str(adbc_sync_driver.dialect)

    # Test basic types exist for all dialects
    basic_types = ["text", "boolean", "timestamp"]
    for type_category in basic_types:
        result = data_dict.get_optimal_type(adbc_sync_driver, type_category)
        assert isinstance(result, str)
        assert len(result) > 0

    # Test dialect-specific type mappings
    if dialect == "postgres":
        assert data_dict.get_optimal_type(adbc_sync_driver, "uuid") == "UUID"
        assert data_dict.get_optimal_type(adbc_sync_driver, "blob") == "BYTEA"
    elif dialect == "sqlite":
        assert data_dict.get_optimal_type(adbc_sync_driver, "uuid") == "TEXT"
        assert data_dict.get_optimal_type(adbc_sync_driver, "boolean") == "INTEGER"
    elif dialect == "bigquery":
        assert data_dict.get_optimal_type(adbc_sync_driver, "uuid") == "STRING"
        assert data_dict.get_optimal_type(adbc_sync_driver, "boolean") == "BOOL"
        assert data_dict.get_optimal_type(adbc_sync_driver, "blob") == "BYTES"

    # Test unknown type defaults
    unknown_type = data_dict.get_optimal_type(adbc_sync_driver, "unknown_type")
    assert unknown_type == "TEXT"


@pytest.mark.adbc
def test_adbc_data_dictionary_available_features(adbc_sync_driver: "AdbcDriver") -> None:
    """Test listing available features for ADBC."""
    data_dict = adbc_sync_driver.data_dictionary

    features = data_dict.list_available_features()
    assert isinstance(features, list)
    assert len(features) > 0

    expected_base_features = [
        "supports_transactions",
        "supports_prepared_statements",
        "supports_window_functions",
        "supports_cte",
    ]

    for feature in expected_base_features:
        assert feature in features


@pytest.mark.adbc
def test_adbc_data_dictionary_consistency(adbc_sync_driver: "AdbcDriver") -> None:
    """Test that data dictionary methods are consistent with each other."""
    data_dict = adbc_sync_driver.data_dictionary

    # Get available features and test that they all return boolean values
    features = data_dict.list_available_features()

    for feature in features:
        result = data_dict.get_feature_flag(adbc_sync_driver, feature)
        assert isinstance(result, bool), f"Feature {feature} should return boolean, got {type(result)}"

    # Test that version detection is consistent
    version1 = data_dict.get_version(adbc_sync_driver)
    version2 = data_dict.get_version(adbc_sync_driver)

    if version1 is not None and version2 is not None:
        assert version1.major == version2.major
        assert version1.minor == version2.minor
        assert version1.patch == version2.patch
