"""Integration tests for AsyncPG PostgreSQL data dictionary."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.driver import VersionInfo

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_version_detection(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test PostgreSQL version detection with real database via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    version = await data_dict.get_version(asyncpg_async_driver)
    assert version is not None
    assert isinstance(version, VersionInfo)
    assert version.major >= 9
    assert version.minor >= 0
    assert version.patch >= 0


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_feature_flags(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test PostgreSQL feature flags with real database via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    # Test always supported features in modern PostgreSQL
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_transactions") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_prepared_statements") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_uuid") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_arrays") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_schemas") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_cte") is True
    assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_window_functions") is True

    # Test version-dependent features (these depend on actual PostgreSQL version)
    version = await data_dict.get_version(asyncpg_async_driver)
    if version and version >= VersionInfo(9, 2, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_json") is True

    if version and version >= VersionInfo(9, 4, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_jsonb") is True

    if version and version >= VersionInfo(8, 2, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_returning") is True

    if version and version >= VersionInfo(9, 5, 0):
        assert await data_dict.get_feature_flag(asyncpg_async_driver, "supports_upsert") is True


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_optimal_types(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test PostgreSQL optimal type selection with real database via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    # Test basic types
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "uuid") == "UUID"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "boolean") == "BOOLEAN"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "timestamp") == "TIMESTAMP WITH TIME ZONE"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "text") == "TEXT"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "blob") == "BYTEA"
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "array") == "ARRAY"

    # Test JSON type based on version
    version = await data_dict.get_version(asyncpg_async_driver)
    if version and version >= VersionInfo(9, 4, 0):
        assert await data_dict.get_optimal_type(asyncpg_async_driver, "json") == "JSONB"
    elif version and version >= VersionInfo(9, 2, 0):
        assert await data_dict.get_optimal_type(asyncpg_async_driver, "json") == "JSON"
    else:
        assert await data_dict.get_optimal_type(asyncpg_async_driver, "json") == "TEXT"

    # Test unknown type defaults to TEXT
    assert await data_dict.get_optimal_type(asyncpg_async_driver, "unknown_type") == "TEXT"


@pytest.mark.asyncpg
async def test_asyncpg_data_dictionary_available_features(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Test listing available features for PostgreSQL via asyncpg."""
    data_dict = asyncpg_async_driver.data_dictionary

    features = data_dict.list_available_features()
    assert isinstance(features, list)
    assert len(features) > 0

    expected_features = [
        "supports_json",
        "supports_jsonb",
        "supports_uuid",
        "supports_arrays",
        "supports_returning",
        "supports_upsert",
        "supports_window_functions",
        "supports_cte",
        "supports_transactions",
        "supports_prepared_statements",
        "supports_schemas",
    ]

    for feature in expected_features:
        assert feature in features
