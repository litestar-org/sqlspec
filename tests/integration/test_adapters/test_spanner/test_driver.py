"""Spanner Sync and Async Driver Integration Tests using pytest-databases."""

import os
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import pytest
from google.cloud.spanner_v1 import Client, Transaction
from pytest_databases.docker.spanner import SpannerService  # type: ignore[import-untyped]

# Import sqlspec types
from sqlspec.adapters.spanner import (  # Assuming these imports exist and are correct
    SyncSpannerConfig,  # type: ignore[import-error]
)
from sqlspec.exceptions import NotFoundError


@pytest.fixture(scope="session")
def spanner_emulator_project(spanner_service: SpannerService) -> str:
    """Return the project ID used by the Spanner emulator."""
    return spanner_service.project


@pytest.fixture(scope="session")
def spanner_emulator_instance(spanner_service: SpannerService) -> str:
    """Return the instance ID used by the Spanner emulator."""
    return spanner_service.instance  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def spanner_emulator_database(spanner_service: SpannerService) -> str:
    """Return the database ID used by the Spanner emulator."""
    return spanner_service.database  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def spanner_emulator_host(spanner_service: SpannerService) -> str:
    """Return the host used by the Spanner emulator service."""
    # pytest-databases service might expose host/port if needed for direct client connection
    # For config, we typically just need project/instance/database if using emulator host env var
    # If direct connection is needed, service.host/service.port would be used.
    # We assume the google-cloud-spanner client uses SPANNER_EMULATOR_HOST env var set by pytest-databases.
    return os.environ.get("SPANNER_EMULATOR_HOST", "localhost:9010")  # Default emulator host


@dataclass
class SimpleModel:
    id: int
    name: str
    value: float


@pytest.fixture(scope="session")
def spanner_sync_config(
    spanner_emulator_project: str,
    spanner_emulator_instance: str,
    spanner_emulator_database: str,
) -> Any:  # -> SyncSpannerConfig:
    """Provides a SyncSpannerConfig configured for the pytest-databases emulator."""
    # The google-cloud-spanner client automatically uses SPANNER_EMULATOR_HOST
    # environment variable if set, which pytest-databases does.
    # So, we don't need to explicitly set credentials or host/port for the emulator.
    return SyncSpannerConfig(
        project=spanner_emulator_project,
        instance_id=spanner_emulator_instance,
        database_id=spanner_emulator_database,
        # No pool config needed for basic tests, defaults should work
    )


@pytest.fixture
def spanner_sync_session(
    spanner_sync_config: Any,
) -> Generator[Any, None, None]:  # -> Generator[SpannerSyncDriver, None, None]:
    """Provides a SpannerSyncDriver session within a transaction."""
    # Use the config's context manager to handle transaction lifecycle
    with spanner_sync_config.provide_session() as driver:
        assert isinstance(driver.connection, Transaction)  # Ensure it's a transaction context
        yield driver
    # Context manager handles cleanup/commit/rollback


# Basic table setup fixture (Sync)
@pytest.fixture(scope="module", autouse=True)
def _setup_sync_table(spanner_sync_config: Any) -> None:  # type: ignore[unused-function]
    """Ensure the test table exists before running sync tests in the module."""
    # Use a direct client for setup DDL as it might be simpler outside transaction scope
    # Note: DDL operations might need specific handling in Spanner (e.g., UpdateDatabaseDdl)
    # This setup assumes direct client interaction works with the emulator.
    client = Client(project=spanner_sync_config.project)
    instance = client.instance(spanner_sync_config.instance_id)
    database = instance.database(spanner_sync_config.database_id)

    # Simple check if table exists (may need adjustment based on emulator behavior)
    try:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                "SELECT table_name FROM information_schema.tables WHERE table_name='test_models_sync'"
            )
            if list(results):
                return
    except Exception:
        pass

    operation = database.update_ddl([
        """
        CREATE TABLE test_models_sync (
            id INT64 NOT NULL,
            name STRING(MAX),
            value FLOAT64
        ) PRIMARY KEY (id)
        """
    ])
    operation.result(timeout=120)  # Wait for DDL operation to complete


def test_sync_spanner_insert_select_one(spanner_sync_session: Any) -> None:  # SpannerSyncDriver
    """Test inserting and selecting a single row synchronously."""
    driver = spanner_sync_session
    # Arrange
    model_id = 1
    model_name = "sync_test"
    model_value = 123.45
    # Ensure clean state within transaction
    driver.insert_update_delete("DELETE FROM test_models_sync WHERE id = @id", {"id": model_id})

    # Act: Insert
    # Use insert_update_delete which returns -1 placeholder for Spanner
    _ = driver.insert_update_delete(
        "INSERT INTO test_models_sync (id, name, value) VALUES (@id, @name, @value)",
        parameters={"id": model_id, "name": model_name, "value": model_value},
    )

    # Act: Select
    result = driver.select_one_or_none(
        "SELECT id, name, value FROM test_models_sync WHERE id = @id",
        parameters={"id": model_id},
        schema_type=SimpleModel,
    )

    # Assert
    assert result is not None
    assert isinstance(result, SimpleModel)
    assert result.id == model_id
    assert result.name == model_name
    assert result.value == model_value


def test_sync_spanner_select_one_or_none_not_found(spanner_sync_session: Any) -> None:  # SpannerSyncDriver
    """Test selecting a non-existent row synchronously."""
    driver = spanner_sync_session
    # Arrange: Ensure ID does not exist
    non_existent_id = 999
    driver.insert_update_delete("DELETE FROM test_models_sync WHERE id = @id", {"id": non_existent_id})

    # Act
    result = driver.select_one_or_none(
        "SELECT * FROM test_models_sync WHERE id = @id", parameters={"id": non_existent_id}, schema_type=SimpleModel
    )

    # Assert
    assert result is None


def test_sync_spanner_select_one_raises_not_found(spanner_sync_session: Any) -> None:  # SpannerSyncDriver
    """Test select_one raises NotFoundError for a non-existent row synchronously."""
    driver = spanner_sync_session
    # Arrange: Ensure ID does not exist
    non_existent_id = 998
    driver.insert_update_delete("DELETE FROM test_models_sync WHERE id = @id", {"id": non_existent_id})

    # Act & Assert
    with pytest.raises(NotFoundError):
        driver.select_one(
            "SELECT * FROM test_models_sync WHERE id = @id", parameters={"id": non_existent_id}, schema_type=SimpleModel
        )
