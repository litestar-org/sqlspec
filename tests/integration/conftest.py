"""Pytest configuration and fixtures for integration tests."""

from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.rustfs import rustfs_access_key as rustfs_access_key
from pytest_databases.docker.rustfs import rustfs_default_bucket_name as rustfs_default_bucket_name
from pytest_databases.docker.rustfs import rustfs_secret_key as rustfs_secret_key
from pytest_databases.docker.rustfs import rustfs_secure as rustfs_secure
from pytest_databases.docker.rustfs import rustfs_service as rustfs_service
from pytest_databases.docker.rustfs import xdist_rustfs_isolation_level as xdist_rustfs_isolation_level

from tests.fixtures.rustfs import ensure_rustfs_bucket

if TYPE_CHECKING:
    from pytest_databases.docker.rustfs import RustfsService


@pytest.fixture
def sample_data() -> list[tuple[str, int]]:
    """Standard sample data for testing across adapters."""
    return [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]


@pytest.fixture
def bulk_data() -> list[tuple[str, int]]:
    """Bulk data for performance testing."""
    return [(f"user_{i}", i * 10) for i in range(100)]


@pytest.fixture
def complex_data() -> list[dict[str, Any]]:
    """Complex data with various types for testing."""
    return [
        {"name": "test1", "value": 100, "data": {"key": "value1"}, "tags": ["tag1", "tag2"]},
        {"name": "test2", "value": 200, "data": {"key": "value2"}, "tags": ["tag2", "tag3"]},
        {"name": "test3", "value": 300, "data": None, "tags": None},
    ]


@pytest.fixture(scope="session")
def rustfs_bucket_name(rustfs_service: "RustfsService", rustfs_default_bucket_name: str) -> str:
    """Return the verified RustFS bucket used by S3-compatible integration tests."""
    return ensure_rustfs_bucket(rustfs_service, rustfs_default_bucket_name)
