"""Pytest configuration and fixtures for integration tests."""

from typing import TYPE_CHECKING

import pytest

from tests.fixtures.rustfs import ensure_rustfs_bucket

if TYPE_CHECKING:
    from pytest_databases.docker.rustfs import RustfsService


@pytest.fixture(scope="session")
def rustfs_bucket_name(rustfs_service: "RustfsService", rustfs_default_bucket_name: str) -> str:
    """Return the verified RustFS bucket used by S3-compatible integration tests."""
    return ensure_rustfs_bucket(rustfs_service, rustfs_default_bucket_name)
