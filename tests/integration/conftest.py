"""Pytest configuration and fixtures for integration tests."""

import contextlib
import os
from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.postgres import _provide_postgres_service
from pytest_databases.docker.rustfs import rustfs_default_bucket_name as rustfs_default_bucket_name
from pytest_databases.docker.rustfs import rustfs_secure as rustfs_secure
from pytest_databases.docker.rustfs import rustfs_service as rustfs_service
from pytest_databases.docker.rustfs import xdist_rustfs_isolation_level as xdist_rustfs_isolation_level

from tests.fixtures.rustfs import ensure_rustfs_bucket

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases._service import DockerService
    from pytest_databases.docker.postgres import PostgresService
    from pytest_databases.docker.rustfs import RustfsService
    from pytest_databases.types import XdistIsolationLevel


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


# HACK: Remove these overrides once pytest-databases ships non-default RustFS credentials.  # noqa: FIX004
# Tracking upstream: https://github.com/litestar-org/pytest-databases/issues/132
@pytest.fixture(scope="session")
def rustfs_access_key() -> str:
    """Return non-default RustFS credentials for native pytest-databases fixture startup."""
    return os.getenv("RUSTFS_ACCESS_KEY", "sqlspec-rustfs")


@pytest.fixture(scope="session")
def rustfs_secret_key() -> str:
    """Return non-default RustFS credentials for native pytest-databases fixture startup."""
    return os.getenv("RUSTFS_SECRET_KEY", "sqlspec-rustfs-secret")


@pytest.fixture(scope="session")
def rustfs_bucket_name(rustfs_service: "RustfsService", rustfs_default_bucket_name: str) -> str:
    """Return the verified RustFS bucket used by S3-compatible integration tests."""
    return ensure_rustfs_bucket(rustfs_service, rustfs_default_bucket_name)


# HACK: Remove this override once pytest-databases exposes a host-port hook.  # noqa: FIX004
# Tracking upstream: https://github.com/litestar-org/pytest-databases/issues/131
@pytest.fixture(scope="session")
def pgvector_service(
    docker_service: "DockerService",
    pgvector_image: str,
    xdist_postgres_isolation_level: "XdistIsolationLevel",
    postgres_host: str,
    postgres_user: str,
    postgres_password: str,
) -> "Generator[PostgresService, None, None]":
    """Override upstream pgvector_service to allow pinning the host port via SQLSPEC_PGVECTOR_PORT."""
    fixed = os.getenv("SQLSPEC_PGVECTOR_PORT")
    if fixed:
        import docker as docker_module

        client = docker_module.from_env()
        name = "pytest_databases_pgvector"
        for stale in client.containers.list(all=True, filters={"name": name}):
            with contextlib.suppress(Exception):
                stale.remove(force=True)
        client.containers.run(
            pgvector_image,
            detach=True,
            remove=True,
            ports={"5432/tcp": int(fixed)},
            labels=["pytest_databases"],
            name=name,
            environment={"POSTGRES_PASSWORD": postgres_password},
        )

    with _provide_postgres_service(
        docker_service,
        image=pgvector_image,
        name="pgvector",
        xdist_postgres_isolate=xdist_postgres_isolation_level,
        host=postgres_host,
        user=postgres_user,
        password=postgres_password,
    ) as service:
        yield service
