"""Pytest configuration and fixtures for integration tests."""

import contextlib
from typing import TYPE_CHECKING, Any
from urllib.request import Request, urlopen

import pytest
from docker.errors import ImageNotFound  # type: ignore[import-untyped]
from pytest_databases.docker.rustfs import RustfsService
from pytest_databases.docker.rustfs import rustfs_access_key as rustfs_access_key
from pytest_databases.docker.rustfs import rustfs_default_bucket_name as rustfs_default_bucket_name
from pytest_databases.docker.rustfs import rustfs_secret_key as rustfs_secret_key
from pytest_databases.docker.rustfs import rustfs_secure as rustfs_secure
from pytest_databases.docker.rustfs import xdist_rustfs_isolation_level as xdist_rustfs_isolation_level
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

from tests.fixtures.rustfs import ensure_rustfs_bucket

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases._service import DockerService
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


@pytest.fixture(scope="session")
def rustfs_service(
    docker_service: "DockerService",
    rustfs_access_key: str,
    rustfs_secret_key: str,
    rustfs_secure: bool,
    rustfs_default_bucket_name: str,
    xdist_rustfs_isolation_level: "XdistIsolationLevel",
) -> "Generator[RustfsService, None, None]":
    """Start RustFS with the current container startup contract.

    pytest-databases 0.18.0 starts the latest RustFS image without a data directory.
    Current RustFS containers require a storage path command, so keep this override
    until the upstream fixture carries that native launch behavior.
    """

    def check(_service: ServiceContainer) -> bool:
        scheme = "https" if rustfs_secure else "http"
        url = f"{scheme}://{_service.host}:{_service.port}/health"
        try:
            with urlopen(url=Request(url, method="GET"), timeout=10) as response:
                return bool(response.status == 200)
        except OSError:
            return False

    worker_num = get_xdist_worker_num()
    name = "rustfs"
    transient = False

    if worker_num is not None and xdist_rustfs_isolation_level == "server":
        name = f"{name}_{worker_num}"
        transient = True

    env = {
        "RUSTFS_ACCESS_KEY": rustfs_access_key,
        "RUSTFS_SECRET_KEY": rustfs_secret_key,
        "RUSTFS_ROOT_USER": rustfs_access_key,
        "RUSTFS_ROOT_PASSWORD": rustfs_secret_key,
        "RUSTFS_ADDRESS": ":9000",
        "RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS": "true",
    }

    with docker_service.run(
        image="rustfs/rustfs:latest",
        name=name,
        container_port=9000,
        command="/data",
        timeout=60,
        pause=0.5,
        env=env,
        check=check,
        transient=transient,
    ) as service:
        scheme = "https" if rustfs_secure else "http"
        endpoint_url = f"{scheme}://{service.host}:{service.port}"
        client = docker_service._client

        try:
            client.images.get("rustfs/rc:latest")
        except ImageNotFound:
            client.images.pull("rustfs/rc:latest")

        command = [
            "sh",
            "-c",
            (
                f"rc alias set local {endpoint_url} {rustfs_access_key} {rustfs_secret_key} && "
                f"rc mb local/{rustfs_default_bucket_name}"
            ),
        ]

        with contextlib.suppress(Exception):
            client.containers.run(image="rustfs/rc:latest", command=command, remove=True, network_mode="host")

        yield RustfsService(
            host=service.host,
            port=service.port,
            container=service.container,
            endpoint=f"{service.host}:{service.port}",
            access_key=rustfs_access_key,
            secret_key=rustfs_secret_key,
            secure=rustfs_secure,
        )


@pytest.fixture(scope="session")
def rustfs_bucket_name(rustfs_service: "RustfsService", rustfs_default_bucket_name: str) -> str:
    """Return the verified RustFS bucket used by S3-compatible integration tests."""
    return ensure_rustfs_bucket(rustfs_service, rustfs_default_bucket_name)
