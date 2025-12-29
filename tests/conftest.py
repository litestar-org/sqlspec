from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from minio import Minio

if TYPE_CHECKING:
    from pytest_databases.docker.minio import MinioService


def is_compiled() -> bool:
    """Detect if sqlspec driver modules are mypyc-compiled.

    Returns:
        True when the driver modules have been compiled with mypyc.
    """
    try:
        from sqlspec.driver import _sync

        return hasattr(_sync, "__file__") and (_sync.__file__ or "").endswith(".so")
    except ImportError:
        return False


# Marker for tests that segfault when running with mypyc-compiled code.
# These tests create interpreted subclasses of compiled base classes,
# which causes garbage collection conflicts during pytest error reporting.
requires_interpreted = pytest.mark.skipif(
    is_compiled(), reason="Test uses interpreted subclass of compiled base (mypyc GC conflict)"
)


pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.bigquery",
    "pytest_databases.docker.spanner",
    "pytest_databases.docker.minio",
]

pytestmark = pytest.mark.anyio
here = Path(__file__).parent


@pytest.fixture(scope="session")
def minio_client(minio_service: MinioService, minio_default_bucket_name: str) -> Generator[Minio, None, None]:
    """Override pytest-databases minio_client to use new minio API with keyword arguments."""
    client = Minio(
        endpoint=minio_service.endpoint,
        access_key=minio_service.access_key,
        secret_key=minio_service.secret_key,
        secure=minio_service.secure,
    )
    try:
        if not client.bucket_exists(bucket_name=minio_default_bucket_name):
            client.make_bucket(bucket_name=minio_default_bucket_name)
    except Exception as e:
        msg = f"Failed to create bucket {minio_default_bucket_name}"
        raise RuntimeError(msg) from e
    yield client


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom pytest command line options."""
    parser.addoption(
        "--run-bigquery-tests",
        action="store_true",
        default=False,
        help="Run BigQuery ADBC tests (requires valid GCP credentials)",
    )


@pytest.fixture
def anyio_backend() -> str:
    """Configure AnyIO to use asyncio backend only.

    Disables trio backend to prevent duplicate test runs and compatibility issues
    with pytest-xdist parallel execution.
    """
    return "asyncio"


@pytest.fixture(autouse=True)
def disable_sync_to_thread_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LITESTAR_WARN_IMPLICIT_SYNC_TO_THREAD", "0")
