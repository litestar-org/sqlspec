import warnings

warnings.filterwarnings(
    "ignore", message="You are using a Python version.*which Google will stop supporting", category=FutureWarning
)

from collections.abc import Generator  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import TYPE_CHECKING  # noqa: E402

import pytest  # noqa: E402
from minio import Minio  # noqa: E402

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


# Marker for tests incompatible with mypyc-compiled base classes.
# These tests create interpreted subclasses of compiled bases, which
# can trigger GC conflicts during pytest error reporting.
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
    "pytest_databases.docker.cockroachdb",
]

pytestmark = pytest.mark.anyio
here = Path(__file__).parent


@pytest.fixture(scope="session")
def minio_client(minio_service: "MinioService", minio_default_bucket_name: str) -> Generator[Minio, None, None]:
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


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip ADBC-marked tests when running against compiled modules."""
    if not is_compiled():
        return

    skip_adbc = pytest.mark.skip(reason="Skip ADBC tests when running against mypyc-compiled modules.")
    skip_compiled = pytest.mark.skip(
        reason="Skip tests that rely on interpreted subclasses or mocks of compiled driver bases."
    )
    for item in items:
        item_path = str(getattr(item, "path", getattr(item, "fspath", "")))
        if item.get_closest_marker("adbc") is not None or "tests/integration/adapters/adbc" in item_path:
            item.add_marker(skip_adbc)
            continue
        if (
            "tests/unit/adapters/" in item_path
            or "tests/unit/driver/" in item_path
            or item_path.endswith("tests/unit/config/test_storage_capabilities.py")
            or "tests/unit/observability/" in item_path
        ):
            item.add_marker(skip_compiled)
            continue
        if {"mock_sync_driver", "mock_async_driver"} & set(getattr(item, "fixturenames", ())):
            item.add_marker(skip_compiled)


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
