from __future__ import annotations

from pathlib import Path

import pytest

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
    return "asyncio"


@pytest.fixture(autouse=True)
def disable_sync_to_thread_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LITESTAR_WARN_IMPLICIT_SYNC_TO_THREAD", "0")
