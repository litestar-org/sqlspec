from __future__ import annotations

from pathlib import Path

import pytest

pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.bigquery",
    "pytest_databases.docker.spanner",
]

pytestmark = pytest.mark.anyio
here = Path(__file__).parent


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def disable_sync_to_thread_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LITESTAR_WARN_IMPLICIT_SYNC_TO_THREAD", "0")
