from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.anyio
here = Path(__file__).parent
root_path = here.parent
pytest_plugins = [
    "pytest_databases.docker.compose",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.mssql",
]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
