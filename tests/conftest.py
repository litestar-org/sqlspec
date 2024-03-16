from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.anyio
here = Path(__file__).parent
root_path = here.parent
pytest_plugins = ["tests.docker_service_fixtures"]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
