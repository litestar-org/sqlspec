from __future__ import annotations

import asyncio
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for each test case."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
