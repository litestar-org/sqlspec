"""Pytest configuration and fixtures for integration tests."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from typing import Any

import pytest


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


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
    """Complex data with various types for comprehensive testing."""
    return [
        {"name": "test1", "value": 100, "data": {"key": "value1"}, "tags": ["tag1", "tag2"]},
        {"name": "test2", "value": 200, "data": {"key": "value2"}, "tags": ["tag2", "tag3"]},
        {"name": "test3", "value": 300, "data": None, "tags": None},
    ]
