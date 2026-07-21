"""Pytest configuration for psycopg integration tests."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.utils.portal import PortalManager

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(autouse=True)
def _cleanup_portal() -> "Generator[None, None, None]":
    """Clean up the portal manager after each test.

    This prevents state leakage between tests when the portal is used
    for async-to-sync bridging in event channel operations.
    """
    yield
    PortalManager().stop()
