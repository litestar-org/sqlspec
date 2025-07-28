"""Fixtures for statement unit tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_psycopg_connection() -> MagicMock:
    """Mock psycopg connection for testing."""
    mock_conn = MagicMock()
    # Add common psycopg connection attributes
    mock_conn.execute = MagicMock()
    mock_conn.executemany = MagicMock()
    mock_conn.cursor = MagicMock()
    return mock_conn


@pytest.fixture
def mock_duckdb_connection() -> MagicMock:
    """Mock DuckDB connection for testing."""
    mock_conn = MagicMock()
    # Add common DuckDB connection attributes
    mock_conn.execute = MagicMock()
    mock_conn.executemany = MagicMock()
    mock_conn.cursor = MagicMock()
    mock_conn.fetchall = MagicMock(return_value=[])
    mock_conn.fetchone = MagicMock(return_value=None)
    mock_conn.rowcount = 0
    return mock_conn
