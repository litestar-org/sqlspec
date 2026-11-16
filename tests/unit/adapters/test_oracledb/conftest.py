"""Shared fixtures for Oracle adapter unit tests."""

from collections.abc import Iterator

import pytest

from sqlspec.adapters.oracledb import core as oracledb_core


@pytest.fixture(autouse=True)
def _clear_oracle_session_state() -> Iterator[None]:
    """Clear Oracle per-connection session state after each test.

    The state maps are keyed by ``id(connection)`` and emptied by a weakref finalizer.
    Test doubles such as ``SimpleNamespace`` cannot be weakly referenced, so this fixture
    empties the maps instead.
    """
    yield
    oracledb_core._INITIAL_SCHEMA_BY_CONN_ID.clear()
    oracledb_core._INITIAL_PRIORITY_BY_CONN_ID.clear()
    oracledb_core._INITIAL_ALTER_SESSION_BY_CONN_ID.clear()
    oracledb_core._ACTIVE_ALTER_FIELDS_BY_CONN_ID.clear()
    oracledb_core._TAGGED_ALTER_CONN_IDS.clear()
    oracledb_core._REGISTERED_SESSION_CLEANUP_IDS.clear()
