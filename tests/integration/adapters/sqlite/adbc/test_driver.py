"""SQLite-backed ADBC driver residuals."""

import pytest

from sqlspec.adapters.adbc import AdbcDriver
from tests.integration.adapters._shared.adbc_backends import sqlite_session, test_sqlite_adbc_specific_features
from tests.integration.adapters._shared.adbc_connection import test_sqlite_connection
from tests.integration.adapters._shared.adbc_driver import (
    test_adbc_for_share_generates_sql,
    test_adbc_for_update_generates_sql,
    test_adbc_for_update_skip_locked_generates_sql,
)

__all__ = (
    "sqlite_session",
    "test_adbc_for_share_generates_sql",
    "test_adbc_for_update_generates_sql",
    "test_adbc_for_update_skip_locked_generates_sql",
    "test_sqlite_adbc_specific_features",
    "test_sqlite_connection",
)


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
def test_sqlite_arrow_schema_bypasses_opaque_uuid_decoding(sqlite_session: AdbcDriver) -> None:
    """SQLite Arrow schemas stay on the ordinary whole-table row path."""
    statement = "SELECT 1 AS value"

    arrow_table = sqlite_session.select_to_arrow(statement).data
    rows = sqlite_session.select(statement)
    arrow_type = arrow_table.schema.field("value").type

    assert getattr(arrow_type, "extension_name", None) != "arrow.opaque"
    assert rows == arrow_table.to_pylist()
