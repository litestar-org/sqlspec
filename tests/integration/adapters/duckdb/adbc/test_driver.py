"""DuckDB-backed ADBC driver residuals."""

import threading

import pytest

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.exceptions import OperationCancelledError, QueryTimeoutError
from tests.integration.adapters._shared.adbc_backends import duckdb_session, test_duckdb_specific_features
from tests.integration.adapters._shared.adbc_connection import test_duckdb_connection

__all__ = ("duckdb_session", "test_duckdb_connection", "test_duckdb_specific_features")


@pytest.mark.xdist_group("duckdb")
@pytest.mark.adbc
def test_duckdb_uuid_schema_bypasses_opaque_uuid_decoding(duckdb_session: AdbcDriver) -> None:
    """DuckDB UUID schemas stay on the ordinary whole-table row path."""
    statement = "SELECT UUID '550e8400-e29b-41d4-a716-446655440000' AS value"

    arrow_table = duckdb_session.select_to_arrow(statement).data
    rows = duckdb_session.select(statement)
    arrow_type = arrow_table.schema.field("value").type

    assert not (
        getattr(arrow_type, "extension_name", None) == "arrow.opaque"
        and getattr(arrow_type, "type_name", None) == "uuid"
    )
    assert rows == arrow_table.to_pylist()


@pytest.mark.xdist_group("duckdb")
@pytest.mark.adbc
def test_duckdb_adbc_explicit_cancel_maps_to_operation_cancelled(duckdb_session: AdbcDriver) -> None:
    cancel = threading.Timer(0.05, duckdb_session.connection.adbc_cancel)
    cancel.start()
    try:
        with pytest.raises(OperationCancelledError) as exc_info:
            duckdb_session.select("SELECT sum(i * j) FROM range(1000000) t(i), range(1000000) u(j)")
    finally:
        cancel.cancel()

    assert not isinstance(exc_info.value, QueryTimeoutError)
