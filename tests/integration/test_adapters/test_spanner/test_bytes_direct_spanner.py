"""Debug test for bytes handling in Spanner."""

import base64
from typing import TYPE_CHECKING

import pytest
from google.cloud.spanner_v1 import param_types

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

pytestmark = [pytest.mark.spanner, pytest.mark.integration]


def test_bytes_direct_write_read(spanner_database: "Database") -> None:
    """Test bytes roundtrip directly with Spanner using base64 encoding.

    The Spanner Python client requires base64-encoded bytes for BYTES parameters.
    This test verifies the correct pattern:
    1. Write: base64.b64encode(raw_bytes)
    2. Read: base64.b64decode(stored_bytes)
    """
    database = spanner_database

    # Create a simple test table
    try:
        database.update_ddl([  # type: ignore[no-untyped-call]
            """
            CREATE TABLE test_bytes_direct (
                id STRING(128) NOT NULL,
                data BYTES(MAX)
            ) PRIMARY KEY (id)
        """
        ]).result(60)
    except Exception:
        pass

    # Clean
    def clean_txn(txn):
        txn.execute_update("DELETE FROM test_bytes_direct WHERE TRUE")

    database.run_in_transaction(clean_txn)  # type: ignore[no-untyped-call]

    # Write using base64-encoded bytes - the correct pattern for Spanner
    test_bytes = b"payload"
    encoded = base64.b64encode(test_bytes)
    write_params = {"id": "test1", "data": encoded}
    write_types = {"id": param_types.STRING, "data": param_types.BYTES}

    def insert_txn(txn):
        txn.execute_update(
            "INSERT INTO test_bytes_direct (id, data) VALUES (@id, @data)", params=write_params, param_types=write_types
        )

    database.run_in_transaction(insert_txn)  # type: ignore[no-untyped-call]

    # Read directly - returns base64-encoded bytes
    with database.snapshot() as snap:  # type: ignore[no-untyped-call]
        result = list(
            snap.execute_sql(
                "SELECT data FROM test_bytes_direct WHERE id = @id",
                params={"id": "test1"},
                param_types={"id": param_types.STRING},
            )
        )

    assert len(result) == 1
    stored_bytes = result[0][0]

    # Decode the base64-encoded bytes back to raw bytes
    if isinstance(stored_bytes, (bytes, str)):
        retrieved = base64.b64decode(stored_bytes)
    else:
        retrieved = stored_bytes

    assert retrieved == test_bytes, f"Expected {test_bytes!r}, got {retrieved!r}"
