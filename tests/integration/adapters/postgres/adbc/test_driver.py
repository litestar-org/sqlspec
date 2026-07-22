"""PostgreSQL-backed ADBC driver residuals."""

from uuid import uuid4

import pytest

from sqlspec.adapters.adbc import AdbcDriver
from tests.integration.adapters._shared.adbc_backends import postgresql_session, test_postgresql_specific_features
from tests.integration.adapters._shared.adbc_connection import (
    test_connection,
    test_connection_info_retrieval,
    test_connection_transaction_handling,
)
from tests.integration.adapters._shared.adbc_driver import test_adbc_postgresql_statement_stack_continue_on_error
from tests.integration.adapters._shared.adbc_edge_cases import (
    test_connection_resilience,
    test_execute_script_edge_cases,
)

__all__ = (
    "postgresql_session",
    "test_adbc_postgresql_statement_stack_continue_on_error",
    "test_connection",
    "test_connection_info_retrieval",
    "test_connection_resilience",
    "test_connection_transaction_handling",
    "test_execute_script_edge_cases",
    "test_postgresql_specific_features",
)


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_uuid_identity_and_same_sql_cache_reuse(postgresql_session: AdbcDriver) -> None:
    """Distinct UUID objects bind through one cached INSERT statement without losing identity."""
    table_name = "adbc_uuid_identity"
    values = [uuid4(), uuid4()]
    insert_sql = f"INSERT INTO {table_name} (position, value) VALUES (?, ?)"

    try:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
        postgresql_session.execute_script(f"CREATE TABLE {table_name} (position INTEGER PRIMARY KEY, value UUID)")
        for position, value in enumerate(values, 1):
            postgresql_session.execute(insert_sql, (position, value))

        rows = postgresql_session.execute(
            f"SELECT position, value::text AS value FROM {table_name} ORDER BY position"
        ).get_data()
        assert [row["value"] for row in rows] == [str(value) for value in values]
    finally:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@pytest.mark.parametrize("uuid_first", [False, True], ids=["ordinary-first", "uuid-first"])
def test_postgresql_uuid_binding_does_not_leak_through_same_sql_cache(
    postgresql_session: AdbcDriver, uuid_first: bool
) -> None:
    """Value-aware UUID SQL never replaces the stable cached statement in either value order."""
    statement = "SELECT pg_typeof($1)::text AS bound_type"
    uuid_value = uuid4()
    values = (uuid_value, "ordinary") if uuid_first else ("ordinary", uuid_value)

    bound_types = [postgresql_session.select_value(statement, value) for value in values]
    uuid_type, ordinary_type = bound_types if uuid_first else reversed(bound_types)

    assert uuid_type == "uuid"
    assert ordinary_type != uuid_type


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_uuid_batch_inference(postgresql_session: AdbcDriver) -> None:
    """Batch binding infers UUID columns across rows and accepts strings and nulls."""
    table_name = "adbc_uuid_batch"
    first_value = uuid4()
    last_value = uuid4()

    try:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
        postgresql_session.execute_script(f"CREATE TABLE {table_name} (position INTEGER PRIMARY KEY, value UUID)")
        postgresql_session.execute_many(
            f"INSERT INTO {table_name} (position, value) VALUES (?, ?)",
            [(1, str(first_value).upper()), (2, None), (3, last_value)],
        )

        rows = postgresql_session.execute(
            f"SELECT position, value::text AS value FROM {table_name} ORDER BY position"
        ).get_data()
        assert [row["value"] for row in rows] == [str(first_value), None, str(last_value)]
    finally:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
