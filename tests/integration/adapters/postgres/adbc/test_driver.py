"""PostgreSQL-backed ADBC driver residuals."""

from uuid import uuid4

import pytest

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.exceptions import SQLSpecError
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


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@pytest.mark.parametrize(
    "predicate",
    [
        pytest.param("id = ANY(CAST(? AS UUID[]))", id="caller-written-cast"),
        pytest.param("id = ANY(?)", id="cast-free"),
    ],
)
def test_postgresql_uuid_array_parameter_matches_rows(postgresql_session: AdbcDriver, predicate: str) -> None:
    """A UUID list binds as one uuid[] parameter, with or without a caller-written cast."""
    table_name = "adbc_uuid_array"
    wanted = [uuid4(), uuid4()]
    unwanted = uuid4()

    try:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
        postgresql_session.execute_script(f"CREATE TABLE {table_name} (id UUID PRIMARY KEY)")
        for value in (*wanted, unwanted):
            postgresql_session.execute(f"INSERT INTO {table_name} (id) VALUES (?)", (value,))

        rows = postgresql_session.execute(
            f"SELECT id::text AS id FROM {table_name} WHERE {predicate} ORDER BY id::text", (wanted,)
        ).get_data()

        assert sorted(row["id"] for row in rows) == sorted(str(value) for value in wanted)
    finally:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_uuid_array_canonicalizes_mixed_string_forms(postgresql_session: AdbcDriver) -> None:
    """Every parseable string form canonicalizes on the way to a uuid[] parameter."""
    first_value = uuid4()
    second_value = uuid4()
    parameters = [first_value, str(second_value).upper()]

    rows = postgresql_session.execute("SELECT unnest(CAST(? AS UUID[]))::text AS value", (parameters,)).get_data()

    assert [row["value"] for row in rows] == [str(first_value), str(second_value)]


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_uuid_array_rejects_null_elements(postgresql_session: AdbcDriver) -> None:
    """The ADBC driver encodes null array elements as empty strings, so they are rejected up front."""
    with pytest.raises(SQLSpecError, match="null value at element 2"):
        postgresql_session.execute("SELECT CAST(? AS UUID[]) AS value", ([uuid4(), None],))


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_uuid_array_batch_binding(postgresql_session: AdbcDriver) -> None:
    """Execute-many detects a UUID array column across rows."""
    table_name = "adbc_uuid_array_batch"
    first_row = [uuid4(), uuid4()]
    second_row = [uuid4()]

    try:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
        postgresql_session.execute_script(
            f"CREATE TABLE {table_name} (position INTEGER PRIMARY KEY, identifiers UUID[])"
        )
        postgresql_session.execute_many(
            f"INSERT INTO {table_name} (position, identifiers) VALUES (?, ?)", [(1, first_row), (2, second_row)]
        )

        rows = postgresql_session.execute(
            f"SELECT position, array_to_string(identifiers, ',') AS identifiers FROM {table_name} ORDER BY position"
        ).get_data()

        assert [row["identifiers"] for row in rows] == [
            ",".join(str(value) for value in first_row),
            ",".join(str(value) for value in second_row),
        ]
    finally:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@pytest.mark.parametrize("array_first", [False, True], ids=["ordinary-first", "array-first"])
def test_postgresql_uuid_array_does_not_leak_through_same_sql_cache(
    postgresql_session: AdbcDriver, array_first: bool
) -> None:
    """Array-valued and ordinary-valued executions of one statement never reuse each other's SQL."""
    statement = "SELECT pg_typeof($1)::text AS bound_type"
    uuid_array = [uuid4(), uuid4()]
    values = (uuid_array, ["plain", "text"]) if array_first else (["plain", "text"], uuid_array)

    bound_types = [postgresql_session.select_value(statement, (value,)) for value in values]
    array_type, ordinary_type = bound_types if array_first else reversed(bound_types)

    assert array_type == "uuid[]"
    assert ordinary_type != array_type


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_lone_uuid_parameter_survives_statement_cache_hits(postgresql_session: AdbcDriver) -> None:
    """A UUID is the only parameter, so no sibling value forces the statement off the cache fast path."""
    table_name = "adbc_uuid_lone_parameter"
    values = [uuid4(), uuid4(), uuid4()]

    try:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
        postgresql_session.execute_script(f"CREATE TABLE {table_name} (value UUID PRIMARY KEY)")
        for value in values:
            postgresql_session.execute(f"INSERT INTO {table_name} (value) VALUES (?)", (value,))

        rows = postgresql_session.execute(
            f"SELECT value::text AS value FROM {table_name} ORDER BY value::text"
        ).get_data()
        assert sorted(row["value"] for row in rows) == sorted(str(value) for value in values)
    finally:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")
