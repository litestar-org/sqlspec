"""PostgreSQL-backed ADBC driver residuals."""

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier
from typing import TYPE_CHECKING, Literal, cast
from uuid import UUID, uuid4

import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError, SQLSpecError
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

if TYPE_CHECKING:
    from pyarrow import RecordBatch

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

        rows = postgresql_session.execute(f"SELECT position, value FROM {table_name} ORDER BY position").get_data()
        assert [row["value"] for row in rows] == values
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

        rows = postgresql_session.execute(f"SELECT position, value FROM {table_name} ORDER BY position").get_data()
        assert [row["value"] for row in rows] == [first_value, None, last_value]
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
            f"SELECT id FROM {table_name} WHERE {predicate} ORDER BY id", (wanted,)
        ).get_data()

        assert [row["id"] for row in rows] == sorted(wanted)
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
            f"SELECT position, identifiers FROM {table_name} ORDER BY position"
        ).get_data()

        assert [row["identifiers"] for row in rows] == [first_row, second_row]
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

        rows = postgresql_session.execute(f"SELECT value FROM {table_name} ORDER BY value").get_data()
        assert [row["value"] for row in rows] == sorted(values)
    finally:
        postgresql_session.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_uuid_row_stream_and_arrow_boundary(postgresql_session: AdbcDriver) -> None:
    """Row APIs decode UUIDs while the Arrow API preserves the opaque extension."""
    value = UUID("550e8400-e29b-41d4-a716-446655440000")
    statement = f"SELECT CAST('{value}' AS UUID) AS value"

    buffered = postgresql_session.select_one(statement)
    with postgresql_session.select_stream(statement, native_only=True, chunk_size=1) as stream:
        streamed = list(stream)
    arrow_result = postgresql_session.select_to_arrow(statement)
    arrow_table = arrow_result.data
    arrow_type = arrow_table.schema.field("value").type

    assert buffered == {"value": value}
    assert streamed == [buffered]
    assert arrow_type.extension_name == "arrow.opaque"
    assert arrow_type.type_name == "uuid"
    assert arrow_table.to_pylist() == [{"value": value.bytes}]


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_reuses_pid_and_preserves_transactional_wrapper(
    adbc_postgres_connection_config: dict[str, str],
) -> None:
    """A reset lease keeps its backend and still supports real rollback and commit."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config:
        with config.provide_session() as session:
            first_pid = session.select_value("SELECT pg_backend_pid()")
            session.execute_script("CREATE TEMP TABLE adbc_pool_rollback (value INTEGER)")
            session.commit()
            session.execute("INSERT INTO adbc_pool_rollback VALUES (?)", 7)
            session.rollback()
            assert session.select_value("SELECT COUNT(*) FROM adbc_pool_rollback") == 0
        with config.provide_session() as session:
            assert session.select_value("SELECT pg_backend_pid()") == first_pid
            session.execute_script("CREATE TEMP TABLE adbc_pool_after_reset (value INTEGER)")
            session.commit()
            session.execute("INSERT INTO adbc_pool_after_reset VALUES (?)", 11)
            session.rollback()
            assert session.select_value("SELECT COUNT(*) FROM adbc_pool_after_reset") == 0
            session.execute("INSERT INTO adbc_pool_after_reset VALUES (?)", 13)
            session.commit()
            assert session.select_value("SELECT value FROM adbc_pool_after_reset") == 13


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_simultaneous_leases_are_exclusive(adbc_postgres_connection_config: dict[str, str]) -> None:
    """Overlapping native operations hold distinct backend connections."""
    barrier = Barrier(2, timeout=30)
    with _pooled_postgresql_config(adbc_postgres_connection_config, max_size=2) as config:

        def borrower(value: int) -> tuple[int, int]:
            with config.provide_session() as session:
                pid = int(session.select_value("SELECT pg_backend_pid()"))
                barrier.wait()
                observed = int(session.select_value("SELECT CAST(? AS INTEGER)", value))
                barrier.wait()
                return pid, observed

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(borrower, [3, 5]))
        assert len({pid for pid, _value in results}) == 2
        assert [value for _pid, value in results] == [3, 5]


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_nested_checkout_times_out_without_stealing_outer_lease(
    adbc_postgres_connection_config: dict[str, str],
) -> None:
    """A saturated nested checkout refuses while the outer backend stays usable."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config, config.provide_session() as outer:
        pid = outer.select_value("SELECT pg_backend_pid()")
        with pytest.raises(ConnectionTimeoutError), config.provide_session():
            pytest.fail("nested checkout bypassed the configured capacity")
        assert outer.select_value("SELECT pg_backend_pid()") == pid


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_failed_body_evicts_native_connection(adbc_postgres_connection_config: dict[str, str]) -> None:
    """A failed transaction is not reused even after driver-level rollback."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config:
        with config.provide_session() as session:
            failed_pid = session.select_value("SELECT pg_backend_pid()")
        with pytest.raises(SQLSpecError), config.provide_session() as session:
            session.execute("SELECT 1 / CAST(? AS INTEGER)", 0)
        with config.provide_session() as session:
            assert session.select_value("SELECT pg_backend_pid()") != failed_pid
            assert session.select_value("SELECT CAST(? AS INTEGER)", 17) == 17


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_active_shutdown_drains_before_new_generation(
    adbc_postgres_connection_config: dict[str, str],
) -> None:
    """Shutdown preserves an active borrower and refuses checkout until it exits."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config:
        with config.provide_session() as session:
            old_pid = session.select_value("SELECT pg_backend_pid()")
            config.close_pool()
            assert session.select_value("SELECT pg_backend_pid()") == old_pid
            with pytest.raises(DatabaseConnectionError), config.provide_session():
                pytest.fail("a draining generation accepted a new borrower")
        with config.provide_session() as session:
            assert session.select_value("SELECT pg_backend_pid()") != old_pid
            assert session.select_value("SELECT CAST(? AS INTEGER)", 19) == 19
        config.close_pool()
        config.close_pool()


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_discards_committed_session_state(adbc_postgres_connection_config: dict[str, str]) -> None:
    """Reuse clears committed session state that a rollback alone cannot undo."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config:
        with config.provide_session() as session:
            pid = session.select_value("SELECT pg_backend_pid()")
            search_path = session.select_value("SELECT current_setting('search_path')")
            session.execute_script("CREATE TEMP TABLE adbc_pool_reset (value INTEGER)")
            session.select_value("SELECT set_config('search_path', ?, false)", "pg_catalog")
            session.execute_script("LISTEN adbc_pool_notifications")
            session.select_value("SELECT pg_advisory_lock(CAST(? AS BIGINT))", 728311)
            session.execute_script("PREPARE adbc_pool_prepared AS SELECT 1")
            session.commit()
        with config.provide_session() as session:
            assert session.select_value("SELECT pg_backend_pid()") == pid
            assert session.select_value("SELECT current_setting('search_path')") == search_path
            assert session.select_value("SELECT to_regclass(?)", "pg_temp.adbc_pool_reset") is None
            assert session.select_value("SELECT COUNT(*) FROM pg_listening_channels()") == 0
            assert (
                session.select_value(
                    "SELECT COUNT(*) FROM pg_locks WHERE locktype = ? AND pid = pg_backend_pid()", "advisory"
                )
                == 0
            )
            assert (
                session.select_value("SELECT COUNT(*) FROM pg_prepared_statements WHERE name = ?", "adbc_pool_prepared")
                == 0
            )


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_postgresql_pool_clones_accept_initial_connection_options(
    adbc_postgres_connection_config: dict[str, str],
) -> None:
    """Fresh clones support explicit native autocommit=False initialization."""
    with (
        _pooled_postgresql_config(
            adbc_postgres_connection_config, conn_kwargs={"adbc.connection.autocommit": "false"}, max_size=2
        ) as config,
        config.provide_session() as first,
        config.provide_session() as second,
    ):
        first_pid = first.select_value("SELECT pg_backend_pid()")
        assert second.select_value("SELECT pg_backend_pid()") != first_pid
        assert first.select_value("SELECT CAST(? AS INTEGER)", 29) == 29
        assert second.select_value("SELECT CAST(? AS INTEGER)", 31) == 31


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@pytest.mark.parametrize("kind", ["rows", "arrow"])
@pytest.mark.parametrize("consumption", ["never", "partial", "exhausted"])
def test_postgresql_pool_lazy_streams_evict_after_every_consumption_mode(
    adbc_postgres_connection_config: dict[str, str], kind: str, consumption: str
) -> None:
    """A lazy stream prevents reuse even when never started or fully exhausted."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config:
        with config.provide_session() as session:
            pid = session.select_value("SELECT pg_backend_pid()")
            statement = "SELECT generate_series(1, 3) AS value"
            stream = (
                session.select_stream(statement, native_only=True, chunk_size=1)
                if kind == "rows"
                else session.select_to_arrow(statement, return_format="reader").data
            )
            iterator = iter(stream)
            if consumption == "partial":
                next(iterator)
            elif consumption == "exhausted":
                parts = list(iterator)
                if kind == "rows":
                    assert parts == [{"value": 1}, {"value": 2}, {"value": 3}]
                else:
                    assert [
                        row["value"] for batch in cast("list[RecordBatch]", parts) for row in batch.to_pylist()
                    ] == [1, 2, 3]
        with config.provide_session() as session:
            assert session.select_value("SELECT pg_backend_pid()") != pid
            assert session.select_value("SELECT CAST(? AS INTEGER)", 23) == 23


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@pytest.mark.parametrize("return_format", ["table", "batches"])
def test_postgresql_pool_eager_arrow_results_survive_session_and_allow_reuse(
    adbc_postgres_connection_config: dict[str, str], return_format: Literal["table", "batches"]
) -> None:
    """Eager Arrow data remains owned by the caller while its connection is reused."""
    with _pooled_postgresql_config(adbc_postgres_connection_config) as config:
        with config.provide_session() as session:
            pid = session.select_value("SELECT pg_backend_pid()")
            data = session.select_to_arrow("SELECT generate_series(1, 3) AS value", return_format=return_format).data
        with config.provide_session() as session:
            assert session.select_value("SELECT pg_backend_pid()") == pid
        rows = data.to_pylist() if return_format == "table" else [row for batch in data for row in batch.to_pylist()]
        assert rows == [{"value": 1}, {"value": 2}, {"value": 3}]


@contextmanager
def _pooled_postgresql_config(
    connection_config: dict[str, str], *, max_size: int = 1, conn_kwargs: dict[str, str] | None = None
) -> Generator[AdbcConfig, None, None]:
    config = AdbcConfig(
        connection_config={
            "driver_name": "adbc_driver_postgresql",
            **connection_config,
            "conn_kwargs": conn_kwargs or {},
        },
        pool_config={"max_size": max_size, "timeout": 0.05},
    )
    try:
        yield config
    finally:
        config.close_pool()
