"""Snowflake public execution, transaction, and migration behavior."""

from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.adapters.snowflake import SnowflakeDriver
from sqlspec.core import StatementStack
from sqlspec.exceptions import PermissionDeniedError, SQLParsingError, SQLSpecError
from sqlspec.utils.correlation import CorrelationContext


def test_select_normalizes_names_and_decodes_variant(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [
            ("ID", 0, None, None, 18, 0, False),
            ("camelCase", 2, None, None, None, None, True),
            ("PAYLOAD", 5, None, None, None, None, True),
        ],
        "rows": [(7, "kept", '{"k":1}')],
    }
    driver = SnowflakeDriver(snowflake_connection)
    assert driver.select("SELECT :id AS ID, 'kept' AS \"camelCase\", payload FROM data", id=7) == [
        {"id": 7, "camelCase": "kept", "payload": {"k": 1}}
    ]
    sql, parameters, _ = snowflake_connection.calls[-1]
    assert "?" in sql
    assert list(parameters) == [7]
    assert snowflake_connection.cursors[-1].closed


def test_dml_description_does_not_turn_insert_into_select(snowflake_connection: Any) -> None:
    result = SnowflakeDriver(snowflake_connection).execute("INSERT INTO data(id) VALUES (:id)", id=1)
    assert result.rows_affected == 1
    assert result.get_data() == []


def test_script_preserves_snowflake_blocks_and_closes_all_cursors(snowflake_connection: Any) -> None:
    script = "BEGIN LET x INTEGER := 1; RETURN x; END; SHOW TABLES ->> SELECT * FROM $1;"
    SnowflakeDriver(snowflake_connection).execute_script(script)
    assert [call[0] for call in snowflake_connection.calls] == [
        "BEGIN LET x INTEGER := 1; RETURN x; END;",
        "SHOW TABLES ->> SELECT * FROM $1",
    ]
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


def test_query_tag_applies_on_repeated_queries(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {"description": [("ID", 0, None, None, 18, 0, False)], "rows": [(1,)]}
    snowflake_connection.config = SimpleNamespace(session_parameters={"QUERY_TAG": "worker"})
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_query_tag_correlation": True})
    with CorrelationContext.context("request-a"):
        driver.select("SELECT 1 AS ID")
    with CorrelationContext.context("request-b"):
        driver.select("SELECT 1 AS ID")
    assert [call[2]["_statement_params"] for call in snowflake_connection.calls] == [
        {"QUERY_TAG": "worker:request-a"},
        {"QUERY_TAG": "worker:request-b"},
    ]


def test_transaction_boundaries_and_savepoints(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection)
    driver.begin()
    driver.commit()
    driver.begin()
    driver.rollback()
    assert [call[0] for call in snowflake_connection.calls] == ["BEGIN", "BEGIN"]
    assert (snowflake_connection.commits, snowflake_connection.rollbacks) == (1, 1)
    for operation in (driver.create_savepoint, driver.release_savepoint, driver.rollback_to_savepoint):
        with pytest.raises(NotImplementedError, match="Snowflake does not support savepoints"):
            operation("savepoint_1")


@pytest.mark.parametrize(
    ("errno", "sqlstate", "expected"), [(1003, "42000", SQLParsingError), (250005, "42501", PermissionDeniedError)]
)
def test_connector_errors_map_outside_context(
    snowflake_connection: Any, errno: int, sqlstate: str, expected: type[SQLSpecError]
) -> None:
    from snowflake.connector.errors import ProgrammingError

    snowflake_connection.fail_on = "SELECT"
    snowflake_connection.failure = ProgrammingError(msg="warehouse refusal", errno=errno, sqlstate=sqlstate)
    with pytest.raises(expected, match="warehouse refusal"):
        SnowflakeDriver(snowflake_connection).select("SELECT 1")
    assert snowflake_connection.cursors[-1].closed


def test_async_query_submission_and_fetch(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {"description": [("ANSWER", 0, None, None, 18, 0, False)], "rows": [(42,)]}
    driver = SnowflakeDriver(snowflake_connection)
    query_id = driver.execute_async_query("SELECT :value", value=42)
    assert query_id == "sfqid-123"
    assert driver.fetch_async_query_result(query_id).get_data() == [{"answer": 42}]
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


def test_migration_schema_restores_exact_case_after_repeated_switch(snowflake_connection: Any) -> None:
    snowflake_connection.results['SELECT CURRENT_DATABASE() AS "database_name", CURRENT_SCHEMA() AS "schema_name"'] = {
        "description": [
            ("database_name", 2, None, None, None, None, False),
            ("schema_name", 2, None, None, None, None, False),
        ],
        "rows": [("WarehouseDB", "MixedSchema")],
    }
    driver = SnowflakeDriver(snowflake_connection)
    driver.set_migration_session_schema("analytics")
    driver.set_migration_session_schema('"OtherSchema"')
    driver.reset_migration_session_schema()
    switches = [call[0] for call in snowflake_connection.calls if call[0].startswith("USE SCHEMA")]
    assert switches == ['USE SCHEMA "ANALYTICS"', 'USE SCHEMA "OtherSchema"', 'USE SCHEMA "WarehouseDB"."MixedSchema"']
    driver.reset_migration_session_schema()
    assert len(snowflake_connection.calls) == 4


def test_migration_schema_retries_failed_restore(snowflake_connection: Any) -> None:
    from snowflake.connector.errors import ProgrammingError

    snowflake_connection.results['SELECT CURRENT_DATABASE() AS "database_name", CURRENT_SCHEMA() AS "schema_name"'] = {
        "description": [
            ("database_name", 2, None, None, None, None, False),
            ("schema_name", 2, None, None, None, None, False),
        ],
        "rows": [("DB", "Mixed")],
    }
    driver = SnowflakeDriver(snowflake_connection)
    driver.set_migration_session_schema("new_schema")
    snowflake_connection.fail_on = '"DB"."Mixed"'
    snowflake_connection.failure = ProgrammingError(msg="temporarily denied", errno=1003, sqlstate="42000")
    with pytest.raises(SQLParsingError):
        driver.reset_migration_session_schema()
    snowflake_connection.fail_on = None
    driver.reset_migration_session_schema()
    assert snowflake_connection.calls[-1][0] == 'USE SCHEMA "DB"."Mixed"'


@pytest.mark.parametrize("schema", ["", "bad; DROP TABLE users", "schema..other", '"unterminated'])
def test_migration_schema_rejects_invalid_identifiers_without_sql(snowflake_connection: Any, schema: str) -> None:
    with pytest.raises(SQLSpecError):
        SnowflakeDriver(snowflake_connection).set_migration_session_schema(schema)
    assert snowflake_connection.calls == []


def test_migration_schema_without_current_schema_refuses_unrestorable_switch(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [
            ("database_name", 2, None, None, None, None, True),
            ("schema_name", 2, None, None, None, None, True),
        ],
        "rows": [("DB", None)],
    }
    with pytest.raises(SQLSpecError, match="restorable current schema"):
        SnowflakeDriver(snowflake_connection).set_migration_session_schema("target")
    assert len(snowflake_connection.calls) == 1


def test_migration_schema_restores_schema_without_current_database(snowflake_connection: Any) -> None:
    snowflake_connection.default_result = {
        "description": [
            ("database_name", 2, None, None, None, None, True),
            ("schema_name", 2, None, None, None, None, True),
        ],
        "rows": [(None, "OriginalSchema")],
    }
    driver = SnowflakeDriver(snowflake_connection)
    driver.set_migration_session_schema("target")
    driver.reset_migration_session_schema()
    assert snowflake_connection.calls[-1][0] == 'USE SCHEMA "OriginalSchema"'


@pytest.mark.parametrize("transaction_start", ["implicit", "raw_begin"])
def test_execute_stack_preserves_caller_server_transaction(snowflake_connection: Any, transaction_start: str) -> None:
    driver = SnowflakeDriver(snowflake_connection)
    if transaction_start == "implicit":
        snowflake_connection.autocommit_enabled = False
        driver.execute("INSERT INTO orders(id) VALUES (?)", (1,))
    else:
        driver.execute("BEGIN")
    driver.execute_stack(StatementStack().push_execute("INSERT INTO orders(id) VALUES (?)", (2,)))
    assert snowflake_connection.commits == 0
    assert snowflake_connection.transaction_active
    driver.rollback()
    assert snowflake_connection.committed_rows == []


def test_execute_stack_refreshes_server_state_after_ddl_auto_commit(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection)
    driver.begin()
    driver.execute("CREATE TABLE orders(id INTEGER)")
    driver.execute_stack(StatementStack().push_execute("INSERT INTO orders(id) VALUES (?)", (1,)))
    assert snowflake_connection.commits == 1
    assert snowflake_connection.transaction_active is False
    assert len(snowflake_connection.committed_rows) == 1


def test_script_second_statement_failure_closes_all_owned_cursors(snowflake_connection: Any) -> None:
    from snowflake.connector.errors import ProgrammingError

    snowflake_connection.fail_on = "missing_table"
    snowflake_connection.failure = ProgrammingError(msg="statement failed", errno=1003, sqlstate="42000")
    with pytest.raises(SQLParsingError, match="statement failed"):
        SnowflakeDriver(snowflake_connection).execute_script(
            "INSERT INTO orders VALUES (1); INSERT INTO missing_table VALUES (2);"
        )
    assert [call[0] for call in snowflake_connection.calls] == [
        "INSERT INTO orders VALUES (1)",
        "INSERT INTO missing_table VALUES (2)",
    ]
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


@pytest.mark.parametrize("paramstyle", ["qmark", "numeric", "pyformat", "format"])
def test_script_parameters_compile_statically_in_configured_modes(snowflake_connection: Any, paramstyle: str) -> None:
    from sqlspec.adapters.snowflake.core import build_statement_config

    driver = SnowflakeDriver(snowflake_connection, statement_config=build_statement_config(paramstyle=paramstyle))
    result = driver.execute_script(
        "INSERT INTO orders VALUES (:first); // ignored ; ? :ghost\n INSERT INTO orders VALUES (:second);",
        first=1,
        second=2,
    )
    assert result.total_statements == 2
    assert snowflake_connection.calls[0][0] == "INSERT INTO orders VALUES (1)"
    assert snowflake_connection.calls[1][0].endswith("INSERT INTO orders VALUES (2)")
    assert [call[1] for call in snowflake_connection.calls] == [None, None]
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


@pytest.mark.parametrize("paramstyle", ["qmark", "numeric", "pyformat", "format"])
def test_execute_many_insert_select_uses_scalar_bindings_without_transaction_control(
    snowflake_connection: Any, paramstyle: str
) -> None:
    from sqlspec.adapters.snowflake.core import build_statement_config

    snowflake_connection.paramstyle = paramstyle
    driver = SnowflakeDriver(snowflake_connection, statement_config=build_statement_config(paramstyle=paramstyle))
    result = driver.execute_many(
        "INSERT INTO orders (id, payload) SELECT :id, PARSE_JSON(:payload)",
        [{"id": 1, "payload": '{"k":1}'}, {"id": 2, "payload": '{"k":2}'}],
    )
    assert result.rows_affected == 2
    assert snowflake_connection.batch_calls == 0
    assert len(snowflake_connection.committed_rows) == 2
    assert snowflake_connection.commits == 0
    assert all(call[0].startswith("INSERT") for call in snowflake_connection.calls)
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


@pytest.mark.parametrize("caller_transaction", [False, True])
def test_execute_many_insert_select_failure_respects_transaction_owner(
    snowflake_connection: Any, caller_transaction: bool
) -> None:
    from snowflake.connector.errors import ProgrammingError

    driver = SnowflakeDriver(snowflake_connection)
    if caller_transaction:
        driver.execute("BEGIN")
    snowflake_connection.fail_on_insert = 2
    snowflake_connection.failure = ProgrammingError(msg="second row rejected", errno=1003, sqlstate="42000")
    with pytest.raises(SQLParsingError, match="second row rejected"):
        driver.execute_many("INSERT INTO orders (id) SELECT :id", [{"id": 1}, {"id": 2}])
    assert len(snowflake_connection.committed_rows) == (0 if caller_transaction else 1)
    assert snowflake_connection.rollbacks == 0
    assert len(snowflake_connection.pending_rows) == (1 if caller_transaction else 0)
    assert snowflake_connection.commits == 0
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


def test_execute_many_values_retains_fast_batch_with_quoted_select_column(snowflake_connection: Any) -> None:
    result = SnowflakeDriver(snowflake_connection).execute_many(
        'INSERT INTO orders ("SELECT") VALUES (:id)', [{"id": 1}, {"id": 2}]
    )
    assert result.rows_affected == 2
    assert snowflake_connection.batch_calls == 1
    assert len(snowflake_connection.calls) == 1


@pytest.mark.parametrize("paramstyle", ["qmark", "numeric", "pyformat", "format"])
def test_execute_many_insert_select_leaves_fresh_autocommit_false_rows_pending(
    snowflake_connection: Any, paramstyle: str
) -> None:
    from sqlspec.adapters.snowflake.core import build_statement_config

    snowflake_connection.paramstyle = paramstyle
    snowflake_connection.autocommit_enabled = False
    driver = SnowflakeDriver(snowflake_connection, statement_config=build_statement_config(paramstyle=paramstyle))
    result = driver.execute_many("INSERT INTO orders (id) SELECT :id", [{"id": 1}, {"id": 2}])
    assert result.rows_affected == 2
    assert len(snowflake_connection.pending_rows) == 2
    assert snowflake_connection.committed_rows == []
    assert snowflake_connection.commits == 0
    driver.rollback()
    assert snowflake_connection.pending_rows == []
    assert snowflake_connection.committed_rows == []


@pytest.mark.parametrize("enable_json_deserialization", [False, True])
@pytest.mark.parametrize("enable_lowercase_column_names", [False, True])
def test_dict_cursor_rows_and_async_query_tag_correlation(
    snowflake_connection: Any, enable_json_deserialization: bool, enable_lowercase_column_names: bool
) -> None:
    snowflake_connection.default_result = {
        "description": [
            ("ID", 0, None, None, 18, 0, False),
            ("MixedName", 2, None, None, None, None, True),
            ("PAYLOAD", 5, None, None, None, None, True),
        ],
        "rows": [{"ID": 9, "MixedName": "kept", "PAYLOAD": '{"flag":1}'}],
    }
    snowflake_connection.config = SimpleNamespace(session_parameters={"QUERY_TAG": "batch"})
    driver = SnowflakeDriver(
        snowflake_connection,
        driver_features={
            "enable_json_deserialization": enable_json_deserialization,
            "enable_lowercase_column_names": enable_lowercase_column_names,
            "enable_query_tag_correlation": True,
        },
    )
    assert driver.driver_features["storage_capabilities"]["partition_strategies"] == ["fixed", "rows_per_chunk"]
    rows = driver.select("SELECT ID, MixedName, PAYLOAD FROM items")
    id_key = "id" if enable_lowercase_column_names else "ID"
    payload_key = "payload" if enable_lowercase_column_names else "PAYLOAD"
    expected_payload: Any = {"flag": 1} if enable_json_deserialization else '{"flag":1}'
    assert rows == [{id_key: 9, "MixedName": "kept", payload_key: expected_payload}]

    with CorrelationContext.context("job-42"):
        query_id = driver.execute_async_query("SELECT :val", val=1)
    assert query_id == "sfqid-123"
    assert snowflake_connection.calls[-1][2] == {"_statement_params": {"QUERY_TAG": "batch:job-42"}}

