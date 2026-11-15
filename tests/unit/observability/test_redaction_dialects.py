"""Dialect-aware redaction of double-quoted string literals."""

import pytest

from sqlspec.exceptions import StackExecutionError
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime, RedactionConfig, StatementEvent
from sqlspec.observability._common import redact_sql_literals


@pytest.mark.parametrize("dialect", ["mysql", "mariadb", "bigquery", "asyncmy", "AsyncmyConfig"])
def test_mysql_double_quoted_string_masked(dialect: str) -> None:
    redacted = redact_sql_literals('SELECT * FROM t WHERE email = "a@b.c"', dialect=dialect)

    assert "a@b.c" not in redacted
    assert redacted == "SELECT * FROM t WHERE email = '***'"


@pytest.mark.parametrize("dialect", ["mysql", "bigquery"])
def test_mysql_backslash_escaped_double_quote_masked(dialect: str) -> None:
    sql = 'SELECT 1 FROM t WHERE note = "say \\"hi\\" now" AND id = 7'

    redacted = redact_sql_literals(sql, dialect=dialect)

    assert "say" not in redacted
    assert "hi" not in redacted
    assert "now" not in redacted
    assert "AND id" in redacted


@pytest.mark.parametrize("dialect", [None, "postgres", "sqlite", "oracle"])
def test_postgres_double_quoted_identifier_kept(dialect: "str | None") -> None:
    redacted = redact_sql_literals('SELECT "users".id FROM "users" WHERE name = \'bob\'', dialect=dialect)

    assert '"users".id FROM "users"' in redacted
    assert "bob" not in redacted


def test_runtime_redaction_uses_driver_dialect() -> None:
    captured: list[StatementEvent] = []
    config = ObservabilityConfig(
        statement_observers=(captured.append,), redaction=RedactionConfig(mask_literals=True, mask_parameters=False)
    )
    runtime = ObservabilityRuntime(config=config, bind_key="aio", config_name="AiosqliteConfig")

    event = runtime.create_statement_event(
        sql='SELECT * FROM t WHERE email = "a@b.c"',
        parameters=(),
        driver="AiosqliteDriver",
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=0,
        duration_s=0.001,
        storage_backend=None,
        db_system="mysql",
    )

    assert "a@b.c" not in event.sql


def test_stack_error_masks_mysql_strings() -> None:
    error = StackExecutionError(0, 'INSERT INTO t VALUES ("secret-value")', RuntimeError("boom"), adapter="asyncmy")

    assert "secret-value" not in str(error)
