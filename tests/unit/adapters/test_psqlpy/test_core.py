# pyright: reportArgumentType=false
"""Unit tests for psqlpy core helpers."""

from collections.abc import Sequence
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from psqlpy import exceptions as psqlpy_exceptions
from sqlglot import exp, parse_one

from sqlspec.adapters.psqlpy import core as psqlpy_core
from sqlspec.adapters.psqlpy.core import (
    build_statement_config,
    coerce_numeric_for_write,
    collect_rows,
    format_execute_many_parameters,
    get_parameter_casts,
    prepare_parameters_with_casts,
)
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import SQL
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.driver import CachedQuery
from sqlspec.exceptions import (
    ConnectionTimeoutError,
    DataError,
    IntegrityError,
    OperationalError,
    PermissionDeniedError,
    SQLSpecError,
)


@pytest.mark.parametrize(
    ("native_type", "expected_type"),
    [
        (psqlpy_exceptions.DataError, DataError),
        (psqlpy_exceptions.OperationalError, OperationalError),
        (psqlpy_exceptions.IntegrityError, IntegrityError),
    ],
)
def test_create_mapped_exception_falls_back_to_native_psqlpy_type(
    native_type: type[Exception], expected_type: type[Exception]
) -> None:
    """Unmatched psqlpy errors retain their broad native classification."""
    mapped = psqlpy_core.create_mapped_exception(native_type("opaque native failure"))

    assert type(mapped) is expected_type


def test_create_mapped_exception_message_match_precedes_native_type() -> None:
    """Specific message classification takes precedence over the broad native type."""
    error = psqlpy_exceptions.OperationalError("permission denied for relation accounts")

    mapped = psqlpy_core.create_mapped_exception(error)

    assert type(mapped) is PermissionDeniedError


def test_create_mapped_exception_leaves_not_supported_error_generic() -> None:
    """Native NotSupportedError remains generic until SQLSpec exposes a matching exception."""
    mapped = psqlpy_core.create_mapped_exception(psqlpy_exceptions.NotSupportedError("opaque native failure"))

    assert type(mapped) is SQLSpecError


def test_format_execute_many_parameters_no_coercion_reuses_list_rows() -> None:
    """Formatting should preserve list rows when no numeric coercion is requested."""
    records = [[1, "a"], [2, "b"]]
    formatted = format_execute_many_parameters(records, coerce_numeric=False)
    assert formatted is records
    assert formatted[0] is records[0]
    assert formatted[1] is records[1]


def test_format_execute_many_parameters_no_coercion_converts_tuples() -> None:
    """Tuple rows should be converted to list rows for execute_many."""
    records = [(1, "a"), (2, "b")]
    formatted = format_execute_many_parameters(records, coerce_numeric=False)
    assert formatted == [[1, "a"], [2, "b"]]


def test_format_execute_many_parameters_with_coercion_converts_float_to_decimal() -> None:
    """Numeric write coercion should convert floats to Decimal values."""
    records = [(1.5, "a"), (2, "b")]
    formatted = format_execute_many_parameters(records, coerce_numeric=True)
    assert formatted[0][0] == Decimal("1.5")
    assert formatted[1][0] == 2


def test_format_execute_many_parameters_with_coercion_converts_float_subclass() -> None:
    """Numeric write coercion should not skip float subclasses in execute_many rows."""

    class MyFloat(float):
        pass

    records = [(MyFloat("1.5"), "a"), (2, "b")]
    formatted = format_execute_many_parameters(records, coerce_numeric=True)
    assert type(formatted[0][0]) is Decimal
    assert formatted[0][0] == Decimal("1.5")
    assert formatted[1][0] == 2


def test_coerce_numeric_for_write_preserves_identity_when_unchanged() -> None:
    """Nested payloads without float values should keep their existing container identities."""
    payload = [[1, (Decimal("1.5"),)], ("a", None)]
    coerced = coerce_numeric_for_write(payload)
    assert coerced is payload
    assert coerced[0] is payload[0]
    assert coerced[0][1] is payload[0][1]
    assert coerced[1] is payload[1]


def test_coerce_numeric_for_write_copies_only_changed_branch() -> None:
    """Numeric write coercion should allocate only along list/tuple branches containing float values."""
    payload = [[1.5, (2.5,)], ("a", [Decimal("3.5")])]
    coerced = coerce_numeric_for_write(payload)
    assert coerced == [[Decimal("1.5"), (Decimal("2.5"),)], ("a", [Decimal("3.5")])]
    assert coerced is not payload
    assert coerced[0] is not payload[0]
    assert coerced[0][1] is not payload[0][1]
    assert coerced[1] is payload[1]


def test_coerce_numeric_for_write_preserves_dict_floats() -> None:
    """JSON dict payloads should retain float values untouched during numeric write coercion."""
    payload: dict[str, Any] = {"score": 1.5, "nested": {"weight": 2.5}}
    coerced = cast("dict[str, Any]", coerce_numeric_for_write(payload))
    assert coerced is payload
    assert type(coerced["score"]) is float
    assert type(coerced["nested"]["weight"]) is float


def test_decimal_is_not_registered_for_float_coercion() -> None:
    statement_config = build_statement_config()
    assert Decimal not in statement_config.parameter_config.type_coercion_map


def test_coerce_numeric_for_write_preserves_decimal_identity() -> None:
    value = Decimal("1.23456789012345678")
    coerced = coerce_numeric_for_write(value)
    assert coerced is value


def test_coerce_numeric_for_write_still_converts_float_to_decimal() -> None:
    assert coerce_numeric_for_write(1.5) == Decimal("1.5")


def test_build_statement_config_builds_base_profile_once(monkeypatch) -> None:
    calls = 0
    original = psqlpy_core.build_statement_config_from_profile

    def wrapped(*args: Any, **kwargs: Any) -> object:
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(psqlpy_core, "build_statement_config_from_profile", wrapped)
    build_statement_config()
    assert calls == 1


def test_get_parameter_casts_reads_processed_state_from_cached_statement() -> None:
    statement = SQL(
        "SELECT :value::jsonb", {"value": {"key": "value"}}, statement_config=psqlpy_core.default_statement_config
    )
    statement.compile()
    assert get_parameter_casts(statement) == {1: "JSONB"}
    state = statement.get_processed_state()
    cached = CachedQuery(
        compiled_sql=state.compiled_sql,
        parameter_profile=state.parameter_profile,
        input_named_parameters=state.input_named_parameters,
        applied_wrap_types=state.applied_wrap_types,
        parameter_casts=state.parameter_casts,
        operation_type=state.operation_type,
        operation_profile=state.operation_profile,
        param_count=1,
        processed_state=state,
    )
    assert get_parameter_casts(cached) == {1: "JSONB"}


def test_get_parameter_casts_handles_unprocessed_statement() -> None:
    assert get_parameter_casts(SQL("SELECT 1")) == {}


def test_format_execute_many_parameters_handles_scalar_input() -> None:
    """Scalar execute_many payloads should be normalized to a list containing one row."""
    formatted = format_execute_many_parameters(5, coerce_numeric=False)
    assert formatted == [[5]]


def test_format_table_identifier_preserves_quoted_dots() -> None:
    assert psqlpy_core.format_table_identifier('"analytics.schema"."orders.table"') == (
        '"analytics.schema"."orders.table"'
    )


def test_optional_dependency_globals_are_resolved_at_import_time() -> None:
    assert hasattr(psqlpy_core, "_JSONB_TYPE")


def test_collect_rows_names_from_first_row() -> None:
    """collect_rows should derive column order from first dict row key order."""
    result = SimpleNamespace(result=lambda: [{"id": 1, "name": "x"}])
    (rows, columns) = collect_rows(result)
    assert rows == [{"id": 1, "name": "x"}]
    assert columns == ["id", "name"]


def test_collect_rows_empty_result() -> None:
    """collect_rows should return empty structures for empty query results."""
    result = SimpleNamespace(result=lambda: [])
    (rows, columns) = collect_rows(result)
    assert rows == []
    assert columns == []


def test_collect_rows_prefers_metadata_column_names_when_available() -> None:
    """collect_rows should derive names from first row keys even with metadata available."""
    result = SimpleNamespace(result=lambda: [{"id": 1, "name": "x"}], column_names=("id", "name"))
    (rows, columns) = collect_rows(result)
    assert rows == [{"id": 1, "name": "x"}]
    assert columns == ["id", "name"]


def test_collect_rows_accepts_raw_list_payload() -> None:
    """collect_rows should accept pre-resolved row lists as direct payloads."""
    payload = [{"id": 1, "name": "x"}]
    (rows, columns) = collect_rows(payload)
    assert rows is payload
    assert columns == ["id", "name"]


def test_prepare_parameters_with_casts_supports_subclass_type_dispatch() -> None:

    class MyInt(int):
        pass

    statement_config = build_statement_config()
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(type_coercion_map={int: lambda value: value + 1})
    )
    prepared = prepare_parameters_with_casts([MyInt(4)], {}, statement_config)
    assert prepared == [5]


def test_prepare_parameters_with_casts_supports_virtual_abc_dispatch() -> None:
    statement_config = build_statement_config()
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(
            type_coercion_map={Sequence: lambda value: tuple(value)}
        )
    )
    prepared = prepare_parameters_with_casts([[1, 2]], {}, statement_config)
    assert prepared == [(1, 2)]


@pytest.mark.parametrize("tag", ["", "NOT A COMMAND TAG", "SELECT"])
def test_extract_rows_affected_returns_zero_for_unparsable_tag(tag: str) -> None:
    """Unparsable command tags should report zero rows affected, not the -1 sentinel."""
    assert psqlpy_core.extract_rows_affected(tag) == 0


def test_extract_rows_affected_parses_valid_tag() -> None:
    """A well-formed command tag should still report the parsed row count."""
    assert psqlpy_core.extract_rows_affected("INSERT 0 3") == 3
    assert psqlpy_core.extract_rows_affected("UPDATE 5") == 5


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO events (id, payload) VALUES ($1, $2)",
        "UPDATE events SET payload = $1 WHERE id = $2",
        "DELETE FROM events WHERE id = $1",
        "WITH source AS (SELECT $2 AS id) UPDATE events SET payload = $1 FROM source WHERE events.id = source.id",
    ],
)
def test_dml_count_query_wraps_supported_statements(sql: str) -> None:
    """Single DML (including top-level non-recursive WITH CTEs) should be wrapped in a one-row PostgreSQL count query."""
    rewritten = psqlpy_core._dml_count_query(sql)  # pyright: ignore[reportPrivateUsage]

    assert rewritten is not None
    expression = parse_one(rewritten, dialect="postgres")
    assert isinstance(expression, exp.Select)
    assert expression.find(exp.Count) is not None
    dml = expression.find(exp.Insert, exp.Update, exp.Delete)
    assert dml is not None
    assert dml.args.get("returning") is not None
    assert rewritten.count("RETURNING 1") == 1


def test_dml_count_query_preserves_placeholders_quotes_and_existing_with() -> None:
    """The rewrite should preserve compiled placeholders and a nested subquery WITH clause."""
    sql = 'INSERT INTO "events" ("id", "payload") SELECT "id", $1 FROM (WITH source AS (SELECT $2 AS "id") SELECT "id" FROM source) AS sub'

    rewritten = psqlpy_core._dml_count_query(sql)  # pyright: ignore[reportPrivateUsage]

    assert rewritten is not None
    assert "$1" in rewritten
    assert "$2" in rewritten
    assert '"events"' in rewritten
    assert "WITH source AS" in rewritten


def test_dml_count_query_uses_collision_free_cte_alias() -> None:
    """A user CTE using the private base name should force a deterministic suffix."""
    sql = (
        "INSERT INTO events (id, payload) "
        "SELECT id, $1 FROM (WITH _sqlspec_affected AS (SELECT $2 AS id) SELECT id FROM _sqlspec_affected) AS sub"
    )

    rewritten = psqlpy_core._dml_count_query(sql)  # pyright: ignore[reportPrivateUsage]

    assert rewritten is not None
    assert rewritten.startswith("WITH _sqlspec_affected_1 AS")
    assert "WITH _sqlspec_affected AS" in rewritten


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM events",
        "MERGE INTO events USING source ON events.id = source.id WHEN MATCHED THEN DELETE",
        "CREATE TABLE events (id INT)",
        "UPDATE events SET payload = $1 WHERE id = $2 RETURNING id",
        "WITH RECURSIVE source AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM source WHERE id < 2) UPDATE events SET payload = $1 FROM source WHERE events.id = source.id",
    ],
)
def test_dml_count_query_bypasses_unsupported_or_returning_statements(sql: str) -> None:
    """Statements outside the supported non-returning DML set should retain their native path."""
    assert psqlpy_core._dml_count_query(sql) is None  # pyright: ignore[reportPrivateUsage]


def test_dml_count_query_surfaces_parse_errors() -> None:
    """Invalid compiled SQL should return None rather than raising during count query rewrite."""
    assert psqlpy_core._dml_count_query("UPDATE events SET payload =") is None  # pyright: ignore[reportPrivateUsage]


def test_create_mapped_exception_maps_connection_timeout_and_data_parse_errors() -> None:
    """Connection timeouts should map to ConnectionTimeoutError and DataError with 'parse' should stay DataError."""
    conn_err = psqlpy_exceptions.Error("connection timed out while connecting to server")
    assert type(psqlpy_core.create_mapped_exception(conn_err)) is ConnectionTimeoutError

    data_err = psqlpy_exceptions.DataError("invalid input syntax for type json: failed to parse value")
    assert type(psqlpy_core.create_mapped_exception(data_err)) is DataError


@pytest.mark.anyio
async def test_psqlpy_dispatch_execute_strips_sqlcommenter_before_dml_count_cache_lookup() -> None:
    """PsqlpyDriver.dispatch_execute strips trailing sqlcommenter comments before _dml_count_query cache lookup."""
    psqlpy_core._dml_count_query.cache_clear()
    fetched_queries: list[str] = []

    class _FakeResult:
        def result(self) -> list[dict[str, int]]:
            return [{psqlpy_core._DML_COUNT_COLUMN: 1}]

    def _fake_fetch(query: str, _params: Any) -> _FakeResult:
        fetched_queries.append(query)
        return _FakeResult()

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=_fake_fetch), execute=AsyncMock())
    stmt_config = build_statement_config().replace(
        enable_sqlcommenter=True, sqlcommenter_enable_context=True, sqlcommenter_enable_traceparent=True
    )
    driver = PsqlpyDriver(connection=cast("Any", connection), statement_config=stmt_config)

    with (
        patch(
            "sqlspec.core.sqlcommenter.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "1111111111111111"),
        ),
        SQLCommenterContext.scope({"route": "/items/1"}),
    ):
        stmt1 = driver.prepare_statement("UPDATE events SET payload = $1 WHERE id = $2", ("a", 1))
        await driver.dispatch_execute(cast("Any", connection), stmt1)

    with (
        patch(
            "sqlspec.core.sqlcommenter.get_trace_context",
            return_value=("0af7651916cd43dd8448eb211c80319c", "2222222222222222"),
        ),
        SQLCommenterContext.scope({"route": "/items/2"}),
    ):
        stmt2 = driver.prepare_statement("UPDATE events SET payload = $1 WHERE id = $2", ("b", 2))
        await driver.dispatch_execute(cast("Any", connection), stmt2)

    cache_info = psqlpy_core._dml_count_query.cache_info()
    assert cache_info.misses == 1
    assert cache_info.hits >= 1
    assert len(fetched_queries) == 2
    assert "1111111111111111" in fetched_queries[0]
    assert "2222222222222222" in fetched_queries[1]
    assert "1111111111111111" not in fetched_queries[1]
