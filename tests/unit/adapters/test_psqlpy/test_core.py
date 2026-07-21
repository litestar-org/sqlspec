# pyright: reportArgumentType=false
"""Unit tests for psqlpy core helpers."""

from collections.abc import Sequence
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from psqlpy import exceptions as psqlpy_exceptions
from sqlglot import exp, parse_one

from sqlspec.adapters.psqlpy import core as psqlpy_core
from sqlspec.adapters.psqlpy.core import (
    build_statement_config,
    coerce_numeric_for_write,
    coerce_records_for_execute_many,
    collect_rows,
    default_statement_config,
    encode_records_for_binary_copy,
    format_execute_many_parameters,
    get_parameter_casts,
    prepare_parameters_with_casts,
)
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import SQL
from sqlspec.exceptions import DataError, IntegrityError, OperationalError, PermissionDeniedError, SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection


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
    payload = {"items": [1, {"value": Decimal("1.5")}], "meta": ("a", None)}
    coerced = coerce_numeric_for_write(payload)
    assert coerced is payload
    assert coerced["items"] is payload["items"]
    assert coerced["items"][1] is payload["items"][1]
    assert coerced["meta"] is payload["meta"]


def test_coerce_numeric_for_write_copies_only_changed_branch() -> None:
    """Numeric write coercion should allocate only along branches containing float values."""
    payload = {"changed": [1.5, {"value": 2.5}], "unchanged": ("a", {"value": Decimal("3.5")})}
    coerced = coerce_numeric_for_write(payload)
    assert coerced == {
        "changed": [Decimal("1.5"), {"value": Decimal("2.5")}],
        "unchanged": ("a", {"value": Decimal("3.5")}),
    }
    assert coerced is not payload
    assert coerced["changed"] is not payload["changed"]
    assert coerced["changed"][1] is not payload["changed"][1]
    assert coerced["unchanged"] is payload["unchanged"]


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
    class _ProcessedState:
        parameter_casts = {1: "JSONB"}

    class _Statement:
        def get_processed_state(self) -> _ProcessedState:
            return _ProcessedState()

    assert get_parameter_casts(_Statement()) == {1: "JSONB"}


def test_format_execute_many_parameters_handles_scalar_input() -> None:
    """Scalar execute_many payloads should be normalized to a list containing one row."""
    formatted = format_execute_many_parameters(5, coerce_numeric=False)
    assert formatted == [[5]]


def test_coerce_records_for_execute_many_delegates_to_formatter() -> None:
    """coerce_records_for_execute_many should keep behavior via shared formatter."""
    records = [(1.25, "x"), (3, "y")]
    formatted = coerce_records_for_execute_many(records)
    assert formatted[0][0] == Decimal("1.25")
    assert formatted[1] == [3, "y"]


def test_coerce_records_for_execute_many_parses_json_text_values() -> None:
    """JSON object and array text from Arrow rows should become psqlpy JSON values."""
    records = [(1, '{"name":"alpha"}', '["north","east"]', "plain")]
    unparsed = coerce_records_for_execute_many(records)
    formatted = coerce_records_for_execute_many(records, parse_json_text=True)
    assert unparsed == [[1, '{"name":"alpha"}', '["north","east"]', "plain"]]
    assert formatted == [[1, {"name": "alpha"}, ["north", "east"], "plain"]]


def test_encode_records_for_binary_copy_preserves_copy_format() -> None:
    """The public copy encoder should keep the same escaped wire payload."""
    records = [("plain", "needs\tescape", "line\nbreak", None, True, b"bytes")]
    payload = encode_records_for_binary_copy(records)
    assert payload == b"plain\tneeds\\tescape\tline\\nbreak\t\\\\N\tt\tbytes\n"


def test_encode_records_for_binary_copy_uses_global_string_writer(monkeypatch) -> None:
    """The copy encoder should read the cached StringWriter type directly."""

    class StubStringWriter:
        def __init__(self) -> None:
            self._parts: list[str] = []

        def write(self, value: str) -> None:
            self._parts.append(value)

        def getvalue(self) -> str:
            return "".join(self._parts)

    monkeypatch.setattr(psqlpy_core, "_STRING_WRITER_TYPE", StubStringWriter)
    payload = encode_records_for_binary_copy([("plain", "line\nbreak")])
    assert payload == b"plain\tline\\nbreak\n"


def test_format_table_identifier_preserves_quoted_dots() -> None:
    assert psqlpy_core.format_table_identifier('"analytics.schema"."orders.table"') == (
        '"analytics.schema"."orders.table"'
    )


def test_no_lazy_optional_dependency_getter_functions_in_psqlpy_core() -> None:
    assert not hasattr(psqlpy_core, "_get_jsonb_type")
    assert not hasattr(psqlpy_core, "_librt_string_writer_type")


def test_no_optional_dependency_resolved_sentinel_flags_in_psqlpy_core() -> None:
    assert not hasattr(psqlpy_core, "_JSONB_RESOLVED")
    assert not hasattr(psqlpy_core, "_STRING_WRITER_RESOLVED")


def test_optional_dependency_globals_are_resolved_at_import_time() -> None:
    assert hasattr(psqlpy_core, "_JSONB_TYPE")
    assert hasattr(psqlpy_core, "_STRING_WRITER_TYPE")


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


def test_psqlpy_driver_no_longer_caches_output_converter() -> None:
    """The driver should no longer construct the dead psqlpy output converter."""
    import sqlspec.adapters.psqlpy.driver as psqlpy_driver
    import sqlspec.adapters.psqlpy.type_converter as psqlpy_type_converter

    assert not hasattr(psqlpy_driver, "_type_converter")
    assert not hasattr(psqlpy_type_converter, "PostgreSQLOutputConverter")
    assert "PostgreSQLOutputConverter" not in psqlpy_type_converter.__all__


def test_prepare_parameters_with_casts_supports_virtual_abc_dispatch() -> None:
    statement_config = build_statement_config()
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(
            type_coercion_map={Sequence: lambda value: tuple(value)}
        )
    )
    prepared = prepare_parameters_with_casts([[1, 2]], {}, statement_config)
    assert prepared == [(1, 2)]


class _Cursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[Any, ...]] = []

    async def execute(self, *args: Any) -> str:
        self.execute_calls.append(args)
        return "OK"


def _connection() -> "PsqlpyConnection":
    return cast("PsqlpyConnection", object())


class _Driver(PsqlpyDriver):
    def __init__(self, compiled_sql: str, parameters: object = None) -> None:
        super().__init__(connection=_connection())
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return (self.compiled_sql, self.compiled_parameters)


@pytest.mark.anyio
async def test_driver_psqlpy_execute_script_multi_statement_with_params_executes_all() -> None:
    driver = _Driver("INSERT INTO t VALUES ($1); INSERT INTO t VALUES ($1)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)
    result = await driver.dispatch_execute_script(cast("PsqlpyConnection", _Cursor()), cast("SQL", statement))
    assert result.statement_count == 2
    assert result.successful_statements == 2


@pytest.mark.anyio
async def test_driver_psqlpy_execute_script_uses_empty_params_for_each_sub_statement() -> None:
    driver = _Driver("SELECT 1; SELECT 2")
    statement = SimpleNamespace(statement_config=default_statement_config)
    cursor = _Cursor()
    await driver.dispatch_execute_script(cast("PsqlpyConnection", cursor), cast("SQL", statement))
    assert cursor.execute_calls == [("SELECT 1", []), ("SELECT 2", [])]


@pytest.mark.anyio
async def test_driver_psqlpy_execute_script_passes_single_statement_parameters() -> None:
    driver = _Driver("INSERT INTO t VALUES ($1)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)
    cursor = _Cursor()
    await driver.dispatch_execute_script(cast("PsqlpyConnection", cursor), cast("SQL", statement))
    assert cursor.execute_calls == [("INSERT INTO t VALUES ($1)", [1])]


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
    ],
)
def test_dml_count_query_wraps_supported_statements(sql: str) -> None:
    """Single DML should be wrapped in a one-row PostgreSQL count query."""
    rewritten = psqlpy_core._dml_count_query(sql)  # pyright: ignore[reportPrivateUsage]

    assert rewritten is not None
    expression = parse_one(rewritten, dialect="postgres")
    assert isinstance(expression, exp.Select)
    assert expression.find(exp.Count) is not None
    dml = expression.find((exp.Insert, exp.Update, exp.Delete))
    assert dml is not None
    assert dml.args.get("returning") is not None
    assert rewritten.count("RETURNING 1") == 1


def test_dml_count_query_preserves_placeholders_quotes_and_existing_with() -> None:
    """The rewrite should preserve compiled placeholders and a DML-owned WITH clause."""
    sql = (
        'WITH source AS (SELECT $2 AS "id") '
        'UPDATE "events" SET "payload" = $1 FROM source WHERE "events"."id" = source."id"'
    )

    rewritten = psqlpy_core._dml_count_query(sql)  # pyright: ignore[reportPrivateUsage]

    assert rewritten is not None
    assert "$1" in rewritten
    assert "$2" in rewritten
    assert '"events"' in rewritten
    assert "WITH source AS" in rewritten


def test_dml_count_query_uses_collision_free_cte_alias() -> None:
    """A user CTE using the private base name should force a deterministic suffix."""
    sql = (
        "WITH _sqlspec_affected AS (SELECT $2 AS id) "
        "UPDATE events SET payload = $1 FROM _sqlspec_affected "
        "WHERE events.id = _sqlspec_affected.id"
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
    ],
)
def test_dml_count_query_bypasses_unsupported_or_returning_statements(sql: str) -> None:
    """Statements outside the supported non-returning DML set should retain their native path."""
    assert psqlpy_core._dml_count_query(sql) is None  # pyright: ignore[reportPrivateUsage]


def test_dml_count_query_surfaces_parse_errors() -> None:
    """Invalid compiled SQL should raise rather than report a false row count."""
    with pytest.raises(SQLSpecError, match="Unable to build psqlpy DML row count query"):
        psqlpy_core._dml_count_query("UPDATE events SET payload =")  # pyright: ignore[reportPrivateUsage]
