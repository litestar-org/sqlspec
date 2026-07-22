"""Unit tests for PostgreSQL-family ADBC UUID parameter binding."""

from typing import Any, cast
from uuid import UUID

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import core as adbc_core
from sqlspec.adapters.adbc._typing import AdbcConnection
from sqlspec.adapters.adbc.core import get_statement_config, prepare_postgres_uuid_bindings
from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import UUID_UTILS_INSTALLED

UUID_VALUE = UUID("550e8400-e29b-41d4-a716-446655440000")
OTHER_UUID_VALUE = UUID("550e8400-e29b-41d4-a716-446655440001")


class _AdbcUuidCursor:
    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql: str, parameters: object = None) -> None:
        self.executed.append((sql, parameters))

    def fetch_arrow_table(self) -> pa.Table:
        return pa.table({"value": [str(UUID_VALUE)]})

    def close(self) -> None:
        self.closed = True


class _AdbcUuidConnection:
    def __init__(self, dialect: str = "postgres") -> None:
        self.dialect = dialect
        self.cursor_obj = _AdbcUuidCursor()

    def adbc_get_info(self) -> dict[str, str]:
        return {"vendor_name": self.dialect, "driver_name": self.dialect}

    def cursor(self) -> _AdbcUuidCursor:
        return self.cursor_obj


def _make_driver(dialect: str = "postgres") -> tuple[AdbcDriver, StatementConfig]:
    base_dialect = "postgres" if dialect in {"pgvector", "paradedb"} else dialect
    config = get_statement_config(base_dialect)
    if dialect in {"pgvector", "paradedb"}:
        config = config.replace(dialect=dialect)
    connection = _AdbcUuidConnection(dialect)
    driver = AdbcDriver(cast("AdbcConnection", connection), statement_config=config, dialect=dialect)
    return driver, config


def _compile(sql: str, parameters: object, *, dialect: str = "postgres", is_many: bool = False) -> tuple[str, object]:
    driver, config = _make_driver(dialect)
    statement = SQL(sql, cast("Any", parameters), statement_config=config, is_many=is_many)
    return driver._compiled_sql(statement, config)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    ("sql", "parameters"),
    [
        ("SELECT ?, ?", (UUID_VALUE, "ordinary")),
        ("SELECT :identifier, :label", {"identifier": UUID_VALUE, "label": "ordinary"}),
        ("SELECT @identifier, @label", {"identifier": UUID_VALUE, "label": "ordinary"}),
        ("SELECT $identifier, $label", {"identifier": UUID_VALUE, "label": "ordinary"}),
        ("SELECT %(identifier)s, %(label)s", {"identifier": UUID_VALUE, "label": "ordinary"}),
        ("SELECT %s, %s", (UUID_VALUE, "ordinary")),
        ("SELECT :1, :2", (UUID_VALUE, "ordinary")),
        ("SELECT $1, $2", (UUID_VALUE, "ordinary")),
    ],
)
def test_postgres_uuid_binding_normalizes_supported_authoring_styles(sql: str, parameters: object) -> None:
    compiled_sql, compiled_parameters = _compile(sql, parameters)

    assert compiled_sql == "SELECT CAST($1 AS UUID), $2"
    assert list(cast("Any", compiled_parameters)) == [str(UUID_VALUE), "ordinary"]


def test_out_of_text_order_numeric_ordinals_use_parameter_ordinals() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT $2, $1", ("ordinary", UUID_VALUE))

    assert compiled_sql == "SELECT CAST($2 AS UUID), $1"
    assert compiled_parameters == ["ordinary", str(UUID_VALUE)]


def test_out_of_text_order_named_parameters_use_normalized_ordinals() -> None:
    compiled_sql, compiled_parameters = _compile(
        "SELECT :label, :identifier", {"identifier": UUID_VALUE, "label": "ordinary"}
    )

    assert compiled_sql == "SELECT $1, CAST($2 AS UUID)"
    assert compiled_parameters == ("ordinary", str(UUID_VALUE))


@pytest.mark.parametrize("dialect", ["postgres", "postgresql", "pgvector", "paradedb"])
def test_postgres_family_aliases_bind_uuid_parameters(dialect: str) -> None:
    compiled_sql, compiled_parameters = _compile("SELECT ?", (UUID_VALUE,), dialect=dialect)

    assert compiled_sql == "SELECT CAST($1 AS UUID)"
    assert list(cast("Any", compiled_parameters)) == [str(UUID_VALUE)]


def test_reused_uuid_placeholder_is_rewritten_at_every_occurrence() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT $1, $1", (UUID_VALUE,))

    assert compiled_sql == "SELECT CAST($1 AS UUID), CAST($1 AS UUID)"
    assert compiled_parameters == [str(UUID_VALUE)]


@pytest.mark.parametrize(
    ("sql", "expected_sql"),
    [
        ("SELECT CAST($1 AS UUID)", "SELECT CAST($1 AS UUID)"),
        ("SELECT $1::uuid", "SELECT CAST($1 AS UUID)"),
        ("SELECT $1::public.uuid", "SELECT CAST($1 AS public.uuid)"),
        ('SELECT $1::"public"."uuid"', 'SELECT CAST($1 AS "public"."uuid")'),
    ],
)
def test_existing_uuid_cast_is_reused(sql: str, expected_sql: str) -> None:
    compiled_sql, compiled_parameters = _compile(sql, (UUID_VALUE,))

    assert compiled_sql == expected_sql
    assert compiled_parameters == [str(UUID_VALUE)]


@pytest.mark.parametrize(
    ("sql", "expected_sql"),
    [
        ("SELECT CAST($1 AS TEXT)", "SELECT CAST($1 AS TEXT)"),
        ("SELECT $1::varchar", "SELECT $1::varchar"),
        ("SELECT $1::public.my_uuid", "SELECT $1::public.my_uuid"),
    ],
)
def test_different_explicit_cast_remains_authoritative(sql: str, expected_sql: str) -> None:
    compiled_sql, compiled_parameters = _compile(sql, (UUID_VALUE,))

    assert compiled_sql == expected_sql
    assert compiled_parameters == [UUID_VALUE]


def test_different_explicit_cast_skips_all_reused_occurrences() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT $1::text, $1", (UUID_VALUE,))

    assert compiled_sql == "SELECT $1::text, $1"
    assert compiled_parameters == [UUID_VALUE]


def test_existing_uuid_cast_is_reused_for_other_occurrences() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT CAST($1 AS UUID), $1", (UUID_VALUE,))

    assert compiled_sql == "SELECT CAST($1 AS UUID), CAST($1 AS UUID)"
    assert compiled_parameters == [str(UUID_VALUE)]


def test_parenthesized_different_cast_remains_authoritative() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT ($1)::text", (UUID_VALUE,))

    assert compiled_sql == "SELECT ($1)::text"
    assert compiled_parameters == [UUID_VALUE]


def test_parenthesized_uuid_cast_is_reused() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT ($1)::uuid", (UUID_VALUE,))

    assert compiled_sql == "SELECT CAST(($1) AS UUID)"
    assert compiled_parameters == [str(UUID_VALUE)]


def test_non_postgres_dialect_leaves_uuid_unchanged() -> None:
    compiled_sql, compiled_parameters = _compile("SELECT ?", (UUID_VALUE,), dialect="sqlite")

    assert compiled_sql == "SELECT ?"
    assert compiled_parameters == [UUID_VALUE]


@pytest.mark.parametrize("nested", [[UUID_VALUE], {"identifier": UUID_VALUE}])
def test_nested_uuid_values_are_not_rewritten(nested: object) -> None:
    compiled_sql, compiled_parameters = _compile("SELECT ?", (nested,))

    assert compiled_sql == "SELECT $1"
    if isinstance(nested, dict):
        assert compiled_parameters == ['{"identifier":"550e8400-e29b-41d4-a716-446655440000"}']
    else:
        assert list(cast("Any", compiled_parameters)) == [nested]


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid_utils not installed")
def test_uuid_utils_values_are_normalized() -> None:
    import uuid_utils

    value = uuid_utils.UUID(str(UUID_VALUE))
    compiled_sql, compiled_parameters = _compile("SELECT ?", (value,))

    assert compiled_sql == "SELECT CAST($1 AS UUID)"
    assert compiled_parameters == [str(UUID_VALUE)]


def test_batch_uuid_inference_uses_every_row_and_accepts_none_and_uuid_strings() -> None:
    compiled_sql, compiled_parameters = _compile(
        "INSERT INTO values_table (identifier, label) VALUES (?, ?)",
        [(str(UUID_VALUE).upper(), "first"), (None, "second"), (UUID_VALUE, "third")],
        is_many=True,
    )

    assert compiled_sql == "INSERT INTO values_table (identifier, label) VALUES (CAST($1 AS UUID), $2)"
    assert compiled_parameters == [(str(UUID_VALUE), "first"), (None, "second"), (str(UUID_VALUE), "third")]


@pytest.mark.parametrize(
    "parameters", [[(UUID_VALUE, "first"), (42, "second")], [(UUID_VALUE, "first"), ("not-a-uuid", "second")]]
)
def test_batch_uuid_inference_rejects_incompatible_values(parameters: object) -> None:
    with pytest.raises(SQLSpecError, match="UUID parameter ordinal 1"):
        _compile("INSERT INTO values_table VALUES (?, ?)", parameters, is_many=True)


def test_batch_uuid_inference_rejects_inconsistent_row_shapes() -> None:
    with pytest.raises(SQLSpecError, match="Parameter count mismatch"):
        _compile("INSERT INTO values_table VALUES (?, ?)", [(UUID_VALUE, "first"), (UUID_VALUE,)], is_many=True)


def test_batch_without_uuid_objects_is_unchanged() -> None:
    parameters = [(str(UUID_VALUE),), (None,)]

    compiled_sql, compiled_parameters = _compile("INSERT INTO values_table VALUES (?)", parameters, is_many=True)

    assert compiled_sql == "INSERT INTO values_table VALUES ($1)"
    assert compiled_parameters == parameters


def test_batch_with_scalar_rows_is_left_unchanged() -> None:
    parameters = ["first", "second"]

    sql, converted = prepare_postgres_uuid_bindings("SELECT $1", parameters, is_many=True, dialect="postgres")

    assert sql == "SELECT $1"
    assert converted is parameters


def test_batch_with_inconsistent_row_shapes_is_left_unchanged() -> None:
    parameters = [(UUID_VALUE, "first"), (UUID_VALUE,)]

    sql, converted = prepare_postgres_uuid_bindings("SELECT $1, $2", parameters, is_many=True, dialect="postgres")

    assert sql == "SELECT $1, $2"
    assert converted is parameters


def test_structural_rewrite_cache_is_bounded_and_keyed_by_dialect_and_ordinals() -> None:
    rewrite = cast("Any", adbc_core)._rewrite_postgres_uuid_placeholders
    rewrite.cache_clear()

    first = rewrite("SELECT $1, $2", (1,), "postgres")
    second = rewrite("SELECT $1, $2", (1,), "postgres")
    third = rewrite("SELECT $1, $2", (2,), "postgres")
    fourth = rewrite("SELECT $1, $2", (1,), "pgvector")

    assert first == second == ("SELECT CAST($1 AS UUID), $2", (1,))
    assert third == ("SELECT $1, CAST($2 AS UUID)", (2,))
    assert fourth == first
    assert rewrite.cache_info().hits == 1
    assert rewrite.cache_info().misses == 3
    assert rewrite.cache_info().maxsize == 256


def test_structural_rewrite_uses_ast_and_ignores_literal_and_comment_placeholders() -> None:
    rewrite = cast("Any", adbc_core)._rewrite_postgres_uuid_placeholders

    rewritten, effective = rewrite("SELECT '$1' AS literal, $1 -- $1\n", (1,), "postgres")

    assert "'$1' AS literal" in rewritten
    assert "CAST($1" in rewritten
    assert rewritten.count("AS UUID)") == 1
    assert "/* $1 */" in rewritten
    assert effective == (1,)


@pytest.mark.parametrize(
    ("dialect", "sql", "operator"),
    [
        ("pgvector", "SELECT $1, embedding <=> $2", "<=>"),
        ("pgvector", "SELECT $1, embedding <#> $2", "<#>"),
        ("pgvector", "SELECT $1, embedding <+> $2", "<+>"),
        ("pgvector", "SELECT $1, embedding <~> $2", "<~>"),
        ("paradedb", "SELECT $1 FROM documents WHERE title @@@ $2", "@@@"),
        ("paradedb", "SELECT $1 FROM documents WHERE tags &&& $2", "&&&"),
        ("paradedb", "SELECT $1 FROM documents WHERE title ||| $2", "|||"),
    ],
)
def test_extension_dialect_operators_survive_uuid_ast_rewrite(dialect: str, sql: str, operator: str) -> None:
    compiled_sql, compiled_parameters = _compile(sql, (UUID_VALUE, "rhs"), dialect=dialect)

    assert "CAST($1 AS UUID)" in compiled_sql
    assert operator in compiled_sql
    assert list(cast("Any", compiled_parameters)) == [str(UUID_VALUE), "rhs"]


def test_structural_rewrite_reports_sqlglot_parse_errors() -> None:
    rewrite = cast("Any", adbc_core)._rewrite_postgres_uuid_placeholders

    with pytest.raises(SQLSpecError, match="Failed to parse PostgreSQL ADBC SQL for UUID parameter binding"):
        rewrite("SELECT (", (1,), "postgres")


def test_value_dependent_binding_does_not_leak_between_same_sql_calls() -> None:
    driver, config = _make_driver()

    def compile_value(value: object) -> tuple[str, object]:
        statement = SQL("SELECT ?", (value,), statement_config=config)
        return driver._compiled_sql(statement, config)  # pyright: ignore[reportPrivateUsage]

    ordinary_before = compile_value("ordinary")
    uuid_call = compile_value(UUID_VALUE)
    ordinary_after = compile_value("ordinary")

    assert ordinary_before == ("SELECT $1", ["ordinary"])
    assert uuid_call == ("SELECT CAST($1 AS UUID)", [str(UUID_VALUE)])
    assert ordinary_after == ordinary_before


def test_value_dependent_binding_does_not_leak_in_reverse_order() -> None:
    driver, config = _make_driver()

    def compile_value(value: object) -> tuple[str, object]:
        statement = SQL("SELECT ?", (value,), statement_config=config)
        return driver._compiled_sql(statement, config)  # pyright: ignore[reportPrivateUsage]

    uuid_before = compile_value(UUID_VALUE)
    ordinary_call = compile_value("ordinary")
    uuid_after = compile_value(OTHER_UUID_VALUE)

    assert uuid_before == ("SELECT CAST($1 AS UUID)", [str(UUID_VALUE)])
    assert ordinary_call == ("SELECT $1", ["ordinary"])
    assert uuid_after == ("SELECT CAST($1 AS UUID)", [str(OTHER_UUID_VALUE)])


def test_select_to_arrow_uses_uuid_rewrite() -> None:
    connection = _AdbcUuidConnection()
    config = get_statement_config("postgres")
    driver = AdbcDriver(cast("AdbcConnection", connection), statement_config=config, dialect="postgres")

    result = driver.select_to_arrow("SELECT ? AS value", UUID_VALUE)

    assert result.data.to_pydict() == {"value": [str(UUID_VALUE)]}
    assert connection.cursor_obj.executed == [("SELECT CAST($1 AS UUID) AS value", [str(UUID_VALUE)])]
    assert connection.cursor_obj.closed is True
