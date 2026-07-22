"""Input case records for shared adapter contract tests."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import pytest

from sqlspec import SQL, Select, sql
from sqlspec.builder import Explain
from sqlspec.core.statement import StatementConfig
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    UniqueViolationError,
)
from sqlspec.loader import SQLFileLoader
from tests.integration.adapters._shared._schema import ContractRow, ContractTable


@dataclass(frozen=True)
class StatementInputCase:
    """Statement shape that should produce the same selected rows."""

    id: str
    statement_factory: "Callable[[str, str | None], object]"
    parameters: object | None
    setup_rows: tuple[ContractRow, ...]
    expected_data: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class ParameterProfileCase:
    """Parameter binding case for shared adapter contracts."""

    id: str
    expected_result_data: tuple[dict[str, object], ...] | None
    expected_rows_affected: int | None
    expected_verification_data: tuple[dict[str, object], ...] | None
    parameters: object | None
    setup_rows: tuple[ContractRow, ...]
    statement: str
    verification_parameters: object | None
    verification_statement: str | None


@dataclass(frozen=True)
class ParameterStyleCase:
    """Parameter style case for shared adapter contracts."""

    id: str
    expected_result_data: tuple[dict[str, object], ...] | None
    expected_rows_affected: int | None
    expected_verification_data: tuple[dict[str, object], ...] | None
    method: Literal["execute", "execute_many"]
    parameters: object | None
    setup_rows: tuple[ContractRow, ...]
    statement: object
    verification_parameters: object | None
    verification_statement: str | None


@dataclass(frozen=True)
class ExplainCase:
    """EXPLAIN artifact factory that should execute and return plan rows."""

    id: str
    build: "Callable[[ContractTable, str], object]"


@dataclass(frozen=True)
class ExceptionViolationCase:
    """Constraint violation that should normalize to a shared sqlspec exception type."""

    id: str
    setup_script: str
    seed_statement: str | None
    seed_parameters: tuple[object, ...] | None
    trigger_statement: str
    trigger_parameters: tuple[object, ...]
    expected_exception: type[Exception]
    teardown_script: str


def _raw_qmark_statement(table: str, dialect: "str | None" = None) -> str:
    return f"SELECT name, value FROM {table} WHERE value >= ? ORDER BY value"


def _sql_object_statement(table: str, dialect: "str | None" = None) -> SQL:
    return SQL(f"SELECT name, value FROM {table} WHERE value >= :minimum ORDER BY value", minimum=20)


def _builder_statement(table: str, dialect: "str | None" = None) -> Select:
    return (
        sql
        .select("name", "value", dialect=dialect)
        .from_(table)
        .where("value >= :minimum", minimum=20)
        .order_by("value")
    )


def _loader_statement(table: str, dialect: "str | None" = None) -> SQL:
    with TemporaryDirectory() as temp_dir:
        sql_path = Path(temp_dir) / "contract_queries.sql"
        sql_path.write_text(
            f"-- name: select_contract_items\nSELECT name, value FROM {table}\nWHERE value >= :minimum\nORDER BY value;"
        )
        loader = SQLFileLoader()
        loader.load_sql(sql_path)
        return loader.get_sql("select_contract_items")


STATEMENT_INPUT_CASES = (
    StatementInputCase(
        id="raw_qmark_input",
        statement_factory=_raw_qmark_statement,
        parameters=(20,),
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        expected_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
    ),
    StatementInputCase(
        id="sql_object_named_input",
        statement_factory=_sql_object_statement,
        parameters=None,
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        expected_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
    ),
    StatementInputCase(
        id="builder_named_input",
        statement_factory=_builder_statement,
        parameters=None,
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        expected_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
    ),
    StatementInputCase(
        id="loader_named_input",
        statement_factory=_loader_statement,
        parameters={"minimum": 20},
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        expected_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
    ),
)

PARAMETER_PROFILE_CASES = (
    ParameterProfileCase(
        id="qmark_insert",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (?, ?, ?)",
        parameters=("qmark", 10, "qmark-note"),
        expected_rows_affected=1,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=({"name": "qmark", "value": 10, "note": "qmark-note"},),
    ),
    ParameterProfileCase(
        id="named_insert",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (:name, :value, :note)",
        parameters={"name": "named", "value": 20, "note": "named-note"},
        expected_rows_affected=1,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=({"name": "named", "value": 20, "note": "named-note"},),
    ),
    ParameterProfileCase(
        id="none_value",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (:name, :value, :note)",
        parameters={"name": "none-value", "value": 30, "note": None},
        expected_rows_affected=1,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=({"name": "none-value", "value": 30, "note": None},),
    ),
    ParameterProfileCase(
        id="repeated_named_select",
        setup_rows=(ContractRow("repeat", 40), ContractRow("other", 50, "repeat")),
        statement=("SELECT name, value FROM contract_items WHERE name = :target OR note = :target ORDER BY value"),
        parameters={"target": "repeat"},
        expected_rows_affected=None,
        expected_result_data=({"name": "repeat", "value": 40}, {"name": "other", "value": 50}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterProfileCase(
        id="injection_looking_string",
        setup_rows=(ContractRow("safe_data", 60),),
        statement="SELECT name, value FROM contract_items WHERE name = ?",
        parameters=("'; DROP TABLE contract_items; --",),
        expected_rows_affected=None,
        expected_result_data=(),
        verification_statement="SELECT COUNT(*) AS count FROM contract_items",
        verification_parameters=None,
        expected_verification_data=({"count": 1},),
    ),
)

PARAMETER_STYLE_CASES = (
    ParameterStyleCase(
        id="qmark_tuple_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= ? ORDER BY value",
        parameters=(20,),
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="qmark_list_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= ? ORDER BY value",
        parameters=[20],
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="named_dict_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= :minimum ORDER BY value",
        parameters={"minimum": 20},
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="numeric_dollar_tuple_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= $1 ORDER BY value",
        parameters=(20,),
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="named_at_dict_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= @minimum ORDER BY value",
        parameters={"minimum": 20},
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="named_dollar_dict_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= $minimum ORDER BY value",
        parameters={"minimum": 20},
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="positional_colon_tuple_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= :1 ORDER BY value",
        parameters=(20,),
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="named_pyformat_dict_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= %(minimum)s ORDER BY value",
        parameters={"minimum": 20},
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="positional_pyformat_tuple_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= %s ORDER BY value",
        parameters=(20,),
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="mixed_qmark_named_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= ? AND name <> :excluded ORDER BY value",
        parameters=(20, "gamma"),
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20},),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="mixed_named_styles_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement="SELECT name, value FROM contract_items WHERE value >= @minimum AND name <> $excluded ORDER BY value",
        parameters={"minimum": 20, "excluded": "gamma"},
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20},),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="sql_object_named_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("gamma", 30)),
        statement=SQL("SELECT name, value FROM contract_items WHERE value >= :minimum ORDER BY value", minimum=20),
        parameters=None,
        expected_rows_affected=None,
        expected_result_data=({"name": "beta", "value": 20}, {"name": "gamma", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="repeated_named_select",
        method="execute",
        setup_rows=(ContractRow("repeat", 40), ContractRow("other", 50, "repeat")),
        statement="SELECT name, value FROM contract_items WHERE name = :target OR note = :target ORDER BY value",
        parameters={"target": "repeat"},
        expected_rows_affected=None,
        expected_result_data=({"name": "repeat", "value": 40}, {"name": "other", "value": 50}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="like_qmark_select",
        method="execute",
        setup_rows=(ContractRow("alpha", 10), ContractRow("beta", 20), ContractRow("alphabet", 30)),
        statement="SELECT name, value FROM contract_items WHERE name LIKE ? ORDER BY value",
        parameters=("alpha%",),
        expected_rows_affected=None,
        expected_result_data=({"name": "alpha", "value": 10}, {"name": "alphabet", "value": 30}),
        verification_statement=None,
        verification_parameters=None,
        expected_verification_data=None,
    ),
    ParameterStyleCase(
        id="qmark_insert",
        method="execute",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (?, ?, ?)",
        parameters=("qmark", 10, "qmark-note"),
        expected_rows_affected=1,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=({"name": "qmark", "value": 10, "note": "qmark-note"},),
    ),
    ParameterStyleCase(
        id="named_insert",
        method="execute",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (:name, :value, :note)",
        parameters={"name": "named", "value": 20, "note": "named-note"},
        expected_rows_affected=1,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=({"name": "named", "value": 20, "note": "named-note"},),
    ),
    ParameterStyleCase(
        id="none_named_insert",
        method="execute",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (:name, :value, :note)",
        parameters={"name": "none-value", "value": 30, "note": None},
        expected_rows_affected=1,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=({"name": "none-value", "value": 30, "note": None},),
    ),
    ParameterStyleCase(
        id="execute_many_qmark_tuples",
        method="execute_many",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (?, ?, ?)",
        parameters=[("batch1", 10, None), ("batch2", 20, None), ("batch3", 30, None)],
        expected_rows_affected=3,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=(
            {"name": "batch1", "value": 10, "note": None},
            {"name": "batch2", "value": 20, "note": None},
            {"name": "batch3", "value": 30, "note": None},
        ),
    ),
    ParameterStyleCase(
        id="execute_many_named_dicts",
        method="execute_many",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (:name, :value, :note)",
        parameters=[
            {"name": "dict1", "value": 100, "note": None},
            {"name": "dict2", "value": 200, "note": None},
            {"name": "dict3", "value": 300, "note": None},
        ],
        expected_rows_affected=3,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=(
            {"name": "dict1", "value": 100, "note": None},
            {"name": "dict2", "value": 200, "note": None},
            {"name": "dict3", "value": 300, "note": None},
        ),
    ),
    ParameterStyleCase(
        id="execute_many_named_pyformat_dicts",
        method="execute_many",
        setup_rows=(),
        statement="INSERT INTO contract_items (name, value, note) VALUES (%(name)s, %(value)s, %(note)s)",
        parameters=[
            {"name": "pyformat1", "value": 100, "note": None},
            {"name": "pyformat2", "value": 200, "note": None},
            {"name": "pyformat3", "value": 300, "note": None},
        ],
        expected_rows_affected=3,
        expected_result_data=None,
        verification_statement="SELECT name, value, note FROM contract_items ORDER BY value",
        verification_parameters=None,
        expected_verification_data=(
            {"name": "pyformat1", "value": 100, "note": None},
            {"name": "pyformat2", "value": 200, "note": None},
            {"name": "pyformat3", "value": 300, "note": None},
        ),
    ),
    ParameterStyleCase(
        id="injection_looking_qmark_value",
        method="execute",
        setup_rows=(ContractRow("safe_data", 60),),
        statement="SELECT name, value FROM contract_items WHERE name = ?",
        parameters=("'; DROP TABLE contract_items; --",),
        expected_rows_affected=None,
        expected_result_data=(),
        verification_statement="SELECT COUNT(*) AS count FROM contract_items",
        verification_parameters=None,
        expected_verification_data=({"count": 1},),
    ),
)


def _explain_select(table: ContractTable, dialect: str) -> object:
    return Explain(f"SELECT name, value, note FROM {table.name}", dialect=dialect).build()


def _explain_where(table: ContractTable, dialect: str) -> object:
    return Explain(f"SELECT name FROM {table.name} WHERE value > 0", dialect=dialect).build()


def _explain_self_join(table: ContractTable, dialect: str) -> object:
    return Explain(
        f"SELECT a.name FROM {table.name} a JOIN {table.name} b ON a.value = b.value", dialect=dialect
    ).build()


def _explain_subquery(table: ContractTable, dialect: str) -> object:
    return Explain(
        f"SELECT name FROM {table.name} WHERE value IN (SELECT value FROM {table.name})", dialect=dialect
    ).build()


def _explain_insert(table: ContractTable, dialect: str) -> object:
    return Explain(f"INSERT INTO {table.name} (name, value, note) VALUES ('plan', 1, NULL)", dialect=dialect).build()


def _explain_update(table: ContractTable, dialect: str) -> object:
    return Explain(f"UPDATE {table.name} SET value = 100 WHERE name = 'plan'", dialect=dialect).build()


def _explain_delete(table: ContractTable, dialect: str) -> object:
    return Explain(f"DELETE FROM {table.name} WHERE name = 'plan'", dialect=dialect).build()


def _explain_query_builder(table: ContractTable, dialect: str) -> object:
    return sql.select("name", dialect=dialect).from_(table.name).where("value > 0").explain().build()


def _explain_sql_factory(table: ContractTable, dialect: str) -> object:
    return sql.explain(f"SELECT name FROM {table.name}", dialect=dialect).build()


def _explain_sql_object(table: ContractTable, dialect: str) -> object:
    return SQL(
        f"SELECT value FROM {table.name} WHERE value >= 0", statement_config=StatementConfig(dialect=dialect)
    ).explain()


EXPLAIN_CASES = (
    ExplainCase(id="select", build=_explain_select),
    ExplainCase(id="where", build=_explain_where),
    ExplainCase(id="self-join", build=_explain_self_join),
    ExplainCase(id="subquery", build=_explain_subquery),
    ExplainCase(id="insert", build=_explain_insert),
    ExplainCase(id="update", build=_explain_update),
    ExplainCase(id="delete", build=_explain_delete),
    ExplainCase(id="query-builder", build=_explain_query_builder),
    ExplainCase(id="sql-factory", build=_explain_sql_factory),
    ExplainCase(id="sql-object", build=_explain_sql_object),
)


EXCEPTION_VIOLATION_CASES = (
    ExceptionViolationCase(
        id="unique",
        setup_script="""
            DROP TABLE IF EXISTS contract_unique;
            CREATE TABLE contract_unique (email VARCHAR(255) UNIQUE NOT NULL);
        """,
        seed_statement="INSERT INTO contract_unique (email) VALUES (?)",
        seed_parameters=("duplicate@example.com",),
        trigger_statement="INSERT INTO contract_unique (email) VALUES (?)",
        trigger_parameters=("duplicate@example.com",),
        expected_exception=UniqueViolationError,
        teardown_script="DROP TABLE IF EXISTS contract_unique",
    ),
    ExceptionViolationCase(
        id="not-null",
        setup_script="""
            DROP TABLE IF EXISTS contract_not_null;
            CREATE TABLE contract_not_null (label VARCHAR(255), required_field VARCHAR(255) NOT NULL);
        """,
        seed_statement=None,
        seed_parameters=None,
        trigger_statement="INSERT INTO contract_not_null (label) VALUES (?)",
        trigger_parameters=("missing-required",),
        expected_exception=NotNullViolationError,
        teardown_script="DROP TABLE IF EXISTS contract_not_null",
    ),
    ExceptionViolationCase(
        id="check",
        setup_script="""
            DROP TABLE IF EXISTS contract_check;
            CREATE TABLE contract_check (age INTEGER CHECK (age >= 18));
        """,
        seed_statement=None,
        seed_parameters=None,
        trigger_statement="INSERT INTO contract_check (age) VALUES (?)",
        trigger_parameters=(5,),
        expected_exception=CheckViolationError,
        teardown_script="DROP TABLE IF EXISTS contract_check",
    ),
    ExceptionViolationCase(
        id="foreign-key",
        setup_script="""
            DROP TABLE IF EXISTS contract_fk_child;
            DROP TABLE IF EXISTS contract_fk_parent;
            CREATE TABLE contract_fk_parent (id INTEGER PRIMARY KEY, name VARCHAR(255));
            CREATE TABLE contract_fk_child (
                child_id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES contract_fk_parent(id)
            );
        """,
        seed_statement=None,
        seed_parameters=None,
        trigger_statement="INSERT INTO contract_fk_child (child_id, parent_id) VALUES (?, ?)",
        trigger_parameters=(1, 999),
        expected_exception=ForeignKeyViolationError,
        teardown_script="""
            DROP TABLE IF EXISTS contract_fk_child;
            DROP TABLE IF EXISTS contract_fk_parent;
        """,
    ),
)

STATEMENT_INPUT_PARAMS = tuple(pytest.param(case, id=case.id) for case in STATEMENT_INPUT_CASES)
PARAMETER_PROFILE_PARAMS = tuple(pytest.param(case, id=case.id) for case in PARAMETER_PROFILE_CASES)
PARAMETER_STYLE_PARAMS = tuple(pytest.param(case, id=case.id) for case in PARAMETER_STYLE_CASES)
PARAMETER_STYLE_EXECUTE_PARAMS = tuple(
    pytest.param(case, id=case.id) for case in PARAMETER_STYLE_CASES if case.method == "execute"
)
PARAMETER_STYLE_EXECUTE_MANY_PARAMS = tuple(
    pytest.param(case, id=case.id) for case in PARAMETER_STYLE_CASES if case.method == "execute_many"
)
EXPLAIN_PARAMS = tuple(pytest.param(case, id=case.id) for case in EXPLAIN_CASES)
EXCEPTION_VIOLATION_PARAMS = tuple(pytest.param(case, id=case.id) for case in EXCEPTION_VIOLATION_CASES)
