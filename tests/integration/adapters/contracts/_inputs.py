"""Input case records for shared adapter contract tests."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from sqlspec import SQL, sql
from sqlspec.loader import SQLFileLoader
from tests.integration.adapters.contracts._schema import ContractRow


@dataclass(frozen=True)
class StatementInputCase:
    """Statement shape that should produce the same selected rows."""

    id: str
    statement_factory: Callable[[], object]
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


def _raw_qmark_statement() -> str:
    return "SELECT name, value FROM contract_items WHERE value >= ? ORDER BY value"


def _sql_object_statement() -> SQL:
    return SQL("SELECT name, value FROM contract_items WHERE value >= :minimum ORDER BY value", minimum=20)


def _builder_statement() -> SQL:
    return (
        sql
        .select("name", "value")
        .from_("contract_items")
        .where("value >= :minimum", minimum=20)
        .order_by("value")
        .to_statement()
    )


def _loader_statement() -> SQL:
    with TemporaryDirectory() as temp_dir:
        sql_path = Path(temp_dir) / "contract_queries.sql"
        sql_path.write_text(
            "-- name: select_contract_items\n"
            "SELECT name, value FROM contract_items\n"
            "WHERE value >= :minimum\n"
            "ORDER BY value;"
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

STATEMENT_INPUT_PARAMS = tuple(pytest.param(case, id=case.id) for case in STATEMENT_INPUT_CASES)
PARAMETER_PROFILE_PARAMS = tuple(pytest.param(case, id=case.id) for case in PARAMETER_PROFILE_CASES)
