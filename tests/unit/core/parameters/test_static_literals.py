"""Static literal rendering for inlined script statements."""

from enum import Enum, IntEnum

import pytest

from sqlspec import SQL
from sqlspec.adapters.asyncpg.core import default_statement_config as postgres_config
from sqlspec.adapters.bigquery.core import default_statement_config as bigquery_config
from sqlspec.adapters.duckdb.core import default_statement_config as duckdb_config
from sqlspec.adapters.mssql_python.core import default_statement_config as tsql_config
from sqlspec.adapters.mysqlconnector.core import default_statement_config as mysql_config
from sqlspec.adapters.oracledb.core import default_statement_config as oracle_config
from sqlspec.adapters.spanner.core import default_statement_config as spanner_config
from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.exceptions import SQLSpecError

QUOTE_AND_BACKSLASH = "a\\'b"


class Status(str, Enum):  # noqa: UP042
    ACTIVE = "active"


class Level(IntEnum):
    HIGH = 3


def _script_sql(config: StatementConfig, value: object) -> str:
    named = "@v" if config.dialect == "spanner" else ":v"
    return SQL(f"SELECT 0-{named}", v=value, statement_config=config).as_script().compile()[0]


@pytest.mark.parametrize(
    ("config", "value", "expected"),
    [
        (postgres_config, QUOTE_AND_BACKSLASH, "SELECT 0-E'a\\\\''b'"),
        (mysql_config, QUOTE_AND_BACKSLASH, "SELECT 0-_utf8mb4 X'615C2762'"),
        (spanner_config, QUOTE_AND_BACKSLASH, "SELECT 0-'a\\\\\\'b'"),
        (spanner_config, "a'b", "SELECT 0-'a\\'b'"),
        (bigquery_config, QUOTE_AND_BACKSLASH, "SELECT 0-'a\\\\\\'b'"),
        (tsql_config, QUOTE_AND_BACKSLASH, "SELECT 0-'a\\''b'"),
        (oracle_config, QUOTE_AND_BACKSLASH, "SELECT 0-'a\\''b'"),
        (duckdb_config, -1, "SELECT 0- -1"),
        (postgres_config, Status.ACTIVE, "SELECT 0-'active'"),
        (mysql_config, Status.ACTIVE, "SELECT 0-'active'"),
        (duckdb_config, Level.HIGH, "SELECT 0-3"),
    ],
    ids=[
        "postgres-backslash",
        "mysql-backslash",
        "spanner-backslash",
        "spanner-quote",
        "bigquery-backslash",
        "tsql-backslash",
        "oracle-backslash",
        "negative-number",
        "postgres-enum",
        "mysql-enum",
        "int-enum",
    ],
)
def test_static_literals_per_dialect(config: StatementConfig, value: object, expected: str) -> None:
    assert _script_sql(config, value) == expected


def test_static_literal_without_dialect_rejects_backslash() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NAMED_COLON, supported_parameter_styles={ParameterStyle.NAMED_COLON}
        )
    )
    with pytest.raises(SQLSpecError, match="without a dialect"):
        _script_sql(config, QUOTE_AND_BACKSLASH)
