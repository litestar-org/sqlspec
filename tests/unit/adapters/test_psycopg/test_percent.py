"""Psycopg literal percent preservation."""

import pytest

from sqlspec.adapters.psycopg.core import default_statement_config, escape_literal_percent


@pytest.mark.parametrize("parameters", [(1,), {"value": 1}, None, ()])
def test_literal_percent_preserves_sql_text(parameters: object) -> None:
    sql = "select '%s', '50%', 5 % 2 where 1 = %(value)s"
    escaped = escape_literal_percent(sql, parameters, default_statement_config.parameter_validator)
    if parameters:
        assert escaped % {"value": "1"} == sql.replace("%(value)s", "1")
    else:
        assert escaped == sql
