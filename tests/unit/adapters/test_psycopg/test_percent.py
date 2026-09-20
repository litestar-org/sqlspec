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


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("select '50%%', %s", "select '50%%', %s"),
        ("select 5 %% 2, %s", "select 5 %% 2, %s"),
        ("select '50%', %s", "select '50%%', %s"),
        ("select '50%% and 20%', %s", "select '50%% and 20%%', %s"),
        ("select '%%%s', %s", "select '%%%%s', %s"),
    ],
)
def test_existing_percent_escapes_survive_repeated_preparation(sql: str, expected: str) -> None:
    validator = default_statement_config.parameter_validator
    for _ in range(3):
        assert escape_literal_percent(sql, (1,), validator) == expected
    assert escape_literal_percent(expected, (1,), validator) == expected
