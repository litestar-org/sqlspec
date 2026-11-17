"""Psycopg literal percent preservation."""

import pytest

from sqlspec.adapters.psycopg.core import default_statement_config, escape_literal_percent, source_uses_pyformat


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
        assert escape_literal_percent(sql, (1,), validator, preserve_percent_escapes=True) == expected
    assert escape_literal_percent(expected, (1,), validator, preserve_percent_escapes=True) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT 'a%%b' AS x, %s AS a", "SELECT 'a%%%%b' AS x, %s AS a"),
        ("SELECT '100%' AS x, %s AS a", "SELECT '100%%' AS x, %s AS a"),
    ],
)
def test_qmark_source_keeps_double_percent_literal(sql: str, expected: str) -> None:
    validator = default_statement_config.parameter_validator
    assert escape_literal_percent(sql, ("A",), validator, preserve_percent_escapes=False) == expected
    assert escape_literal_percent(sql, ("A",), validator, preserve_percent_escapes=False) % ("A",) == expected % ("A",)


@pytest.mark.parametrize(
    ("raw_sql", "expected"),
    [
        ("SELECT %s", True),
        ("SELECT %(v)s", True),
        ("SELECT ?", False),
        ("SELECT :v", False),
        ("SELECT $1", False),
        ("SELECT '%s'", False),
    ],
)
def test_source_uses_pyformat_detects_styles(raw_sql: str, expected: bool) -> None:
    assert source_uses_pyformat(raw_sql, default_statement_config.parameter_validator) is expected


@pytest.mark.parametrize(
    ("sql", "parameters", "from_pyformat", "from_other_styles"),
    [
        pytest.param(
            "SELECT id FROM t WHERE id = %s AND status = %s",
            (1, "a"),
            "SELECT id FROM t WHERE id = %s AND status = %s",
            "SELECT id FROM t WHERE id = %s AND status = %s",
            id="placeholders-only",
        ),
        pytest.param(
            "SELECT id FROM t WHERE id = %(id)s AND status = %(status)s",
            {"id": 1, "status": "a"},
            "SELECT id FROM t WHERE id = %(id)s AND status = %(status)s",
            "SELECT id FROM t WHERE id = %(id)s AND status = %(status)s",
            id="named-placeholders-only",
        ),
        pytest.param(
            "SELECT '100%' AS label FROM t WHERE id = %s",
            (1,),
            "SELECT '100%%' AS label FROM t WHERE id = %s",
            "SELECT '100%%' AS label FROM t WHERE id = %s",
            id="string-literal",
        ),
        pytest.param(
            "SELECT id FROM t WHERE name LIKE '%x%' AND id = %s",
            (1,),
            "SELECT id FROM t WHERE name LIKE '%%x%%' AND id = %s",
            "SELECT id FROM t WHERE name LIKE '%%x%%' AND id = %s",
            id="like-pattern",
        ),
        pytest.param(
            "SELECT id FROM t WHERE name LIKE '%%x%%' AND id = %s",
            (1,),
            "SELECT id FROM t WHERE name LIKE '%%x%%' AND id = %s",
            "SELECT id FROM t WHERE name LIKE '%%%%x%%%%' AND id = %s",
            id="doubled-percent",
        ),
        pytest.param(
            "SELECT id % 2, 10%3, 7 %% 4 FROM t WHERE id = %s",
            (1,),
            "SELECT id %% 2, 10%%3, 7 %% 4 FROM t WHERE id = %s",
            "SELECT id %% 2, 10%%3, 7 %%%% 4 FROM t WHERE id = %s",
            id="modulo",
        ),
        pytest.param(
            "SELECT '%s', '%(id)s' FROM t WHERE id = %(id)s -- 50% of %s\n AND name = %(name)s",
            {"id": 1, "name": "a"},
            "SELECT '%%s', '%%(id)s' FROM t WHERE id = %(id)s -- 50%% of %%s\n AND name = %(name)s",
            "SELECT '%%s', '%%(id)s' FROM t WHERE id = %(id)s -- 50%% of %%s\n AND name = %(name)s",
            id="placeholder-text-in-literal-and-comment",
        ),
        pytest.param(
            "SELECT $1, :name, ?, %s, %(key)s, 5 % 2, '%'",
            (1,),
            "SELECT $1, :name, ?, %s, %(key)s, 5 %% 2, '%%'",
            "SELECT $1, :name, ?, %s, %(key)s, 5 %% 2, '%%'",
            id="mixed-placeholder-styles",
        ),
        pytest.param(
            "SELECT '%%%s', %s, '%%%' || %s || '%",
            (1, 2),
            "SELECT '%%%%s', %s, '%%%%' || %s || '%%",
            "SELECT '%%%%%%s', %s, '%%%%%%' || %s || '%%",
            id="odd-percent-runs",
        ),
        pytest.param(
            "SELECT $$50% of %s$$, 'café 5%', %s %",
            [1],
            "SELECT $$50%% of %%s$$, 'café 5%%', %s %%",
            "SELECT $$50%% of %%s$$, 'café 5%%', %s %%",
            id="dollar-quote-unicode-trailing-percent",
        ),
    ],
)
def test_literal_percent_escaping_by_source_style(
    sql: str, parameters: object, from_pyformat: str, from_other_styles: str
) -> None:
    """Literal percents are doubled, placeholders are kept, and a pyformat source keeps its own escapes."""
    validator = default_statement_config.parameter_validator

    assert escape_literal_percent(sql, parameters, validator, preserve_percent_escapes=True) == from_pyformat
    assert escape_literal_percent(sql, parameters, validator, preserve_percent_escapes=False) == from_other_styles
    assert escape_literal_percent(sql, parameters, validator) == from_other_styles


@pytest.mark.parametrize("parameters", [None, (), [], {}])
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM t WHERE name LIKE '%x%'",
        "SELECT id % 2, '100%', '%s', 'a%%b' FROM t",
        "SELECT id FROM t WHERE id = %s AND name LIKE '%'",
    ],
)
@pytest.mark.parametrize("preserve_percent_escapes", [False, True])
def test_sql_without_parameters_is_sent_as_written(
    sql: str, parameters: object, preserve_percent_escapes: bool
) -> None:
    """psycopg leaves percents alone when no parameters are passed, so nothing is escaped."""
    validator = default_statement_config.parameter_validator

    escaped = escape_literal_percent(sql, parameters, validator, preserve_percent_escapes=preserve_percent_escapes)

    assert escaped == sql


@pytest.mark.parametrize(
    ("raw_sql", "expected"),
    [
        ("SELECT id FROM t WHERE id = $1 AND name LIKE '%x%'", False),
        ("SELECT id % 2 FROM t WHERE id = :id", False),
        ("SELECT '%(id)s' -- %s\n FROM t WHERE id = ?", False),
        ("SELECT 10 %% 3 FROM t WHERE id = $1", False),
        ("SELECT id FROM t WHERE id = $1 AND name = %s", True),
        ("SELECT id % 2 FROM t WHERE id = %(id)s", True),
        ("SELECT id FROM t", False),
    ],
)
def test_source_uses_pyformat_ignores_percents_that_are_not_placeholders(raw_sql: str, expected: bool) -> None:
    assert source_uses_pyformat(raw_sql, default_statement_config.parameter_validator) is expected
