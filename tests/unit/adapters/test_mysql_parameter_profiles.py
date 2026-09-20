"""MySQL native parameter binding contracts."""

from importlib import import_module

import pytest

from sqlspec import SQL


@pytest.mark.parametrize("adapter", ["aiomysql", "asyncmy", "mysqlconnector", "pymysql"])
def test_mysql_parameters_bind_natively(adapter: str) -> None:
    config = import_module(f"sqlspec.adapters.{adapter}.core").default_statement_config
    statement = SQL("select * from t where name = ?", "v", statement_config=config)
    sql, parameters = statement.compile()
    assert "%s" in sql
    assert list(parameters) == ["v"]
    script_sql, script_parameters = statement.as_script().compile()
    assert "'v'" in script_sql
    assert script_parameters is None


@pytest.mark.parametrize("adapter", ["aiomysql", "asyncmy", "pymysql"])
@pytest.mark.parametrize("parameters", [["v"], None, []])
def test_literal_percent_is_escaped_only_when_parameters_are_bound(adapter: str, parameters: object) -> None:
    core = import_module(f"sqlspec.adapters.{adapter}.core")
    sql = "select date_format(d,'%Y-%m'), 5 % 2 from t where n like 'a%' and m = '%s' and x = %s -- 50%"
    result = core.escape_literal_percent(sql, parameters, core.default_statement_config.parameter_validator)
    if parameters:
        assert result % ("X",) == sql.replace("x = %s", "x = X")
    else:
        assert result == sql


@pytest.mark.parametrize(
    "adapter,class_name", [("pymysql", "PyMysqlDriver"), ("aiomysql", "AiomysqlDriver"), ("asyncmy", "AsyncmyDriver")]
)
@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "insert into t (a) values (%s) on duplicate key update a = a % 2",
            "insert into t (a) values (%s) on duplicate key update a = a % 2",
        ),
        ("update t set a = %s where n like 'a%'", "update t set a = %s where n like 'a%%'"),
    ],
)
async def test_execute_many_percent_escaping_respects_bulk_insert_shape(
    adapter: str, class_name: str, sql: str, expected: str
) -> None:
    from unittest.mock import AsyncMock, Mock

    module = import_module(f"sqlspec.adapters.{adapter}.driver")
    driver = getattr(module, class_name)(Mock())
    cursor = Mock(rowcount=1)
    if adapter != "pymysql":
        cursor.executemany = AsyncMock()
    statement = SQL(sql, [(1,)], statement_config=driver.statement_config, is_many=True)
    result = driver.dispatch_execute_many(cursor, statement)
    if adapter != "pymysql":
        await result
    assert cursor.executemany.call_args.args[0] == expected


@pytest.mark.parametrize(
    "adapter,class_name", [("pymysql", "PyMysqlDriver"), ("aiomysql", "AiomysqlDriver"), ("asyncmy", "AsyncmyDriver")]
)
async def test_cached_literal_placeholder_is_escaped(adapter: str, class_name: str) -> None:
    from unittest.mock import AsyncMock, Mock, patch

    module = import_module(f"sqlspec.adapters.{adapter}.driver")
    driver_type = getattr(module, class_name)
    driver = driver_type(Mock())
    cached = Mock(compiled_sql="select '%s', %s")
    execute = Mock(return_value="result") if adapter == "pymysql" else AsyncMock(return_value="result")
    with (
        patch.object(driver_type, "_cached_statement", return_value=Mock()),
        patch.object(driver_type, "_execute_cached_statement", execute),
    ):
        result = driver._execute_cache_hit("select '%s', ?", (1,), cached)
        if adapter != "pymysql":
            result = await result
    assert result == "result"
    execute.assert_called_once()
