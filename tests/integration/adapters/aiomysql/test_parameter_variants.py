"""aiomysql-specific parameter variants not covered by generic contracts."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.aiomysql import AiomysqlDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def aiomysql_parameter_variants(aiomysql_clean_driver: AiomysqlDriver) -> AsyncGenerator[AiomysqlDriver, None]:
    """Provide aiomysql data for native pyformat parameter variants."""
    await aiomysql_clean_driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_parameter_variants_aiomysql (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            description TEXT
        )
    """)
    await aiomysql_clean_driver.execute_script("DELETE FROM test_parameter_variants_aiomysql")
    await aiomysql_clean_driver.execute_many(
        "INSERT INTO test_parameter_variants_aiomysql (name, value, description) VALUES (?, ?, ?)",
        [("test1", 100, "First test"), ("test2", 200, "Second test"), ("test3", 300, None)],
    )
    yield aiomysql_clean_driver
    await aiomysql_clean_driver.execute_script("DROP TABLE IF EXISTS test_parameter_variants_aiomysql")


async def test_aiomysql_native_pyformat_select(aiomysql_parameter_variants: AiomysqlDriver) -> None:
    """aiomysql accepts native positional pyformat parameters."""
    result = await aiomysql_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_aiomysql WHERE name = %s AND value > %s", ("test2", 150)
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


async def test_aiomysql_named_pyformat_select(aiomysql_parameter_variants: AiomysqlDriver) -> None:
    """aiomysql converts named pyformat parameters."""
    result = await aiomysql_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_aiomysql WHERE name = %(name)s AND value < %(maximum)s",
        {"name": "test3", "maximum": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test3", "value": 300}]


async def test_aiomysql_sql_object_native_pyformat(aiomysql_parameter_variants: AiomysqlDriver) -> None:
    """aiomysql accepts native pyformat parameters inside SQL objects."""
    statement = SQL("SELECT name, value FROM test_parameter_variants_aiomysql WHERE value BETWEEN %s AND %s", 150, 250)
    result = await aiomysql_parameter_variants.execute(statement)

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


async def test_aiomysql_execute_many_native_pyformat(aiomysql_parameter_variants: AiomysqlDriver) -> None:
    """aiomysql binds native pyformat parameters in execute_many."""
    result = await aiomysql_parameter_variants.execute_many(
        "INSERT INTO test_parameter_variants_aiomysql (name, value, description) VALUES (%s, %s, %s)",
        [("batch1", 1000, "Batch 1"), ("batch2", 2000, "Batch 2")],
    )
    selected = await aiomysql_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_aiomysql WHERE name LIKE ? ORDER BY name", ("batch%",)
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 2
    assert selected.get_data() == [{"name": "batch1", "value": 1000}, {"name": "batch2", "value": 2000}]
