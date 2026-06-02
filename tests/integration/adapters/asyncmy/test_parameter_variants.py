"""AsyncMy-specific parameter variants not covered by generic contracts."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.asyncmy import AsyncmyDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def asyncmy_parameter_variants(asyncmy_clean_driver: AsyncmyDriver) -> AsyncGenerator[AsyncmyDriver, None]:
    """Provide AsyncMy data for native pyformat parameter variants."""
    await asyncmy_clean_driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_parameter_variants_asyncmy (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            description TEXT
        )
    """)
    await asyncmy_clean_driver.execute_script("DELETE FROM test_parameter_variants_asyncmy")
    await asyncmy_clean_driver.execute_many(
        "INSERT INTO test_parameter_variants_asyncmy (name, value, description) VALUES (?, ?, ?)",
        [("test1", 100, "First test"), ("test2", 200, "Second test"), ("test3", 300, None)],
    )
    yield asyncmy_clean_driver
    await asyncmy_clean_driver.execute_script("DROP TABLE IF EXISTS test_parameter_variants_asyncmy")


async def test_asyncmy_native_pyformat_select(asyncmy_parameter_variants: AsyncmyDriver) -> None:
    """AsyncMy accepts native positional pyformat parameters."""
    result = await asyncmy_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_asyncmy WHERE name = %s AND value > %s", ("test2", 150)
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


async def test_asyncmy_named_pyformat_select(asyncmy_parameter_variants: AsyncmyDriver) -> None:
    """AsyncMy converts named pyformat parameters."""
    result = await asyncmy_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_asyncmy WHERE name = %(name)s AND value < %(maximum)s",
        {"name": "test3", "maximum": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test3", "value": 300}]


async def test_asyncmy_sql_object_native_pyformat(asyncmy_parameter_variants: AsyncmyDriver) -> None:
    """AsyncMy accepts native pyformat parameters inside SQL objects."""
    statement = SQL("SELECT name, value FROM test_parameter_variants_asyncmy WHERE value BETWEEN %s AND %s", 150, 250)
    result = await asyncmy_parameter_variants.execute(statement)

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


async def test_asyncmy_execute_many_native_pyformat(asyncmy_parameter_variants: AsyncmyDriver) -> None:
    """AsyncMy binds native pyformat parameters in execute_many."""
    result = await asyncmy_parameter_variants.execute_many(
        "INSERT INTO test_parameter_variants_asyncmy (name, value, description) VALUES (%s, %s, %s)",
        [("batch1", 1000, "Batch 1"), ("batch2", 2000, "Batch 2")],
    )
    selected = await asyncmy_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_asyncmy WHERE name LIKE ? ORDER BY name", ("batch%",)
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 2
    assert selected.get_data() == [{"name": "batch1", "value": 1000}, {"name": "batch2", "value": 2000}]
