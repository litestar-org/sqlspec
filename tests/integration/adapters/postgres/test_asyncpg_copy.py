"""AsyncPG COPY integration coverage."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.asyncpg.core import default_statement_config
from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg import AsyncpgDriver

pytestmark = pytest.mark.xdist_group("postgres")


async def test_asyncpg_copy_from_stdin_uses_public_driver_path(contract_asyncpg_driver: "AsyncpgDriver") -> None:
    await contract_asyncpg_driver.execute_script(
        "DROP TABLE IF EXISTS asyncpg_copy_items; CREATE TABLE asyncpg_copy_items (id INTEGER, name TEXT)"
    )
    try:
        csv_config = default_statement_config.replace(
            execution_args={
                "postgres_copy_data": "1,Alice\n2,Bob",
                "postgres_copy_columns": ("id", "name"),
                "postgres_copy_format": "csv",
                "postgres_copy_delimiter": ",",
            }
        )
        await contract_asyncpg_driver.execute(
            SQL("COPY asyncpg_copy_items (id, name) FROM STDIN", statement_config=csv_config)
        )

        metadata_config = default_statement_config.replace(
            execution_args={"postgres_copy_data": "3\tCarol", "postgres_copy_table": "asyncpg_copy_items"}
        )
        await contract_asyncpg_driver.execute(SQL("COPY metadata_target FROM STDIN", statement_config=metadata_config))

        result = await contract_asyncpg_driver.select("SELECT id, name FROM asyncpg_copy_items ORDER BY id")
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Carol"}]
    finally:
        await contract_asyncpg_driver.execute_script("DROP TABLE IF EXISTS asyncpg_copy_items")


async def test_asyncpg_copy_from_stdin_requires_table_name(contract_asyncpg_driver: "AsyncpgDriver") -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1"})

    with pytest.raises(SQLSpecError, match="postgres_copy_table"):
        await contract_asyncpg_driver.execute(SQL("COPY (SELECT 1) FROM STDIN", statement_config=config))
