"""CockroachDB column metadata coverage for keys, identity columns, and owned sequences."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

pytestmark = pytest.mark.xdist_group("cockroachdb")

SETUP_SQL = """
DROP TABLE IF EXISTS "ColumnMetadataItems";
CREATE TABLE "ColumnMetadataItems" (
    id INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "userName" STRING NOT NULL,
    note STRING
);
"""
TEARDOWN_SQL = 'DROP TABLE IF EXISTS "ColumnMetadataItems"'


async def test_columns_report_keys_identity_and_sequences(cockroach_asyncpg_config: "CockroachAsyncpgConfig") -> None:
    """Exact-case tables report primary keys, identity kinds, and owned sequences."""
    async with cockroach_asyncpg_config.provide_session() as driver:
        await driver.execute_script(SETUP_SQL)
        try:
            columns = await driver.data_dictionary.get_columns(driver, table='"ColumnMetadataItems"')

            metadata = {
                column["column_name"]: (
                    bool(column["is_primary"]),
                    column.get("identity_generation") or "",
                    column.get("sequence_name"),
                )
                for column in columns
                if column.get("is_hidden") != "YES"
            }
            assert metadata == {
                "id": (True, "a", 'public."ColumnMetadataItems_id_seq"'),
                "userName": (False, "", None),
                "note": (False, "", None),
            }
        finally:
            await driver.execute_script(TEARDOWN_SQL)
