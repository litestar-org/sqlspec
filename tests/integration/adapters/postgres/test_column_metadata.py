"""PostgreSQL column metadata coverage for keys, identity columns, and owned sequences."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg import AsyncpgDriver
    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.data_dictionary import ColumnMetadata

pytestmark = pytest.mark.xdist_group("postgres")

SETUP_SQL = """
DROP SCHEMA IF EXISTS column_metadata CASCADE;
CREATE SCHEMA column_metadata;
CREATE TABLE column_metadata."MixedItems" (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "userName" TEXT NOT NULL,
    counter SERIAL,
    note TEXT
);
"""
TEARDOWN_SQL = "DROP SCHEMA IF EXISTS column_metadata CASCADE"


def _metadata(columns: "list[ColumnMetadata]") -> "dict[str, tuple[bool, str, object]]":
    return {
        str(column["column_name"]): (
            bool(column["is_primary"]),
            str(column.get("identity_generation") or ""),
            column.get("sequence_name"),
        )
        for column in columns
    }


EXPECTED = {
    "id": (True, "a", 'column_metadata."MixedItems_id_seq"'),
    "userName": (False, "", None),
    "counter": (False, "", 'column_metadata."MixedItems_counter_seq"'),
    "note": (False, "", None),
}


async def test_columns_report_keys_identity_and_sequences(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """Exact-case tables report primary keys, identity kinds, and owned sequences."""
    driver = asyncpg_async_driver
    await driver.execute_script(SETUP_SQL)
    try:
        columns = await driver.data_dictionary.get_columns(driver, table='"MixedItems"', schema="column_metadata")

        assert _metadata(columns) == EXPECTED
    finally:
        await driver.execute_script(TEARDOWN_SQL)


async def test_unqualified_columns_resolve_through_search_path(asyncpg_async_driver: "AsyncpgDriver") -> None:
    """A table without a schema is found through the session search path."""
    driver = asyncpg_async_driver
    await driver.execute_script(SETUP_SQL)
    try:
        await driver.execute_script("SET search_path TO public, column_metadata")

        columns = await driver.data_dictionary.get_columns(driver, table='"MixedItems"')

        assert _metadata(columns) == EXPECTED
        assert {column["schema_name"] for column in columns} == {"column_metadata"}
    finally:
        await driver.execute_script(f"RESET search_path; {TEARDOWN_SQL}")


def test_columns_report_keys_identity_and_sequences_sync(psycopg_sync_config: "PsycopgSyncConfig") -> None:
    """The sync dictionary reports the same column metadata."""
    with psycopg_sync_config.provide_session() as driver:
        driver.execute_script(SETUP_SQL)
        try:
            columns = driver.data_dictionary.get_columns(driver, table='"MixedItems"', schema="column_metadata")

            assert _metadata(columns) == EXPECTED
        finally:
            driver.execute_script(TEARDOWN_SQL)
