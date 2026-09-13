"""MySQL column metadata coverage for primary keys and column types."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy import AsyncmyDriver

pytestmark = pytest.mark.xdist_group("mysql")


async def test_columns_report_primary_keys_and_column_types(asyncmy_driver: "AsyncmyDriver") -> None:
    """Column metadata for one table flags primary-key columns and reports full column types."""
    driver = asyncmy_driver
    await driver.execute_script(
        "DROP TABLE IF EXISTS `column_metadata_items`; "
        "CREATE TABLE `column_metadata_items` (tenant VARCHAR(20), item_id INT, amount DECIMAL(10, 2), "
        "PRIMARY KEY (tenant, item_id))"
    )
    try:
        columns = await driver.data_dictionary.get_columns(driver, table="column_metadata_items")

        assert [(column["column_name"], bool(column["is_primary"]), column["column_type"]) for column in columns] == [
            ("tenant", True, "varchar(20)"),
            ("item_id", True, "int"),
            ("amount", False, "decimal(10,2)"),
        ]
    finally:
        await driver.execute_script("DROP TABLE IF EXISTS `column_metadata_items`")
