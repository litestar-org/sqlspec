"""MySQL coverage for table fixture loading and export."""

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sqlspec.utils.fixtures import export_table_fixtures_async, load_table_fixtures_async

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy import AsyncmyDriver

pytestmark = pytest.mark.xdist_group("mysql")

SETUP_SQL = """
DROP TABLE IF EXISTS `fixture_order`;
CREATE TABLE `fixture_order` (
    id INT PRIMARY KEY,
    `userName` VARCHAR(50) NOT NULL,
    `group` VARCHAR(50) NOT NULL,
    created_at DATETIME(6),
    day DATE,
    amount DECIMAL(12, 4),
    payload BLOB,
    document JSON,
    doubled INT GENERATED ALWAYS AS (id * 2) VIRTUAL,
    shout VARCHAR(50) GENERATED ALWAYS AS (UPPER(`userName`)) STORED
);
INSERT INTO `fixture_order` (id, `userName`, `group`, created_at, day, amount, payload, document) VALUES
    (1, 'Ann', 'a', '2024-01-02 03:04:05.123456', '2024-01-02', 12.3450, X'00FF10', '{"a": [1, "x"]}'),
    (2, 'Ben', 'b', NULL, NULL, NULL, NULL, '[1, "two", null]');
"""
TEARDOWN_SQL = "DROP TABLE IF EXISTS `fixture_order`"
SELECT_SQL = "SELECT * FROM `fixture_order` ORDER BY id"


async def test_export_load_roundtrip_and_upsert(asyncmy_driver: "AsyncmyDriver", tmp_path: Path) -> None:
    """Typed columns round-trip and conflict keys upsert with ON DUPLICATE KEY UPDATE."""
    driver = asyncmy_driver
    await driver.execute_script(SETUP_SQL)
    try:
        before = await driver.select(SELECT_SQL)

        await export_table_fixtures_async(driver, tmp_path, ["fixture_order"])
        await driver.execute("DELETE FROM `fixture_order`")
        counts = await load_table_fixtures_async(driver, tmp_path)

        assert counts == {"fixture_order": 2}
        assert await driver.select(SELECT_SQL) == before

        (tmp_path / "fixture_order.json.gz").unlink()
        (tmp_path / "fixture_order.json").write_text(
            json.dumps([{"id": 1, "userName": "Ann", "group": "z"}, {"id": 3, "userName": "Cy", "group": "c"}]),
            encoding="utf-8",
        )
        await load_table_fixtures_async(driver, tmp_path, conflict_keys={"fixture_order": ["id"]})

        rows = await driver.select("SELECT id, `group` FROM `fixture_order` ORDER BY id")
        assert rows == [{"id": 1, "group": "z"}, {"id": 2, "group": "b"}, {"id": 3, "group": "c"}]
    finally:
        await driver.execute_script(TEARDOWN_SQL)


async def test_generated_columns_are_skipped(asyncmy_driver: "AsyncmyDriver", tmp_path: Path) -> None:
    """Virtual and stored generated columns are left out of exports and ignored in loaded files."""
    driver = asyncmy_driver
    await driver.execute_script(SETUP_SQL)
    try:
        await export_table_fixtures_async(driver, tmp_path / "out", ["fixture_order"], compress=False)
        exported = json.loads((tmp_path / "out" / "fixture_order.json").read_text())
        rows = [{"id": 4, "userName": "Di", "group": "d", "doubled": 0, "shout": "x"}]
        (tmp_path / "fixture_order.json").write_text(json.dumps(rows), encoding="utf-8")
        await driver.execute("DELETE FROM `fixture_order`")

        await load_table_fixtures_async(driver, tmp_path)

        assert all("doubled" not in row and "shout" not in row for row in exported)
        loaded = await driver.select("SELECT id, doubled, shout FROM `fixture_order`")
        assert loaded == [{"id": 4, "doubled": 8, "shout": "DI"}]
    finally:
        await driver.execute_script(TEARDOWN_SQL)
