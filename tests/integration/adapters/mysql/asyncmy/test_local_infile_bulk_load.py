"""Native asyncmy LOCAL INFILE behavior against MySQL."""

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path

import pyarrow as pa
import pytest
from asyncmy.cursors import Cursor, SSCursor, SSDictCursor
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.exceptions import SQLSpecError

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.asyncmy]


@pytest.fixture(params=[Cursor, SSCursor, SSDictCursor], ids=["buffered", "unbuffered", "unbuffered_dict"])
async def asyncmy_infile_config(
    mysql_service: MySQLService, request: pytest.FixtureRequest
) -> AsyncGenerator[AsyncmyConfig, None]:
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "db": mysql_service.db,
            "autocommit": True,
            "cursor_cls": request.param,
            "allow_local_infile": True,
        }
    )
    original = 0
    try:
        async with config.provide_session() as driver:
            original = await driver.select_value("SELECT @@GLOBAL.local_infile")
            await driver.execute("SET GLOBAL local_infile = 1")
            await driver.execute("DROP TABLE IF EXISTS asyncmy_native_infile")
            await driver.execute(
                "CREATE TABLE asyncmy_native_infile (id INT PRIMARY KEY, text_value LONGTEXT, flag BOOLEAN) "
                "CHARACTER SET utf8mb4"
            )
        yield config
    finally:
        try:
            async with config.provide_session() as driver:
                await driver.execute("DROP TABLE IF EXISTS asyncmy_native_infile")
                await driver.execute("SET GLOBAL local_infile = 1" if original else "SET GLOBAL local_infile = 0")
        finally:
            await config.close_pool()


async def test_native_bulk_roundtrip_multichunk_and_overwrite(
    asyncmy_infile_config: AsyncmyConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tempfile

    tmp_path = tmp_path / "quoted'percent%雪"
    tmp_path.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    texts = ["é\tline\nreturn\rback\\slash\x00\x1a", None, "", "雪" * 400_000]
    arrow = pa.table({"id": [1, 2, 3, 4], "text_value": texts, "flag": [True, False, True, False]})
    async with asyncmy_infile_config.provide_session() as driver:
        before = await driver.select_one("SHOW SESSION STATUS LIKE 'Com_load'")
        job = await driver.load_from_arrow("asyncmy_native_infile", arrow)
        after = await driver.select_one("SHOW SESSION STATUS LIKE 'Com_load'")
        assert int(after["Value"]) == int(before["Value"]) + 1
        assert job.telemetry["rows_processed"] == 4
        rows = await driver.select("SELECT id, text_value, flag FROM asyncmy_native_infile ORDER BY id")
        assert [row["text_value"] for row in rows] == texts
        assert [row["flag"] for row in rows] == [1, 0, 1, 0]
        assert list(tmp_path.iterdir()) == []
        await driver.load_from_arrow("asyncmy_native_infile", arrow.slice(0, 1), overwrite=True)
        assert await driver.select_value("SELECT COUNT(*) FROM asyncmy_native_infile") == 1
        await driver.load_from_arrow("asyncmy_native_infile", arrow.slice(0, 0))
        assert await driver.select_value("SELECT COUNT(*) FROM asyncmy_native_infile") == 1
        await driver.execute(
            "INSERT INTO asyncmy_native_infile (id, text_value) VALUES (:id, :value)", {"id": 5, "value": "after"}
        )
        assert await driver.select_value("SELECT COUNT(*) FROM asyncmy_native_infile") == 2


async def test_native_server_error_cleans_payload_and_pool_recovers(
    asyncmy_infile_config: AsyncmyConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tempfile

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    async with asyncmy_infile_config.provide_session() as driver:
        connection = driver.connection
        with pytest.raises(SQLSpecError):
            await driver.load_from_arrow("asyncmy_native_infile_missing", pa.table({"id": [1]}))
        assert not connection.connected
    assert list(tmp_path.iterdir()) == []  # noqa: ASYNC240
    async with asyncmy_infile_config.provide_session() as driver:
        assert await driver.select_value("SELECT 1") == 1


async def test_native_cancellation_discards_connection_and_pool_recovers(
    asyncmy_infile_config: AsyncmyConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tempfile

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    async with asyncmy_infile_config.provide_session() as blocker:
        await blocker.execute("LOCK TABLES asyncmy_native_infile WRITE")
        try:
            async with asyncmy_infile_config.provide_session() as driver:
                connection = driver.connection
                connection_id = await driver.select_value("SELECT CONNECTION_ID()")
                task = asyncio.create_task(driver.load_from_arrow("asyncmy_native_infile", pa.table({"id": [1]})))
                try:
                    async with asyncmy_infile_config.provide_session() as observer:
                        for _ in range(200):
                            process = await observer.select_one_or_none(
                                "SELECT INFO FROM information_schema.PROCESSLIST WHERE ID = :id", {"id": connection_id}
                            )
                            if process and str(process["INFO"]).startswith("LOAD DATA LOCAL INFILE"):
                                break
                            await asyncio.sleep(0.01)
                        else:
                            pytest.fail("Native LOAD DATA query did not reach MySQL")
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    assert not connection.connected
                finally:
                    if not task.done():
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await task
        finally:
            await blocker.execute("UNLOCK TABLES")
    assert list(tmp_path.iterdir()) == []  # noqa: ASYNC240
    async with asyncmy_infile_config.provide_session() as driver:
        assert await driver.select_value("SELECT 1") == 1
