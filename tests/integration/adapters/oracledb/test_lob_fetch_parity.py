"""Oracle LOB fetch parity for streaming and Arrow paths."""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleBlob, OracleClob, OracleSyncDriver

pytestmark = pytest.mark.xdist_group("oracle")

_CLOB_TEXT = "streamed oracle clob payload"
_BLOB_BYTES = b"streamed-oracle-blob"


def _drop_sync(driver: "OracleSyncDriver", table_name: str) -> None:
    driver.execute_script(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def _drop_async(driver: "OracleAsyncDriver", table_name: str) -> None:
    await driver.execute_script(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_stream_returns_concrete_lobs_by_default(oracle_sync_session: "OracleSyncDriver") -> None:
    """Native sync streaming should inherit the default ``fetch_lobs=False`` behavior."""
    table_name = "lob_fetch_stream_sync"
    _drop_sync(oracle_sync_session, table_name)
    oracle_sync_session.execute_script(f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, content CLOB, data BLOB)")
    oracle_sync_session.execute(
        f"INSERT INTO {table_name} (id, content, data) VALUES (:id, :content, :data)",
        {"id": 1, "content": OracleClob(_CLOB_TEXT), "data": OracleBlob(_BLOB_BYTES)},
    )

    try:
        rows = list(oracle_sync_session.select_stream(f"SELECT content, data FROM {table_name}", chunk_size=1))

        assert rows == [{"content": _CLOB_TEXT, "data": _BLOB_BYTES}]
    finally:
        _drop_sync(oracle_sync_session, table_name)


def test_sync_stream_fetch_lobs_true_keeps_locator(oracle_sync_session: "OracleSyncDriver") -> None:
    """Per-call ``fetch_lobs=True`` keeps the Oracle locator escape hatch."""
    table_name = "lob_fetch_stream_locator_sync"
    _drop_sync(oracle_sync_session, table_name)
    oracle_sync_session.execute_script(f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, content CLOB)")
    oracle_sync_session.execute(
        f"INSERT INTO {table_name} (id, content) VALUES (:id, :content)",
        {"id": 1, "content": OracleClob(_CLOB_TEXT)},
    )

    try:
        with oracle_sync_session.select_stream(
            f"SELECT content FROM {table_name}", chunk_size=1, fetch_lobs=True
        ) as stream:
            row = next(stream)
            locator = row["content"]
            assert callable(locator.read)
            assert locator.read() == _CLOB_TEXT
    finally:
        _drop_sync(oracle_sync_session, table_name)


async def test_async_stream_returns_concrete_lobs_by_default(oracle_async_session: "OracleAsyncDriver") -> None:
    """Native async streaming should inherit the default ``fetch_lobs=False`` behavior."""
    table_name = "lob_fetch_stream_async"
    await _drop_async(oracle_async_session, table_name)
    await oracle_async_session.execute_script(
        f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, content CLOB, data BLOB)"
    )
    await oracle_async_session.execute(
        f"INSERT INTO {table_name} (id, content, data) VALUES (:id, :content, :data)",
        {"id": 1, "content": OracleClob(_CLOB_TEXT), "data": OracleBlob(_BLOB_BYTES)},
    )

    try:
        rows: list[dict[str, object]] = [
            row async for row in oracle_async_session.select_stream(f"SELECT content, data FROM {table_name}", chunk_size=1)
        ]

        assert rows == [{"content": _CLOB_TEXT, "data": _BLOB_BYTES}]
    finally:
        await _drop_async(oracle_async_session, table_name)


async def test_arrow_export_returns_concrete_clob_values(oracle_async_session: "OracleAsyncDriver") -> None:
    """Oracle Arrow export should forward ``fetch_lobs=False`` and return CLOB text."""
    table_name = "lob_fetch_arrow_async"
    await _drop_async(oracle_async_session, table_name)
    await oracle_async_session.execute_script(f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, content CLOB)")
    await oracle_async_session.execute(
        f"INSERT INTO {table_name} (id, content) VALUES (:id, :content)",
        {"id": 1, "content": OracleClob(_CLOB_TEXT)},
    )

    try:
        result = await oracle_async_session.select_to_arrow(f"SELECT content FROM {table_name}")

        assert result.get_data().to_pydict() == {"content": [_CLOB_TEXT]}
    finally:
        await _drop_async(oracle_async_session, table_name)
