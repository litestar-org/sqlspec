"""Test Oracle CLOB automatic hydration with msgspec.

Tests that CLOB values are automatically read into strings before msgspec
struct hydration, preventing LOB handle leakage into user code.
"""

import json

import msgspec
import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver

pytestmark = pytest.mark.xdist_group("oracle")


class DocumentRecord(msgspec.Struct):
    """Document record with CLOB content."""

    id: int
    content: str


class NullableDocumentRecord(msgspec.Struct):
    """Document record with nullable CLOB content."""

    id: int
    content: str | None


class ArticleRecord(msgspec.Struct):
    """Article with CLOB and VARCHAR2 fields."""

    id: int
    title: str
    body: str


class JsonDocumentRecord(msgspec.Struct):
    """Document with JSON data stored in CLOB."""

    id: int
    metadata: dict


class BinaryDocumentRecord(msgspec.Struct):
    """Document with BLOB binary data."""

    id: int
    data: bytes


LARGE_TEXT_CONTENT = "x" * 5000
LARGE_JSON_CONTENT = {"key": "value", "nested": {"data": "x" * 5000}}
LARGE_BINARY_CONTENT = b"\x00\x01\x02\x03" * 2000


@pytest.mark.asyncio
async def test_oracle_async_clob_msgspec_hydration(oracle_async_session: OracleAsyncDriver) -> None:
    """Test async CLOB automatic hydration into msgspec struct.

    Verifies that CLOB values are read into strings before msgspec sees them,
    preventing AsyncLOB handle leakage.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_clob_msgspec'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_clob_msgspec (
            id NUMBER PRIMARY KEY,
            content CLOB
        )
    """)

    await oracle_async_session.execute(
        "INSERT INTO test_clob_msgspec (id, content) VALUES (:1, :2)", (1, LARGE_TEXT_CONTENT)
    )

    result = await oracle_async_session.execute("SELECT id, content FROM test_clob_msgspec WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=DocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, DocumentRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.content, str)
    assert hydrated.content == LARGE_TEXT_CONTENT
    assert len(hydrated.content) == 5000

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_clob_msgspec'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def test_oracle_sync_clob_msgspec_hydration(oracle_sync_session: OracleSyncDriver) -> None:
    """Test sync CLOB automatic hydration into msgspec struct.

    Verifies that CLOB values are read into strings before msgspec sees them,
    preventing LOB handle leakage.
    """
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_clob_msgspec_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_clob_msgspec_sync (
            id NUMBER PRIMARY KEY,
            content CLOB
        )
    """)

    oracle_sync_session.execute(
        "INSERT INTO test_clob_msgspec_sync (id, content) VALUES (:1, :2)", (1, LARGE_TEXT_CONTENT)
    )

    result = oracle_sync_session.execute("SELECT id, content FROM test_clob_msgspec_sync WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=DocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, DocumentRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.content, str)
    assert hydrated.content == LARGE_TEXT_CONTENT
    assert len(hydrated.content) == 5000

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_clob_msgspec_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


@pytest.mark.asyncio
async def test_oracle_async_mixed_clob_varchar2_msgspec(oracle_async_session: OracleAsyncDriver) -> None:
    """Test msgspec hydration with mixed CLOB and VARCHAR2 columns.

    Verifies that both VARCHAR2 and CLOB columns work correctly in the same struct.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_types'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_mixed_types (
            id NUMBER PRIMARY KEY,
            title VARCHAR2(200),
            body CLOB
        )
    """)

    await oracle_async_session.execute(
        "INSERT INTO test_mixed_types (id, title, body) VALUES (:1, :2, :3)", (1, "Short Title", LARGE_TEXT_CONTENT)
    )

    result = await oracle_async_session.execute("SELECT id, title, body FROM test_mixed_types WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=ArticleRecord)
    assert hydrated is not None
    assert isinstance(hydrated, ArticleRecord)
    assert hydrated.id == 1
    assert hydrated.title == "Short Title"
    assert isinstance(hydrated.body, str)
    assert hydrated.body == LARGE_TEXT_CONTENT

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_types'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def test_oracle_sync_mixed_clob_varchar2_msgspec(oracle_sync_session: OracleSyncDriver) -> None:
    """Test msgspec hydration with mixed CLOB and VARCHAR2 columns (sync).

    Verifies that both VARCHAR2 and CLOB columns work correctly in the same struct.
    """
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_mixed_sync (
            id NUMBER PRIMARY KEY,
            title VARCHAR2(200),
            body CLOB
        )
    """)

    oracle_sync_session.execute(
        "INSERT INTO test_mixed_sync (id, title, body) VALUES (:1, :2, :3)", (1, "Short Title", LARGE_TEXT_CONTENT)
    )

    result = oracle_sync_session.execute("SELECT id, title, body FROM test_mixed_sync WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=ArticleRecord)
    assert hydrated is not None
    assert isinstance(hydrated, ArticleRecord)
    assert hydrated.id == 1
    assert hydrated.title == "Short Title"
    assert isinstance(hydrated.body, str)
    assert hydrated.body == LARGE_TEXT_CONTENT

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


@pytest.mark.asyncio
async def test_oracle_async_json_in_clob_detection(oracle_async_session: OracleAsyncDriver) -> None:
    """Test JSON detection in CLOB with msgspec hydration.

    Verifies that JSON stored in CLOB is automatically detected and parsed,
    then successfully hydrated into msgspec struct.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_json_clob'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_json_clob (
            id NUMBER PRIMARY KEY,
            metadata CLOB
        )
    """)

    json_text = json.dumps(LARGE_JSON_CONTENT)
    await oracle_async_session.execute("INSERT INTO test_json_clob (id, metadata) VALUES (:1, :2)", (1, json_text))

    result = await oracle_async_session.execute("SELECT id, metadata FROM test_json_clob WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=JsonDocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, JsonDocumentRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.metadata, dict)
    assert hydrated.metadata == LARGE_JSON_CONTENT
    assert hydrated.metadata["key"] == "value"
    assert len(hydrated.metadata["nested"]["data"]) == 5000

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_json_clob'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def test_oracle_sync_json_in_clob_detection(oracle_sync_session: OracleSyncDriver) -> None:
    """Test JSON detection in CLOB with msgspec hydration (sync).

    Verifies that JSON stored in CLOB is automatically detected and parsed,
    then successfully hydrated into msgspec struct.
    """
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_json_clob_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_json_clob_sync (
            id NUMBER PRIMARY KEY,
            metadata CLOB
        )
    """)

    json_text = json.dumps(LARGE_JSON_CONTENT)
    oracle_sync_session.execute("INSERT INTO test_json_clob_sync (id, metadata) VALUES (:1, :2)", (1, json_text))

    result = oracle_sync_session.execute("SELECT id, metadata FROM test_json_clob_sync WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=JsonDocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, JsonDocumentRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.metadata, dict)
    assert hydrated.metadata == LARGE_JSON_CONTENT
    assert hydrated.metadata["key"] == "value"
    assert len(hydrated.metadata["nested"]["data"]) == 5000

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_json_clob_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


@pytest.mark.asyncio
async def test_oracle_async_blob_remains_bytes(oracle_async_session: OracleAsyncDriver) -> None:
    """Test that BLOB columns still return bytes, not strings.

    Verifies binary data handling is unchanged and BLOB values are not
    converted to strings.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_blob_msgspec'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_blob_msgspec (
            id NUMBER PRIMARY KEY,
            data BLOB
        )
    """)

    await oracle_async_session.execute(
        "INSERT INTO test_blob_msgspec (id, data) VALUES (:1, :2)", (1, LARGE_BINARY_CONTENT)
    )

    result = await oracle_async_session.execute("SELECT id, data FROM test_blob_msgspec WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=BinaryDocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, BinaryDocumentRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.data, bytes)
    assert hydrated.data == LARGE_BINARY_CONTENT
    assert len(hydrated.data) == 8000

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_blob_msgspec'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def test_oracle_sync_blob_remains_bytes(oracle_sync_session: OracleSyncDriver) -> None:
    """Test that BLOB columns still return bytes, not strings (sync).

    Verifies binary data handling is unchanged and BLOB values are not
    converted to strings.
    """
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_blob_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_blob_sync (
            id NUMBER PRIMARY KEY,
            data BLOB
        )
    """)

    oracle_sync_session.execute("INSERT INTO test_blob_sync (id, data) VALUES (:1, :2)", (1, LARGE_BINARY_CONTENT))

    result = oracle_sync_session.execute("SELECT id, data FROM test_blob_sync WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=BinaryDocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, BinaryDocumentRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.data, bytes)
    assert hydrated.data == LARGE_BINARY_CONTENT
    assert len(hydrated.data) == 8000

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_blob_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


@pytest.mark.asyncio
async def test_oracle_async_multiple_clob_columns(oracle_async_session: OracleAsyncDriver) -> None:
    """Test msgspec hydration with multiple CLOB columns.

    Verifies that all CLOB columns are properly read and hydrated.
    """

    class MultiClobRecord(msgspec.Struct):
        id: int
        content1: str
        content2: str
        content3: str

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_multi_clob'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_multi_clob (
            id NUMBER PRIMARY KEY,
            content1 CLOB,
            content2 CLOB,
            content3 CLOB
        )
    """)

    content1 = "a" * 5000
    content2 = "b" * 6000
    content3 = "c" * 7000

    await oracle_async_session.execute(
        "INSERT INTO test_multi_clob (id, content1, content2, content3) VALUES (:1, :2, :3, :4)",
        (1, content1, content2, content3),
    )

    result = await oracle_async_session.execute(
        "SELECT id, content1, content2, content3 FROM test_multi_clob WHERE id = :1", (1,)
    )

    hydrated = result.get_first(schema_type=MultiClobRecord)
    assert hydrated is not None
    assert isinstance(hydrated, MultiClobRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.content1, str)
    assert isinstance(hydrated.content2, str)
    assert isinstance(hydrated.content3, str)
    assert hydrated.content1 == content1
    assert hydrated.content2 == content2
    assert hydrated.content3 == content3
    assert len(hydrated.content1) == 5000
    assert len(hydrated.content2) == 6000
    assert len(hydrated.content3) == 7000

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_multi_clob'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def test_oracle_sync_multiple_clob_columns(oracle_sync_session: OracleSyncDriver) -> None:
    """Test msgspec hydration with multiple CLOB columns (sync).

    Verifies that all CLOB columns are properly read and hydrated.
    """

    class MultiClobRecord(msgspec.Struct):
        id: int
        content1: str
        content2: str
        content3: str

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_multi_clob_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_multi_clob_sync (
            id NUMBER PRIMARY KEY,
            content1 CLOB,
            content2 CLOB,
            content3 CLOB
        )
    """)

    content1 = "a" * 5000
    content2 = "b" * 6000
    content3 = "c" * 7000

    oracle_sync_session.execute(
        "INSERT INTO test_multi_clob_sync (id, content1, content2, content3) VALUES (:1, :2, :3, :4)",
        (1, content1, content2, content3),
    )

    result = oracle_sync_session.execute(
        "SELECT id, content1, content2, content3 FROM test_multi_clob_sync WHERE id = :1", (1,)
    )

    hydrated = result.get_first(schema_type=MultiClobRecord)
    assert hydrated is not None
    assert isinstance(hydrated, MultiClobRecord)
    assert hydrated.id == 1
    assert isinstance(hydrated.content1, str)
    assert isinstance(hydrated.content2, str)
    assert isinstance(hydrated.content3, str)
    assert hydrated.content1 == content1
    assert hydrated.content2 == content2
    assert hydrated.content3 == content3
    assert len(hydrated.content1) == 5000
    assert len(hydrated.content2) == 6000
    assert len(hydrated.content3) == 7000

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_multi_clob_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


@pytest.mark.asyncio
async def test_oracle_async_empty_clob_msgspec(oracle_async_session: OracleAsyncDriver) -> None:
    """Test msgspec hydration with NULL CLOB values.

    Verifies that NULL CLOBs are handled correctly. Oracle returns NULL for
    empty string CLOBs.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_empty_clob'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_empty_clob (
            id NUMBER PRIMARY KEY,
            content CLOB
        )
    """)

    await oracle_async_session.execute("INSERT INTO test_empty_clob (id, content) VALUES (:1, :2)", (1, ""))

    result = await oracle_async_session.execute("SELECT id, content FROM test_empty_clob WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=NullableDocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, NullableDocumentRecord)
    assert hydrated.id == 1
    assert hydrated.content is None

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_empty_clob'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def test_oracle_sync_empty_clob_msgspec(oracle_sync_session: OracleSyncDriver) -> None:
    """Test msgspec hydration with NULL CLOB values (sync).

    Verifies that NULL CLOBs are handled correctly. Oracle returns NULL for
    empty string CLOBs.
    """
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_empty_clob_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_empty_clob_sync (
            id NUMBER PRIMARY KEY,
            content CLOB
        )
    """)

    oracle_sync_session.execute("INSERT INTO test_empty_clob_sync (id, content) VALUES (:1, :2)", (1, ""))

    result = oracle_sync_session.execute("SELECT id, content FROM test_empty_clob_sync WHERE id = :1", (1,))

    hydrated = result.get_first(schema_type=NullableDocumentRecord)
    assert hydrated is not None
    assert isinstance(hydrated, NullableDocumentRecord)
    assert hydrated.id == 1
    assert hydrated.content is None

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_empty_clob_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )
