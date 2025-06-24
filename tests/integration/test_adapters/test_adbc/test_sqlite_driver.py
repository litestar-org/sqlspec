"""Integration tests for ADBC SQLite driver implementation."""

from __future__ import annotations

import math
import tempfile
from collections.abc import Generator

import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_sqlite_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session with test table."""
    config = AdbcConfig(
        uri=":memory:",
        driver_name="adbc_driver_sqlite.dbapi.connect",
        statement_config=SQLConfig(strict_mode=False),  # Allow DDL statements for tests
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup is automatic with in-memory database


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_connection() -> None:
    """Test basic ADBC SQLite connection."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite.dbapi.connect")

    # Test connection creation
    with config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)

    # Test session creation
    with config.provide_session() as session:
        assert session is not None
        assert isinstance(session, AdbcDriver)
        result = session.execute("SELECT 1 as test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("adbc_sqlite")
def test_basic_crud(adbc_sqlite_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC SQLite."""
    # INSERT
    insert_result = adbc_sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42))
    assert isinstance(insert_result, SQLResult)
    # ADBC drivers may not support rowcount and return -1
    assert insert_result.rows_affected in (-1, 1)

    # SELECT
    select_result = adbc_sqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = adbc_sqlite_session.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    # ADBC drivers may not support rowcount and return -1
    assert update_result.rows_affected in (-1, 1)

    # Verify UPDATE
    verify_result = adbc_sqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = adbc_sqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    # ADBC drivers may not support rowcount and return -1
    assert delete_result.rows_affected in (-1, 1)

    # Verify DELETE
    empty_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.xdist_group("adbc_sqlite")
def test_parameter_styles(adbc_sqlite_session: AdbcDriver) -> None:
    """Test parameter binding styles with ADBC SQLite."""
    # SQLite primarily uses ? (qmark) style
    # Insert test data
    adbc_sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_value", 42))

    # Test positional parameters
    result = adbc_sqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_value",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["name"] == "test_value"
    assert result.data[0]["value"] == 42

    # Test multiple positional parameters
    result2 = adbc_sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name = ? AND value = ?", ("test_value", 42)
    )
    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert len(result2.data) == 1


@pytest.mark.xdist_group("adbc_sqlite")
def test_multiple_parameters(adbc_sqlite_session: AdbcDriver) -> None:
    """Test queries with multiple parameters in SQLite."""
    # Insert test data
    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    adbc_sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    # Query with multiple parameters
    result = adbc_sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE value >= ? AND value <= ? ORDER BY value", (25, 30)
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 2
    assert result.data[0]["name"] == "Alice"
    assert result.data[1]["name"] == "Bob"


@pytest.mark.xdist_group("adbc_sqlite")
def test_execute_many_basic(adbc_sqlite_session: AdbcDriver) -> None:
    """Test basic execute_many functionality with ADBC SQLite."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = adbc_sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", params_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = adbc_sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("adbc_sqlite")
def test_execute_many_mixed_types(adbc_sqlite_session: AdbcDriver) -> None:
    """Test execute_many with mixed data types."""
    # Create table with various types
    adbc_sqlite_session.execute_script("""
        CREATE TABLE mixed_types_test (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            int_col INTEGER,
            real_col REAL,
            blob_col BLOB
        )
    """)

    # Prepare mixed type data
    test_data = [("text1", 100, 1.5, b"bytes1"), ("text2", 200, 2.5, b"bytes2"), ("text3", 300, 3.5, b"bytes3")]

    result = adbc_sqlite_session.execute_many(
        "INSERT INTO mixed_types_test (text_col, int_col, real_col, blob_col) VALUES (?, ?, ?, ?)", test_data
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify data
    verify_result = adbc_sqlite_session.execute("SELECT * FROM mixed_types_test ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 3
    assert verify_result.data[0]["text_col"] == "text1"
    assert verify_result.data[0]["int_col"] == 100
    assert abs(verify_result.data[0]["real_col"] - 1.5) < 0.001

    # Cleanup
    adbc_sqlite_session.execute_script("DROP TABLE mixed_types_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_execute_script_ddl(adbc_sqlite_session: AdbcDriver) -> None:
    """Test execute_script with DDL statements."""
    ddl_script = """
        -- Create a new table
        CREATE TABLE script_test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Create an index
        CREATE INDEX idx_script_test_data ON script_test_table(data);

        -- Insert some data
        INSERT INTO script_test_table (data) VALUES ('test1'), ('test2'), ('test3');
    """

    result = adbc_sqlite_session.execute_script(ddl_script)
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    # Verify table was created and data inserted
    verify_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count FROM script_test_table")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["count"] == 3

    # Verify index exists using SQLite's pragma
    index_result = adbc_sqlite_session.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_script_test_data'"
    )
    assert isinstance(index_result, SQLResult)
    assert index_result.data is not None
    assert len(index_result.data) == 1

    # Cleanup
    adbc_sqlite_session.execute_script("DROP TABLE script_test_table")


@pytest.mark.xdist_group("adbc_sqlite")
def test_execute_script_transaction(adbc_sqlite_session: AdbcDriver) -> None:
    """Test execute_script with transaction handling."""
    # ADBC SQLite runs in autocommit mode, so we can't use explicit transactions in scripts
    # Test multiple operations without explicit transaction
    transaction_script = """
        -- Multiple operations (will be executed in autocommit mode)
        INSERT INTO test_table (name, value) VALUES ('tx_test1', 100);
        INSERT INTO test_table (name, value) VALUES ('tx_test2', 200);
        UPDATE test_table SET value = value + 10 WHERE name LIKE 'tx_test%';
    """

    result = adbc_sqlite_session.execute_script(transaction_script)
    assert isinstance(result, SQLResult)

    # Verify transaction results
    verify_result = adbc_sqlite_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'tx_test%' ORDER BY name"
    )
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["value"] == 110  # 100 + 10
    assert verify_result.data[1]["value"] == 210  # 200 + 10


@pytest.mark.xdist_group("adbc_sqlite")
def test_execute_script_comments(adbc_sqlite_session: AdbcDriver) -> None:
    """Test execute_script with comments and formatting."""
    script_with_comments = """
        -- This is a comment
        INSERT INTO test_table (name, value) VALUES ('comment_test', 999);

        /* This is a
           multi-line comment */
        UPDATE test_table
        SET value = 1000
        WHERE name = 'comment_test';

        -- Another comment
        SELECT COUNT(*) FROM test_table; -- inline comment
    """

    result = adbc_sqlite_session.execute_script(script_with_comments)
    assert isinstance(result, SQLResult)

    # Verify the operations were executed
    verify_result = adbc_sqlite_session.execute("SELECT value FROM test_table WHERE name = 'comment_test'")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    assert verify_result.data[0]["value"] == 1000


@pytest.mark.xdist_group("adbc_sqlite")
def test_basic_types(adbc_sqlite_session: AdbcDriver) -> None:
    """Test basic SQLite data types."""
    # Create table with SQLite types
    adbc_sqlite_session.execute_script("""
        CREATE TABLE basic_types_test (
            int_col INTEGER,
            text_col TEXT,
            real_col REAL,
            blob_col BLOB,
            null_col TEXT
        )
    """)

    # Insert test data
    import struct

    blob_data = struct.pack("i", 42)  # Binary data

    adbc_sqlite_session.execute(
        """
        INSERT INTO basic_types_test (int_col, text_col, real_col, blob_col, null_col)
        VALUES (?, ?, ?, ?, ?)
        """,
        (42, "text value", math.pi, blob_data, None),
    )

    # Verify data
    result = adbc_sqlite_session.execute("SELECT * FROM basic_types_test")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1

    row = result.data[0]
    assert row["int_col"] == 42
    assert row["text_col"] == "text value"
    assert abs(row["real_col"] - math.pi) < 0.00001
    assert row["blob_col"] == blob_data
    assert row["null_col"] is None

    # Cleanup
    adbc_sqlite_session.execute_script("DROP TABLE basic_types_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_blob_type(adbc_sqlite_session: AdbcDriver) -> None:
    """Test SQLite BLOB type handling."""
    # Create table with blob column
    adbc_sqlite_session.execute_script("""
        CREATE TABLE blob_test (
            id INTEGER PRIMARY KEY,
            data BLOB
        )
    """)

    # Test various blob data
    test_blobs = [
        b"Simple bytes",
        b"\x00\x01\x02\x03\x04",  # Binary data with null bytes
        b"",  # Empty blob
    ]

    for i, blob_data in enumerate(test_blobs):
        adbc_sqlite_session.execute("INSERT INTO blob_test (id, data) VALUES (?, ?)", (i, blob_data))

    # Verify blob data
    result = adbc_sqlite_session.execute("SELECT id, data FROM blob_test ORDER BY id")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 3

    for i, expected_blob in enumerate(test_blobs):
        assert result.data[i]["data"] == expected_blob

    # Cleanup
    adbc_sqlite_session.execute_script("DROP TABLE blob_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_fetch_arrow_table(adbc_sqlite_session: AdbcDriver) -> None:
    """Test SQLite fetch_arrow_table functionality."""
    # Insert test data
    test_data = [("Alice", 25, 50000.0), ("Bob", 30, 60000.0), ("Charlie", 35, 70000.0)]

    adbc_sqlite_session.execute_script("""
        CREATE TABLE arrow_test (
            name TEXT,
            age INTEGER,
            salary REAL
        )
    """)

    adbc_sqlite_session.execute_many("INSERT INTO arrow_test (name, age, salary) VALUES (?, ?, ?)", test_data)

    # Test fetch_arrow_table
    result = adbc_sqlite_session.fetch_arrow_table("SELECT * FROM arrow_test ORDER BY name")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.data.num_columns == 3
    assert result.column_names == ["name", "age", "salary"]

    # Verify data content
    names = result.data.column("name").to_pylist()
    ages = result.data.column("age").to_pylist()
    salaries = result.data.column("salary").to_pylist()

    assert names == ["Alice", "Bob", "Charlie"]
    assert ages == [25, 30, 35]
    assert salaries == [50000.0, 60000.0, 70000.0]

    # Cleanup
    adbc_sqlite_session.execute_script("DROP TABLE arrow_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_to_parquet(adbc_sqlite_session: AdbcDriver) -> None:
    """Test SQLite to_parquet functionality."""
    # Insert test data
    adbc_sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("parquet1", 111))
    adbc_sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("parquet2", 222))

    statement = SQL("SELECT id, name, value FROM test_table ORDER BY id")

    with tempfile.NamedTemporaryFile() as tmp:
        adbc_sqlite_session.export_to_storage(statement, destination_uri=tmp.name)  # type: ignore[attr-defined]

        # Read back the Parquet file - export_to_storage appends .parquet extension
        table = pq.read_table(f"{tmp.name}.parquet")
        assert table.num_rows == 2
        assert set(table.column_names) >= {"id", "name", "value"}

        # Verify data
        data = table.to_pylist()
        assert any(row["name"] == "parquet1" and row["value"] == 111 for row in data)
        assert any(row["name"] == "parquet2" and row["value"] == 222 for row in data)


@pytest.mark.xdist_group("adbc_sqlite")
def test_multiple_backends_consistency(adbc_sqlite_session: AdbcDriver) -> None:
    """Test consistency across different ADBC backends."""
    # Insert test data
    test_data = [("backend_test1", 100), ("backend_test2", 200)]
    adbc_sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    # Test basic query
    result = adbc_sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 2
    assert result.data[0]["name"] == "backend_test1"
    assert result.data[0]["value"] == 100

    # Test aggregation
    agg_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count, SUM(value) as total FROM test_table")
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["count"] == 2
    assert agg_result.data[0]["total"] == 300


@pytest.mark.xdist_group("adbc_sqlite")
def test_insert_returning(adbc_sqlite_session: AdbcDriver) -> None:
    """Test INSERT with RETURNING clause (SQLite 3.35.0+)."""
    # Check SQLite version to see if RETURNING is supported
    version_result = adbc_sqlite_session.execute("SELECT sqlite_version() as version")
    assert isinstance(version_result, SQLResult)
    assert version_result.data is not None
    version_str = version_result.data[0]["version"]
    major, minor, patch = map(int, version_str.split(".")[:3])

    if major < 3 or (major == 3 and minor < 35):
        pytest.skip(f"SQLite {version_str} does not support RETURNING clause (requires 3.35.0+)")

    # Test INSERT with RETURNING
    result = adbc_sqlite_session.execute(
        "INSERT INTO test_table (name, value) VALUES (?, ?) RETURNING id, name, value", ("returning_test", 999)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["name"] == "returning_test"
    assert result.data[0]["value"] == 999
    assert result.data[0]["id"] is not None
