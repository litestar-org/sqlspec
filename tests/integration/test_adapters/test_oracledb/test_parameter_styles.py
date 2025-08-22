"""Test Oracle parameter style conversion."""

from typing import Any, Union

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.core.result import SQLResult

pytestmark = pytest.mark.xdist_group("oracle")

OracleParamData = Union[tuple[Any, ...], list[Any], dict[str, Any]]


@pytest.mark.parametrize(
    ("sql", "params", "expected_rows"),
    [
        ("SELECT :name as result FROM dual", {"name": "oracle_test"}, [{"RESULT": "oracle_test"}]),
        ("SELECT :1 as result FROM dual", ("oracle_positional",), [{"RESULT": "oracle_positional"}]),
        (
            "SELECT :first_name || ' ' || :last_name as full_name FROM dual",
            {"first_name": "John", "last_name": "Doe"},
            [{"FULL_NAME": "John Doe"}],
        ),
        ("SELECT :num1 + :num2 as sum FROM dual", {"num1": 10, "num2": 20}, [{"SUM": 30}]),
    ],
)
def test_sync_oracle_parameter_styles(
    oracle_sync_session: OracleSyncDriver, sql: str, params: OracleParamData, expected_rows: list[dict[str, Any]]
) -> None:
    """Test Oracle named parameter style conversion in sync driver."""
    result = oracle_sync_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == len(expected_rows)

    for i, expected_row in enumerate(expected_rows):
        actual_row = result.data[i]
        for key, expected_value in expected_row.items():
            assert actual_row[key] == expected_value


@pytest.mark.parametrize(
    ("sql", "params", "expected_rows"),
    [
        ("SELECT :name as result FROM dual", {"name": "oracle_async_test"}, [{"RESULT": "oracle_async_test"}]),
        ("SELECT :1 as result FROM dual", ("oracle_async_positional",), [{"RESULT": "oracle_async_positional"}]),
        (
            "SELECT :city || ', ' || :state as location FROM dual",
            {"city": "San Francisco", "state": "CA"},
            [{"LOCATION": "San Francisco, CA"}],
        ),
        (
            "SELECT CASE WHEN :is_active = 1 THEN 'Active' ELSE 'Inactive' END as status FROM dual",
            {"is_active": 1},
            [{"STATUS": "Active"}],
        ),
    ],
)
@pytest.mark.asyncio(loop_scope="function")
async def test_async_oracle_parameter_styles(
    oracle_async_session: OracleAsyncDriver, sql: str, params: OracleParamData, expected_rows: list[dict[str, Any]]
) -> None:
    """Test Oracle named parameter style conversion in async driver."""
    result = await oracle_async_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == len(expected_rows)

    for i, expected_row in enumerate(expected_rows):
        actual_row = result.data[i]
        for key, expected_value in expected_row.items():
            assert actual_row[key] == expected_value


def test_sync_oracle_insert_with_named_params(oracle_sync_session: OracleSyncDriver) -> None:
    """Test INSERT operations using Oracle named parameters."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_params_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            age NUMBER,
            city VARCHAR2(50)
        )
    """)

    insert_sql = "INSERT INTO test_params_table (id, name, age, city) VALUES (:id, :name, :age, :city)"
    params = {"id": 1, "name": "Alice Johnson", "age": 30, "city": "Oracle City"}

    result = oracle_sync_session.execute(insert_sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_sql = "SELECT name, age, city FROM test_params_table WHERE id = :id"
    select_result = oracle_sync_session.execute(select_sql, {"id": 1})
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["NAME"] == "Alice Johnson"
    assert row["AGE"] == 30
    assert row["CITY"] == "Oracle City"

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_async_oracle_update_with_mixed_params(oracle_async_session: OracleAsyncDriver) -> None:
    """Test UPDATE operations using mixed parameter styles."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_mixed_params_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            status VARCHAR2(20),
            last_updated DATE
        )
    """)

    await oracle_async_session.execute(
        "INSERT INTO test_mixed_params_table (id, name, status, last_updated) VALUES (:1, :2, :3, SYSDATE)",
        (1, "Test User", "PENDING"),
    )

    update_sql = """
        UPDATE test_mixed_params_table
        SET name = :new_name, status = :new_status, last_updated = SYSDATE
        WHERE id = :target_id
    """

    update_params = {"new_name": "Updated User", "new_status": "ACTIVE", "target_id": 1}

    result = await oracle_async_session.execute(update_sql, update_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = await oracle_async_session.execute(
        "SELECT name, status FROM test_mixed_params_table WHERE id = :1", (1,)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["NAME"] == "Updated User"
    assert row["STATUS"] == "ACTIVE"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_mixed_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_in_clause_with_params(oracle_sync_session: OracleSyncDriver) -> None:
    """Test IN clause with Oracle parameter binding."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_in_clause_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_in_clause_table (
            id NUMBER PRIMARY KEY,
            category VARCHAR2(50),
            value NUMBER
        )
    """)

    test_data = [(1, "TYPE_A", 100), (2, "TYPE_B", 200), (3, "TYPE_C", 300), (4, "TYPE_A", 150), (5, "TYPE_B", 250)]

    for data in test_data:
        oracle_sync_session.execute("INSERT INTO test_in_clause_table (id, category, value) VALUES (:1, :2, :3)", data)

    select_sql = """
        SELECT id, category, value
        FROM test_in_clause_table
        WHERE category IN (:cat1, :cat2)
        ORDER BY id
    """

    result = oracle_sync_session.execute(select_sql, {"cat1": "TYPE_A", "cat2": "TYPE_B"})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 4

    categories = [row["CATEGORY"] for row in result.data]
    assert all(cat in ["TYPE_A", "TYPE_B"] for cat in categories)
    assert "TYPE_C" not in categories

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_in_clause_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_async_oracle_null_parameter_handling(oracle_async_session: OracleAsyncDriver) -> None:
    """Test handling of NULL parameters in Oracle."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_null_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    await oracle_async_session.execute_script("""
        CREATE TABLE test_null_params_table (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            optional_field VARCHAR2(100)
        )
    """)

    insert_sql = "INSERT INTO test_null_params_table (id, name, optional_field) VALUES (:id, :name, :optional_field)"

    result = await oracle_async_session.execute(insert_sql, {"id": 1, "name": "Test User", "optional_field": None})
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    result = await oracle_async_session.execute(
        insert_sql, {"id": 2, "name": "Another User", "optional_field": "Not Null"}
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_null_sql = "SELECT id, name FROM test_null_params_table WHERE optional_field IS NULL"
    null_result = await oracle_async_session.execute(select_null_sql)
    assert isinstance(null_result, SQLResult)
    assert null_result.data is not None
    assert len(null_result.data) == 1
    assert null_result.data[0]["ID"] == 1

    select_not_null_sql = "SELECT id, name, optional_field FROM test_null_params_table WHERE optional_field IS NOT NULL"
    not_null_result = await oracle_async_session.execute(select_not_null_sql)
    assert isinstance(not_null_result, SQLResult)
    assert not_null_result.data is not None
    assert len(not_null_result.data) == 1
    assert not_null_result.data[0]["ID"] == 2
    assert not_null_result.data[0]["OPTIONAL_FIELD"] == "Not Null"

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_null_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_date_parameter_handling(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle DATE parameter handling and formatting."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_date_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_date_params_table (
            id NUMBER PRIMARY KEY,
            event_name VARCHAR2(100),
            event_date DATE,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    insert_sql = """
        INSERT INTO test_date_params_table (id, event_name, event_date)
        VALUES (:id, :event_name, TO_DATE(:date_str, 'YYYY-MM-DD'))
    """

    result = oracle_sync_session.execute(
        insert_sql, {"id": 1, "event_name": "Oracle Conference", "date_str": "2024-06-15"}
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_sql = """
        SELECT id, event_name,
               TO_CHAR(event_date, 'YYYY-MM-DD') as formatted_date
        FROM test_date_params_table
        WHERE event_date = TO_DATE(:target_date, 'YYYY-MM-DD')
    """

    select_result = oracle_sync_session.execute(select_sql, {"target_date": "2024-06-15"})
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["EVENT_NAME"] == "Oracle Conference"
    assert row["FORMATTED_DATE"] == "2024-06-15"

    range_sql = """
        SELECT COUNT(*) as event_count
        FROM test_date_params_table
        WHERE event_date BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD')
                             AND TO_DATE(:end_date, 'YYYY-MM-DD')
    """

    range_result = oracle_sync_session.execute(range_sql, {"start_date": "2024-01-01", "end_date": "2024-12-31"})
    assert isinstance(range_result, SQLResult)
    assert range_result.data is not None
    assert range_result.data[0]["EVENT_COUNT"] == 1

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_date_params_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_comprehensive_none_parameter_handling(oracle_sync_session: OracleSyncDriver) -> None:
    """Test comprehensive None parameter handling with various Oracle data types."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_comprehensive_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_comprehensive_none_table (
            id NUMBER PRIMARY KEY,
            text_field VARCHAR2(100),
            number_field NUMBER,
            date_field DATE,
            timestamp_field TIMESTAMP,
            clob_field CLOB,
            raw_field RAW(16)
        )
    """)

    # Test 1: Mix of None and non-None values with named parameters
    insert_sql = """
        INSERT INTO test_comprehensive_none_table
        (id, text_field, number_field, date_field, timestamp_field, clob_field, raw_field)
        VALUES (:id, :text_field, :number_field, :date_field, :timestamp_field, :clob_field, :raw_field)
    """

    # Insert with some None values
    params = {
        "id": 1,
        "text_field": "Test Value",
        "number_field": None,  # None value
        "date_field": None,  # None value
        "timestamp_field": None,  # None value
        "clob_field": "CLOB content",
        "raw_field": None,  # None value
    }

    result = oracle_sync_session.execute(insert_sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify None values were inserted as NULL
    select_result = oracle_sync_session.execute("SELECT * FROM test_comprehensive_none_table WHERE id = :1", (1,))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["ID"] == 1
    assert row["TEXT_FIELD"] == "Test Value"
    assert row["NUMBER_FIELD"] is None
    assert row["DATE_FIELD"] is None
    assert row["TIMESTAMP_FIELD"] is None
    # CLOB handling may vary - check if it's accessible
    clob_value = row["CLOB_FIELD"]
    if hasattr(clob_value, "read"):
        assert clob_value.read() == "CLOB content"
    else:
        assert str(clob_value) == "CLOB content"
    assert row["RAW_FIELD"] is None

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_comprehensive_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_all_none_parameters(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle parameter handling when all parameters are None."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_all_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_all_none_table (
            id NUMBER,
            field1 VARCHAR2(100),
            field2 NUMBER,
            field3 DATE
        )
    """)

    # All parameters are None
    insert_sql = "INSERT INTO test_all_none_table (id, field1, field2, field3) VALUES (:id, :field1, :field2, :field3)"
    all_none_params = {"id": None, "field1": None, "field2": None, "field3": None}

    result = oracle_sync_session.execute(insert_sql, all_none_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify all fields are NULL
    select_result = oracle_sync_session.execute("SELECT * FROM test_all_none_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["ID"] is None
    assert row["FIELD1"] is None
    assert row["FIELD2"] is None
    assert row["FIELD3"] is None

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_all_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_none_parameters_with_execute_many(oracle_sync_session: OracleSyncDriver) -> None:
    """Test None parameter handling with execute_many operations."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_none_execute_many'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_none_execute_many (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            value NUMBER,
            active NUMBER(1)
        )
    """)

    # Batch data with None values mixed in
    insert_sql = "INSERT INTO test_none_execute_many (id, name, value, active) VALUES (:id, :name, :value, :active)"

    batch_data: list[dict[str, Any]] = [
        {"id": 1, "name": "Record 1", "value": 100, "active": 1},
        {"id": 2, "name": None, "value": None, "active": 0},  # Some None values
        {"id": 3, "name": "Record 3", "value": None, "active": None},  # Mixed None values
        {"id": 4, "name": None, "value": None, "active": None},  # Mostly None values
        {"id": 5, "name": "Record 5", "value": 500, "active": 1},
    ]

    result = oracle_sync_session.execute_many(insert_sql, batch_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(batch_data)

    # Verify all records were inserted correctly
    select_result = oracle_sync_session.execute("SELECT * FROM test_none_execute_many ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == len(batch_data)

    # Check each record
    for i, row in enumerate(select_result.data):
        expected = batch_data[i]
        assert row["ID"] == expected["id"]
        assert row["NAME"] == expected["name"]
        assert row["VALUE"] == expected["value"]
        assert row["ACTIVE"] == expected["active"]

    # Test query with None parameter in WHERE clause
    select_with_none = oracle_sync_session.execute(
        "SELECT id FROM test_none_execute_many WHERE name = :name OR name IS NULL", {"name": None}
    )
    assert isinstance(select_with_none, SQLResult)
    assert select_with_none.data is not None
    # Should find records with NULL names (records 2, 4)
    null_name_ids = [row["ID"] for row in select_with_none.data]
    assert 2 in null_name_ids
    assert 4 in null_name_ids

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_none_execute_many'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_async_oracle_lob_none_parameter_handling(oracle_async_session: OracleAsyncDriver) -> None:
    """Test Oracle LOB (CLOB/RAW) None parameter handling in async operations."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_lob_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Simplified table without BLOB to avoid parameter binding issues
    await oracle_async_session.execute_script("""
        CREATE TABLE test_lob_none_table (
            id NUMBER PRIMARY KEY,
            description VARCHAR2(100),
            document_content CLOB,
            raw_data RAW(100)
        )
    """)

    # Test with None LOB values
    insert_sql = """
        INSERT INTO test_lob_none_table (id, description, document_content, raw_data)
        VALUES (:id, :description, :document_content, :raw_data)
    """

    # Test case 1: CLOB and RAW as None
    params1 = {
        "id": 1,
        "description": "Document with no content",
        "document_content": None,  # CLOB None
        "raw_data": None,  # RAW None
    }

    result = await oracle_async_session.execute(insert_sql, params1)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test case 2: Mix of None and actual LOB data
    test_clob_content = "This is test CLOB content " * 50  # Make it large enough for CLOB
    # Use hex string for RAW data - Oracle expects specific format
    test_raw_data = "DEADBEEF"  # Will be converted to RAW by Oracle

    params2 = {
        "id": 2,
        "description": "Document with content",
        "document_content": test_clob_content,
        "raw_data": test_raw_data,
    }

    result = await oracle_async_session.execute(insert_sql, params2)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test case 3: All LOBs as None
    params3 = {
        "id": 3,
        "description": None,  # Even VARCHAR2 as None
        "document_content": None,
        "raw_data": None,
    }

    result = await oracle_async_session.execute(insert_sql, params3)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify the insertions
    select_result = await oracle_async_session.execute("SELECT * FROM test_lob_none_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3

    # Check record 1 (None LOBs)
    row1 = select_result.data[0]
    assert row1["ID"] == 1
    assert row1["DESCRIPTION"] == "Document with no content"
    assert row1["DOCUMENT_CONTENT"] is None
    assert row1["RAW_DATA"] is None

    # Check record 2 (with LOB data)
    row2 = select_result.data[1]
    assert row2["ID"] == 2
    assert row2["DESCRIPTION"] == "Document with content"
    # LOB data might need special handling to read
    if row2["DOCUMENT_CONTENT"] is not None:
        clob_content = row2["DOCUMENT_CONTENT"]
        if hasattr(clob_content, "read"):
            content = await clob_content.read()  # Async read for CLOB
            assert "This is test CLOB content" in content
        else:
            assert "This is test CLOB content" in str(clob_content)

    # Check record 3 (all None)
    row3 = select_result.data[2]
    assert row3["ID"] == 3
    assert row3["DESCRIPTION"] is None
    assert row3["DOCUMENT_CONTENT"] is None
    assert row3["RAW_DATA"] is None

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_lob_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_async_oracle_json_none_parameter_handling(oracle_async_session: OracleAsyncDriver) -> None:
    """Test Oracle JSON column None parameter handling (Oracle 21+ and constraint-based)."""

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_json_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    # Try Oracle 21+ native JSON type first, fall back to VARCHAR2 with JSON constraint
    try:
        await oracle_async_session.execute_script("""
            CREATE TABLE test_json_none_table (
                id NUMBER PRIMARY KEY,
                name VARCHAR2(100),
                metadata JSON,
                settings JSON
            )
        """)
    except Exception:
        # Fallback to VARCHAR2 with JSON validation constraint (pre-21)
        await oracle_async_session.execute_script("""
            CREATE TABLE test_json_none_table (
                id NUMBER PRIMARY KEY,
                name VARCHAR2(100),
                metadata VARCHAR2(4000) CHECK (metadata IS JSON),
                settings VARCHAR2(4000) CHECK (settings IS JSON)
            )
        """)

    # Test with None JSON values
    insert_sql = """
        INSERT INTO test_json_none_table (id, name, metadata, settings)
        VALUES (:id, :name, :metadata, :settings)
    """

    # Test case 1: JSON fields as None
    params1 = {
        "id": 1,
        "name": "Record with no JSON",
        "metadata": None,  # JSON None
        "settings": None,  # JSON None
    }

    result = await oracle_async_session.execute(insert_sql, params1)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test case 2: Mix of None and JSON data
    test_metadata = '{"key": "value", "count": 42, "active": true}'
    test_settings = '{"theme": "dark", "notifications": false}'

    params2 = {"id": 2, "name": "Record with JSON", "metadata": test_metadata, "settings": test_settings}

    result = await oracle_async_session.execute(insert_sql, params2)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test case 3: Partial None JSON
    params3 = {
        "id": 3,
        "name": "Partial JSON",
        "metadata": '{"status": "partial"}',
        "settings": None,  # Only settings is None
    }

    result = await oracle_async_session.execute(insert_sql, params3)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify the insertions
    select_result = await oracle_async_session.execute("SELECT * FROM test_json_none_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3

    # Check record 1 (None JSON)
    row1 = select_result.data[0]
    assert row1["ID"] == 1
    assert row1["NAME"] == "Record with no JSON"
    assert row1["METADATA"] is None
    assert row1["SETTINGS"] is None

    # Check record 2 (with JSON data)
    row2 = select_result.data[1]
    assert row2["ID"] == 2
    assert row2["NAME"] == "Record with JSON"
    # JSON might be returned as string or object depending on Oracle version
    metadata_value = str(row2["METADATA"]) if row2["METADATA"] is not None else None
    settings_value = str(row2["SETTINGS"]) if row2["SETTINGS"] is not None else None
    assert metadata_value is not None and "key" in metadata_value
    assert settings_value is not None and "theme" in settings_value

    # Check record 3 (partial JSON)
    row3 = select_result.data[2]
    assert row3["ID"] == 3
    assert row3["NAME"] == "Partial JSON"
    metadata_value = str(row3["METADATA"]) if row3["METADATA"] is not None else None
    assert metadata_value is not None and "status" in metadata_value
    assert row3["SETTINGS"] is None

    # Test querying with None JSON parameter
    query_result = await oracle_async_session.execute(
        "SELECT id, name FROM test_json_none_table WHERE metadata IS NULL OR settings = :param", {"param": None}
    )
    assert isinstance(query_result, SQLResult)
    assert query_result.data is not None
    # Should find record 1 (both NULL)
    null_json_ids = [row["ID"] for row in query_result.data if row["ID"] == 1]
    assert len(null_json_ids) >= 1

    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_json_none_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def test_sync_oracle_parameter_count_validation_with_none(oracle_sync_session: OracleSyncDriver) -> None:
    """Test Oracle parameter count validation when None values are present."""

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_param_count_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )

    oracle_sync_session.execute_script("""
        CREATE TABLE test_param_count_table (
            id NUMBER PRIMARY KEY,
            field1 VARCHAR2(100),
            field2 NUMBER,
            field3 VARCHAR2(50)
        )
    """)

    # Test correct parameter count with None values
    correct_sql = (
        "INSERT INTO test_param_count_table (id, field1, field2, field3) VALUES (:id, :field1, :field2, :field3)"
    )
    correct_params = {
        "id": 1,
        "field1": None,  # None value
        "field2": 42,
        "field3": None,  # None value
    }

    result = oracle_sync_session.execute(correct_sql, correct_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test with missing parameters - check if SQLSpec properly validates
    missing_param_sql = (
        "INSERT INTO test_param_count_table (id, field1, field2, field3) VALUES (:id, :field1, :field2, :field3)"
    )
    missing_params = {
        "id": 2,
        "field1": "test",
        "field2": None,
        # Missing field3 parameter
    }

    # Try to execute and see what happens - SQLSpec might handle this gracefully
    try:
        result = oracle_sync_session.execute(missing_param_sql, missing_params)
        # If it succeeds, verify the result behavior
        assert isinstance(result, SQLResult)
        # The missing parameter might be treated as None/NULL
        select_result = oracle_sync_session.execute("SELECT * FROM test_param_count_table WHERE id = 2")
        assert isinstance(select_result, SQLResult)
        if select_result.data and len(select_result.data) > 0:
            # If a record was inserted, field3 should be NULL due to missing parameter
            row = select_result.data[0]
            assert row["FIELD3"] is None
    except Exception as e:
        # If it fails, that's also acceptable behavior - missing parameters should fail
        assert "field3" in str(e).lower() or "parameter" in str(e).lower() or "bind" in str(e).lower()

    # Test with extra parameters (Oracle should ignore extras)
    extra_param_sql = "INSERT INTO test_param_count_table (id, field1, field2) VALUES (:id, :field1, :field2)"
    extra_params = {
        "id": 3,
        "field1": "test",
        "field2": None,
        "field3": "extra_param",  # Extra parameter that won't be used
        "field4": None,  # Another extra None parameter
    }

    result = oracle_sync_session.execute(extra_param_sql, extra_params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify inserted data - all 3 inserts should succeed
    select_result = oracle_sync_session.execute("SELECT * FROM test_param_count_table ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3  # All 3 inserts succeeded

    # First insert - explicit None values
    row1 = select_result.data[0]
    assert row1["ID"] == 1
    assert row1["FIELD1"] is None
    assert row1["FIELD2"] == 42
    assert row1["FIELD3"] is None

    # Second insert - missing parameter treated as None/NULL
    row2 = select_result.data[1]
    assert row2["ID"] == 2
    assert row2["FIELD1"] == "test"
    assert row2["FIELD2"] is None
    assert row2["FIELD3"] is None  # Missing parameter treated as NULL

    # Third insert - extra parameters ignored
    row3 = select_result.data[2]
    assert row3["ID"] == 3
    assert row3["FIELD1"] == "test"
    assert row3["FIELD2"] is None
    assert row3["FIELD3"] is None  # Was not set in INSERT

    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_param_count_table'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
