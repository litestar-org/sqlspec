"""Test ADBC edge cases and specialized functionality."""

import math
from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core import SQLResult
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

# xdist_group is assigned per test based on database backend to enable parallel execution


@pytest.fixture
def adbc_postgresql_session(postgres_service: "PostgresService") -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for edge case testing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        yield session


@pytest.fixture
def adbc_sqlite_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session for edge case testing."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_null_parameter_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test NULL parameter handling edge cases with ADBC."""

    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS null_param_test (
            id SERIAL PRIMARY KEY,
            nullable_text TEXT,
            nullable_int INTEGER,
            nullable_bool BOOLEAN,
            required_text TEXT NOT NULL
        )
    """)

    test_cases = [
        (None, None, None, "required1"),
        ("text1", None, True, "required2"),
        (None, 42, None, "required3"),
        ("text2", 100, False, "required4"),
    ]

    for text_val, int_val, bool_val, required_val in test_cases:
        result = adbc_postgresql_session.execute(
            """
            INSERT INTO null_param_test (nullable_text, nullable_int, nullable_bool, required_text)
            VALUES ($1, $2, $3, $4)
        """,
            (text_val, int_val, bool_val, required_val),
        )
        adbc_postgresql_session.commit()
        assert isinstance(result, SQLResult)
        assert result.rows_affected in (-1, 0, 1)

    null_text_result = adbc_postgresql_session.execute("""
        SELECT * FROM null_param_test WHERE nullable_text IS NULL
    """)
    assert isinstance(null_text_result, SQLResult)
    assert null_text_result.data is not None
    assert len(null_text_result.data) == 2

    null_where_result = adbc_postgresql_session.execute(
        """
        SELECT * FROM null_param_test
        WHERE (nullable_text = $1 OR ($1 IS NULL AND nullable_text IS NULL))
    """,
        (None,),
    )
    assert isinstance(null_where_result, SQLResult)

    null_many_data = [(None, 1, "batch1"), ("text", None, "batch2"), (None, None, "batch3")]

    many_result = adbc_postgresql_session.execute_many(
        """
        INSERT INTO null_param_test (nullable_text, nullable_int, required_text)
        VALUES ($1, $2, $3)
    """,
        null_many_data,
    )
    assert isinstance(many_result, SQLResult)
    assert many_result.rows_affected == 3

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS null_param_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_parameter_style_variations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test parameter style handling variations with ADBC."""

    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS param_style_test (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER,
            flag BOOLEAN
        )
    """)

    result1 = adbc_postgresql_session.execute(
        """
        INSERT INTO param_style_test (name, value, flag) VALUES ($1, $2, $3)
    """,
        ("param_test1", 100, True),
    )
    assert isinstance(result1, SQLResult)

    result2 = adbc_postgresql_session.execute(
        """
        INSERT INTO param_style_test (name) VALUES ($1)
    """,
        ("single_param",),
    )
    assert isinstance(result2, SQLResult)

    result3 = adbc_postgresql_session.execute(
        """
        INSERT INTO param_style_test (name, value, flag)
        VALUES ($1, $2, $2 > 0)  -- $2 used twice in different contexts
    """,
        ("repeat_param", 42),
    )
    assert isinstance(result3, SQLResult)

    complex_result = adbc_postgresql_session.execute(
        """
        SELECT
            name,
            value,
            CASE WHEN value = $1 THEN 'match' ELSE 'no_match' END as match_status
        FROM param_style_test
        WHERE name LIKE $2 || '%' AND value IS NOT NULL
        ORDER BY id
    """,
        (42, "param"),
    )
    assert isinstance(complex_result, SQLResult)
    assert complex_result.data is not None

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS param_style_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_execute_script_edge_cases(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script edge cases with ADBC."""

    mixed_script = """
        CREATE TABLE IF NOT EXISTS script_test (
            id SERIAL PRIMARY KEY,
            data TEXT
        );

        INSERT INTO script_test (data) VALUES ('script_data1');
        INSERT INTO script_test (data) VALUES ('script_data2');

        UPDATE script_test SET data = 'updated_' || data WHERE id = 1;

        -- Comment in script
        SELECT COUNT(*) FROM script_test;
    """

    result = adbc_postgresql_session.execute_script(mixed_script)

    assert result is None or isinstance(result, (str, SQLResult))

    verify_result = adbc_postgresql_session.execute("SELECT * FROM script_test ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["data"] == "updated_script_data1"
    assert verify_result.data[1]["data"] == "script_data2"

    comment_script = """
        -- This is a comment
        ;  -- Empty statement

        INSERT INTO script_test (data) VALUES ('comment_test');

        ; -- Another empty statement
        -- Final comment
    """

    comment_result = adbc_postgresql_session.execute_script(comment_script)
    assert comment_result is None or isinstance(comment_result, (str, SQLResult))

    transaction_script = """
        BEGIN;
        INSERT INTO script_test (data) VALUES ('transaction_test1');
        INSERT INTO script_test (data) VALUES ('transaction_test2');
        COMMIT;
    """

    trans_result = adbc_postgresql_session.execute_script(transaction_script)
    assert trans_result is None or isinstance(trans_result, (str, SQLResult))

    final_count = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM script_test")
    assert isinstance(final_count, SQLResult)
    assert final_count.data is not None
    assert final_count.data[0]["count"] >= 4

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS script_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_returning_clause_support(adbc_postgresql_session: AdbcDriver) -> None:
    """Test RETURNING clause support with ADBC."""

    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS returning_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    insert_returning = adbc_postgresql_session.execute(
        """
        INSERT INTO returning_test (name) VALUES ($1) RETURNING id, name, created_at
    """,
        ("returning_test1",),
    )

    assert isinstance(insert_returning, SQLResult)
    assert insert_returning.data is not None
    assert len(insert_returning.data) == 1

    returned_row = insert_returning.data[0]
    assert returned_row["id"] is not None
    assert returned_row["name"] == "returning_test1"
    assert returned_row["created_at"] is not None

    update_returning = adbc_postgresql_session.execute(
        """
        UPDATE returning_test
        SET name = $1
        WHERE name = $2
        RETURNING id, name
    """,
        ("updated_name", "returning_test1"),
    )

    assert isinstance(update_returning, SQLResult)
    assert update_returning.data is not None
    assert len(update_returning.data) == 1
    assert update_returning.data[0]["name"] == "updated_name"

    delete_returning = adbc_postgresql_session.execute(
        """
        DELETE FROM returning_test
        WHERE name = $1
        RETURNING id, name
    """,
        ("updated_name",),
    )

    assert isinstance(delete_returning, SQLResult)
    assert delete_returning.data is not None
    assert len(delete_returning.data) == 1
    assert delete_returning.data[0]["name"] == "updated_name"

    count_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM returning_test")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == 0

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS returning_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_data_type_edge_cases(adbc_postgresql_session: AdbcDriver) -> None:
    """Test edge cases in data type handling with ADBC."""

    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS data_type_edge_test (
            id SERIAL PRIMARY KEY,
            big_integer BIGINT,
            small_integer SMALLINT,
            real_number REAL,
            double_number DOUBLE PRECISION,
            char_fixed CHAR(10),
            varchar_var VARCHAR(255),
            text_unlimited TEXT,
            bytea_binary BYTEA,
            uuid_field UUID,
            interval_field INTERVAL
        )
    """)

    edge_cases = [
        (
            9223372036854775807,
            32767,
            3.4028235e38,
            1.7976931348623157e308,
            "CHAR_TEST",
            "VARCHAR_TEST",
            "TEXT_TEST",
            b"\\x48656c6c6f",
        ),
        (
            -9223372036854775808,
            -32768,
            -3.4028235e38,
            -1.7976931348623157e308,
            "MIN_VALUES",
            "MIN_VARCHAR",
            "MIN_TEXT",
            b"\\x576f726c64",
        ),
        (0, 0, 0.0, 0.0, "ZERO_VAL", "", "", b""),
    ]

    for big_int, small_int, real_val, double_val, char_val, varchar_val, text_val, bytea_val in edge_cases:
        result = adbc_postgresql_session.execute(
            """
            INSERT INTO data_type_edge_test
            (big_integer, small_integer, real_number, double_number, char_fixed, varchar_var, text_unlimited, bytea_binary)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
            (big_int, small_int, real_val, double_val, char_val, varchar_val, text_val, bytea_val),
        )

        assert isinstance(result, SQLResult)

    edge_result = adbc_postgresql_session.execute("SELECT * FROM data_type_edge_test ORDER BY id")
    assert isinstance(edge_result, SQLResult)
    assert edge_result.data is not None
    assert len(edge_result.data) == len(edge_cases)

    max_row = edge_result.data[0]
    assert max_row["big_integer"] == 9223372036854775807
    assert max_row["small_integer"] == 32767

    min_row = edge_result.data[1]
    assert min_row["big_integer"] == -9223372036854775808
    assert min_row["small_integer"] == -32768

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS data_type_edge_test")


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
def test_sqlite_specific_edge_cases(adbc_sqlite_session: AdbcDriver) -> None:
    """Test SQLite-specific edge cases with ADBC."""

    adbc_sqlite_session.execute_script("""
        CREATE TABLE dynamic_type_test (
            id INTEGER PRIMARY KEY,
            flexible_column
        )
    """)

    flexible_data = [(1, "text_value"), (2, 42), (3, math.pi), (4, b"binary_data"), (5, None)]

    for row_id, value in flexible_data:
        result = adbc_sqlite_session.execute(
            """
            INSERT INTO dynamic_type_test (id, flexible_column) VALUES (?, ?)
        """,
            (row_id, value),
        )
        assert isinstance(result, SQLResult)

    dynamic_result = adbc_sqlite_session.execute("""
        SELECT
            id,
            flexible_column,
            typeof(flexible_column) as column_type
        FROM dynamic_type_test
        ORDER BY id
    """)

    assert isinstance(dynamic_result, SQLResult)
    assert dynamic_result.data is not None
    assert len(dynamic_result.data) == 5

    types_found = [row["column_type"] for row in dynamic_result.data if row["column_type"]]
    assert "text" in types_found or "TEXT" in types_found
    assert "integer" in types_found or "INTEGER" in types_found
    assert "real" in types_found or "REAL" in types_found

    func_result = adbc_sqlite_session.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(flexible_column) as non_null_count,
            GROUP_CONCAT(DISTINCT typeof(flexible_column)) as all_types,
            sqlite_version() as sqlite_ver
        FROM dynamic_type_test
    """)

    assert isinstance(func_result, SQLResult)
    assert func_result.data is not None

    func_row = func_result.data[0]
    assert func_row["total_rows"] == 5
    assert func_row["non_null_count"] == 4
    assert func_row["sqlite_ver"] is not None


@pytest.mark.xdist_group("duckdb")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_duckdb_specific_edge_cases() -> None:
    """Test DuckDB-specific edge cases with ADBC."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE advanced_types_test (
                id INTEGER,
                complex_array INTEGER[][],
                nested_struct STRUCT(
                    name VARCHAR,
                    scores INTEGER[],
                    metadata STRUCT(created TIMESTAMP, active BOOLEAN)
                ),
                map_field MAP(VARCHAR, DOUBLE)
            )
        """)

        result = session.execute("""
            INSERT INTO advanced_types_test VALUES (
                1,
                [[1, 2], [3, 4], [5, 6]],
                {'name': 'complex_test', 'scores': [95, 87, 92], 'metadata': {'created': '2024-01-15T10:00:00', 'active': true}},
                MAP(['key1', 'key2', 'key3'], [1.1, 2.2, 3.3])
            )
        """)
        assert isinstance(result, SQLResult)

        complex_result = session.execute("""
            SELECT
                id,
                complex_array,
                nested_struct,
                map_field,
                array_length(complex_array, 1) as array_depth1_length,
                nested_struct.name as struct_name,
                list_avg(nested_struct.scores) as avg_score
            FROM advanced_types_test
        """)

        assert isinstance(complex_result, SQLResult)
        assert complex_result.data is not None
        assert len(complex_result.data) == 1

        row = complex_result.data[0]
        assert row["id"] == 1
        assert row["complex_array"] is not None
        assert row["nested_struct"] is not None
        assert row["map_field"] is not None
        assert row["array_depth1_length"] == 3


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_connection_resilience(adbc_postgresql_session: AdbcDriver) -> None:
    """Test connection resilience and error recovery with ADBC."""

    with pytest.raises(Exception):
        adbc_postgresql_session.execute("INVALID SQL SYNTAX HERE")

    recovery_result = adbc_postgresql_session.execute("SELECT 1 as recovery_test")
    assert isinstance(recovery_result, SQLResult)
    assert recovery_result.data is not None
    assert recovery_result.data[0]["recovery_test"] == 1

    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS constraint_test (
            id SERIAL PRIMARY KEY,
            unique_value TEXT UNIQUE
        )
    """)

    adbc_postgresql_session.execute("INSERT INTO constraint_test (unique_value) VALUES ($1)", ("unique1",))

    with pytest.raises(Exception):
        adbc_postgresql_session.execute("INSERT INTO constraint_test (unique_value) VALUES ($1)", ("unique1",))

    post_error_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM constraint_test")
    assert isinstance(post_error_result, SQLResult)
    assert post_error_result.data is not None
    assert post_error_result.data[0]["count"] == 1

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS constraint_test")
