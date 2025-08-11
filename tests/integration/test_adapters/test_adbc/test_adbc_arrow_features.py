"""Test ADBC Arrow-specific features and integrations."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core.result import SQLResult
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for Arrow testing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
def test_arrow_table_metadata_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test Arrow table metadata handling with ADBC."""
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS arrow_metadata_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER CHECK (age >= 0),
            salary DECIMAL(10,2),
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tags TEXT[],
            metadata JSONB
        )
    """)

    adbc_postgresql_session.execute(
        """
        INSERT INTO arrow_metadata_test (name, age, salary, is_active, tags, metadata)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
    """,
        ("John Doe", 30, 75000.50, True, ["developer", "senior"], {"department": "engineering", "level": "senior"}),
    )

    result = adbc_postgresql_session.execute("SELECT * FROM arrow_metadata_test")
    assert isinstance(result, SQLResult)

    expected_columns = ["id", "name", "age", "salary", "is_active", "created_at", "tags", "metadata"]
    for col in expected_columns:
        assert col in result.column_names

    assert result.data is not None
    row = result.data[0]
    assert isinstance(row["id"], int)
    assert isinstance(row["name"], str)
    assert isinstance(row["age"], int)
    assert isinstance(row["is_active"], bool)
    assert isinstance(row["tags"], list)

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS arrow_metadata_test")


@pytest.mark.xdist_group("postgres")
def test_arrow_null_value_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test Arrow NULL value handling with ADBC."""
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS arrow_null_test (
            id SERIAL PRIMARY KEY,
            nullable_text TEXT,
            nullable_int INTEGER,
            nullable_bool BOOLEAN,
            nullable_decimal DECIMAL(10,2),
            nullable_array INTEGER[]
        )
    """)

    test_cases = [
        ("text1", 42, True, 123.45, [1, 2, 3]),
        (None, None, None, None, None),
        ("text2", None, False, None, None),
        (None, 100, None, 200.00, [4, 5]),
    ]

    for case in test_cases:
        adbc_postgresql_session.execute(
            """
            INSERT INTO arrow_null_test
            (nullable_text, nullable_int, nullable_bool, nullable_decimal, nullable_array)
            VALUES ($1, $2, $3, $4, $5)
        """,
            case,
        )

    result = adbc_postgresql_session.execute("SELECT * FROM arrow_null_test ORDER BY id")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 4

    no_nulls_row = result.data[0]
    assert no_nulls_row["nullable_text"] == "text1"
    assert no_nulls_row["nullable_int"] == 42
    assert no_nulls_row["nullable_bool"] is True
    assert float(no_nulls_row["nullable_decimal"]) == 123.45
    assert no_nulls_row["nullable_array"] == [1, 2, 3]

    all_nulls_row = result.data[1]
    assert all_nulls_row["nullable_text"] is None
    assert all_nulls_row["nullable_int"] is None
    assert all_nulls_row["nullable_bool"] is None
    assert all_nulls_row["nullable_decimal"] is None
    assert all_nulls_row["nullable_array"] is None

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS arrow_null_test")


@pytest.mark.xdist_group("postgres")
def test_arrow_large_dataset_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test Arrow handling of larger datasets with ADBC."""
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS arrow_large_test (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER,
            data TEXT
        )
    """)

    batch_size = 100
    total_rows = 1000

    for batch_start in range(0, total_rows, batch_size):
        batch_data = [
            (f"name_{i:04d}", i * 10, f"data_string_{i}_" + "x" * 50)
            for i in range(batch_start, min(batch_start + batch_size, total_rows))
        ]

        adbc_postgresql_session.execute_many(
            """
            INSERT INTO arrow_large_test (name, value, data) VALUES ($1, $2, $3)
        """,
            batch_data,
        )

    result = adbc_postgresql_session.execute("SELECT COUNT(*) as total_count FROM arrow_large_test")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["total_count"] == total_rows

    page_size = 50
    page_result = adbc_postgresql_session.execute(
        """
        SELECT * FROM arrow_large_test
        ORDER BY id
        LIMIT $1 OFFSET $2
    """,
        (page_size, 100),
    )

    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == page_size

    for i, row in enumerate(page_result.data):
        expected_id = 101 + i
        assert row["name"] == f"name_{expected_id - 1:04d}"
        assert row["value"] == (expected_id - 1) * 10

    agg_result = adbc_postgresql_session.execute("""
        SELECT
            COUNT(*) as count,
            MIN(value) as min_value,
            MAX(value) as max_value,
            AVG(value) as avg_value,
            SUM(value) as sum_value
        FROM arrow_large_test
    """)

    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None

    agg_row = agg_result.data[0]
    assert agg_row["count"] == total_rows
    assert agg_row["min_value"] == 0
    assert agg_row["max_value"] == (total_rows - 1) * 10
    expected_avg = sum(range(total_rows)) * 10 / total_rows
    assert abs(float(agg_row["avg_value"]) - expected_avg) < 0.01

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS arrow_large_test")


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
@pytest.mark.xfail(reason="DuckDB ADBC driver has not fully implemented executemany support yet")
def test_arrow_duckdb_advanced_analytics() -> None:
    """Test DuckDB advanced analytics with Arrow."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE analytics_test (
                id INTEGER,
                category TEXT,
                value DOUBLE,
                timestamp TIMESTAMP,
                tags TEXT[]
            )
        """)

        analytical_data = [
            (1, "A", 100.5, "2024-01-01 10:00:00", ["tag1", "tag2"]),
            (2, "B", 200.3, "2024-01-01 11:00:00", ["tag2", "tag3"]),
            (3, "A", 150.7, "2024-01-01 12:00:00", ["tag1", "tag3"]),
            (4, "C", 300.2, "2024-01-01 13:00:00", ["tag1"]),
            (5, "B", 250.8, "2024-01-01 14:00:00", ["tag2"]),
        ]

        session.execute_many(
            """
            INSERT INTO analytics_test VALUES (?, ?, ?, ?, ?)
        """,
            analytical_data,
        )

        analytical_query = session.execute("""
            SELECT
                category,
                COUNT(*) as record_count,
                AVG(value) as avg_value,
                STDDEV(value) as stddev_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value,
                ARRAY_AGG(DISTINCT unnest(tags)) as all_tags,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp
            FROM analytics_test
            GROUP BY category
            ORDER BY category
        """)

        assert isinstance(analytical_query, SQLResult)
        assert analytical_query.data is not None
        assert len(analytical_query.data) == 3

        category_a = next(row for row in analytical_query.data if row["category"] == "A")
        assert category_a["record_count"] == 2
        assert abs(category_a["avg_value"] - 125.6) < 0.1

        window_query = session.execute("""
            SELECT
                id,
                category,
                value,
                LAG(value) OVER (PARTITION BY category ORDER BY timestamp) as prev_value,
                LEAD(value) OVER (PARTITION BY category ORDER BY timestamp) as next_value,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as value_rank
            FROM analytics_test
            ORDER BY category, timestamp
        """)

        assert isinstance(window_query, SQLResult)
        assert window_query.data is not None
        assert len(window_query.data) == 5


@pytest.mark.xdist_group("adbc_sqlite")
def test_arrow_sqlite_binary_data() -> None:
    """Test Arrow binary data handling with SQLite."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE binary_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                binary_data BLOB,
                binary_size INTEGER
            )
        """)

        binary_test_cases = [
            ("small_binary", b"small data", len(b"small data")),
            ("empty_binary", b"", 0),
            ("null_binary", None, None),
            ("large_binary", b"x" * 1000, 1000),
            ("mixed_binary", bytes(range(256)), 256),
        ]

        for name, binary_data, size in binary_test_cases:
            session.execute(
                """
                INSERT INTO binary_test (name, binary_data, binary_size) VALUES (?, ?, ?)
            """,
                (name, binary_data, size),
            )

        result = session.execute("SELECT * FROM binary_test ORDER BY name")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == len(binary_test_cases)

        expected_by_name = {name: (data, size) for name, data, size in binary_test_cases}
        for row in result.data:
            expected_data, expected_size = expected_by_name[row["name"]]
            assert row["binary_data"] == expected_data
            assert row["binary_size"] == expected_size

        large_binary_result = session.execute("""
            SELECT name, length(binary_data) as actual_size, binary_size
            FROM binary_test
            WHERE binary_size > 100
        """)

        assert isinstance(large_binary_result, SQLResult)
        assert large_binary_result.data is not None
        assert len(large_binary_result.data) == 2

        for row in large_binary_result.data:
            assert row["actual_size"] == row["binary_size"]


@pytest.mark.xdist_group("postgres")
@pytest.mark.xfail(reason="ADBC PostgreSQL driver has array binding issues with Arrow schema inference")
def test_arrow_postgresql_array_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL array operations with Arrow."""
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS array_operations_test (
            id SERIAL PRIMARY KEY,
            name TEXT,
            int_array INTEGER[],
            text_array TEXT[],
            nested_array INTEGER[][]
        )
    """)

    array_test_data = [
        ("arrays1", [1, 2, 3, 4, 5], ["a", "b", "c"], [[1, 2], [3, 4]]),
        ("arrays2", [10, 20, 30], ["x", "y", "z"], [[10, 20], [30, 40]]),
        ("arrays3", [], [], []),
        ("arrays4", None, None, None),
    ]

    for name, int_arr, text_arr, nested_arr in array_test_data:
        adbc_postgresql_session.execute(
            """
            INSERT INTO array_operations_test (name, int_array, text_array, nested_array)
            VALUES ($1, $2, $3, $4)
        """,
            (name, int_arr, text_arr, nested_arr),
        )

    array_ops_result = adbc_postgresql_session.execute("""
        SELECT
            name,
            int_array,
            array_length(int_array, 1) as int_array_length,
            text_array,
            array_length(text_array, 1) as text_array_length,
            int_array[1] as first_int,
            text_array[1] as first_text,
            array_cat(int_array, ARRAY[99, 100]) as concatenated_array
        FROM array_operations_test
        WHERE int_array IS NOT NULL
        ORDER BY name
    """)

    assert isinstance(array_ops_result, SQLResult)
    assert array_ops_result.data is not None
    assert len(array_ops_result.data) == 3

    first_row = array_ops_result.data[0]
    assert first_row["name"] == "arrays1"
    assert first_row["int_array"] == [1, 2, 3, 4, 5]
    assert first_row["int_array_length"] == 5
    assert first_row["text_array"] == ["a", "b", "c"]
    assert first_row["text_array_length"] == 3
    assert first_row["first_int"] == 1
    assert first_row["first_text"] == "a"
    assert first_row["concatenated_array"] == [1, 2, 3, 4, 5, 99, 100]

    containment_result = adbc_postgresql_session.execute("""
        SELECT
            name,
            int_array @> ARRAY[2, 3] as contains_2_3,
            int_array && ARRAY[3, 6, 9] as overlaps_with_3_6_9,
            cardinality(int_array) as array_cardinality
        FROM array_operations_test
        WHERE int_array IS NOT NULL AND array_length(int_array, 1) > 0
        ORDER BY name
    """)

    assert isinstance(containment_result, SQLResult)
    assert containment_result.data is not None

    arrays1_row = containment_result.data[0]
    assert arrays1_row["contains_2_3"] is True
    assert arrays1_row["overlaps_with_3_6_9"] is True
    assert arrays1_row["array_cardinality"] == 5

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS array_operations_test")
