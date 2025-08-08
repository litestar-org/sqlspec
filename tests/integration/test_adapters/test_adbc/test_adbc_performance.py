"""Test ADBC performance characteristics and optimizations using CORE_ROUND_3 architecture."""

import time
from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core.result import SQLResult

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for performance testing."""
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
    """Create an ADBC SQLite session for performance testing."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
def test_bulk_insert_performance(adbc_postgresql_session: AdbcDriver) -> None:
    """Test bulk insert performance with ADBC using CORE_ROUND_3."""
    # Create performance test table
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS bulk_insert_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            value INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Generate test data
    bulk_size = 1000
    test_data = [
        (f"name_{i:04d}", i * 10, f"Description for item {i} with some longer text to test string handling")
        for i in range(bulk_size)
    ]

    # Measure bulk insert performance
    start_time = time.time()
    result = adbc_postgresql_session.execute_many(
        """
        INSERT INTO bulk_insert_test (name, value, description) VALUES ($1, $2, $3)
    """,
        test_data,
    )
    insert_time = time.time() - start_time

    assert isinstance(result, SQLResult)
    assert result.rows_affected == bulk_size

    # Verify all data was inserted
    count_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM bulk_insert_test")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == bulk_size

    # Performance assertion (should complete within reasonable time)
    assert insert_time < 10.0, f"Bulk insert took {insert_time:.2f}s, expected < 10s"

    # Clean up
    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS bulk_insert_test")


@pytest.mark.xdist_group("postgres")
def test_large_result_set_performance(adbc_postgresql_session: AdbcDriver) -> None:
    """Test large result set retrieval performance with ADBC using CORE_ROUND_3."""
    # Create table with larger dataset
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS large_result_test (
            id SERIAL PRIMARY KEY,
            category VARCHAR(10),
            value DOUBLE PRECISION,
            data TEXT
        )
    """)

    # Insert test data in batches for better performance
    batch_size = 500
    total_rows = 5000
    categories = ["A", "B", "C", "D", "E"]

    for batch_start in range(0, total_rows, batch_size):
        batch_data = []
        for i in range(batch_start, min(batch_start + batch_size, total_rows)):
            category = categories[i % len(categories)]
            value = float(i) * 1.5
            data = f"Data row {i} with category {category}"
            batch_data.append((category, value, data))

        adbc_postgresql_session.execute_many(
            """
            INSERT INTO large_result_test (category, value, data) VALUES ($1, $2, $3)
        """,
            batch_data,
        )

    # Test large result set retrieval
    start_time = time.time()
    result = adbc_postgresql_session.execute("SELECT * FROM large_result_test ORDER BY id")
    query_time = time.time() - start_time

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == total_rows

    # Performance assertion
    assert query_time < 5.0, f"Large result query took {query_time:.2f}s, expected < 5s"

    # Test aggregation performance on large dataset
    start_time = time.time()
    agg_result = adbc_postgresql_session.execute("""
        SELECT
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            STDDEV(value) as stddev_value
        FROM large_result_test
        GROUP BY category
        ORDER BY category
    """)
    agg_time = time.time() - start_time

    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert len(agg_result.data) == len(categories)

    # Performance assertion for aggregation
    assert agg_time < 2.0, f"Aggregation query took {agg_time:.2f}s, expected < 2s"

    # Clean up
    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS large_result_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_sqlite_memory_performance(adbc_sqlite_session: AdbcDriver) -> None:
    """Test SQLite in-memory performance with ADBC using CORE_ROUND_3."""
    # Create performance test table
    adbc_sqlite_session.execute_script("""
        CREATE TABLE performance_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value REAL,
            binary_data BLOB
        )
    """)

    # Generate test data with binary components
    test_size = 2000
    test_data = []
    for i in range(test_size):
        name = f"sqlite_item_{i:04d}"
        value = float(i) * 2.5
        binary_data = bytes([i % 256] * (i % 100 + 1))  # Variable-length binary data
        test_data.append((name, value, binary_data))

    # Test batch insert performance
    start_time = time.time()
    adbc_sqlite_session.execute_many(
        """
        INSERT INTO performance_test (name, value, binary_data) VALUES (?, ?, ?)
    """,
        test_data,
    )
    time.time() - start_time

    # Verify insertion
    count_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count FROM performance_test")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == test_size

    # Test query performance with binary data
    start_time = time.time()
    binary_query_result = adbc_sqlite_session.execute(
        """
        SELECT
            name,
            value,
            length(binary_data) as binary_length,
            substr(binary_data, 1, 10) as binary_sample
        FROM performance_test
        WHERE value > ? AND length(binary_data) > ?
        ORDER BY value
        LIMIT 100
    """,
        (1000.0, 50),
    )
    time.time() - start_time

    assert isinstance(binary_query_result, SQLResult)
    assert binary_query_result.data is not None

    # Test complex aggregation
    start_time = time.time()
    agg_result = adbc_sqlite_session.execute("""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            SUM(length(binary_data)) as total_binary_bytes,
            MIN(length(binary_data)) as min_binary_length,
            MAX(length(binary_data)) as max_binary_length
        FROM performance_test
    """)
    time.time() - start_time

    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None

    row = agg_result.data[0]
    assert row["total_count"] == test_size
    assert row["total_binary_bytes"] > 0


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_analytical_performance() -> None:
    """Test DuckDB analytical performance with ADBC using CORE_ROUND_3."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        # Create analytical test table
        session.execute_script("""
            CREATE TABLE analytical_perf_test (
                id INTEGER,
                timestamp TIMESTAMP,
                category VARCHAR,
                value DOUBLE,
                dimensions INTEGER[],
                metadata JSON
            )
        """)

        # Generate analytical test data
        data_size = 10000
        categories = ["analytics", "metrics", "events", "logs", "traces"]

        # Insert in batches for better performance
        batch_size = 1000
        for batch_start in range(0, data_size, batch_size):
            batch_data = []
            for i in range(batch_start, min(batch_start + batch_size, data_size)):
                timestamp = f"2024-01-{(i % 30) + 1:02d} {(i % 24):02d}:{(i % 60):02d}:00"
                category = categories[i % len(categories)]
                value = float(i) * 1.1
                dimensions = [i % 10, (i * 2) % 10, (i * 3) % 10]
                metadata = f'{{"id": {i}, "batch": {batch_start // batch_size}}}'
                batch_data.append((i, timestamp, category, value, dimensions, metadata))

            session.execute_many(
                """
                INSERT INTO analytical_perf_test VALUES (?, ?, ?, ?, ?, ?)
            """,
                batch_data,
            )

        # Test complex analytical query performance
        start_time = time.time()
        complex_query_result = session.execute("""
            SELECT
                category,
                DATE_TRUNC('hour', timestamp) as hour_bucket,
                COUNT(*) as record_count,
                AVG(value) as avg_value,
                STDDEV(value) as stddev_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value) as p95_value,
                ARRAY_AGG(DISTINCT dimensions[1]) as unique_dim1_values
            FROM analytical_perf_test
            WHERE timestamp >= '2024-01-15'
            GROUP BY category, DATE_TRUNC('hour', timestamp)
            HAVING COUNT(*) > 10
            ORDER BY hour_bucket, category
            LIMIT 100
        """)
        time.time() - start_time

        assert isinstance(complex_query_result, SQLResult)
        assert complex_query_result.data is not None

        # Test window function performance
        start_time = time.time()
        window_result = session.execute("""
            SELECT
                category,
                value,
                LAG(value, 1) OVER (PARTITION BY category ORDER BY id) as prev_value,
                LEAD(value, 1) OVER (PARTITION BY category ORDER BY id) as next_value,
                AVG(value) OVER (PARTITION BY category ORDER BY id ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) as rolling_avg,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as value_rank
            FROM analytical_perf_test
            WHERE MOD(id, 100) = 0  -- Sample every 100th row
            ORDER BY category, id
        """)
        time.time() - start_time

        assert isinstance(window_result, SQLResult)
        assert window_result.data is not None

        # Test array operations performance
        start_time = time.time()
        array_ops_result = session.execute("""
            SELECT
                category,
                COUNT(*) as count,
                ARRAY_AGG(DISTINCT dimensions[1] ORDER BY dimensions[1]) as unique_dim1,
                AVG(list_sum(dimensions)) as avg_dimension_sum,
                STDDEV(list_sum(dimensions)) as stddev_dimension_sum
            FROM analytical_perf_test
            GROUP BY category
            ORDER BY count DESC
        """)
        time.time() - start_time

        assert isinstance(array_ops_result, SQLResult)
        assert array_ops_result.data is not None
        assert len(array_ops_result.data) == len(categories)


@pytest.mark.xdist_group("postgres")
def test_prepared_statement_performance(adbc_postgresql_session: AdbcDriver) -> None:
    """Test prepared statement performance benefits with ADBC using CORE_ROUND_3."""
    # Create test table
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS prepared_stmt_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER,
            category VARCHAR(20)
        )
    """)

    # Insert initial data
    initial_data = [(f"item_{i}", i * 5, f"cat_{i % 5}") for i in range(100)]
    adbc_postgresql_session.execute_many(
        """
        INSERT INTO prepared_stmt_test (name, value, category) VALUES ($1, $2, $3)
    """,
        initial_data,
    )

    # Test repeated query execution (simulating prepared statement reuse)
    query_sql = "SELECT * FROM prepared_stmt_test WHERE category = $1 AND value > $2 ORDER BY value LIMIT 10"
    test_parameters = [("cat_0", 25), ("cat_1", 50), ("cat_2", 75), ("cat_0", 100), ("cat_3", 125)]

    # Measure repeated query performance
    start_time = time.time()
    results = []
    for category, min_value in test_parameters:
        result = adbc_postgresql_session.execute(query_sql, (category, min_value))
        assert isinstance(result, SQLResult)
        results.append(result)

    time.time() - start_time

    # Verify all queries returned results
    total_results = sum(len(result.data or []) for result in results)
    assert total_results > 0

    # Test single complex query vs multiple simple queries
    start_time = time.time()
    complex_result = adbc_postgresql_session.execute("""
        SELECT
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM prepared_stmt_test
        GROUP BY category
        HAVING AVG(value) > 50
        ORDER BY avg_value DESC
    """)
    time.time() - start_time

    assert isinstance(complex_result, SQLResult)
    assert complex_result.data is not None

    # Clean up
    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS prepared_stmt_test")


@pytest.mark.xdist_group("postgres")
def test_connection_pooling_simulation(adbc_postgresql_session: AdbcDriver) -> None:
    """Test connection reuse patterns with ADBC using CORE_ROUND_3."""
    # Create test table
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS connection_test (
            id SERIAL PRIMARY KEY,
            session_id VARCHAR(20),
            operation_count INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Simulate multiple "sessions" reusing the same connection
    session_count = 10
    operations_per_session = 20

    start_time = time.time()

    for session_id in range(session_count):
        session_name = f"session_{session_id:02d}"

        # Simulate multiple operations per session
        for op_count in range(operations_per_session):
            adbc_postgresql_session.execute(
                """
                INSERT INTO connection_test (session_id, operation_count)
                VALUES ($1, $2)
            """,
                (session_name, op_count),
            )

            # Occasionally query data (read operation)
            if op_count % 5 == 0:
                query_result = adbc_postgresql_session.execute(
                    """
                    SELECT COUNT(*) as count FROM connection_test
                    WHERE session_id = $1
                """,
                    (session_name,),
                )
                assert isinstance(query_result, SQLResult)

    time.time() - start_time
    session_count * operations_per_session * 1.2  # Including reads

    # Verify all data was inserted correctly
    final_count_result = adbc_postgresql_session.execute("SELECT COUNT(*) as total FROM connection_test")
    assert isinstance(final_count_result, SQLResult)
    assert final_count_result.data is not None
    expected_inserts = session_count * operations_per_session
    assert final_count_result.data[0]["total"] == expected_inserts

    # Clean up
    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS connection_test")
