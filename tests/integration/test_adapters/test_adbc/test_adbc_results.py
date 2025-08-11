"""Test ADBC result handling with Arrow table processing."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core.result import SQLResult
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_session(postgres_service: PostgresService) -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC PostgreSQL session for result testing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS result_test (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER,
                price DECIMAL(10,2),
                is_active BOOLEAN DEFAULT true,
                tags TEXT[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            )
        """)

        # Insert test data
        test_data = [
            ("Product A", 100, 19.99, True, ["electronics", "gadget"], {"category": "tech", "rating": 4.5}),
            ("Product B", 200, 29.99, False, ["home", "kitchen"], {"category": "home", "rating": 3.8}),
            ("Product C", 150, 24.99, True, ["outdoor", "sport"], {"category": "sports", "rating": 4.2}),
        ]

        session.execute_many(
            """
            INSERT INTO result_test (name, value, price, is_active, tags, metadata)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
            test_data,
        )

        yield session

        # Cleanup
        try:
            session.execute_script("DROP TABLE IF EXISTS result_test")
        except Exception:
            pass


@pytest.mark.xdist_group("postgres")
def test_sql_result_basic_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test basic SQLResult operations with ADBC."""
    result = adbc_postgresql_session.execute("SELECT * FROM result_test ORDER BY name")
    assert isinstance(result, SQLResult)

    # Test data access
    assert result.data is not None
    assert len(result.data) == 3

    # Test column names
    expected_columns = {"id", "name", "value", "price", "is_active", "tags", "created_at", "metadata"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Test get_count()
    assert result.get_count() == 3

    # Test is_empty()
    assert not result.is_empty()

    # Test get_first()
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "Product A"


@pytest.mark.xdist_group("postgres")
def test_sql_result_arrow_data_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test Arrow data type handling in SQLResult with ADBC."""
    result = adbc_postgresql_session.execute(
        """
        SELECT
            name,
            value,
            price,
            is_active,
            tags,
            created_at,
            metadata
        FROM result_test
        WHERE name = $1
    """,
        ("Product A",),
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]

    # Test string handling
    assert isinstance(row["name"], str)
    assert row["name"] == "Product A"

    # Test integer handling
    assert isinstance(row["value"], int)
    assert row["value"] == 100

    # Test decimal/numeric handling
    assert row["price"] is not None
    # Price might be returned as float or Decimal depending on driver
    assert float(row["price"]) == 19.99

    # Test boolean handling
    assert isinstance(row["is_active"], bool)
    assert row["is_active"] is True

    # Test array handling (PostgreSQL arrays)
    assert row["tags"] is not None
    assert isinstance(row["tags"], list)
    assert "electronics" in row["tags"]
    assert "gadget" in row["tags"]

    # Test timestamp handling
    assert row["created_at"] is not None

    # Test JSON/JSONB handling
    assert row["metadata"] is not None


@pytest.mark.xdist_group("postgres")
def test_sql_result_empty_results(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult empty result handling with ADBC."""
    result = adbc_postgresql_session.execute("SELECT * FROM result_test WHERE name = $1", ("NonExistent",))
    assert isinstance(result, SQLResult)

    # Test empty result properties
    assert result.is_empty()
    assert result.get_count() == 0
    assert result.get_first() is None
    assert result.data is not None
    assert len(result.data) == 0


@pytest.mark.xdist_group("postgres")
def test_sql_result_null_value_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult NULL value handling with ADBC."""
    # Insert row with NULL values
    adbc_postgresql_session.execute(
        """
        INSERT INTO result_test (name, value, price, is_active, tags, metadata)
        VALUES ($1, NULL, NULL, NULL, NULL, NULL)
    """,
        ("Null Product",),
    )

    result = adbc_postgresql_session.execute("SELECT * FROM result_test WHERE name = $1", ("Null Product",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]
    assert row["name"] == "Null Product"
    assert row["value"] is None
    assert row["price"] is None
    assert row["is_active"] is None
    assert row["tags"] is None
    assert row["metadata"] is None


@pytest.mark.xdist_group("postgres")
def test_sql_result_aggregation_results(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult with aggregation queries using ADBC."""
    result = adbc_postgresql_session.execute("""
        SELECT
            COUNT(*) as total_products,
            AVG(value) as avg_value,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(value) as total_value,
            COUNT(CASE WHEN is_active THEN 1 END) as active_count
        FROM result_test
    """)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]
    assert row["total_products"] == 3
    assert float(row["avg_value"]) == 150.0  # (100 + 200 + 150) / 3
    assert float(row["min_price"]) == 19.99
    assert float(row["max_price"]) == 29.99
    assert row["total_value"] == 450  # 100 + 200 + 150
    assert row["active_count"] == 2  # Product A and C are active


@pytest.mark.xdist_group("postgres")
def test_sql_result_complex_queries(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult with complex queries using ADBC."""
    # Test JOIN-like query using LATERAL
    result = adbc_postgresql_session.execute("""
        SELECT
            r.name,
            r.value,
            r.is_active,
            CASE
                WHEN r.value > 150 THEN 'high'
                WHEN r.value > 100 THEN 'medium'
                ELSE 'low'
            END as value_category,
            array_length(r.tags, 1) as tag_count
        FROM result_test r
        WHERE r.is_active = true
        ORDER BY r.value DESC
    """)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2  # Only active products

    # First row should be Product C (value=150)
    first_row = result.data[0]
    assert first_row["name"] == "Product C"
    assert first_row["value"] == 150
    assert first_row["value_category"] == "medium"
    assert first_row["tag_count"] == 2

    # Second row should be Product A (value=100)
    second_row = result.data[1]
    assert second_row["name"] == "Product A"
    assert second_row["value"] == 100
    assert second_row["value_category"] == "low"
    assert second_row["tag_count"] == 2


@pytest.mark.xdist_group("postgres")
def test_sql_result_column_name_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult column name handling with aliases using ADBC."""
    result = adbc_postgresql_session.execute(
        """
        SELECT
            name as product_name,
            value as product_value,
            price as product_price,
            is_active as is_available
        FROM result_test
        WHERE name = $1
    """,
        ("Product A",),
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    # Test column names include aliases
    expected_columns = ["product_name", "product_value", "product_price", "is_available"]
    for col in expected_columns:
        assert col in result.column_names

    row = result.data[0]
    assert row["product_name"] == "Product A"
    assert row["product_value"] == 100
    assert float(row["product_price"]) == 19.99
    assert row["is_available"] is True


@pytest.mark.xdist_group("postgres")
def test_sql_result_large_result_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult handling of larger result sets using ADBC."""
    # Insert additional test data
    bulk_data = [(f"Bulk Product {i}", i * 10, i * 2.5, i % 2 == 0) for i in range(1, 101)]
    adbc_postgresql_session.execute_many(
        """
        INSERT INTO result_test (name, value, price, is_active)
        VALUES ($1, $2, $3, $4)
    """,
        bulk_data,
    )

    # Query all data
    result = adbc_postgresql_session.execute("SELECT * FROM result_test ORDER BY id")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() >= 100  # At least 100 bulk items plus original 3

    # Test pagination-like behavior
    page_result = adbc_postgresql_session.execute("""
        SELECT name, value FROM result_test
        WHERE name LIKE 'Bulk Product%'
        ORDER BY value
        LIMIT 10 OFFSET 20
    """)
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10

    # Verify ordering
    values = [row["value"] for row in page_result.data]
    assert values == sorted(values)  # Should be in ascending order


@pytest.fixture
def adbc_sqlite_session() -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC SQLite session for Arrow testing."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE arrow_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                data BLOB,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert test data
        session.execute_many(
            """
            INSERT INTO arrow_test (name, data) VALUES (?, ?)
        """,
            [
                ("test1", b"binary_data_1"),
                ("test2", b"binary_data_2"),
                ("test3", None),  # Test NULL binary data
            ],
        )

        yield session


@pytest.mark.xdist_group("adbc_sqlite")
def test_sql_result_arrow_sqlite_types(adbc_sqlite_session: AdbcDriver) -> None:
    """Test SQLResult Arrow type handling with SQLite."""
    result = adbc_sqlite_session.execute("SELECT * FROM arrow_test ORDER BY id")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3

    # Test various SQLite data types through Arrow
    first_row = result.data[0]
    assert isinstance(first_row["id"], int)
    assert isinstance(first_row["name"], str)
    assert first_row["name"] == "test1"
    assert first_row["data"] == b"binary_data_1"
    assert first_row["timestamp"] is not None

    # Test NULL binary data
    third_row = result.data[2]
    assert third_row["data"] is None


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_sql_result_arrow_duckdb_advanced_types() -> None:
    """Test SQLResult with DuckDB advanced Arrow types."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        # DuckDB supports advanced Arrow types
        result = session.execute("""
            SELECT
                [1, 2, 3, 4, 5] as int_array,
                ['a', 'b', 'c'] as string_array,
                {'name': 'John', 'age': 30} as struct_data,
                [[1, 2], [3, 4]] as nested_array,
                MAP(['key1', 'key2'], ['value1', 'value2']) as map_data
        """)

        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1

        row = result.data[0]

        # Test array handling
        assert row["int_array"] == [1, 2, 3, 4, 5]
        assert row["string_array"] == ["a", "b", "c"]

        # Test complex types (exact format may vary by DuckDB version)
        assert row["struct_data"] is not None
        assert row["nested_array"] is not None
        assert row["map_data"] is not None
