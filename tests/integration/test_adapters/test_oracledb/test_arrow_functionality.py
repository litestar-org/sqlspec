"""Test Arrow functionality for OracleDB drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleSyncConfig, OracleSyncDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def oracledb_arrow_session(oracle_23ai_service: OracleService) -> "Generator[OracleSyncDriver, None, None]":
    """Create an OracleDB session for Arrow testing using real Oracle service."""

    config = OracleSyncConfig(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Set up test data
        session.execute_script("""
            CREATE TABLE test_arrow_data_data (
                id NUMBER,
                name VARCHAR2(100),
                value NUMBER,
                price NUMBER(10,2),
                is_active NUMBER(1)
            )
        """)

        session.execute_script("""
            INSERT ALL
                INTO test_arrow_data_data VALUES (1, 'Product A', 100, 19.99, 1)
                INTO test_arrow_data_data VALUES (2, 'Product B', 200, 29.99, 1)
                INTO test_arrow_data_data VALUES (3, 'Product C', 300, 39.99, 0)
                INTO test_arrow_data_data VALUES (4, 'Product D', 400, 49.99, 1)
                INTO test_arrow_data_data VALUES (5, 'Product E', 500, 59.99, 0)
            SELECT * FROM DUAL
        """)

        yield session


@pytest.mark.xdist_group("oracle")
def test_oracledb_fetch_arrow_table(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test fetch_arrow_table method with OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow_data ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check values
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.xdist_group("oracle")
def test_oracledb_to_parquet(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test to_parquet export with OracleDB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        oracledb_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow_data WHERE is_active = 1",
            str(output_path),
        )

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data
        names = table["name"].to_pylist()
        assert "Product A" in names
        assert "Product C" not in names  # Inactive product


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_with_parameters(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test fetch_arrow_table with parameters on OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow_data WHERE value >= :min_val AND value <= :max_val ORDER BY value",
        {"min_val": 200, "max_val": 400},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_empty_result(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test fetch_arrow_table with empty result on OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow_data WHERE value > :threshold",
        {"threshold": 1000},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 0
    assert result.data.num_columns >= 5  # Schema should still be present


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_data_types(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Arrow data type mapping for OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow_data WHERE ROWNUM = 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names

    # Verify Oracle-specific type mappings
    assert pa.types.is_integer(result.data.schema.field("id").type)
    assert pa.types.is_string(result.data.schema.field("name").type)


@pytest.mark.xdist_group("oracle")
def test_oracledb_to_arrow_with_sql_object(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow_data WHERE is_active = :active", parameters={"active": 1})
    result = oracledb_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_with_oracle_functions(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Arrow functionality with Oracle-specific functions."""
    result = oracledb_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            price,
            UPPER(name) as name_upper,
            ROUND(price * 1.1, 2) as price_with_tax,
            ROW_NUMBER() OVER (ORDER BY value) as row_num,
            SYSDATE as query_time
        FROM test_arrow_data
        WHERE value BETWEEN :min_val AND :max_val
        ORDER BY value
    """,
        {"min_val": 200, "max_val": 400},
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3  # Products B, C, D
    assert "name_upper" in result.column_names
    assert "price_with_tax" in result.column_names
    assert "row_num" in result.column_names
    assert "query_time" in result.column_names

    # Verify Oracle function results
    upper_names = result.data["name_upper"].to_pylist()
    assert all(name.isupper() for name in upper_names)

    row_nums = result.data["row_num"].to_pylist()
    assert row_nums == [1, 2, 3]  # Sequential row numbers


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_with_hierarchical_queries(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Arrow functionality with Oracle hierarchical queries."""
    # Create a hierarchical table structure
    oracledb_arrow_session.execute_script("""
        CREATE TABLE product_hierarchy (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100),
            parent_id NUMBER,
            level_num NUMBER
        )
    """)

    oracledb_arrow_session.execute_many(
        "INSERT INTO product_hierarchy (id, name, parent_id, level_num) VALUES (:1, :2, :3, :4)",
        [
            (1, "Electronics", None, 1),
            (2, "Computers", 1, 2),
            (3, "Laptops", 2, 3),
            (4, "Gaming Laptops", 3, 4),
            (5, "Mobile", 1, 2),
        ],
    )

    result = oracledb_arrow_session.fetch_arrow_table("""
        SELECT
            id,
            name,
            parent_id,
            LEVEL as hierarchy_level,
            SYS_CONNECT_BY_PATH(name, '/') as path,
            CONNECT_BY_ISLEAF as is_leaf
        FROM product_hierarchy
        START WITH parent_id IS NULL
        CONNECT BY PRIOR id = parent_id
        ORDER SIBLINGS BY name
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert "hierarchy_level" in result.column_names
    assert "path" in result.column_names
    assert "is_leaf" in result.column_names

    # Verify hierarchical query results
    levels = result.data["hierarchy_level"].to_pylist()
    assert min(levels) == 1  # Root level
    assert max(levels) == 4  # Deepest level

    paths = result.data["path"].to_pylist()
    assert any("/Electronics" in path for path in paths)


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_with_analytical_functions(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Arrow functionality with Oracle analytical functions."""
    result = oracledb_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            price,
            RANK() OVER (ORDER BY value DESC) as value_rank,
            DENSE_RANK() OVER (ORDER BY price DESC) as price_rank,
            NTILE(3) OVER (ORDER BY value) as value_tercile,
            LAG(value, 1) OVER (ORDER BY id) as prev_value,
            LEAD(value, 1) OVER (ORDER BY id) as next_value,
            FIRST_VALUE(value) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) as first_value,
            LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as last_value
        FROM test_arrow_data
        ORDER BY id
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert "value_rank" in result.column_names
    assert "price_rank" in result.column_names
    assert "value_tercile" in result.column_names
    assert "prev_value" in result.column_names
    assert "next_value" in result.column_names

    # Verify analytical function results
    ranks = result.data["value_rank"].to_pylist()
    assert len(set(ranks)) == 5  # All ranks should be unique

    terciles = result.data["value_tercile"].to_pylist()
    assert set(terciles) == {1, 2, 3}  # Should have all three terciles


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_with_pivot_operations(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Arrow functionality with Oracle PIVOT operations."""
    result = oracledb_arrow_session.fetch_arrow_table("""
        SELECT * FROM (
            SELECT
                CASE WHEN value <= 200 THEN 'Low'
                     WHEN value <= 400 THEN 'Medium'
                     ELSE 'High' END as value_category,
                is_active,
                price
            FROM test_arrow_data
        )
        PIVOT (
            SUM(price) as total_price,
            COUNT(*) as count
            FOR is_active IN (0 as inactive, 1 as active)
        )
        ORDER BY value_category
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() >= 1  # At least one category

    # Check for pivot columns
    column_names = result.column_names
    assert "value_category" in column_names

    # Verify pivot result structure
    categories = result.data["value_category"].to_pylist()
    assert any(cat in ["Low", "Medium", "High"] for cat in categories)


@pytest.mark.xdist_group("oracle")
def test_oracledb_parquet_export_with_clob_handling(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Parquet export with CLOB data handling."""
    # Create table with CLOB column
    oracledb_arrow_session.execute_script("""
        CREATE TABLE test_clob (
            id NUMBER,
            description CLOB,
            metadata CLOB
        )
    """)

    oracledb_arrow_session.execute_many(
        "INSERT INTO test_clob (id, description, metadata) VALUES (:1, :2, :3)",
        [
            (1, "Short description", '{"type": "product", "category": "electronics"}'),
            (
                2,
                "A very long description that would normally be stored in a CLOB field because it exceeds the VARCHAR2 size limits",
                '{"type": "service", "priority": "high"}',
            ),
        ],
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "clob_export.parquet"

        oracledb_arrow_session.export_to_storage(
            "SELECT id, description, metadata FROM test_clob ORDER BY id",
            str(output_path),
        )

        assert output_path.exists()

        # Verify CLOB data can be read from Parquet
        table = pq.read_table(output_path)
        assert table.num_rows == 2

        descriptions = table["description"].to_pylist()
        assert "Short description" in descriptions
        assert any(len(desc) > 100 for desc in descriptions)  # Long CLOB content
