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
        statement_config=SQLConfig(
            strict_mode=False,
            enable_transformations=False,  # Disable literal parameterization
        ),
    )

    with config.provide_session() as session:
        # Clean up any existing test tables first
        session.execute_script("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_arrow_data';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
        """)

        # Set up test data - corrected table name
        session.execute_script("""
            CREATE TABLE test_arrow_data (
                id NUMBER,
                name VARCHAR2(100),
                value NUMBER,
                price NUMBER(10,2),
                is_active NUMBER(1)
            )
        """)

        session.execute_script("""
            INSERT ALL
                INTO test_arrow_data VALUES (1, 'Product A', 100, 19.99, 1)
                INTO test_arrow_data VALUES (2, 'Product B', 200, 29.99, 1)
                INTO test_arrow_data VALUES (3, 'Product C', 300, 39.99, 0)
                INTO test_arrow_data VALUES (4, 'Product D', 400, 49.99, 1)
                INTO test_arrow_data VALUES (5, 'Product E', 500, 59.99, 0)
            SELECT * FROM DUAL
        """)

        yield session

        # Clean up after the test
        try:
            session.execute_script("""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE test_arrow_data';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN RAISE; END IF;
                END;
            """)
        except Exception:
            # Ignore cleanup errors
            pass


@pytest.mark.xdist_group("oracle")
def test_oracledb_fetch_arrow_table(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test fetch_arrow_table method with OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow_data ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names (Oracle returns uppercase column names)
    expected_columns = {"ID", "NAME", "VALUE", "PRICE", "IS_ACTIVE"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check values
    names = result.data["NAME"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.xdist_group("oracle")
def test_oracledb_to_parquet(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test to_parquet export with OracleDB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        oracledb_arrow_session.export_to_storage("SELECT * FROM test_arrow_data WHERE is_active = 1", str(output_path))

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data (Oracle returns uppercase column names)
        names = table["NAME"].to_pylist()
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
    assert result.num_rows == 3
    values = result.data["VALUE"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_empty_result(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test fetch_arrow_table with empty result on OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow_data WHERE value > :threshold", {"threshold": 1000}
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    assert result.data.num_columns >= 5  # Schema should still be present


@pytest.mark.xdist_group("oracle")
def test_oracledb_arrow_data_types(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test Arrow data type mapping for OracleDB."""
    result = oracledb_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow_data WHERE ROWNUM = 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns (Oracle returns uppercase)
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "ID" in column_names
    assert "NAME" in column_names
    assert "VALUE" in column_names
    assert "PRICE" in column_names
    assert "IS_ACTIVE" in column_names

    # Verify Oracle-specific type mappings
    assert pa.types.is_integer(result.data.schema.field("ID").type)
    assert pa.types.is_string(result.data.schema.field("NAME").type)


@pytest.mark.xdist_group("oracle")
def test_oracledb_to_arrow_with_sql_object(oracledb_arrow_session: OracleSyncDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow_data WHERE is_active = :active", parameters={"active": 1})
    result = oracledb_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["NAME"].to_pylist()
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
    assert result.num_rows == 3  # Products B, C, D
    assert "NAME_UPPER" in result.column_names
    assert "PRICE_WITH_TAX" in result.column_names
    assert "ROW_NUM" in result.column_names
    assert "QUERY_TIME" in result.column_names

    # Verify Oracle function results
    upper_names = result.data["NAME_UPPER"].to_pylist()
    assert all(name and name.isupper() for name in upper_names if name is not None)

    row_nums = result.data["ROW_NUM"].to_pylist()
    assert row_nums == [1, 2, 3]  # Sequential row numbers


@pytest.mark.xdist_group("oracle")
@pytest.mark.xfail(reason="SYS_CONNECT_BY_PATH function syntax needs adjustment")
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
            SYS_CONNECT_BY_PATH(name, ' -> ') as path,
            CONNECT_BY_ISLEAF as is_leaf
        FROM product_hierarchy
        START WITH parent_id IS NULL
        CONNECT BY PRIOR id = parent_id
        ORDER SIBLINGS BY name
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert "HIERARCHY_LEVEL" in result.column_names
    assert "PATH" in result.column_names
    assert "IS_LEAF" in result.column_names

    # Verify hierarchical query results
    levels = result.data["HIERARCHY_LEVEL"].to_pylist()
    non_null_levels = [level for level in levels if level is not None]
    assert min(non_null_levels) == 1  # Root level
    assert max(non_null_levels) == 4  # Deepest level

    paths = result.data["PATH"].to_pylist()
    assert any("Electronics" in str(path) for path in paths if path is not None)


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
    assert result.num_rows == 5
    assert "VALUE_RANK" in result.column_names
    assert "PRICE_RANK" in result.column_names
    assert "VALUE_TERCILE" in result.column_names
    assert "PREV_VALUE" in result.column_names
    assert "NEXT_VALUE" in result.column_names

    # Verify analytical function results
    ranks = result.data["VALUE_RANK"].to_pylist()
    assert len(set(ranks)) == 5  # All ranks should be unique

    terciles = result.data["VALUE_TERCILE"].to_pylist()
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
    assert result.num_rows >= 1  # At least one category

    # Check for pivot columns (Oracle returns uppercase)
    column_names = result.column_names
    assert "VALUE_CATEGORY" in column_names

    # Verify pivot result structure
    categories = result.data["VALUE_CATEGORY"].to_pylist()
    assert any(cat in ["Low", "Medium", "High"] for cat in categories)
