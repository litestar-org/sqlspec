"""Integration tests for OracleDB Arrow query support."""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver

pytestmark = pytest.mark.xdist_group("oracle")


async def _safe_drop_table(driver: "OracleAsyncDriver", table_name: str) -> None:
    await driver.execute(
        f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """
    )
    await driver.commit()


async def test_select_to_arrow_basic(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test basic select_to_arrow functionality."""
    import pyarrow as pa

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_users (id NUMBER, name VARCHAR2(100), age NUMBER)")
    await driver.execute("INSERT INTO arrow_users VALUES (1, 'Alice', 30)")
    await driver.execute("INSERT INTO arrow_users VALUES (2, 'Bob', 25)")
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_users ORDER BY id")

        assert result is not None
        assert isinstance(result.data, (pa.Table, pa.RecordBatch))
        assert result.rows_affected == 2

        df = result.to_pandas()
        assert len(df) == 2
        assert list(df["name"]) == ["Alice", "Bob"]
    finally:
        await _safe_drop_table(driver, "arrow_users")


async def test_select_to_arrow_table_format(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow with table return format (default)."""
    import pyarrow as pa

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_table_test (id NUMBER, value VARCHAR2(100))")
    await driver.execute(
        "INSERT ALL INTO arrow_table_test VALUES (1, 'a') INTO arrow_table_test VALUES (2, 'b') INTO arrow_table_test VALUES (3, 'c') SELECT * FROM dual"
    )
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_table_test ORDER BY id", return_format="table")

        assert isinstance(result.data, pa.Table)
        assert result.rows_affected == 3
    finally:
        await _safe_drop_table(driver, "arrow_table_test")


async def test_select_to_arrow_batch_format(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow with batch return format."""
    import pyarrow as pa

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_batch_test (id NUMBER, value VARCHAR2(100))")
    await driver.execute(
        "INSERT ALL INTO arrow_batch_test VALUES (1, 'a') INTO arrow_batch_test VALUES (2, 'b') SELECT * FROM dual"
    )
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_batch_test ORDER BY id", return_format="batch")

        assert isinstance(result.data, pa.RecordBatch)
        assert result.rows_affected == 2
    finally:
        await _safe_drop_table(driver, "arrow_batch_test")


async def test_select_to_arrow_with_parameters(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow with query parameters."""

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_params_test (id NUMBER, value NUMBER)")
    await driver.execute(
        "INSERT ALL INTO arrow_params_test VALUES (1, 100) INTO arrow_params_test VALUES (2, 200) INTO arrow_params_test VALUES (3, 300) SELECT * FROM dual"
    )
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_params_test WHERE value > :1 ORDER BY id", (150,))

        assert result.rows_affected == 2
        df = result.to_pandas()
        assert list(df["value"]) == [200, 300]
    finally:
        await _safe_drop_table(driver, "arrow_params_test")


async def test_select_to_arrow_empty_result(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow with empty result set."""

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_empty_test (id NUMBER)")
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_empty_test")

        assert result.rows_affected == 0
        assert len(result.to_pandas()) == 0
    finally:
        await _safe_drop_table(driver, "arrow_empty_test")


async def test_select_to_arrow_null_handling(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow with NULL values."""

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_null_test (id NUMBER, value VARCHAR2(100))")
    await driver.execute(
        "INSERT ALL INTO arrow_null_test VALUES (1, 'a') INTO arrow_null_test VALUES (2, NULL) INTO arrow_null_test VALUES (3, 'c') SELECT * FROM dual"
    )
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_null_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 3
        assert df.iloc[1]["value"] is None or df.isna().iloc[1]["value"]
    finally:
        await _safe_drop_table(driver, "arrow_null_test")


async def test_select_to_arrow_to_polars(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow conversion to Polars DataFrame."""

    pytest.importorskip("polars")

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_polars_test (id NUMBER, value VARCHAR2(100))")
    await driver.execute(
        "INSERT ALL INTO arrow_polars_test VALUES (1, 'a') INTO arrow_polars_test VALUES (2, 'b') SELECT * FROM dual"
    )
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_polars_test ORDER BY id")
        df = result.to_polars()

        assert len(df) == 2
        assert df["value"].to_list() == ["a", "b"]
    finally:
        await _safe_drop_table(driver, "arrow_polars_test")


async def test_select_to_arrow_large_dataset(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test select_to_arrow with larger dataset."""

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_large_test (id NUMBER, value NUMBER)")
    await driver.commit()

    await driver.execute(
        """
        BEGIN
            FOR i IN 1..1000 LOOP
                INSERT INTO arrow_large_test VALUES (i, i * 10);
            END LOOP;
            COMMIT;
        END;
        """
    )

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_large_test ORDER BY id")

        assert result.rows_affected == 1000
        df = result.to_pandas()
        assert len(df) == 1000
        assert df["value"].sum() == sum(i * 10 for i in range(1, 1001))
    finally:
        await _safe_drop_table(driver, "arrow_large_test")


async def test_select_to_arrow_type_preservation(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test that Oracle types are properly converted to Arrow types."""

    driver = oracle_async_session

    await driver.execute(
        """
        CREATE TABLE arrow_types_test (
            id NUMBER,
            name VARCHAR2(100),
            price NUMBER,
            created_at DATE,
            is_active NUMBER(1)
        )
        """
    )
    await driver.execute(
        """
        INSERT ALL
            INTO arrow_types_test VALUES (1, 'Item 1', 19.99, DATE '2025-01-01', 1)
            INTO arrow_types_test VALUES (2, 'Item 2', 29.99, DATE '2025-01-02', 0)
        SELECT * FROM dual
        """
    )
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_types_test ORDER BY id")

        df = result.to_pandas()
        assert len(df) == 2
        assert df["name"].dtype == object
        assert set(df["is_active"].unique()) <= {0, 1}
    finally:
        await _safe_drop_table(driver, "arrow_types_test")


async def test_select_to_arrow_clob_handling(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test CLOB handling when streaming to Arrow."""

    driver = oracle_async_session

    await driver.execute("CREATE TABLE arrow_clob_test (id NUMBER, description CLOB)")
    await driver.execute("INSERT INTO arrow_clob_test VALUES (1, RPAD('A', 2048, 'A'))")
    await driver.commit()

    try:
        result = await driver.select_to_arrow("SELECT * FROM arrow_clob_test")

        df = result.to_pandas()
        assert len(df) == 1
        assert isinstance(df["description"].iloc[0], str)
    finally:
        await _safe_drop_table(driver, "arrow_clob_test")
