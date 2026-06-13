"""Integration tests for Oracle sparse VECTOR passthrough."""

from collections.abc import Callable
from typing import Any, cast

import pytest

from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OraclePoolParams,
    OracleSyncConfig,
    OracleSyncDriver,
)
from sqlspec.adapters.oracledb.core import ORACLEDB_SUPPORTS_SPARSE_VECTORS
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("oracle")


def _sparse_vector_type() -> "type[object]":
    if not ORACLEDB_SUPPORTS_SPARSE_VECTORS:
        pytest.skip("python-oracledb does not provide SparseVector")

    import oracledb

    sparse_type: type[object] | None = getattr(oracledb, "SparseVector", None)
    if sparse_type is None:
        pytest.skip("python-oracledb does not provide SparseVector")
    return sparse_type


def _sparse_vector() -> object:
    sparse_vector = cast("Callable[[int, list[int], list[float]], object]", _sparse_vector_type())
    return sparse_vector(8, [0, 5], [1.0, 2.5])


def _assert_sparse_vector(value: object) -> None:
    sparse_type = _sparse_vector_type()
    assert isinstance(value, sparse_type)
    assert value.num_dimensions == 8  # pyright: ignore[reportAttributeAccessIssue]
    assert list(value.indices) == [0, 5]  # pyright: ignore[reportAttributeAccessIssue]
    assert list(value.values) == [1.0, 2.5]  # pyright: ignore[reportAttributeAccessIssue]


def _assert_dense_list(value: object) -> None:
    assert isinstance(value, list)
    assert value == [0.25, 0.5, 0.75]


def _drop_table_sync(driver: OracleSyncDriver, table_name: str) -> None:
    driver.execute_script(
        f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """
    )


async def _drop_table_async(driver: OracleAsyncDriver, table_name: str) -> None:
    await driver.execute_script(
        f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """
    )


def _create_sparse_table_sync(driver: OracleSyncDriver, table_name: str) -> None:
    version_info = driver.data_dictionary.get_version(driver)
    if version_info is None or version_info.major < 23:
        pytest.skip("database does not support sparse VECTOR")

    _drop_table_sync(driver, table_name)
    try:
        driver.execute(
            f"""
            CREATE TABLE {table_name} (
                id NUMBER PRIMARY KEY,
                sparse_vector VECTOR(8, FLOAT32, SPARSE),
                dense_vector VECTOR(3, FLOAT32)
            )
            """
        )
    except SQLSpecError:
        pytest.skip("database does not support sparse VECTOR")


async def _create_sparse_table_async(driver: OracleAsyncDriver, table_name: str) -> None:
    version_info = await driver.data_dictionary.get_version(driver)
    if version_info is None or version_info.major < 23:
        pytest.skip("database does not support sparse VECTOR")

    await _drop_table_async(driver, table_name)
    try:
        await driver.execute(
            f"""
            CREATE TABLE {table_name} (
                id NUMBER PRIMARY KEY,
                sparse_vector VECTOR(8, FLOAT32, SPARSE),
                dense_vector VECTOR(3, FLOAT32)
            )
            """
        )
    except SQLSpecError:
        pytest.skip("database does not support sparse VECTOR")


def _row_value(row: Any, key: str) -> object:
    if key in row:
        return row[key]
    return row[key.upper()]


def test_sync_sparse_vector_round_trip_native_value(oracle_sync_session: OracleSyncDriver) -> None:
    """Sparse VECTOR columns round-trip as native python-oracledb values."""
    table_name = "test_sparse_vectors_sync"
    _create_sparse_table_sync(oracle_sync_session, table_name)
    try:
        oracle_sync_session.execute(
            f"INSERT INTO {table_name} (id, sparse_vector, dense_vector) VALUES (:1, :2, :3)",
            (1, _sparse_vector(), "[0.25, 0.5, 0.75]"),
        )
        row = oracle_sync_session.select_one(f"SELECT sparse_vector FROM {table_name} WHERE id = :1", (1,))
        _assert_sparse_vector(_row_value(row, "sparse_vector"))
    finally:
        _drop_table_sync(oracle_sync_session, table_name)


def test_sync_sparse_vector_ignores_list_return_format(oracle_connection_config: OraclePoolParams) -> None:
    """Sparse VECTOR remains native while dense VECTOR honors list return format."""
    table_name = "test_sparse_vectors_list_sync"
    config = OracleSyncConfig(
        connection_config=OraclePoolParams(**oracle_connection_config), driver_features={"vector_return_format": "list"}
    )
    with config.provide_session() as driver:
        _create_sparse_table_sync(driver, table_name)
        try:
            driver.execute(
                f"INSERT INTO {table_name} (id, sparse_vector, dense_vector) VALUES (:1, :2, :3)",
                (1, _sparse_vector(), "[0.25, 0.5, 0.75]"),
            )
            row = driver.select_one(f"SELECT sparse_vector, dense_vector FROM {table_name} WHERE id = :1", (1,))
            _assert_sparse_vector(_row_value(row, "sparse_vector"))
            _assert_dense_list(_row_value(row, "dense_vector"))
        finally:
            _drop_table_sync(driver, table_name)


async def test_async_sparse_vector_round_trip_native_value(oracle_async_session: OracleAsyncDriver) -> None:
    """Sparse VECTOR columns round-trip as native python-oracledb values."""
    table_name = "test_sparse_vectors_async"
    await _create_sparse_table_async(oracle_async_session, table_name)
    try:
        await oracle_async_session.execute(
            f"INSERT INTO {table_name} (id, sparse_vector, dense_vector) VALUES (:1, :2, :3)",
            (1, _sparse_vector(), "[0.25, 0.5, 0.75]"),
        )
        row = await oracle_async_session.select_one(f"SELECT sparse_vector FROM {table_name} WHERE id = :1", (1,))
        _assert_sparse_vector(_row_value(row, "sparse_vector"))
    finally:
        await _drop_table_async(oracle_async_session, table_name)


async def test_async_sparse_vector_ignores_list_return_format(oracle_connection_config: OraclePoolParams) -> None:
    """Sparse VECTOR remains native while dense VECTOR honors list return format."""
    table_name = "test_sparse_vectors_list_async"
    config = OracleAsyncConfig(
        connection_config=OraclePoolParams(**oracle_connection_config), driver_features={"vector_return_format": "list"}
    )
    async with config.provide_session() as driver:
        await _create_sparse_table_async(driver, table_name)
        try:
            await driver.execute(
                f"INSERT INTO {table_name} (id, sparse_vector, dense_vector) VALUES (:1, :2, :3)",
                (1, _sparse_vector(), "[0.25, 0.5, 0.75]"),
            )
            row = await driver.select_one(f"SELECT sparse_vector, dense_vector FROM {table_name} WHERE id = :1", (1,))
            _assert_sparse_vector(_row_value(row, "sparse_vector"))
            _assert_dense_list(_row_value(row, "dense_vector"))
        finally:
            await _drop_table_async(driver, table_name)
