# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""MySQL-family exception contract tests."""

import inspect
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("mysql")

MYSQL_ADAPTERS = [
    pytest.param("aiomysql", id="aiomysql"),
    pytest.param("asyncmy", id="asyncmy"),
    pytest.param("mysqlconnector", marks=pytest.mark.mysql_connector, id="mysqlconnector"),
    pytest.param("pymysql", marks=[pytest.mark.mysql, pytest.mark.pymysql], id="pymysql"),
]


def _mysql_config(adapter: str, mysql_service: MySQLService) -> Any:
    if adapter == "aiomysql":
        return AiomysqlConfig(
            connection_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "db": mysql_service.db,
                "autocommit": True,
                "minsize": 1,
                "maxsize": 5,
            }
        )
    if adapter == "asyncmy":
        return AsyncmyConfig(
            connection_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
                "minsize": 1,
                "maxsize": 5,
            }
        )
    if adapter == "mysqlconnector":
        return MysqlConnectorAsyncConfig(
            connection_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
                "use_pure": True,
            }
        )
    return PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        }
    )


async def _close_pool(config: Any) -> None:
    close_pool = getattr(config, "close_pool", None)
    if close_pool is None:
        config.connection_instance = None
        return

    result = close_pool()
    if inspect.isawaitable(result):
        await result

    config.connection_instance = None


@contextmanager
def _pymysql_session(mysql_service: MySQLService) -> Generator[Any, None, None]:
    config = _mysql_config("pymysql", mysql_service)
    try:
        with config.provide_session() as driver:
            yield driver
    finally:
        config.close_pool()


@asynccontextmanager
async def _async_mysql_session(adapter: str, mysql_service: MySQLService) -> AsyncGenerator[Any, None]:
    config = _mysql_config(adapter, mysql_service)
    try:
        async with config.provide_session() as driver:
            yield driver
    finally:
        await _close_pool(config)


async def _execute(driver: Any, statement: str, parameters: tuple[Any, ...] | None = None) -> Any:
    result = driver.execute(statement) if parameters is None else driver.execute(statement, parameters)
    if inspect.isawaitable(result):
        return await result
    return result


async def _execute_script(driver: Any, script: str) -> Any:
    result = driver.execute_script(script)
    if inspect.isawaitable(result):
        return await result
    return result


async def _with_mysql_family_driver(adapter: str, mysql_service: MySQLService, assertion: Any) -> None:
    if adapter == "pymysql":
        with _pymysql_session(mysql_service) as driver:
            await assertion(driver)
        return

    async with _async_mysql_session(adapter, mysql_service) as driver:
        await assertion(driver)


@pytest.mark.parametrize("adapter", MYSQL_ADAPTERS)
async def test_mysql_family_unique_violation(adapter: str, mysql_service: MySQLService) -> None:
    """MySQL-family adapters map unique constraint failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_unique_constraint;
            CREATE TABLE test_unique_constraint (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL
            );
            """,
        )

        await _execute(driver, "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",))

        with pytest.raises(UniqueViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",))

        assert "unique" in str(exc_info.value).lower() or "1062" in str(exc_info.value)

        await _execute(driver, "DROP TABLE test_unique_constraint")

    await _with_mysql_family_driver(adapter, mysql_service, assertion)


@pytest.mark.parametrize("adapter", MYSQL_ADAPTERS)
async def test_mysql_family_foreign_key_violation(adapter: str, mysql_service: MySQLService) -> None:
    """MySQL-family adapters map foreign key failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_fk_child;
            DROP TABLE IF EXISTS test_fk_parent;
            CREATE TABLE test_fk_parent (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100)
            ) ENGINE=InnoDB;
            CREATE TABLE test_fk_child (
                id INT AUTO_INCREMENT PRIMARY KEY,
                parent_id INT NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES test_fk_parent(id)
            ) ENGINE=InnoDB;
            """,
        )

        with pytest.raises(ForeignKeyViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_fk_child (parent_id) VALUES (%s)", (999,))

        assert "foreign key" in str(exc_info.value).lower() or any(
            code in str(exc_info.value) for code in ["1216", "1452"]
        )

        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_fk_child;
            DROP TABLE IF EXISTS test_fk_parent;
            """,
        )

    await _with_mysql_family_driver(adapter, mysql_service, assertion)


@pytest.mark.parametrize("adapter", MYSQL_ADAPTERS)
async def test_mysql_family_not_null_violation(adapter: str, mysql_service: MySQLService) -> None:
    """MySQL-family adapters map NOT NULL failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_not_null;
            CREATE TABLE test_not_null (
                id INT AUTO_INCREMENT PRIMARY KEY,
                required_field VARCHAR(100) NOT NULL
            );
            """,
        )

        with pytest.raises(NotNullViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_not_null (id) VALUES (%s)", (1,))

        assert "cannot be null" in str(exc_info.value).lower() or any(
            code in str(exc_info.value) for code in ["1048", "1364"]
        )

        await _execute(driver, "DROP TABLE test_not_null")

    await _with_mysql_family_driver(adapter, mysql_service, assertion)


@pytest.mark.parametrize("adapter", MYSQL_ADAPTERS)
async def test_mysql_family_check_violation(adapter: str, mysql_service: MySQLService) -> None:
    """MySQL-family adapters map CHECK constraint failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_check_constraint;
            CREATE TABLE test_check_constraint (
                id INT AUTO_INCREMENT PRIMARY KEY,
                age INT CHECK (age >= 18)
            );
            """,
        )

        with pytest.raises(CheckViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_check_constraint (age) VALUES (%s)", (15,))

        assert "check" in str(exc_info.value).lower() or "3819" in str(exc_info.value)

        await _execute(driver, "DROP TABLE test_check_constraint")

    await _with_mysql_family_driver(adapter, mysql_service, assertion)


@pytest.mark.parametrize("adapter", MYSQL_ADAPTERS)
async def test_mysql_family_sql_parsing_error(adapter: str, mysql_service: MySQLService) -> None:
    """MySQL-family adapters map syntax failures."""

    async def assertion(driver: Any) -> None:
        with pytest.raises(SQLParsingError) as exc_info:
            await _execute(driver, "SELCT * FROM nonexistent_table")

        assert "syntax" in str(exc_info.value).lower() or "1064" in str(exc_info.value)

    await _with_mysql_family_driver(adapter, mysql_service, assertion)
