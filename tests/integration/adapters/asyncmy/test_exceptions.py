"""Exception handling integration tests for asyncmy adapter."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def asyncmy_exception_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create an asyncmy session for exception testing."""
    config = AsyncmyConfig(
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

    try:
        async with config.provide_session() as session:
            yield session
    finally:
        await config.close_pool()


async def test_unique_violation(asyncmy_exception_session: AsyncmyDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    await asyncmy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    await asyncmy_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        await asyncmy_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "1062" in str(exc_info.value)

    await asyncmy_exception_session.execute("DROP TABLE test_unique_constraint")


async def test_foreign_key_violation(asyncmy_exception_session: AsyncmyDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    await asyncmy_exception_session.execute_script("""
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
    """)

    with pytest.raises(ForeignKeyViolationError) as exc_info:
        await asyncmy_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES (%s)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or any(code in str(exc_info.value) for code in ["1216", "1452"])

    await asyncmy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child;
        DROP TABLE IF EXISTS test_fk_parent;
    """)


async def test_not_null_violation(asyncmy_exception_session: AsyncmyDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    await asyncmy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id INT AUTO_INCREMENT PRIMARY KEY,
            required_field VARCHAR(100) NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        await asyncmy_exception_session.execute("INSERT INTO test_not_null (id) VALUES (%s)", (1,))

    assert "cannot be null" in str(exc_info.value).lower() or any(
        code in str(exc_info.value) for code in ["1048", "1364"]
    )

    await asyncmy_exception_session.execute("DROP TABLE test_not_null")


async def test_check_violation(asyncmy_exception_session: AsyncmyDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    await asyncmy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id INT AUTO_INCREMENT PRIMARY KEY,
            age INT CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        await asyncmy_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES (%s)", (15,))

    assert "check" in str(exc_info.value).lower() or "3819" in str(exc_info.value)

    await asyncmy_exception_session.execute("DROP TABLE test_check_constraint")


async def test_sql_parsing_error(asyncmy_exception_session: AsyncmyDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        await asyncmy_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower() or "1064" in str(exc_info.value)
