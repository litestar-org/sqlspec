"""Exception handling integration tests for PyMySQL adapter."""

import pytest

from sqlspec.adapters.pymysql import PyMysqlDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql]


@pytest.fixture
def pymysql_exception_session(pymysql_driver: PyMysqlDriver) -> PyMysqlDriver:
    """Use PyMySQL driver for exception testing."""
    return pymysql_driver


def test_unique_violation(pymysql_exception_session: PyMysqlDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    pymysql_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    pymysql_exception_session.execute("INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",))

    with pytest.raises(UniqueViolationError) as exc_info:
        pymysql_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "1062" in str(exc_info.value)

    pymysql_exception_session.execute("DROP TABLE test_unique_constraint")


def test_foreign_key_violation(pymysql_exception_session: PyMysqlDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    pymysql_exception_session.execute_script("""
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
        pymysql_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES (%s)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or any(code in str(exc_info.value) for code in ["1216", "1452"])

    pymysql_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child;
        DROP TABLE IF EXISTS test_fk_parent;
    """)


def test_not_null_violation(pymysql_exception_session: PyMysqlDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    pymysql_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id INT AUTO_INCREMENT PRIMARY KEY,
            required_field VARCHAR(100) NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        pymysql_exception_session.execute("INSERT INTO test_not_null (id) VALUES (%s)", (1,))

    assert "cannot be null" in str(exc_info.value).lower() or any(
        code in str(exc_info.value) for code in ["1048", "1364"]
    )

    pymysql_exception_session.execute("DROP TABLE test_not_null")


def test_check_violation(pymysql_exception_session: PyMysqlDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    pymysql_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id INT AUTO_INCREMENT PRIMARY KEY,
            age INT CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        pymysql_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES (%s)", (15,))

    assert "check" in str(exc_info.value).lower() or "3819" in str(exc_info.value)

    pymysql_exception_session.execute("DROP TABLE test_check_constraint")


def test_sql_parsing_error(pymysql_exception_session: PyMysqlDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        pymysql_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower() or "1064" in str(exc_info.value)
