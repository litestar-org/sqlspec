"""Test parameter conversion and validation for PyMySQL driver.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target MySQL POSITIONAL_PYFORMAT style.

PyMySQL Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: POSITIONAL_PYFORMAT (%s)
- Input: NAMED_COLON (:name) -> Output: POSITIONAL_PYFORMAT (%s)
- Input: NAMED_PYFORMAT (%(name)s) -> Output: POSITIONAL_PYFORMAT (%s)
- Input: POSITIONAL_PYFORMAT (%s) -> Output: POSITIONAL_PYFORMAT (%s) (no conversion)

This implements MySQL's 2-phase parameter processing.
"""

import math
from collections.abc import Generator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver, default_statement_config
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
def pymysql_parameter_session(mysql_service: MySQLService) -> Generator[PyMysqlDriver, None]:
    """Create a pymysql session for parameter conversion testing."""
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        },
        statement_config=default_statement_config,
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_parameter_conversion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                description TEXT
            )
        """)

        session.execute_script("DELETE FROM test_parameter_conversion")

        session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("test1", 100, "First test"),
        )
        session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("test2", 200, "Second test"),
        )
        session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", ("test3", 300, None)
        )

        yield session

        session.execute_script("DROP TABLE IF EXISTS test_parameter_conversion")


class TestQmarkConversion:
    """Test QMARK (?) to POSITIONAL_PYFORMAT (%s) conversion."""

    def test_qmark_single_parameter(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test single ? placeholder gets converted to %s."""
        result = pymysql_parameter_session.execute("SELECT * FROM test_parameter_conversion WHERE name = ?", ("test1",))

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    def test_qmark_multiple_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test multiple ? placeholders get converted to %s."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ? AND value > ?", ("test1", 50)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"
        assert result.data[0]["value"] == 100

    def test_qmark_in_insert(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test ? placeholders in INSERT statements."""
        pymysql_parameter_session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("qmark_insert", 500, "Inserted via QMARK"),
        )

        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", ("qmark_insert",)
        )
        assert len(result.data) == 1
        assert result.data[0]["value"] == 500

    def test_qmark_in_update(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test ? placeholders in UPDATE statements."""
        pymysql_parameter_session.execute(
            "UPDATE test_parameter_conversion SET value = ? WHERE name = ?", (999, "test1")
        )

        result = pymysql_parameter_session.execute("SELECT * FROM test_parameter_conversion WHERE name = ?", ("test1",))
        assert result.data[0]["value"] == 999


class TestNamedColonConversion:
    """Test NAMED_COLON (:name) to POSITIONAL_PYFORMAT (%s) conversion."""

    def test_named_colon_single_parameter(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test single :name placeholder gets converted."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    def test_named_colon_multiple_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test multiple :name placeholders get converted."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name AND value > :min_val",
            {"name": "test2", "min_val": 100},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"

    def test_named_colon_repeated_parameter(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test same :name used multiple times."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :val OR description LIKE :val", {"val": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1

    def test_named_colon_in_insert(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test :name placeholders in INSERT statements."""
        pymysql_parameter_session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (:name, :value, :desc)",
            {"name": "colon_insert", "value": 600, "desc": "Inserted via NAMED_COLON"},
        )

        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = :name", {"name": "colon_insert"}
        )
        assert len(result.data) == 1
        assert result.data[0]["value"] == 600


class TestNamedPyformatConversion:
    """Test NAMED_PYFORMAT (%(name)s) to POSITIONAL_PYFORMAT (%s) conversion."""

    def test_named_pyformat_single_parameter(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test single %(name)s placeholder gets converted."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %(name)s", {"name": "test1"}
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    def test_named_pyformat_multiple_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test multiple %(name)s placeholders get converted."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %(test_name)s AND value < %(max_val)s",
            {"test_name": "test3", "max_val": 350},
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test3"


class TestPositionalPyformatNative:
    """Test POSITIONAL_PYFORMAT (%s) works natively without conversion."""

    def test_pyformat_single_parameter(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test single %s placeholder works directly."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %s", ("test1",)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test1"

    def test_pyformat_multiple_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test multiple %s placeholders work directly."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = %s AND value > %s", ("test2", 150)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"


class TestSQLObjectConversion:
    """Test parameter conversion with SQL objects."""

    def test_sql_object_with_qmark(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test SQL object with ? placeholders."""
        sql_qmark = SQL("SELECT * FROM test_parameter_conversion WHERE name = ? OR name = ?", "test1", "test3")
        result = pymysql_parameter_session.execute(sql_qmark)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2
        names = [row["name"] for row in result.data]
        assert "test1" in names
        assert "test3" in names

    def test_sql_object_with_pyformat(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test SQL object with %s placeholders."""
        sql_pyformat = SQL("SELECT * FROM test_parameter_conversion WHERE value BETWEEN %s AND %s", 150, 250)
        result = pymysql_parameter_session.execute(sql_pyformat)

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test2"


class TestDataTypeConversion:
    """Test parameter conversion with different data types."""

    def test_integer_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test integer parameters are handled correctly."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE value >= ? AND value <= ?", (100, 200)
        )

        assert isinstance(result, SQLResult)
        assert len(result.data) == 2

    def test_float_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test float parameters are handled correctly."""
        pymysql_parameter_session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_floats (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                float_val DOUBLE
            )
        """)

        pymysql_parameter_session.execute("INSERT INTO test_floats (name, float_val) VALUES (?, ?)", ("pi", math.pi))

        result = pymysql_parameter_session.execute("SELECT * FROM test_floats WHERE float_val > ?", (3.0,))

        assert len(result.data) == 1
        assert abs(result.data[0]["float_val"] - math.pi) < 0.0001

        pymysql_parameter_session.execute_script("DROP TABLE IF EXISTS test_floats")

    def test_none_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test None/NULL parameters are handled correctly."""
        result = pymysql_parameter_session.execute("SELECT * FROM test_parameter_conversion WHERE description IS NULL")

        assert isinstance(result, SQLResult)
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test3"

    def test_boolean_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test boolean parameters are converted to integers for MySQL."""
        pymysql_parameter_session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_bools (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                active TINYINT(1)
            )
        """)

        pymysql_parameter_session.execute("INSERT INTO test_bools (name, active) VALUES (?, ?)", ("bool_test", True))

        result = pymysql_parameter_session.execute("SELECT * FROM test_bools WHERE active = ?", (True,))

        assert len(result.data) == 1
        assert result.data[0]["name"] == "bool_test"

        pymysql_parameter_session.execute_script("DROP TABLE IF EXISTS test_bools")


class TestExecuteMany:
    """Test parameter conversion with execute_many."""

    def test_execute_many_with_qmark(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test execute_many with ? placeholders."""
        data = [("batch1", 1001, "Batch 1"), ("batch2", 1002, "Batch 2"), ("batch3", 1003, "Batch 3")]

        result = pymysql_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

        select_result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name LIKE ? ORDER BY name", ("batch%",)
        )
        assert len(select_result.data) == 3

    def test_execute_many_with_pyformat(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test execute_many with %s placeholders."""
        data = [("pyformat1", 2001, "Pyformat 1"), ("pyformat2", 2002, "Pyformat 2")]

        result = pymysql_parameter_session.execute_many(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (%s, %s, %s)", data
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 2


class TestEdgeCases:
    """Test edge cases in parameter conversion."""

    def test_empty_string_parameter(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test empty string parameters."""
        pymysql_parameter_session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)", ("empty_desc", 0, "")
        )

        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE description = ?", ("",)
        )
        assert len(result.data) == 1
        assert result.data[0]["name"] == "empty_desc"

    def test_special_characters_in_parameters(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test special characters in parameter values."""
        special_value = 'O\'Reilly & Sons "Test" <script>'
        pymysql_parameter_session.execute(
            "INSERT INTO test_parameter_conversion (name, value, description) VALUES (?, ?, ?)",
            ("special", 999, special_value),
        )

        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", ("special",)
        )
        assert len(result.data) == 1
        assert result.data[0]["description"] == special_value

    def test_sql_injection_prevention(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test that parameter escaping prevents SQL injection."""
        malicious_input = "'; DROP TABLE test_parameter_conversion; --"

        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name = ?", (malicious_input,)
        )

        assert len(result.data) == 0

        # Verify table still exists
        count_result = pymysql_parameter_session.execute("SELECT COUNT(*) as count FROM test_parameter_conversion")
        assert count_result.data[0]["count"] >= 3

    def test_like_with_wildcards(self, pymysql_parameter_session: PyMysqlDriver) -> None:
        """Test LIKE queries with wildcard parameters."""
        result = pymysql_parameter_session.execute(
            "SELECT * FROM test_parameter_conversion WHERE name LIKE ?", ("test%",)
        )

        assert len(result.data) >= 3
        for row in result.data:
            assert row["name"].startswith("test")
