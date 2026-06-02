"""PyMySQL-specific parameter variants not covered by generic contracts."""

import math
from collections.abc import Generator

import pytest

from sqlspec.adapters.pymysql import PyMysqlDriver
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
def pymysql_parameter_variants(pymysql_clean_driver: PyMysqlDriver) -> Generator[PyMysqlDriver, None, None]:
    """Provide PyMySQL data for native and scalar parameter variants."""
    pymysql_clean_driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_parameter_variants_pymysql (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            description TEXT,
            active TINYINT(1),
            float_value DOUBLE
        )
    """)
    pymysql_clean_driver.execute_script("DELETE FROM test_parameter_variants_pymysql")
    pymysql_clean_driver.execute_many(
        "INSERT INTO test_parameter_variants_pymysql (name, value, description, active, float_value) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("test1", 100, "First test", True, 1.5),
            ("test2", 200, "Second test", False, 2.5),
            ("test3", 300, None, True, math.pi),
        ],
    )
    yield pymysql_clean_driver
    pymysql_clean_driver.execute_script("DROP TABLE IF EXISTS test_parameter_variants_pymysql")


def test_pymysql_native_pyformat_select(pymysql_parameter_variants: PyMysqlDriver) -> None:
    """PyMySQL accepts native positional pyformat parameters."""
    result = pymysql_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_pymysql WHERE name = %s AND value > %s", ("test2", 150)
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


def test_pymysql_named_pyformat_select(pymysql_parameter_variants: PyMysqlDriver) -> None:
    """PyMySQL converts named pyformat parameters."""
    result = pymysql_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_pymysql WHERE name = %(name)s AND value < %(maximum)s",
        {"name": "test3", "maximum": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test3", "value": 300}]


def test_pymysql_float_parameter(pymysql_parameter_variants: PyMysqlDriver) -> None:
    """PyMySQL binds floating-point parameters."""
    result = pymysql_parameter_variants.execute(
        "SELECT name, float_value FROM test_parameter_variants_pymysql WHERE float_value > ? ORDER BY value", (3.0,)
    )

    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test3"
    assert abs(result.get_data()[0]["float_value"] - math.pi) < 0.0001


def test_pymysql_boolean_parameter(pymysql_parameter_variants: PyMysqlDriver) -> None:
    """PyMySQL binds Python bools to TINYINT parameters."""
    result = pymysql_parameter_variants.execute(
        "SELECT name FROM test_parameter_variants_pymysql WHERE active = ? ORDER BY value", (True,)
    )

    assert result.get_data() == [{"name": "test1"}, {"name": "test3"}]


def test_pymysql_empty_string_parameter(pymysql_parameter_variants: PyMysqlDriver) -> None:
    """PyMySQL distinguishes empty strings from NULL parameters."""
    pymysql_parameter_variants.execute(
        "INSERT INTO test_parameter_variants_pymysql (name, value, description) VALUES (?, ?, ?)", ("empty_desc", 0, "")
    )
    result = pymysql_parameter_variants.execute(
        "SELECT name FROM test_parameter_variants_pymysql WHERE description = ?", ("",)
    )

    assert result.get_data() == [{"name": "empty_desc"}]


def test_pymysql_special_character_parameter(pymysql_parameter_variants: PyMysqlDriver) -> None:
    """PyMySQL preserves quoted and markup-like parameter text."""
    special_value = 'O\'Reilly & Sons "Test" <script>'
    pymysql_parameter_variants.execute(
        "INSERT INTO test_parameter_variants_pymysql (name, value, description) VALUES (?, ?, ?)",
        ("special", 999, special_value),
    )
    result = pymysql_parameter_variants.execute(
        "SELECT description FROM test_parameter_variants_pymysql WHERE name = ?", ("special",)
    )

    assert result.get_data() == [{"description": special_value}]
