from typing import Optional

import pytest

from sqlspec._sql import SQLFactory
from sqlspec.exceptions import SQLBuilderError

sql = SQLFactory()


@pytest.mark.parametrize(
    ("method", "sql_string", "should_raise", "expected_message"),
    [
        ("select", "SELECT * FROM users", False, None),
        ("select", "WITH cte AS (SELECT 1) SELECT * FROM cte", False, None),
        ("select", "INSERT INTO users VALUES (1)", True, "expects a SELECT or WITH statement, got INSERT"),
        ("select", "UPDATE users SET name = 'x'", True, "expects a SELECT or WITH statement, got UPDATE"),
        ("insert", "INSERT INTO users VALUES (1)", False, None),
        ("insert", "SELECT * FROM users", False, None),  # insert-from-select allowed
        ("insert", "UPDATE users SET name = 'x'", True, "expects INSERT or SELECT"),
        ("update", "UPDATE users SET name = 'x'", False, None),
        ("update", "SELECT * FROM users", True, "expects UPDATE statement, got SELECT"),
        ("update", "INSERT INTO users VALUES (1)", True, "expects UPDATE statement, got INSERT"),
        ("delete", "DELETE FROM users WHERE id = 1", False, None),
        ("delete", "SELECT * FROM users", True, "expects DELETE statement, got SELECT"),
        (
            "merge",
            "MERGE INTO users USING src ON users.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name",
            False,
            None,
        ),
        ("merge", "SELECT * FROM users", True, "expects MERGE statement, got SELECT"),
    ],
)
def test_sqlfactory_builder_validation(
    method: str, sql_string: str, should_raise: bool, expected_message: Optional[str]
) -> None:
    """Test SQLFactory builder methods for correct SQL type validation.

    Args:
        method: The builder method name (e.g., 'select', 'insert').
        sql_string: The SQL string to validate.
        should_raise: Whether an error is expected.
        expected_message: The expected error message substring, if any.

    Asserts:
        - That the correct error is raised for invalid SQL types.
        - That valid SQL returns a builder of the correct type.
    """
    factory_method = getattr(sql, method)
    if should_raise:
        with pytest.raises(SQLBuilderError) as exc_info:
            factory_method(sql_string)
        if expected_message is not None:
            assert expected_message in str(exc_info.value)
    else:
        builder = factory_method(sql_string)
        assert builder is not None


@pytest.mark.parametrize(
    ("sql_string", "should_raise", "expected_message"),
    [
        ("SELECT * FROM users", False, None),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", False, None),
        ("INSERT INTO users VALUES (1)", True, "only supports SELECT statements. Detected type: INSERT"),
        ("UPDATE users SET name = 'x'", True, "only supports SELECT statements. Detected type: UPDATE"),
        ("DELETE FROM users WHERE id = 1", True, "only supports SELECT statements. Detected type: DELETE"),
        (
            "MERGE INTO users USING src ON users.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name",
            True,
            "only supports SELECT statements. Detected type: MERGE",
        ),
    ],
)
def test_sqlfactory_call_validation(sql_string: str, should_raise: bool, expected_message: Optional[str]) -> None:
    """Test SQLFactory callable interface for SELECT/CTE validation.

    Args:
        sql_string: The SQL string to validate.
        should_raise: Whether an error is expected.
        expected_message: The expected error message substring, if any.

    Asserts:
        - That only SELECT/CTE statements are accepted by the callable interface.
        - That the correct error is raised for other statement types.
    """
    if should_raise:
        with pytest.raises(SQLBuilderError) as exc_info:
            sql(sql_string)
        if expected_message is not None:
            assert expected_message in str(exc_info.value)
    else:
        builder = sql(sql_string)
        assert builder is not None
