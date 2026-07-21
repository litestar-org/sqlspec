"""Assertion helpers for shared adapter contract tests."""

from sqlspec import SQLResult

ExpectedRows = tuple[dict[str, object], ...]


def assert_sql_result(result: object, *, rows_affected: int | None = None) -> SQLResult:
    """Assert that a driver call returned a SQLResult."""
    assert isinstance(result, SQLResult)
    if rows_affected is not None:
        assert result.rows_affected == rows_affected
    return result


def assert_result_data(result: object, expected_data: ExpectedRows) -> None:
    """Assert SQLResult data using plain dict rows."""
    sql_result = assert_sql_result(result)
    assert sql_result.get_data() == list(expected_data)
