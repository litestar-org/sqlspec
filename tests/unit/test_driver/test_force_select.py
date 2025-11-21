"""Tests for the _should_force_select safety net."""

from sqlspec.adapters.bigquery import bigquery_statement_config
from sqlspec.core import SQL, ProcessedState
from sqlspec.driver._common import CommonDriverAttributesMixin


class _DummyDriver(CommonDriverAttributesMixin):
    """Minimal driver to expose _should_force_select for testing."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(connection=None, statement_config=bigquery_statement_config)


class _CursorWithStatementType:
    """Cursor exposing a statement_type attribute."""

    def __init__(self, statement_type: str | None) -> None:
        self.statement_type = statement_type
        self.description = None


class _CursorWithDescription:
    """Cursor exposing a description attribute."""

    def __init__(self, has_description: bool) -> None:
        self.description = [("col",)] if has_description else None
        self.statement_type = None


def _make_unknown_statement(sql_text: str = "select 1") -> "SQL":
    stmt = SQL(sql_text)
    stmt._processed_state = ProcessedState(  # pylint: disable=protected-access
        compiled_sql=sql_text, execution_parameters={}, operation_type="UNKNOWN"
    )
    return stmt


def _make_select_statement(sql_text: str = "select 1") -> "SQL":
    stmt = SQL(sql_text)
    stmt._processed_state = ProcessedState(  # pylint: disable=protected-access
        compiled_sql=sql_text, execution_parameters={}, operation_type="SELECT"
    )
    return stmt


def test_force_select_uses_statement_type_select() -> None:
    driver = _DummyDriver()
    stmt = _make_unknown_statement()
    cursor = _CursorWithStatementType("SELECT")

    assert driver._should_force_select(stmt, cursor) is True  # pylint: disable=protected-access


def test_force_select_uses_description_when_unknown() -> None:
    driver = _DummyDriver()
    stmt = _make_unknown_statement()
    cursor = _CursorWithDescription(True)

    assert driver._should_force_select(stmt, cursor) is True  # pylint: disable=protected-access


def test_force_select_false_when_no_metadata() -> None:
    driver = _DummyDriver()
    stmt = _make_unknown_statement()
    cursor = _CursorWithDescription(False)

    assert driver._should_force_select(stmt, cursor) is False  # pylint: disable=protected-access


def test_force_select_ignored_when_operation_known() -> None:
    driver = _DummyDriver()
    stmt = _make_select_statement()
    cursor = _CursorWithDescription(True)

    assert driver._should_force_select(stmt, cursor) is False  # pylint: disable=protected-access
