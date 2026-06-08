"""Execute-time validation of declared parameters (Ch5, sqlspec-smgc.5).

The single shared hook in ``prepare_statement`` validates declared params on the
original user params before style conversion, for every adapter and every
execution method. Declared => validated (present + typed); undeclared => untouched.
"""

from typing import Any

import pytest

from sqlspec.core import ParameterDeclaration, StatementConfig
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.core.statement import SQL
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from tests.conftest import requires_interpreted

# pyright: reportPrivateUsage=false

pytestmark = requires_interpreted


class _MockDriver(SyncDriverAdapterBase):
    def __init__(self) -> None:
        self.statement_config = StatementConfig()

    @property
    def connection(self) -> "Any":
        return None

    def dispatch_execute(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError

    def dispatch_execute_many(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError

    def with_cursor(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError

    def handle_database_exceptions(self, *args: "Any", **kwargs: "Any") -> "Any":
        raise NotImplementedError

    def begin(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError


@pytest.fixture
def driver() -> _MockDriver:
    return _MockDriver()


def _declared(*decls: ParameterDeclaration) -> "tuple[ParameterDeclaration, ...]":
    return decls


def test_required_missing_raises(driver: _MockDriver) -> None:
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "int")))
    with pytest.raises(SQLSpecError, match="a"):
        driver.prepare_statement(sql, ())


def test_required_present_passes(driver: _MockDriver) -> None:
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "int")))
    prepared = driver.prepare_statement(sql, ({"a": 1},))
    assert prepared.named_parameters == {"a": 1}


def test_type_mismatch_raises(driver: _MockDriver) -> None:
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "int")))
    with pytest.raises(SQLSpecError, match="a"):
        driver.prepare_statement(sql, ({"a": "not-an-int"},))


def test_type_match_passes(driver: _MockDriver) -> None:
    sql = SQL("select :a, :b", declared_parameters=_declared(ParameterDeclaration("a", "int"), ParameterDeclaration("b", "str")))
    prepared = driver.prepare_statement(sql, ({"a": 1, "b": "x"},))
    assert prepared.named_parameters == {"a": 1, "b": "x"}


def test_none_value_allowed_when_present(driver: _MockDriver) -> None:
    """None means SQL NULL; the key is present so the param is supplied; type check skipped."""
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "int")))
    prepared = driver.prepare_statement(sql, ({"a": None},))
    assert prepared.named_parameters == {"a": None}


def test_unresolved_type_is_skipped(driver: _MockDriver) -> None:
    """A type string not in the registry is documentation-only; no isinstance check."""
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "Money")))
    prepared = driver.prepare_statement(sql, ({"a": object()},))
    assert "a" in prepared.named_parameters


def test_extra_params_tolerated(driver: _MockDriver) -> None:
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "int")))
    prepared = driver.prepare_statement(sql, ({"a": 1, "unexpected": 99},))
    assert prepared.named_parameters["a"] == 1


def test_filter_injected_params_do_not_trip_validation(driver: _MockDriver) -> None:
    """LimitOffsetFilter adds limit/offset after validation; they are never declared."""
    sql = SQL("select :a", declared_parameters=_declared(ParameterDeclaration("a", "int")))
    prepared = driver.prepare_statement(sql, ({"a": 1}, LimitOffsetFilter(limit=10, offset=0)))
    assert prepared.named_parameters["a"] == 1


def test_undeclared_query_is_untouched(driver: _MockDriver) -> None:
    """No declarations => no validation, even with empty params."""
    sql = SQL("select :a")
    prepared = driver.prepare_statement(sql, ())
    assert prepared.declared_parameters == ()


def test_positional_binding_skips_name_checks(driver: _MockDriver) -> None:
    """Positional binding can't be name-matched; arity was checked at load (Ch4)."""
    sql = SQL("select ?", 1, declared_parameters=_declared(ParameterDeclaration("a", "int")), statement_config=StatementConfig())
    prepared = driver.prepare_statement(sql, ())
    assert prepared.positional_parameters == [1]


def test_execute_many_validates_first_row(driver: _MockDriver) -> None:
    bad = SQL("select :a", [{"b": 2}, {"a": 1}], is_many=True, declared_parameters=_declared(ParameterDeclaration("a", "int")), statement_config=StatementConfig())
    with pytest.raises(SQLSpecError, match="a"):
        driver.prepare_statement(bad, ())


def test_execute_many_first_row_valid_passes(driver: _MockDriver) -> None:
    good = SQL("select :a", [{"a": 1}, {"a": 2}], is_many=True, declared_parameters=_declared(ParameterDeclaration("a", "int")), statement_config=StatementConfig())
    prepared = driver.prepare_statement(good, ())
    assert prepared.is_many
