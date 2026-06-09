"""declared_parameters slot carriage + pool-leak safety (Ch3, sqlspec-smgc.3).

Validates all seven propagation/reset sites for the SQL._declared_parameters slot,
plus loader population and driver-derivation preservation.
"""

from sqlspec.core import ParameterDeclaration
from sqlspec.core._pool import get_sql_pool
from sqlspec.core.statement import SQL

_SENTINEL = (ParameterDeclaration("a", "int", required=False),)


def test_default_declared_parameters_is_empty_tuple() -> None:
    sql = SQL("select 1")
    assert sql.declared_parameters == ()


def test_copy_full_path_preserves_declared_parameters() -> None:
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    new = sql.copy(statement="select :b")
    assert new.declared_parameters == _SENTINEL


def test_copy_fast_path_preserves_declared_parameters() -> None:
    # parameters-only fast path -> _create_empty_copy
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    new = sql.copy(parameters={"a": 1})
    assert new.declared_parameters == _SENTINEL


def test_init_from_sql_object_preserves_declared_parameters() -> None:
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    new = SQL(sql)
    assert new.declared_parameters == _SENTINEL


def test_reset_clears_declared_parameters() -> None:
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    sql.reset()
    assert sql.declared_parameters == ()


def test_pool_recycle_does_not_leak_declared_parameters() -> None:
    """PRIMARY leak vector: a recycled SQL must NOT inherit a prior query's declarations."""
    pool = get_sql_pool()
    leaky = SQL("select :a")
    leaky._declared_parameters = _SENTINEL
    pool.release(leaky)  # resetter is SQL.reset -> must clear the slot

    recycled = pool.acquire()
    try:
        assert recycled._declared_parameters == ()
    finally:
        pool.release(recycled)


def test_get_sql_populates_declared_parameters() -> None:
    from sqlspec.loader import SQLFileLoader

    loader = SQLFileLoader()
    loader.add_named_sql("q", "select :a", parameters=[ParameterDeclaration("a", "int", description="id")])
    sql = loader.get_sql("q")
    assert sql.declared_parameters == (ParameterDeclaration("a", "int", description="id"),)


def test_undeclared_get_sql_has_empty_declarations() -> None:
    from sqlspec.loader import SQLFileLoader

    loader = SQLFileLoader()
    loader.add_named_sql("plain", "select 1")
    assert loader.get_sql("plain").declared_parameters == ()


def test_constructor_kwarg_sets_declarations() -> None:
    sql = SQL("select :a", {"a": 1}, declared_parameters=_SENTINEL)
    assert sql.declared_parameters == _SENTINEL


def test_declarations_survive_deepcopy() -> None:
    import copy

    sql = SQL("select :a", {"a": 1}, declared_parameters=_SENTINEL)
    new = copy.deepcopy(sql)
    assert new.declared_parameters == _SENTINEL


def test_declarations_survive_pickle_roundtrip() -> None:
    import pickle

    sql = SQL("select :a", {"a": 1}, declared_parameters=_SENTINEL)
    new = pickle.loads(pickle.dumps(sql))
    assert new.declared_parameters == _SENTINEL


def test_declarations_survive_driver_prepare_with_filter() -> None:
    """Declarations must survive prepare_statement rebuild + filter application."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core.filters import LimitOffsetFilter

    config = SqliteConfig(pool_config={"database": ":memory:"})
    with config.provide_session() as session:
        base = SQL("select :a")
        base._declared_parameters = _SENTINEL
        prepared = session.prepare_statement(base, ({"a": 1}, LimitOffsetFilter(limit=5, offset=0)))
        assert prepared.declared_parameters == _SENTINEL
