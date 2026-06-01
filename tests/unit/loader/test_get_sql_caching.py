"""Regression tests for SQLFileLoader.get_sql compiled statement caching."""

from collections.abc import Callable

from sqlspec.core import SQL
from sqlspec.loader import SQLFileLoader


def test_get_sql_compiles_once_per_statement(monkeypatch) -> None:
    """Repeated get_sql calls should reuse the compiled SQL object."""
    compile_calls = 0
    original_compile: Callable[[SQL], tuple[str, object]] = SQL.compile

    def counting_compile(sql: SQL) -> tuple[str, object]:
        nonlocal compile_calls
        compile_calls += 1
        return original_compile(sql)

    monkeypatch.setattr(SQL, "compile", counting_compile)

    loader = SQLFileLoader()
    loader.add_named_sql("find-user", "SELECT * FROM users WHERE id = :id")

    first = loader.get_sql("find-user")
    second = loader.get_sql("find_user")

    assert first is second
    assert compile_calls == 1


def test_get_sql_compiles_each_unique_statement_once(monkeypatch) -> None:
    """The compiled SQL cache is keyed by normalized statement name."""
    compile_calls = 0
    original_compile: Callable[[SQL], tuple[str, object]] = SQL.compile

    def counting_compile(sql: SQL) -> tuple[str, object]:
        nonlocal compile_calls
        compile_calls += 1
        return original_compile(sql)

    monkeypatch.setattr(SQL, "compile", counting_compile)

    loader = SQLFileLoader()
    loader.add_named_sql("find-user", "SELECT * FROM users WHERE id = :id")
    loader.add_named_sql("list-users", "SELECT * FROM users")

    loader.get_sql("find-user")
    loader.get_sql("find-user")
    loader.get_sql("list-users")
    loader.get_sql("list-users")

    assert compile_calls == 2


def test_clear_cache_clears_compiled_statements() -> None:
    """clear_cache clears the compiled SQL object cache."""
    loader = SQLFileLoader()
    loader.add_named_sql("find-user", "SELECT * FROM users WHERE id = :id")

    loader.get_sql("find-user")
    assert loader._compiled_statements

    loader.clear_cache()

    assert loader._compiled_statements == {}
