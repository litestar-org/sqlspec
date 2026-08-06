"""Shared MySQL-family insert-statement contracts."""

from collections.abc import Callable

import pytest

from sqlspec.adapters.aiomysql.core import build_insert_statement as build_aiomysql_insert
from sqlspec.adapters.asyncmy.core import build_insert_statement as build_asyncmy_insert
from sqlspec.adapters.mysqlconnector.core import build_insert_statement as build_mysqlconnector_insert
from sqlspec.adapters.pymysql.core import build_insert_statement as build_pymysql_insert


@pytest.mark.parametrize(
    "build_insert_statement",
    (build_aiomysql_insert, build_asyncmy_insert, build_mysqlconnector_insert, build_pymysql_insert),
)
def test_build_insert_statement_preserves_backtick_quoted_dots(
    build_insert_statement: Callable[[str, list[str]], str],
) -> None:
    statement = build_insert_statement("`analytics.db`.`orders.table`", ["id"])

    assert statement == "INSERT INTO `analytics.db`.`orders.table` (`id`) VALUES (%s)"
