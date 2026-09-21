"""Keyset traversal, SQL generation, and cursor validation contracts."""

import copy
import pickle
import random
import sqlite3
from typing import Any

import pytest
from sqlglot import exp

from sqlspec.core import SQL, CursorFilter, CursorKey, StatementConfig
from sqlspec.core._cursor import encode_cursor
from sqlspec.exceptions import ImproperConfigurationError, InvalidCursorError


def _statement(text: str = "SELECT id, v, n FROM t", dialect: str = "sqlite") -> SQL:
    return SQL(text, statement_config=StatementConfig(dialect=dialect))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"field_name": ""},
        {"field_name": "id", "sort_order": "bad"},
        {"field_name": "id", "nulls": "bad"},
        {"field_name": "id", "result_name": "bad name"},
    ],
)
def test_key_validation(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        CursorKey(**kwargs)


def test_key_identity() -> None:
    key = CursorKey("u.id")
    assert key.result_name == "id"
    assert repr(key) == "CursorKey('u.id', 'asc', nulls=None, result_name='id')"
    assert copy.deepcopy(key) == key
    assert pickle.loads(pickle.dumps(key)) == key
    assert len({key, CursorKey("u.id"), CursorKey("id", "desc")}) == 2
    assert key != "id"


@pytest.mark.parametrize(
    "keys,limit,secret",
    [
        ([], 10, None),
        ([CursorKey("id"), CursorKey("id")], 10, None),
        ([CursorKey("a.id"), CursorKey("b.id")], 10, None),
        ([CursorKey("id")], 0, None),
        ([CursorKey("id")], 10, b""),
    ],
)
def test_filter_validation(keys: list[CursorKey], limit: int, secret: bytes | None) -> None:
    with pytest.raises(ValueError):
        CursorFilter(keys, limit, secret=secret)


@pytest.mark.parametrize("keys", ["id", ["id"], [("id", "asc")], [CursorKey("id")]])
def test_cursor_key_shorthand_continuation(keys: Any) -> None:
    first = CursorFilter([CursorKey("id")], 2, secret="secret")
    token = first.encode({"id": 3}, backward=False)
    shorthand = CursorFilter(keys, 2, token, secret="secret")
    explicit = CursorFilter(first.keys, 2, token, secret="secret")
    assert shorthand.keys == first.keys
    assert shorthand.fingerprint == first.fingerprint
    actual = shorthand.append_to_statement(_statement())
    expected = explicit.append_to_statement(_statement())
    assert actual.sql == expected.sql
    assert actual.named_parameters == expected.named_parameters
    assert (
        shorthand.build_page([{"id": 4}, {"id": 5}, {"id": 6}]).next_cursor
        == explicit.build_page([{"id": 4}, {"id": 5}, {"id": 6}]).next_cursor
    )


@pytest.mark.parametrize(
    "keys,expected",
    [
        (["v", "id"], [CursorKey("v"), CursorKey("id")]),
        ([("v", "desc"), ("id", "asc")], [CursorKey("v", "desc"), CursorKey("id")]),
        ([CursorKey("v", "desc", nulls="last"), "id"], [CursorKey("v", "desc", nulls="last"), CursorKey("id")]),
    ],
)
def test_composite_cursor_key_shorthand(keys: Any, expected: list[CursorKey]) -> None:
    shorthand = CursorFilter(keys, 2)
    explicit = CursorFilter(expected, 2)
    assert shorthand.keys == explicit.keys
    assert shorthand.encode({"v": 10, "id": 3}, backward=True) == explicit.encode({"v": 10, "id": 3}, backward=True)


@pytest.mark.parametrize(
    "keys", ["", [""], [("id", "bad")], [("id",)], [("id", "asc", "last")], [3], ["id", CursorKey("id")]]
)
def test_invalid_cursor_key_shorthand(keys: Any) -> None:
    with pytest.raises(ValueError):
        CursorFilter(keys, 2)


def test_eager_cursor_validation() -> None:
    with pytest.raises(InvalidCursorError):
        CursorFilter([CursorKey("id")], 10, "invalid")
    first = CursorFilter([CursorKey("id")], 10)
    token = first.encode({"id": 1}, backward=False)
    with pytest.raises(InvalidCursorError, match="ordering mismatch"):
        CursorFilter([CursorKey("other")], 10, token)
    token = encode_cursor([None], first.fingerprint)
    with pytest.raises(InvalidCursorError, match="ordering mismatch"):
        CursorFilter(first.keys, 10, token)


def test_first_page_and_replaced_modifiers() -> None:
    flt = CursorFilter([CursorKey("id")], 10)
    stmt = flt.append_to_statement(_statement("SELECT id FROM t ORDER BY v LIMIT 3 OFFSET 9"))
    expression = stmt._filter_expression()
    assert expression.args.get("where") is None
    assert expression.args.get("offset") is None
    assert expression.args["order"].sql() == "ORDER BY id ASC"
    assert stmt.named_parameters == {"cursor_limit": 11}
    assert flt.extract_parameters() == ([], {"cursor_limit": 11})


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "oracle", "sqlite", "tsql", "bigquery"])
def test_bound_predicate_and_default_order(dialect: str) -> None:
    first = CursorFilter([CursorKey("id")], 10)
    hostile = "'; DROP TABLE t; --"
    flt = CursorFilter(first.keys, 10, first.encode({"id": hostile}, backward=False))
    stmt = flt.append_to_statement(_statement(dialect=dialect))
    expression = stmt._filter_expression()
    assert expression.args["order"].sql(dialect=dialect) == "ORDER BY id ASC"
    assert hostile not in expression.sql(dialect=dialect)
    assert stmt.named_parameters == {"cursor_limit": 11, "cursor_k0": hostile}
    assert flt.extract_parameters() == ([], {"cursor_limit": 11, "cursor_k0": hostile})


@pytest.mark.parametrize(
    "dialect,fragment",
    [
        ("sqlite", "NULLS LAST"),
        ("bigquery", "NULLS LAST"),
        ("mysql", "CASE WHEN"),
        ("tsql", "CASE WHEN"),
        ("postgres", "ORDER BY v ASC"),
        ("oracle", "ORDER BY v ASC"),
    ],
)
def test_declared_null_order(dialect: str, fragment: str) -> None:
    flt = CursorFilter([CursorKey("v", nulls="last")], 10)
    assert fragment in flt.append_to_statement(_statement(dialect=dialect))._filter_expression().args["order"].sql(
        dialect=dialect
    )


@pytest.mark.parametrize(
    "base",
    [
        "SELECT v, COUNT(*) AS c FROM t GROUP BY v ORDER BY c",
        "SELECT DISTINCT v FROM t ORDER BY v",
        "SELECT v FROM t UNION SELECT v FROM t",
    ],
)
def test_wrapping(base: str) -> None:
    stmt = CursorFilter([CursorKey("v", nulls="last")], 10).append_to_statement(_statement(base))
    expression = stmt._filter_expression()
    subquery = expression.find(exp.Subquery)
    assert subquery is not None
    assert subquery.alias == "cursor_page"
    assert subquery.this.args.get("order") is None


def test_parameter_conflict() -> None:
    first = CursorFilter([CursorKey("id")], 10)
    flt = CursorFilter(first.keys, 10, first.encode({"id": 3}, backward=False))
    stmt = flt.append_to_statement(
        SQL("SELECT id FROM t WHERE id > :cursor_k0 AND id < :cursor_limit", {"cursor_k0": 1, "cursor_limit": 100})
    )
    assert sorted(stmt.named_parameters.values()) == [1, 3, 11, 100]
    assert len(stmt.named_parameters) == 4


@pytest.mark.parametrize(
    "keys",
    [
        [CursorKey("v", "desc", "last"), CursorKey("id")],
        [CursorKey("n", "asc", "first"), CursorKey("v", "desc", "first"), CursorKey("id", "desc")],
        [CursorKey("id")],
    ],
)
def test_sqlite_walk_both_directions(keys: list[CursorKey]) -> None:
    rng = random.Random(3)
    with sqlite3.connect(":memory:") as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, n INTEGER)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(i, rng.choice([None, 1, 2, 3]), rng.choice([None, 4, 5])) for i in range(40)],
        )

        def fetch(flt: CursorFilter) -> Any:
            sql, parameters = flt.append_to_statement(_statement()).compile()
            return flt.build_page([dict(row) for row in conn.execute(sql, parameters)])

        expected = fetch(CursorFilter(keys, 100)).items
        page = fetch(CursorFilter(keys, 7))
        forward = list(page.items)
        while page.next_cursor:
            page = fetch(CursorFilter(keys, 7, page.next_cursor))
            forward.extend(page.items)
        assert forward == expected
        backward = list(page.items)
        while page.previous_cursor:
            page = fetch(CursorFilter(keys, 7, page.previous_cursor))
            backward = list(page.items) + backward
        assert backward == expected


def test_page_flags_and_signing() -> None:
    first = CursorFilter([CursorKey("id")], 2, secret="key")
    page = first.build_page([{"id": 1}, {"id": 2}, {"id": 3}])
    assert page.has_next and not page.has_previous
    assert page.items == [{"id": 1}, {"id": 2}]
    second = CursorFilter(first.keys, 2, page.next_cursor, "key")
    assert second.values == (2,)
    last = second.build_page([{"id": 3}])
    assert last.has_previous and not last.has_next
    assert last.next_cursor is None
    previous = CursorFilter(first.keys, 2, last.previous_cursor, "key")
    assert previous.backward
    assert previous.build_page([{"id": 2}, {"id": 1}]).items == page.items
    for flt in [first, previous]:
        empty = flt.build_page([])
        assert (empty.has_next, empty.has_previous, empty.next_cursor, empty.previous_cursor) == (
            False,
            False,
            None,
            None,
        )
    for secret in [None, "wrong"]:
        with pytest.raises(InvalidCursorError):
            CursorFilter(first.keys, 2, page.next_cursor, secret)
    assert first.get_cache_key() == CursorFilter(first.keys, 2, secret="other").get_cache_key()
    assert first.get_cache_key() != second.get_cache_key()
    assert copy.deepcopy(second).get_cache_key() == second.get_cache_key()
    assert pickle.loads(pickle.dumps(second)).values == (2,)


@pytest.mark.parametrize("row", [{"other": 1}, {"id": None}])
def test_missing_or_null_key(row: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        CursorFilter([CursorKey("id")], 1).build_page([row, row])


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (
            "postgres",
            "SELECT id, v FROM t WHERE (v <= %(cursor_k0)s OR v IS NULL) AND ((v < %(cursor_k0)s OR v IS NULL) OR id > %(cursor_k1)s) ORDER BY v DESC NULLS LAST, id ASC LIMIT %(cursor_limit)s",
        ),
        (
            "mysql",
            "SELECT id, v FROM t WHERE (v <= :cursor_k0 OR v IS NULL) AND ((v < :cursor_k0 OR v IS NULL) OR id > :cursor_k1) ORDER BY v DESC, id ASC LIMIT :cursor_limit",
        ),
        (
            "oracle",
            "SELECT id, v FROM t WHERE (v <= :cursor_k0 OR v IS NULL) AND ((v < :cursor_k0 OR v IS NULL) OR id > :cursor_k1) ORDER BY v DESC NULLS LAST, id ASC FETCH FIRST :cursor_limit ROWS ONLY",
        ),
    ],
)
def test_expanded_predicate_exact(dialect: str, expected: str) -> None:
    first = CursorFilter([CursorKey("v", "desc", "last"), CursorKey("id")], 10)
    flt = CursorFilter(first.keys, 10, first.encode({"v": 20, "id": 3}, backward=False))
    assert (
        flt.append_to_statement(_statement("SELECT id, v FROM t", dialect))._filter_expression().sql(dialect=dialect)
        == expected
    )


@pytest.mark.parametrize("nulls,expected", [("first", "WHERE NOT v IS NULL"), ("last", "WHERE 1 = 0")])
def test_null_boundary_single_key(nulls: Any, expected: str) -> None:
    first = CursorFilter([CursorKey("v", nulls=nulls)], 10)
    flt = CursorFilter(first.keys, 10, first.encode({"v": None}, backward=False))
    assert expected in flt.append_to_statement(_statement())._filter_expression().sql()


def test_grouped_query_traversal() -> None:
    with sqlite3.connect(":memory:") as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE t(v INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (1,), (2,), (3,)])
        first = CursorFilter([CursorKey("v")], 1)
        seen: list[dict[str, Any]] = []
        flt = first
        while True:
            text, parameters = flt.append_to_statement(
                _statement("SELECT v, COUNT(*) AS c FROM t GROUP BY v ORDER BY c")
            ).compile()
            page = flt.build_page([dict(row) for row in conn.execute(text, parameters)])
            seen.extend(page.items)
            if page.next_cursor is None:
                break
            flt = CursorFilter(first.keys, 1, page.next_cursor)
        assert seen == [{"v": 1, "c": 2}, {"v": 2, "c": 1}, {"v": 3, "c": 1}]


@pytest.mark.parametrize("rows", [[{"other": 1}], [{"id": None}], [{"id": None}, {"id": 2}, {"id": 3}]])
def test_every_page_row_has_valid_cursor_keys(rows: list[dict[str, Any]]) -> None:
    with pytest.raises(ImproperConfigurationError):
        CursorFilter([CursorKey("id")], 2).build_page(rows)
