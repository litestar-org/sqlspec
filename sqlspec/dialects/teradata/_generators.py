"""Isolated SQLGlot generation helpers for Teradata Vantage."""

from collections.abc import Callable
from typing import Any, Final, cast

from sqlglot import exp, generator
from sqlglot.generators.teradata import TeradataGenerator
from sqlglot.optimizer.simplify import simplify

__all__ = ("teradata_datatype_sql", "teradata_dispatch", "teradata_select_sql", "teradata_set_operation_sql")


_BASE_SELECT_TRANSFORM: Final[Callable[[Any, Any], str]] = TeradataGenerator.TRANSFORMS[exp.Select]

TERADATA_TYPE_MAPPING: Final[dict[exp.DType, str]] = {
    **TeradataGenerator.TYPE_MAPPING,
    exp.DType.BOOLEAN: "BYTEINT",
    exp.DType.TINYINT: "BYTEINT",
    exp.DType.TEXT: "CLOB",
    exp.DType.VARBINARY: "VARBYTE",
    exp.DType.BINARY: "BYTE",
    exp.DType.BLOB: "BLOB",
}
TERADATA_DEFAULT_TYPE_LENGTHS: Final[dict[exp.DType, int]] = {exp.DType.VARCHAR: 32000, exp.DType.VARBINARY: 32000}


def teradata_datatype_sql(gen: generator.Generator, expression: exp.DataType) -> str:
    """Render Vantage scalar types, retaining explicit sizes and time zones."""
    kind = expression.this
    if kind not in {
        exp.DType.BOOLEAN,
        exp.DType.TINYINT,
        exp.DType.TEXT,
        exp.DType.VARBINARY,
        exp.DType.BINARY,
        exp.DType.BLOB,
        exp.DType.VARCHAR,
    }:
        return gen.datatype_sql(expression)
    name = TERADATA_TYPE_MAPPING.get(kind, "VARCHAR")
    parameters = ", ".join(gen.sql(item) for item in expression.expressions)
    if not parameters and kind in TERADATA_DEFAULT_TYPE_LENGTHS:
        parameters = str(TERADATA_DEFAULT_TYPE_LENGTHS[kind])
    rendered = f"{name}({parameters})" if parameters else name
    collate = gen.sql(expression, "collate")
    return f"{rendered} COLLATE {collate}" if collate else rendered


def teradata_select_sql(gen: generator.Generator, expression: exp.Select) -> str:
    """Render TOP limits or QUALIFY offset pagination without mutating input."""
    working = expression.copy()
    limit = working.args.get("limit")
    if isinstance(limit, exp.Fetch):
        working.set("limit", _fetch_to_limit(limit))
    if working.args.get("offset") is not None:
        return _BASE_SELECT_TRANSFORM(gen, _page_query(working, copy=False))
    return _BASE_SELECT_TRANSFORM(gen, working)


def teradata_set_operation_sql(gen: generator.Generator, expression: exp.SetOperation) -> str:
    """Page a completed set operation using an outer QUALIFY query."""
    if expression.args.get("offset") is not None:
        return _BASE_SELECT_TRANSFORM(gen, _page_query(expression))
    if expression.args.get("limit") is not None:
        query = expression.copy()
        limit = query.args.pop("limit")
        order = query.args.pop("order", None)
        cte = query.args.pop("with_", None)
        _strip_order_table_qualifiers(query, order)
        result = exp.select("*", copy=False).from_(
            exp.Subquery(this=query, alias=exp.TableAlias(this=exp.to_identifier("_sqlspec_paged"))), copy=False
        )
        result.set("limit", limit)
        result.set("order", order)
        result.set("with_", cte)
        return teradata_select_sql(gen, result)
    return gen.set_operations(expression.copy())


TERADATA_TRANSFORMS: Final[dict[type[exp.Expr], Callable[[Any, Any], str]]] = {
    exp.DataType: teradata_datatype_sql,
    exp.Select: teradata_select_sql,
    exp.Union: teradata_set_operation_sql,
    exp.Intersect: teradata_set_operation_sql,
    exp.Except: teradata_set_operation_sql,
}


def teradata_dispatch(
    base_dispatch: dict[type[exp.Expr], Callable[..., str]],
) -> dict[type[exp.Expr], Callable[..., str]]:
    """Overlay Vantage handlers without changing shared SQLGlot tables."""
    return {**base_dispatch, **TERADATA_TRANSFORMS}


def _strip_order_table_qualifiers(query: exp.Query, order: exp.Expr | None) -> None:
    """Strip inner table qualifiers from outer ORDER BY columns on wrapped queries."""
    if order is None:
        return
    projected = {item.alias_or_name for item in query.selects}
    inner_tables = (
        {table.alias_or_name for table in query.find_all(exp.Table)}
        if any(item.is_star for item in query.selects)
        else set()
    )
    for column in order.find_all(exp.Column):
        if column.name in projected or (column.table and column.table in inner_tables):
            column.set("table", None)
            column.set("db", None)
            column.set("catalog", None)


def _page_query(expression: exp.Query, *, copy: bool = True) -> exp.Select:
    query = expression.copy() if copy else expression
    offset = query.args.pop("offset")
    limit = query.args.pop("limit", None)
    order = query.args.pop("order", None)
    start = simplify(exp.Add(this=offset.expression.copy(), expression=exp.Literal.number(1)))
    count: exp.Expr | None
    if isinstance(limit, exp.Limit):
        count = limit.expression
    elif isinstance(limit, exp.Fetch):
        fetch_count = cast("exp.Expr | None", limit.args.get("count"))
        count = fetch_count if fetch_count is not None else exp.Literal.number(1)
    else:
        count = None
    wrap = isinstance(query, exp.SetOperation) or any(
        query.args.get(key) for key in ("distinct", "group", "having", "qualify")
    )
    cte = query.args.pop("with_", None) if wrap else None
    if wrap:
        _strip_order_table_qualifiers(query, order)
        result = exp.select("*", copy=False).from_(
            exp.Subquery(this=query, alias=exp.TableAlias(this=exp.to_identifier("_sqlspec_paged"))), copy=False
        )
        result.set("with_", cte)
    else:
        result = cast("exp.Select", query)
    window = exp.Window(
        this=exp.RowNumber(),
        order=order or exp.Order(expressions=[exp.Ordered(this=exp.Literal.number(1), nulls_first=True)]),
    )
    condition = (
        exp.Between(
            this=window, low=start, high=simplify(exp.Add(this=offset.expression.copy(), expression=count.copy()))
        )
        if count is not None
        else exp.GTE(this=window, expression=start)
    )
    result.set("qualify", exp.Qualify(this=condition))
    return result


def _fetch_to_limit(limit: exp.Fetch) -> exp.Limit:
    opts = limit.args.get("limit_options")
    limit_options: exp.LimitOptions | None = None
    if isinstance(opts, exp.LimitOptions) and (opts.args.get("percent") or opts.args.get("with_ties")):
        limit_options = exp.LimitOptions(
            percent=bool(opts.args.get("percent")), with_ties=bool(opts.args.get("with_ties"))
        )
    return exp.Limit(expression=limit.args.get("count") or exp.Literal.number(1), limit_options=limit_options)
