"""Db2 dialect generator."""

from sqlglot import exp, generator, transforms

from sqlspec.builder._generation import invalidate_generator_dispatch
from sqlspec.dialects.db2._transforms import (
    _add_sysibm_dual,
    _transform_anonymous,
    _transform_date_add,
    _transform_posstr,
    _transform_varchar_format,
)

__all__ = ("DB2Generator",)


_DB2_TYPE_MAPPING: dict[exp.DType, str] = {
    **generator.Generator.TYPE_MAPPING,
    exp.DType.BOOLEAN: "BOOLEAN",
    exp.DType.BLOB: "BLOB",
    exp.DType.TEXT: "CLOB",
    exp.DType.VARCHAR: "VARCHAR",
    exp.DType.NVARCHAR: "VARGRAPHIC",
    exp.DType.NCHAR: "GRAPHIC",
    exp.DType.TIMESTAMP: "TIMESTAMP",
    exp.DType.TIMESTAMPTZ: "TIMESTAMP",
    exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
    exp.DType.DATE: "DATE",
    exp.DType.TIME: "TIME",
    exp.DType.SMALLINT: "SMALLINT",
    exp.DType.INT: "INTEGER",
    exp.DType.BIGINT: "BIGINT",
    exp.DType.FLOAT: "DOUBLE",
    exp.DType.DOUBLE: "DOUBLE",
    exp.DType.DECIMAL: "DECIMAL",
    exp.DType.BINARY: "BLOB",
    exp.DType.VARBINARY: "BLOB",
}

_DECFLOAT = getattr(exp.DType, "DECFLOAT", None)
if _DECFLOAT is not None:
    _DB2_TYPE_MAPPING[_DECFLOAT] = "DECFLOAT"

_DBCLOB = getattr(exp.DType, "DBCLOB", None)
if _DBCLOB is not None:
    _DB2_TYPE_MAPPING[_DBCLOB] = "DBCLOB"


class DB2Generator(generator.Generator):
    """Generator for IBM Db2 SQL syntax."""

    LIMIT_FETCH = "FETCH"

    TYPE_MAPPING = _DB2_TYPE_MAPPING

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.Select: transforms.preprocess([
            transforms.eliminate_distinct_on,
            transforms.eliminate_qualify,
            _add_sysibm_dual,
        ]),
        exp.Anonymous: _transform_anonymous,
        exp.DateAdd: _transform_date_add,
        exp.DateSub: _transform_date_add,
        exp.DatetimeAdd: _transform_date_add,
        exp.DatetimeSub: _transform_date_add,
        exp.StrPosition: _transform_posstr,
        exp.TimeToStr: _transform_varchar_format,
        exp.ToChar: _transform_varchar_format,
    }

    def query_modifiers(self, expression: exp.Expr, *sqls: str) -> str:
        """Render query modifiers with Db2 pagination support (FETCH FIRST / NEXT)."""
        limit = expression.args.get("limit")
        if self.LIMIT_FETCH == "FETCH" and isinstance(limit, exp.Limit):
            direction = "NEXT" if expression.args.get("offset") else "FIRST"
            limit = exp.Fetch(direction=direction, count=exp.maybe_copy(limit.expression))
            expression = expression.copy()
            expression.set("limit", limit)
        return super().query_modifiers(expression, *sqls)

    def offset_sql(self, expression: exp.Offset) -> str:
        """Render OFFSET with ROWS suffix required by Db2."""
        return f"{super().offset_sql(expression)} ROWS"

    def fetch_sql(self, expression: exp.Fetch) -> str:
        """Render FETCH FIRST or NEXT rows for Db2 pagination."""
        direction = expression.args.get("direction")
        direction = f" {direction}" if direction else ""
        count = self.sql(expression, "count")
        count = f" {count}" if count else ""
        limit_options = self.sql(expression, "limit_options")
        limit_options = f"{limit_options}" if limit_options else " ROWS ONLY"
        return f"{self.seg('FETCH')}{direction}{count}{limit_options}"

    def interval_sql(self, expression: exp.Interval) -> str:
        """Render INTERVAL in Db2 labeled duration syntax, e.g. 1 DAY."""
        unit = self.sql(expression, "unit")
        this = self.sql(expression, "this")
        if isinstance(expression.this, exp.Literal) and expression.this.is_string:
            this = expression.this.name
        unit_str = f" {unit.upper()}" if unit else ""
        return f"{this}{unit_str}"

    def parameter_sql(self, expression: exp.Parameter) -> str:
        """Render positional parameter token ?."""
        return "?"


invalidate_generator_dispatch(DB2Generator)
