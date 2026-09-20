"""SQL query builders for safe SQL construction.

Provides fluent interfaces for building SQL queries with
parameter binding and validation.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.builder._base import BuiltQuery, ExpressionBuilder, QueryBuilder
    from sqlspec.builder._column import Column, ColumnExpression, FunctionColumn
    from sqlspec.builder._ddl import (
        AlterOperation,
        AlterTable,
        ColumnDefinition,
        CommentOn,
        ConstraintDefinition,
        CreateIndex,
        CreateMaterializedView,
        CreateSchema,
        CreateTable,
        CreateTableAsSelect,
        CreateView,
        DDLBuilder,
        DropIndex,
        DropMaterializedView,
        DropSchema,
        DropTable,
        DropView,
        RenameTable,
        Truncate,
    )
    from sqlspec.builder._delete import Delete
    from sqlspec.builder._dml import (
        DeleteFromClauseMixin,
        InsertFromSelectMixin,
        InsertIntoClauseMixin,
        InsertValuesMixin,
        ReturningClauseMixin,
        UpdateFromClauseMixin,
        UpdateSetClauseMixin,
        UpdateTableClauseMixin,
    )
    from sqlspec.builder._explain import (
        Explain,
        ExplainMixin,
        build_bigquery_explain,
        build_duckdb_explain,
        build_explain_sql,
        build_generic_explain,
        build_mysql_explain,
        build_oracle_explain,
        build_postgres_explain,
        build_sqlite_explain,
        normalize_dialect_name,
    )
    from sqlspec.builder._expression_wrappers import (
        AggregateExpression,
        ConversionExpression,
        ExpressionWrapper,
        FunctionExpression,
        MathExpression,
        StringExpression,
    )
    from sqlspec.builder._factory import (
        SQLFactory,
        build_copy_from_statement,
        build_copy_statement,
        build_copy_to_statement,
        sql,
    )
    from sqlspec.builder._insert import ConflictBuilder, Insert
    from sqlspec.builder._join import JoinBuilder
    from sqlspec.builder._merge import Merge
    from sqlspec.builder._parsing_utils import (
        extract_expression,
        parse_column_expression,
        parse_condition_expression,
        parse_order_expression,
        parse_table_expression,
        to_expression,
    )
    from sqlspec.builder._select import (
        Case,
        CaseBuilder,
        CommonTableExpressionMixin,
        HavingClauseMixin,
        LimitOffsetClauseMixin,
        OrderByClauseMixin,
        PivotClauseMixin,
        Select,
        SelectClauseMixin,
        SetOperationMixin,
        SubqueryBuilder,
        UnpivotClauseMixin,
        WhereClauseMixin,
        WindowFunctionBuilder,
    )
    from sqlspec.builder._temporal import create_temporal_table, register_version_generators
    from sqlspec.builder._update import Update
    from sqlspec.builder._values import Values
    from sqlspec.builder._vector_distance import VectorDistance
    from sqlspec.exceptions import SQLBuilderError

__all__ = (
    "AggregateExpression",
    "AlterOperation",
    "AlterTable",
    "BuiltQuery",
    "Case",
    "CaseBuilder",
    "Column",
    "ColumnDefinition",
    "ColumnExpression",
    "CommentOn",
    "CommonTableExpressionMixin",
    "ConflictBuilder",
    "ConstraintDefinition",
    "ConversionExpression",
    "CreateIndex",
    "CreateMaterializedView",
    "CreateSchema",
    "CreateTable",
    "CreateTableAsSelect",
    "CreateView",
    "DDLBuilder",
    "Delete",
    "DeleteFromClauseMixin",
    "DropIndex",
    "DropMaterializedView",
    "DropSchema",
    "DropTable",
    "DropView",
    "Explain",
    "ExplainMixin",
    "ExpressionBuilder",
    "ExpressionWrapper",
    "FunctionColumn",
    "FunctionExpression",
    "HavingClauseMixin",
    "Insert",
    "InsertFromSelectMixin",
    "InsertIntoClauseMixin",
    "InsertValuesMixin",
    "JoinBuilder",
    "LimitOffsetClauseMixin",
    "MathExpression",
    "Merge",
    "OrderByClauseMixin",
    "PivotClauseMixin",
    "QueryBuilder",
    "RenameTable",
    "ReturningClauseMixin",
    "SQLBuilderError",
    "SQLFactory",
    "Select",
    "SelectClauseMixin",
    "SetOperationMixin",
    "StringExpression",
    "SubqueryBuilder",
    "Truncate",
    "UnpivotClauseMixin",
    "Update",
    "UpdateFromClauseMixin",
    "UpdateSetClauseMixin",
    "UpdateTableClauseMixin",
    "Values",
    "VectorDistance",
    "WhereClauseMixin",
    "WindowFunctionBuilder",
    "build_bigquery_explain",
    "build_copy_from_statement",
    "build_copy_statement",
    "build_copy_to_statement",
    "build_duckdb_explain",
    "build_explain_sql",
    "build_generic_explain",
    "build_mysql_explain",
    "build_oracle_explain",
    "build_postgres_explain",
    "build_sqlite_explain",
    "create_temporal_table",
    "extract_expression",
    "normalize_dialect_name",
    "parse_column_expression",
    "parse_condition_expression",
    "parse_order_expression",
    "parse_table_expression",
    "register_version_generators",
    "sql",
    "to_expression",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "AggregateExpression": ("sqlspec.builder._expression_wrappers", "AggregateExpression"),
    "AlterOperation": ("sqlspec.builder._ddl", "AlterOperation"),
    "AlterTable": ("sqlspec.builder._ddl", "AlterTable"),
    "BuiltQuery": ("sqlspec.builder._base", "BuiltQuery"),
    "Case": ("sqlspec.builder._select", "Case"),
    "CaseBuilder": ("sqlspec.builder._select", "CaseBuilder"),
    "Column": ("sqlspec.builder._column", "Column"),
    "ColumnDefinition": ("sqlspec.builder._ddl", "ColumnDefinition"),
    "ColumnExpression": ("sqlspec.builder._column", "ColumnExpression"),
    "CommentOn": ("sqlspec.builder._ddl", "CommentOn"),
    "CommonTableExpressionMixin": ("sqlspec.builder._select", "CommonTableExpressionMixin"),
    "ConflictBuilder": ("sqlspec.builder._insert", "ConflictBuilder"),
    "ConstraintDefinition": ("sqlspec.builder._ddl", "ConstraintDefinition"),
    "ConversionExpression": ("sqlspec.builder._expression_wrappers", "ConversionExpression"),
    "CreateIndex": ("sqlspec.builder._ddl", "CreateIndex"),
    "CreateMaterializedView": ("sqlspec.builder._ddl", "CreateMaterializedView"),
    "CreateSchema": ("sqlspec.builder._ddl", "CreateSchema"),
    "CreateTable": ("sqlspec.builder._ddl", "CreateTable"),
    "CreateTableAsSelect": ("sqlspec.builder._ddl", "CreateTableAsSelect"),
    "CreateView": ("sqlspec.builder._ddl", "CreateView"),
    "DDLBuilder": ("sqlspec.builder._ddl", "DDLBuilder"),
    "Delete": ("sqlspec.builder._delete", "Delete"),
    "DeleteFromClauseMixin": ("sqlspec.builder._dml", "DeleteFromClauseMixin"),
    "DropIndex": ("sqlspec.builder._ddl", "DropIndex"),
    "DropMaterializedView": ("sqlspec.builder._ddl", "DropMaterializedView"),
    "DropSchema": ("sqlspec.builder._ddl", "DropSchema"),
    "DropTable": ("sqlspec.builder._ddl", "DropTable"),
    "DropView": ("sqlspec.builder._ddl", "DropView"),
    "Explain": ("sqlspec.builder._explain", "Explain"),
    "ExplainMixin": ("sqlspec.builder._explain", "ExplainMixin"),
    "ExpressionBuilder": ("sqlspec.builder._base", "ExpressionBuilder"),
    "ExpressionWrapper": ("sqlspec.builder._expression_wrappers", "ExpressionWrapper"),
    "FunctionColumn": ("sqlspec.builder._column", "FunctionColumn"),
    "FunctionExpression": ("sqlspec.builder._expression_wrappers", "FunctionExpression"),
    "HavingClauseMixin": ("sqlspec.builder._select", "HavingClauseMixin"),
    "Insert": ("sqlspec.builder._insert", "Insert"),
    "InsertFromSelectMixin": ("sqlspec.builder._dml", "InsertFromSelectMixin"),
    "InsertIntoClauseMixin": ("sqlspec.builder._dml", "InsertIntoClauseMixin"),
    "InsertValuesMixin": ("sqlspec.builder._dml", "InsertValuesMixin"),
    "JoinBuilder": ("sqlspec.builder._join", "JoinBuilder"),
    "LimitOffsetClauseMixin": ("sqlspec.builder._select", "LimitOffsetClauseMixin"),
    "MathExpression": ("sqlspec.builder._expression_wrappers", "MathExpression"),
    "Merge": ("sqlspec.builder._merge", "Merge"),
    "OrderByClauseMixin": ("sqlspec.builder._select", "OrderByClauseMixin"),
    "PivotClauseMixin": ("sqlspec.builder._select", "PivotClauseMixin"),
    "QueryBuilder": ("sqlspec.builder._base", "QueryBuilder"),
    "RenameTable": ("sqlspec.builder._ddl", "RenameTable"),
    "ReturningClauseMixin": ("sqlspec.builder._dml", "ReturningClauseMixin"),
    "SQLBuilderError": ("sqlspec.exceptions", "SQLBuilderError"),
    "SQLFactory": ("sqlspec.builder._factory", "SQLFactory"),
    "Select": ("sqlspec.builder._select", "Select"),
    "SelectClauseMixin": ("sqlspec.builder._select", "SelectClauseMixin"),
    "SetOperationMixin": ("sqlspec.builder._select", "SetOperationMixin"),
    "StringExpression": ("sqlspec.builder._expression_wrappers", "StringExpression"),
    "SubqueryBuilder": ("sqlspec.builder._select", "SubqueryBuilder"),
    "Truncate": ("sqlspec.builder._ddl", "Truncate"),
    "UnpivotClauseMixin": ("sqlspec.builder._select", "UnpivotClauseMixin"),
    "Update": ("sqlspec.builder._update", "Update"),
    "UpdateFromClauseMixin": ("sqlspec.builder._dml", "UpdateFromClauseMixin"),
    "UpdateSetClauseMixin": ("sqlspec.builder._dml", "UpdateSetClauseMixin"),
    "UpdateTableClauseMixin": ("sqlspec.builder._dml", "UpdateTableClauseMixin"),
    "Values": ("sqlspec.builder._values", "Values"),
    "VectorDistance": ("sqlspec.builder._vector_distance", "VectorDistance"),
    "WhereClauseMixin": ("sqlspec.builder._select", "WhereClauseMixin"),
    "WindowFunctionBuilder": ("sqlspec.builder._select", "WindowFunctionBuilder"),
    "build_bigquery_explain": ("sqlspec.builder._explain", "build_bigquery_explain"),
    "build_copy_from_statement": ("sqlspec.builder._factory", "build_copy_from_statement"),
    "build_copy_statement": ("sqlspec.builder._factory", "build_copy_statement"),
    "build_copy_to_statement": ("sqlspec.builder._factory", "build_copy_to_statement"),
    "build_duckdb_explain": ("sqlspec.builder._explain", "build_duckdb_explain"),
    "build_explain_sql": ("sqlspec.builder._explain", "build_explain_sql"),
    "build_generic_explain": ("sqlspec.builder._explain", "build_generic_explain"),
    "build_mysql_explain": ("sqlspec.builder._explain", "build_mysql_explain"),
    "build_oracle_explain": ("sqlspec.builder._explain", "build_oracle_explain"),
    "build_postgres_explain": ("sqlspec.builder._explain", "build_postgres_explain"),
    "build_sqlite_explain": ("sqlspec.builder._explain", "build_sqlite_explain"),
    "create_temporal_table": ("sqlspec.builder._temporal", "create_temporal_table"),
    "extract_expression": ("sqlspec.builder._parsing_utils", "extract_expression"),
    "normalize_dialect_name": ("sqlspec.builder._explain", "normalize_dialect_name"),
    "parse_column_expression": ("sqlspec.builder._parsing_utils", "parse_column_expression"),
    "parse_condition_expression": ("sqlspec.builder._parsing_utils", "parse_condition_expression"),
    "parse_order_expression": ("sqlspec.builder._parsing_utils", "parse_order_expression"),
    "parse_table_expression": ("sqlspec.builder._parsing_utils", "parse_table_expression"),
    "register_version_generators": ("sqlspec.builder._temporal", "register_version_generators"),
    "sql": ("sqlspec.builder._factory", "sql"),
    "to_expression": ("sqlspec.builder._parsing_utils", "to_expression"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
