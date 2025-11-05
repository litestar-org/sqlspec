"""SQL query builders for safe SQL construction.

Provides fluent interfaces for building SQL queries with
parameter binding and validation.
"""

from sqlspec.builder._base import QueryBuilder, SafeQuery
from sqlspec.builder._column import Column, ColumnExpression, FunctionColumn
from sqlspec.builder._ddl import (
    AlterTable,
    CommentOn,
    CreateIndex,
    CreateMaterializedView,
    CreateSchema,
    CreateTable,
    CreateTableAsSelect,
    CreateView,
    DDLBuilder,
    DropIndex,
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
    UpdateFromClauseMixin,
    UpdateSetClauseMixin,
    UpdateTableClauseMixin,
)
from sqlspec.builder._expression_wrappers import (
    AggregateExpression,
    ConversionExpression,
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
from sqlspec.builder._insert import Insert
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
    ReturningClauseMixin,
    Select,
    SelectClauseMixin,
    SetOperationMixin,
    SubqueryBuilder,
    UnpivotClauseMixin,
    WhereClauseMixin,
    WindowFunctionBuilder,
)
from sqlspec.builder._update import Update
from sqlspec.exceptions import SQLBuilderError

__all__ = (
    "AggregateExpression",
    "AlterTable",
    "Case",
    "CaseBuilder",
    "Column",
    "ColumnExpression",
    "CommentOn",
    "CommonTableExpressionMixin",
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
    "DropSchema",
    "DropTable",
    "DropView",
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
    "SafeQuery",
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
    "WhereClauseMixin",
    "WindowFunctionBuilder",
    "build_copy_from_statement",
    "build_copy_statement",
    "build_copy_to_statement",
    "extract_expression",
    "parse_column_expression",
    "parse_condition_expression",
    "parse_order_expression",
    "parse_table_expression",
    "sql",
    "to_expression",
)
