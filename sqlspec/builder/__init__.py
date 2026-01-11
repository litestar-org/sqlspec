"""SQL query builders for safe SQL construction.

Provides fluent interfaces for building SQL queries with
parameter binding and validation.
"""

from sqlspec.builder._base import BuiltQuery, ExpressionBuilder, QueryBuilder
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
from sqlspec.builder._temporal import create_temporal_table, register_version_generators
from sqlspec.builder._update import Update
from sqlspec.builder._vector_expressions import VectorDistance
from sqlspec.exceptions import SQLBuilderError

# Register temporal query SQL generators on module import
register_version_generators()

__all__ = (
    "AggregateExpression",
    "AlterTable",
    "BuiltQuery",
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
    "Explain",
    "ExplainMixin",
    "ExpressionBuilder",
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
