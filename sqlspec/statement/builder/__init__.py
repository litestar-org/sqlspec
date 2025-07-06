"""SQL query builders for safe SQL construction.

This package provides fluent interfaces for building SQL queries with automatic
parameter binding and validation.

# SelectBuilder is now generic and supports as_schema for type-safe schema integration.
"""

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._base import QueryBuilder, SafeQuery
from sqlspec.statement.builder._column import Column, ColumnExpression, FunctionColumn
from sqlspec.statement.builder._ddl import (
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
    TruncateTable,
)
from sqlspec.statement.builder._delete import Delete
from sqlspec.statement.builder._insert import Insert
from sqlspec.statement.builder._merge import Merge
from sqlspec.statement.builder._select import Select
from sqlspec.statement.builder._update import Update
from sqlspec.statement.builder.mixins import WhereClauseMixin

__all__ = (
    "AlterTable",
    "Column",
    "ColumnExpression",
    "CommentOn",
    "CreateIndex",
    "CreateMaterializedView",
    "CreateSchema",
    "CreateTable",
    "CreateTableAsSelect",
    "CreateView",
    "DDLBuilder",
    "Delete",
    "DropIndex",
    "DropSchema",
    "DropTable",
    "DropView",
    "FunctionColumn",
    "Insert",
    "Merge",
    "QueryBuilder",
    "RenameTable",
    "SQLBuilderError",
    "SafeQuery",
    "Select",
    "TruncateTable",
    "Update",
    "WhereClauseMixin",
)
