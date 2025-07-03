"""SQL query builders for safe SQL construction.

This package provides fluent interfaces for building SQL queries with automatic
parameter binding and validation.

# SelectBuilder is now generic and supports as_schema for type-safe schema integration.
"""

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder.base import QueryBuilder, SafeQuery
from sqlspec.statement.builder.column import Column, ColumnExpression, FunctionColumn
from sqlspec.statement.builder.ddl import (
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
from sqlspec.statement.builder.delete import Delete
from sqlspec.statement.builder.insert import Insert
from sqlspec.statement.builder.merge import Merge
from sqlspec.statement.builder.mixins import WhereClauseMixin
from sqlspec.statement.builder.select import Select
from sqlspec.statement.builder.update import Update

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
