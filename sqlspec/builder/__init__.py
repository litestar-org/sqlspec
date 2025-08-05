"""SQL query builders for safe SQL construction.

This package provides fluent interfaces for building SQL queries with automatic
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
from sqlspec.builder._insert import Insert
from sqlspec.builder._merge import Merge
from sqlspec.builder._select import Select
from sqlspec.builder._update import Update
from sqlspec.builder.mixins import WhereClauseMixin
from sqlspec.exceptions import SQLBuilderError

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
    "Truncate",
    "Update",
    "WhereClauseMixin",
)
