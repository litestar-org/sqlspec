"""SQL query builders for safe SQL construction.

This package provides fluent interfaces for building SQL queries with automatic
parameter binding and validation.

# SelectBuilder is now generic and supports as_schema for type-safe schema integration.
"""

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder.base import QueryBuilder, SafeQuery
from sqlspec.statement.builder.ddl import (
    AlterTableBuilder,
    CreateIndexBuilder,
    CreateMaterializedViewBuilder,
    CreateSchemaBuilder,
    CreateTableAsSelectBuilder,
    CreateViewBuilder,
    DDLBuilder,
    DropIndexBuilder,
    DropSchemaBuilder,
    DropTableBuilder,
    DropViewBuilder,
    TruncateTableBuilder,
)
from sqlspec.statement.builder.delete import DeleteBuilder
from sqlspec.statement.builder.insert import InsertBuilder
from sqlspec.statement.builder.merge import MergeBuilder
from sqlspec.statement.builder.mixins import WhereClauseMixin
from sqlspec.statement.builder.select import SelectBuilder
from sqlspec.statement.builder.update import UpdateBuilder

__all__ = (
    "AlterTableBuilder",
    "CreateIndexBuilder",
    "CreateMaterializedViewBuilder",
    "CreateSchemaBuilder",
    "CreateTableAsSelectBuilder",
    "CreateViewBuilder",
    "DDLBuilder",
    "DeleteBuilder",
    "DropIndexBuilder",
    "DropSchemaBuilder",
    "DropTableBuilder",
    "DropViewBuilder",
    "InsertBuilder",
    "MergeBuilder",
    "QueryBuilder",
    "SQLBuilderError",
    "SafeQuery",
    "SelectBuilder",
    "TruncateTableBuilder",
    "UpdateBuilder",
    "WhereClauseMixin",
)
