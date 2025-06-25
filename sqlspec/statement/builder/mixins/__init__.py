"""SQL statement builder mixins."""

from sqlspec.statement.builder.mixins._aggregate_functions import AggregateFunctionsMixin
from sqlspec.statement.builder.mixins._case_builder import CaseBuilderMixin
from sqlspec.statement.builder.mixins._common_table_expr import CommonTableExpressionMixin
from sqlspec.statement.builder.mixins._delete_from import DeleteFromClauseMixin
from sqlspec.statement.builder.mixins._from import FromClauseMixin
from sqlspec.statement.builder.mixins._group_by import GroupByClauseMixin
from sqlspec.statement.builder.mixins._having import HavingClauseMixin
from sqlspec.statement.builder.mixins._insert_from_select import InsertFromSelectMixin
from sqlspec.statement.builder.mixins._insert_into import InsertIntoClauseMixin
from sqlspec.statement.builder.mixins._insert_values import InsertValuesMixin
from sqlspec.statement.builder.mixins._join import JoinClauseMixin
from sqlspec.statement.builder.mixins._limit_offset import LimitOffsetClauseMixin
from sqlspec.statement.builder.mixins._merge_clauses import (
    MergeIntoClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeOnClauseMixin,
    MergeUsingClauseMixin,
)
from sqlspec.statement.builder.mixins._order_by import OrderByClauseMixin
from sqlspec.statement.builder.mixins._pivot import PivotClauseMixin
from sqlspec.statement.builder.mixins._returning import ReturningClauseMixin
from sqlspec.statement.builder.mixins._select_columns import SelectColumnsMixin
from sqlspec.statement.builder.mixins._set_ops import SetOperationMixin
from sqlspec.statement.builder.mixins._unpivot import UnpivotClauseMixin
from sqlspec.statement.builder.mixins._update_from import UpdateFromClauseMixin
from sqlspec.statement.builder.mixins._update_set import UpdateSetClauseMixin
from sqlspec.statement.builder.mixins._update_table import UpdateTableClauseMixin
from sqlspec.statement.builder.mixins._where import WhereClauseMixin
from sqlspec.statement.builder.mixins._window_functions import WindowFunctionsMixin

__all__ = (
    "AggregateFunctionsMixin",
    "CaseBuilderMixin",
    "CommonTableExpressionMixin",
    "DeleteFromClauseMixin",
    "FromClauseMixin",
    "GroupByClauseMixin",
    "HavingClauseMixin",
    "InsertFromSelectMixin",
    "InsertIntoClauseMixin",
    "InsertValuesMixin",
    "JoinClauseMixin",
    "LimitOffsetClauseMixin",
    "MergeIntoClauseMixin",
    "MergeMatchedClauseMixin",
    "MergeNotMatchedBySourceClauseMixin",
    "MergeNotMatchedClauseMixin",
    "MergeOnClauseMixin",
    "MergeUsingClauseMixin",
    "OrderByClauseMixin",
    "PivotClauseMixin",
    "ReturningClauseMixin",
    "SelectColumnsMixin",
    "SetOperationMixin",
    "UnpivotClauseMixin",
    "UpdateFromClauseMixin",
    "UpdateSetClauseMixin",
    "UpdateTableClauseMixin",
    "WhereClauseMixin",
    "WindowFunctionsMixin",
)
