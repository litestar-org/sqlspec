"""Collection filter datastructures."""

from __future__ import annotations

from abc import ABC
from collections import abc  # noqa: TC003
from dataclasses import dataclass
from datetime import datetime  # noqa: TC003
from typing import Generic, Literal, Protocol

from typing_extensions import TypeVar

__all__ = (
    "BeforeAfter",
    "CollectionFilter",
    "InAnyFilter",
    "LimitOffset",
    "NotInCollectionFilter",
    "NotInSearchFilter",
    "OnBeforeAfter",
    "OrderBy",
    "PaginationFilter",
    "SearchFilter",
    "StatementFilter",
)

T = TypeVar("T")
StatementT = TypeVar("StatementT", bound="str")


class StatementFilter(Protocol):
    """Protocol for filters that can be appended to a statement."""

    def append_to_statement(self, statement: StatementT) -> StatementT:
        """Append the filter to the statement."""
        return statement


@dataclass
class BeforeAfter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    before: datetime | None
    """Filter results where field earlier than this."""
    after: datetime | None
    """Filter results where field later than this."""


@dataclass
class OnBeforeAfter(StatementFilter):
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    on_or_before: datetime | None
    """Filter results where field is on or earlier than this."""
    on_or_after: datetime | None
    """Filter results where field on or later than this."""


class InAnyFilter(StatementFilter, ABC):
    """Subclass for methods that have a `prefer_any` attribute."""


@dataclass
class CollectionFilter(InAnyFilter, Generic[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: abc.Collection[T] | None
    """Values for ``IN`` clause.

    An empty list will return an empty result set, however, if ``None``, the filter is not applied to the query, and all rows are returned. """


@dataclass
class NotInCollectionFilter(InAnyFilter, Generic[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: abc.Collection[T] | None
    """Values for ``NOT IN`` clause.

    An empty list or ``None`` will return all rows."""


class PaginationFilter(StatementFilter, ABC):
    """Subclass for methods that function as a pagination type."""


@dataclass
class LimitOffset(PaginationFilter):
    """Data required to add limit/offset filtering to a query."""

    limit: int
    """Value for ``LIMIT`` clause of query."""
    offset: int
    """Value for ``OFFSET`` clause of query."""


@dataclass
class OrderBy(StatementFilter):
    """Data required to construct a ``ORDER BY ...`` clause."""

    field_name: str
    """Name of the model attribute to sort on."""
    sort_order: Literal["asc", "desc"] = "asc"
    """Sort ascending or descending"""


@dataclass
class SearchFilter(StatementFilter):
    """Data required to construct a ``WHERE field_name LIKE '%' || :value || '%'`` clause."""

    field_name: str | set[str]
    """Name of the model attribute to search on."""
    value: str
    """Search value."""
    ignore_case: bool | None = False
    """Should the search be case insensitive."""


@dataclass
class NotInSearchFilter(SearchFilter):
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""
