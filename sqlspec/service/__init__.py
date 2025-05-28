from typing import (
    Generic,
    TypeVar,
)

from sqlspec.base import DriverT
from sqlspec.service._util import ResultConverter, find_filter
from sqlspec.service.pagination import OffsetPagination
from sqlspec.sql.filters import FilterTypeT, LimitOffset, StatementFilter

__all__ = (
    "FilterTypeT",
    "LimitOffset",
    "OffsetPagination",
    "ResultConverter",
    "SqlspecService",
    "StatementFilter",
    "find_filter",
)


T = TypeVar("T")


class SqlspecService(ResultConverter, Generic[DriverT]):
    """Base Service for a Query repo"""

    def __init__(self, driver: "DriverT") -> None:
        self._driver = driver

    @classmethod
    def new(cls, driver: "DriverT") -> "SqlspecService[DriverT]":
        return cls(driver=driver)

    @property
    def driver(self) -> "DriverT":
        """Get the driver instance."""
        return self._driver
