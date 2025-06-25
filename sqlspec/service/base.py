from typing import Generic, TypeVar

from sqlspec.config import DriverT

__all__ = ("SqlspecService",)


T = TypeVar("T")


class SqlspecService(Generic[DriverT]):
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
