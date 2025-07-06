"""SQL result representation."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Optional, TypeVar

RowT = TypeVar("RowT")


@dataclass
class SQLResult(Generic[RowT]):
    """Container for SQL execution results."""

    rows: Sequence[RowT]
    rowcount: int
    lastrowid: Optional[Any] = None

    def __iter__(self) -> Iterator[RowT]:
        """Iterate over rows."""
        return iter(self.rows)

    def __len__(self) -> int:
        """Number of rows."""
        return len(self.rows)

    def __getitem__(self, index: int) -> RowT:
        """Get row by index."""
        return self.rows[index]

    def first(self) -> Optional[RowT]:
        """Get first row or None."""
        return self.rows[0] if self.rows else None

    def one(self) -> RowT:
        """Get exactly one row, raise if not exactly one."""
        if len(self.rows) != 1:
            msg = f"Expected exactly one row, got {len(self.rows)}"
            raise ValueError(msg)
        return self.rows[0]

    def one_or_none(self) -> Optional[RowT]:
        """Get one row or None, raise if more than one."""
        if len(self.rows) > 1:
            msg = f"Expected at most one row, got {len(self.rows)}"
            raise ValueError(msg)
        return self.rows[0] if self.rows else None

    def all(self) -> Sequence[RowT]:
        """Get all rows."""
        return self.rows


__all__ = ("SQLResult",)
