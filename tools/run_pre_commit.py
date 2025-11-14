#!/usr/bin/env python3
"""Run pre-commit hooks without requiring PTY support."""

import os
from typing import TYPE_CHECKING

from pre_commit import util as pre_commit_util
from pre_commit.main import main as pre_commit_main

if TYPE_CHECKING:
    from types import TracebackType


class _PipePty:
    """Lightweight replacement for pre-commit's PTY helper."""

    __slots__ = ("r", "w")

    def __init__(self) -> None:
        self.r: int | None = None
        self.w: int | None = None

    def __enter__(self) -> "_PipePty":
        self.r, self.w = os.pipe()
        return self

    def close_w(self) -> None:
        if self.w is not None:
            os.close(self.w)
            self.w = None

    def close_r(self) -> None:
        if self.r is not None:
            os.close(self.r)
            self.r = None

    def __exit__(
        self,
        exc_type: "type[BaseException] | None",
        exc_value: "BaseException | None",
        traceback: "TracebackType | None",
    ) -> None:
        self.close_w()
        self.close_r()


pre_commit_util.Pty = _PipePty  # type: ignore[assignment]


def main() -> int:
    """Invoke pre-commit with patched PTY handling."""

    return pre_commit_main()


if __name__ == "__main__":
    raise SystemExit(main())
