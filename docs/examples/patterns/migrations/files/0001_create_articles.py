# ruff: noqa: N999
"""Initial migration used by docs examples."""

from typing import Any

__all__ = ("down", "up")


def up(*_args: Any) -> "list[str]":
    """Create the articles table."""
    return [
        """
        CREATE TABLE articles (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT NOT NULL
        )
        """
    ]


def down(*_args: Any) -> "list[str]":
    """Drop the articles table."""
    return ["DROP TABLE IF EXISTS articles"]
