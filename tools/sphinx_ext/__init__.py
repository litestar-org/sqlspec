"""Sphinx extension package for SQLSpec documentation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.sphinx_ext import missing_references

if TYPE_CHECKING:
    from sphinx.application import Sphinx


def setup(app: Sphinx) -> dict[str, bool]:
    """Initialize active Sphinx extensions for SQLSpec."""
    return missing_references.setup(app)
