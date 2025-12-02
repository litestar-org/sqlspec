"""Spanner dialect submodule."""

from sqlspec.adapters.spanner.dialect._spangres import Spangres
from sqlspec.adapters.spanner.dialect._spanner import Spanner

__all__ = ("Spangres", "Spanner")
