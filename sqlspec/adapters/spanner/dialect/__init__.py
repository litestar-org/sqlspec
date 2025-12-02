"""Spanner dialect submodule."""

from sqlspec.adapters.spanner.dialect._spanner import Spanner
from sqlspec.adapters.spanner.dialect._spangres import Spangres

__all__ = ("Spanner", "Spangres")
