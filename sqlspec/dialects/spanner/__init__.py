"""Google Cloud Spanner sqlglot dialects."""

from sqlspec.dialects.spanner._spangres import Spangres
from sqlspec.dialects.spanner._spanner import Spanner

__all__ = ("Spangres", "Spanner")
