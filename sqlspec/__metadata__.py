"""Metadata for the Project."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, metadata, version

__all__ = ("__project__", "__version__")

try:
    __version__ = version("sqlspec")
    """Version of the project."""
    __project__ = metadata("sqlspec")["Name"]
    """Name of the project."""
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.1"
    __project__ = "SQLSpec"
finally:
    del version, PackageNotFoundError, metadata
