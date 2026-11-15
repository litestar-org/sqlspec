"""Shared optional-dependency utilities for storage backends."""

import logging
from typing import Any
from urllib.parse import urlparse, urlunparse

from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_fsspec, ensure_obstore, ensure_pyarrow

__all__ = (
    "_log_storage_event",
    "import_fsspec",
    "import_obstore",
    "import_obstore_store",
    "import_pyarrow",
    "import_pyarrow_csv",
    "import_pyarrow_json",
    "import_pyarrow_parquet",
    "log_storage_event",
    "sanitize_uri_for_log",
)

logger = get_logger(__name__)


def import_fsspec() -> "Any":
    """Import fsspec with optional dependency guard.

    Returns:
        fsspec module.
    """
    ensure_fsspec()
    import fsspec

    return fsspec


def import_obstore() -> "Any":
    """Import obstore with optional dependency guard.

    Returns:
        obstore module.
    """
    ensure_obstore()
    import obstore

    return obstore


def import_obstore_store() -> "Any":
    """Import obstore.store with optional dependency guard.

    Returns:
        obstore.store module.
    """
    ensure_obstore()
    import obstore.store as obstore_store

    return obstore_store


def import_pyarrow() -> "Any":
    """Import PyArrow with optional dependency guard.

    Returns:
        PyArrow module.
    """

    ensure_pyarrow()
    import pyarrow as pa

    return pa


def import_pyarrow_parquet() -> "Any":
    """Import PyArrow parquet module with optional dependency guard.

    Returns:
        PyArrow parquet module.
    """

    ensure_pyarrow()
    import pyarrow.parquet as pq

    return pq


def import_pyarrow_csv() -> "Any":
    """Import PyArrow CSV module with optional dependency guard.

    Returns:
        PyArrow CSV module.
    """

    ensure_pyarrow()
    import pyarrow.csv as pa_csv

    return pa_csv


def import_pyarrow_json() -> "Any":
    """Import PyArrow JSON module with optional dependency guard.

    Returns:
        PyArrow JSON module.
    """

    ensure_pyarrow()
    import pyarrow.json as pa_json

    return pa_json


def sanitize_uri_for_log(uri: str) -> str:
    """Strip userinfo, query parameters, and fragments from storage URIs for log output.

    Args:
        uri: Storage URI or local path.

    Returns:
        The value without credentials, query string, or fragment.
    """
    if "://" not in uri and "?" not in uri:
        return uri
    try:
        parsed = urlparse(uri)
    except ValueError:
        base, _, _ = uri.partition("?")
        scheme_part, sep, rest = base.partition("://")
        if not sep:
            return base
        authority, slash, tail = rest.partition("/")
        sanitized_authority = authority.rsplit("@", 1)[-1] if "@" in authority else authority
        return f"{scheme_part}://{sanitized_authority}{slash}{tail}"
    if not parsed.scheme:
        base, _, _ = uri.partition("?")
        return base
    netloc = parsed.netloc.rsplit("@", 1)[-1] if "@" in parsed.netloc else parsed.netloc
    return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, "", ""))


def log_storage_event(
    event: str,
    *,
    backend_type: str,
    protocol: str,
    operation: str | None = None,
    mode: str | None = None,
    path: str | None = None,
    source_path: str | None = None,
    destination_path: str | None = None,
    count: int | None = None,
    exists: bool | None = None,
) -> None:
    """Emit a structured debug log event for a storage backend operation."""
    fields: dict[str, Any] = {"backend_type": backend_type, "protocol": protocol}
    if operation is not None:
        fields["operation"] = operation
    if mode is not None:
        fields["mode"] = mode
    if path is not None:
        fields["path"] = sanitize_uri_for_log(path)
    if source_path is not None:
        fields["source_path"] = sanitize_uri_for_log(source_path)
    if destination_path is not None:
        fields["destination_path"] = sanitize_uri_for_log(destination_path)
    if count is not None:
        fields["count"] = count
    if exists is not None:
        fields["exists"] = exists
    log_with_context(logger, logging.DEBUG, event, **fields)


_log_storage_event = log_storage_event
