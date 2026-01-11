"""Unit tests for storage utilities."""

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.storage import resolve_storage_path
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.module_loader import ensure_pyarrow


def test_ensure_pyarrow_succeeds_when_installed() -> None:
    """Test ensure_pyarrow succeeds when pyarrow is available."""
    if not PYARROW_INSTALLED:
        pytest.skip("pyarrow not installed")

    ensure_pyarrow()


def test_ensure_pyarrow_raises_when_not_installed() -> None:
    """Test ensure_pyarrow raises error when pyarrow not available."""
    if PYARROW_INSTALLED:
        pytest.skip("pyarrow is installed")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        ensure_pyarrow()


def test_resolve_storage_path_file_protocol_absolute() -> None:
    """Test path resolution for file protocol with absolute path."""
    result = resolve_storage_path("/data/file.txt", base_path="", protocol="file")
    assert result == "data/file.txt"


def test_resolve_storage_path_file_protocol_with_base() -> None:
    """Test path resolution for file protocol with base_path."""
    result = resolve_storage_path("file.txt", base_path="/base", protocol="file")
    assert result == "/base/file.txt"


def test_resolve_storage_path_file_scheme_stripping() -> None:
    """Test file:// scheme stripping."""
    result = resolve_storage_path("file:///data/file.txt", base_path="", protocol="file", strip_file_scheme=True)
    assert result == "data/file.txt"


def test_resolve_storage_path_file_scheme_preserved() -> None:
    """Test file:// scheme preserved when strip_file_scheme=False."""
    result = resolve_storage_path("file:///data/file.txt", base_path="", protocol="file", strip_file_scheme=False)
    assert result == "file:///data/file.txt"


def test_resolve_storage_path_cloud_protocol() -> None:
    """Test path resolution for cloud protocols."""
    result = resolve_storage_path("data/file.txt", base_path="bucket/prefix", protocol="s3")
    assert result == "bucket/prefix/data/file.txt"


def test_resolve_storage_path_cloud_absolute() -> None:
    """Test absolute path handling for cloud protocols."""
    result = resolve_storage_path("/data/file.txt", base_path="", protocol="s3")
    assert result == "/data/file.txt"


def test_resolve_storage_path_no_base_path() -> None:
    """Test path resolution without base_path."""
    result = resolve_storage_path("data/file.txt", base_path="", protocol="s3")
    assert result == "data/file.txt"


def test_resolve_storage_path_pathlib_input() -> None:
    """Test path resolution with pathlib.Path input."""
    from pathlib import Path

    result = resolve_storage_path(Path("data") / "file.txt", base_path="", protocol="file")
    assert result == "data/file.txt"
