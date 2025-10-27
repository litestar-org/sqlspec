"""Unit tests for dependency checking utilities."""

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PANDAS_INSTALLED, POLARS_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.module_loader import ensure_pandas, ensure_polars, ensure_pyarrow


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


def test_ensure_pandas_succeeds_when_installed() -> None:
    """Test ensure_pandas succeeds when pandas is available."""
    if not PANDAS_INSTALLED:
        pytest.skip("pandas not installed")

    ensure_pandas()


def test_ensure_pandas_raises_when_not_installed() -> None:
    """Test ensure_pandas raises error when pandas not available."""
    if PANDAS_INSTALLED:
        pytest.skip("pandas is installed")

    with pytest.raises(MissingDependencyError, match="pandas"):
        ensure_pandas()


def test_ensure_polars_succeeds_when_installed() -> None:
    """Test ensure_polars succeeds when polars is available."""
    if not POLARS_INSTALLED:
        pytest.skip("polars not installed")

    ensure_polars()


def test_ensure_polars_raises_when_not_installed() -> None:
    """Test ensure_polars raises error when polars not available."""
    if POLARS_INSTALLED:
        pytest.skip("polars is installed")

    with pytest.raises(MissingDependencyError, match="polars"):
        ensure_polars()
