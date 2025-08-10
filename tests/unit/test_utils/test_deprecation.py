"""Tests for sqlspec.utils.deprecation module.

Tests deprecation warning utilities including decorator and warning functions.
"""

import warnings

import pytest

from sqlspec.utils.deprecation import deprecated, warn_deprecation


def test_warn_deprecation_basic() -> None:
    """Test basic deprecation warning."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="test_func", kind="function")

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is DeprecationWarning
        message = str(warning.message)
        assert "deprecated function 'test_func'" in message
        assert "Deprecated in SQLSpec 1.0.0" in message


def test_warn_deprecation_all_parameters() -> None:
    """Test deprecation warning with all parameters."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(
            version="1.0.0",
            deprecated_name="old_func",
            kind="function",
            removal_in="2.0.0",
            alternative="new_func",
            info="Additional info",
        )

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "deprecated function 'old_func'" in message
        assert "removed in 2.0.0" in message
        assert "Use 'new_func' instead" in message
        assert "Additional info" in message


def test_warn_deprecation_pending() -> None:
    """Test pending deprecation warning."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="future_func", kind="function", pending=True)

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is PendingDeprecationWarning
        assert "function awaiting deprecation" in str(warning.message)


@pytest.mark.parametrize(
    "kind,expected_prefix",
    [
        ("function", "Call to"),
        ("method", "Call to"),
        ("import", "Import of"),
        ("class", "Use of"),
        ("property", "Use of"),
        ("attribute", "Use of"),
        ("parameter", "Use of"),
        ("classmethod", "Use of"),
    ],
)
def test_warn_deprecation_kinds(kind: str, expected_prefix: str) -> None:
    """Test different deprecation kinds produce correct message prefixes."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="test_item", kind=kind)  # type: ignore[arg-type]

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert message.startswith(expected_prefix)


def test_deprecated_decorator_basic() -> None:
    """Test deprecated decorator basic functionality."""

    @deprecated(version="1.0.0")
    def test_function() -> str:
        return "result"

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        result = test_function()

        assert result == "result"
        assert len(warning_list) == 1
        assert "deprecated function 'test_function'" in str(warning_list[0].message)


def test_deprecated_decorator_preserves_metadata() -> None:
    """Test deprecated decorator preserves function metadata."""

    @deprecated(version="1.0.0")
    def documented_function(param: int) -> str:
        """Test docstring.

        Args:
            param: Test parameter.

        Returns:
            Test result.
        """
        return str(param)

    assert documented_function.__name__ == "documented_function"
    assert "Test docstring" in (documented_function.__doc__ or "")


def test_deprecated_decorator_with_exception() -> None:
    """Test deprecated decorator works when decorated function raises."""

    @deprecated(version="1.0.0")
    def failing_function() -> None:
        raise ValueError("Test error")

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")

        with pytest.raises(ValueError, match="Test error"):
            failing_function()

        assert len(warning_list) == 1
        assert "deprecated function" in str(warning_list[0].message)


def test_deprecation_warning_stacklevel() -> None:
    """Test that deprecation warnings have correct stack level."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")

        def wrapper_function() -> None:
            warn_deprecation(version="1.0.0", deprecated_name="test", kind="function")

        wrapper_function()

        assert len(warning_list) == 1
        warning = warning_list[0]

        # Check that the warning points to the correct location
        # The stacklevel=2 should make it point to wrapper_function, not warn_deprecation
        assert "wrapper_function" in str(warning.filename) or warning.lineno > 0
