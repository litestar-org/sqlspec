"""Tests for sqlspec.utils.deprecation module."""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from sqlspec.utils.deprecation import deprecated, warn_deprecation


@pytest.mark.parametrize(
    ("kind", "expected_access_type"),
    [
        ("import", "Import of"),
        ("function", "Call to"),
        ("method", "Call to"),
        ("class", "Use of"),
        ("property", "Use of"),
        ("attribute", "Use of"),
        ("parameter", "Use of"),
        ("classmethod", "Use of"),
    ],
    ids=[
        "import_kind",
        "function_kind",
        "method_kind",
        "class_kind",
        "property_kind",
        "attribute_kind",
        "parameter_kind",
        "classmethod_kind",
    ],
)
def test_warn_deprecation_access_type_formatting(kind: str, expected_access_type: str) -> None:
    """Test that different kinds produce correct access type text."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(
            version="1.0.0",
            deprecated_name="test_item",
            kind=kind,  # type: ignore[arg-type]
        )

        assert len(warning_list) == 1
        warning_msg = str(warning_list[0].message)
        assert warning_msg.startswith(f"{expected_access_type} deprecated {kind}")


def test_warn_deprecation_basic_message() -> None:
    """Test basic deprecation warning message structure."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="test_function", kind="function")

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is DeprecationWarning
        message = str(warning.message)

        assert "Call to deprecated function 'test_function'" in message
        assert "Deprecated in SQLSpec 1.0.0" in message
        assert "This function will be removed in the next major version" in message


def test_warn_deprecation_with_removal_version() -> None:
    """Test deprecation warning with specific removal version."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="test_method", kind="method", removal_in="2.0.0")

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "This method will be removed in 2.0.0" in message


def test_warn_deprecation_with_alternative() -> None:
    """Test deprecation warning with alternative suggestion."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="old_function", kind="function", alternative="new_function")

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "Use 'new_function' instead" in message


def test_warn_deprecation_with_info() -> None:
    """Test deprecation warning with additional info."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(
            version="1.0.0", deprecated_name="test_class", kind="class", info="Additional context about the deprecation"
        )

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "Additional context about the deprecation" in message


def test_warn_deprecation_pending_warning() -> None:
    """Test pending deprecation warning instead of regular deprecation."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(version="1.0.0", deprecated_name="future_deprecated", kind="function", pending=True)

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is PendingDeprecationWarning
        message = str(warning.message)
        assert "Call to function awaiting deprecation 'future_deprecated'" in message


def test_warn_deprecation_complete_message() -> None:
    """Test deprecation warning with all parameters."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(
            version="1.0.0",
            deprecated_name="complete_test",
            kind="function",
            removal_in="2.0.0",
            alternative="better_function",
            info="This function has been replaced with a more efficient implementation",
        )

        assert len(warning_list) == 1
        message = str(warning_list[0].message)

        expected_parts = [
            "Call to deprecated function 'complete_test'",
            "Deprecated in SQLSpec 1.0.0",
            "This function will be removed in 2.0.0",
            "Use 'better_function' instead",
            "This function has been replaced with a more efficient implementation",
        ]

        for part in expected_parts:
            assert part in message


@pytest.mark.parametrize(
    "kind",
    ["function", "method", "class", "property", "attribute", "parameter", "import", "classmethod"],
    ids=["function", "method", "class", "property", "attribute", "parameter", "import", "classmethod"],
)
def test_warn_deprecation_all_kinds(kind: str) -> None:
    """Test that all supported kinds work correctly."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        warn_deprecation(
            version="1.0.0",
            deprecated_name="test_item",
            kind=kind,  # type: ignore[arg-type]
        )

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert f"{kind} 'test_item'" in message


def test_deprecated_decorator_basic_function() -> None:
    """Test deprecated decorator on a basic function."""

    @deprecated(version="1.0.0")
    def test_function() -> str:
        return "test_result"

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        result = test_function()

        assert result == "test_result"
        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "Call to deprecated function 'test_function'" in message


def test_deprecated_decorator_with_parameters() -> None:
    """Test deprecated decorator with all parameters."""

    @deprecated(
        version="1.0.0",
        removal_in="2.0.0",
        alternative="new_function",
        info="Use the new implementation",
        pending=False,
    )
    def old_function(x: int, y: int) -> int:
        return x + y

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        result = old_function(1, 2)

        assert result == 3
        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "Call to deprecated function 'old_function'" in message
        assert "removed in 2.0.0" in message
        assert "Use 'new_function' instead" in message
        assert "Use the new implementation" in message


def test_deprecated_decorator_pending() -> None:
    """Test deprecated decorator with pending=True."""

    @deprecated(version="1.0.0", pending=True)
    def pending_function() -> None:
        pass

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        pending_function()

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is PendingDeprecationWarning


def test_deprecated_decorator_preserves_function_metadata() -> None:
    """Test that deprecated decorator preserves function metadata."""

    @deprecated(version="1.0.0")
    def documented_function(param: int) -> str:
        """A well documented function.

        Args:
            param: An integer parameter.

        Returns:
            A string representation.
        """
        return str(param)

    assert documented_function.__name__ == "documented_function"
    doc = documented_function.__doc__
    assert doc is not None
    assert "A well documented function" in doc
    for type_name, expected in zip(documented_function.__annotations__.values(), [int, str]):
        assert type_name == expected.__name__, "Annotations should be preserved"


def test_deprecated_decorator_with_kind_auto_detection() -> None:
    """Test that decorator auto-detects function vs method."""

    @deprecated(version="1.0.0")
    def standalone_function() -> None:
        pass

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        standalone_function()

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "deprecated function" in message  # Should detect as function


def test_deprecated_decorator_explicit_kind() -> None:
    """Test deprecated decorator with explicit kind parameter."""

    @deprecated(version="1.0.0", kind="method")
    def test_function() -> None:
        pass

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        test_function()

        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        assert "deprecated method" in message  # Should use explicit kind


def test_deprecated_decorator_method_detection() -> None:
    """Test deprecated decorator on actual method."""

    class TestClass:
        @deprecated(version="1.0.0")
        def test_method(self) -> str:
            return "method_result"

    instance = TestClass()
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        result = instance.test_method()

        assert result == "method_result"
        assert len(warning_list) == 1
        message = str(warning_list[0].message)
        # Should detect as function since inspect.ismethod only works on bound methods
        assert "deprecated function 'test_method'" in message


def test_deprecated_decorator_multiple_calls() -> None:
    """Test that decorator warns on each call."""

    @deprecated(version="1.0.0")
    def repeated_function() -> int:
        return 42

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")

        # Call multiple times
        repeated_function()
        repeated_function()
        repeated_function()

        # Should warn each time
        assert len(warning_list) == 3
        for warning in warning_list:
            assert "deprecated function 'repeated_function'" in str(warning.message)


def test_deprecated_decorator_with_exception() -> None:
    """Test that decorator works correctly when decorated function raises exception."""

    @deprecated(version="1.0.0")
    def failing_function() -> None:
        raise ValueError("Function failed")

    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")

        with pytest.raises(ValueError, match="Function failed"):
            failing_function()

        # Should still warn even when function raises
        assert len(warning_list) == 1
        assert "deprecated function 'failing_function'" in str(warning_list[0].message)


def test_deprecated_decorator_return_value_passthrough() -> None:
    """Test that decorator correctly passes through return values."""

    @deprecated(version="1.0.0")
    def return_complex_value() -> dict[str, Any]:
        return {"key": "value", "number": 42, "nested": {"inner": "data"}}

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = return_complex_value()

    expected = {"key": "value", "number": 42, "nested": {"inner": "data"}}
    assert result == expected


def test_deprecated_decorator_arguments_passthrough() -> None:
    """Test that decorator correctly passes through arguments."""

    @deprecated(version="1.0.0")
    def function_with_args(
        pos_arg: str, *args: int, kwarg: str = "default", **kwargs: Any
    ) -> tuple[str, tuple[int, ...], str, dict[str, Any]]:
        return pos_arg, args, kwarg, kwargs

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = function_with_args("test", 1, 2, 3, kwarg="custom", extra="value")

    expected = ("test", (1, 2, 3), "custom", {"extra": "value"})
    assert result == expected
