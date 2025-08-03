"""Tests for placeholder behavior when optional dependencies are not installed."""

from __future__ import annotations

import sys
from unittest.mock import patch


class TestPlaceholderBehavior:
    """Test placeholder implementations when optional libraries are not installed."""

    def test_attrs_placeholders_when_not_installed(self) -> None:
        """Test that attrs placeholders work when attrs is not installed."""
        # Simulate attrs not being installed
        with patch.dict("sys.modules", {"attrs": None}):
            # Clear relevant modules from cache to force re-import
            modules_to_clear = [mod for mod in sys.modules.keys() if "sqlspec" in mod and "typing" in mod]
            for mod in modules_to_clear:
                if mod in sys.modules:
                    del sys.modules[mod]

            # Import should work even without attrs
            from sqlspec.typing import ATTRS_INSTALLED, attrs_asdict, attrs_define, attrs_field
            from sqlspec.utils.type_guards import is_attrs_instance, is_attrs_schema

            # Should detect that attrs is not installed
            assert not ATTRS_INSTALLED

            # Placeholder functions should be callable
            assert callable(attrs_define)
            assert callable(attrs_field)
            assert callable(attrs_asdict)

            # Type guards should return False for any input
            assert not is_attrs_instance({})
            assert not is_attrs_instance("test")
            assert not is_attrs_schema(dict)
            assert not is_attrs_schema(list)

            # Calling placeholder functions should not raise errors
            result_define = attrs_define()
            assert callable(result_define)  # Should return a lambda function

            attrs_field()
            # attrs_field placeholder returns None, which is acceptable

            # attrs_asdict placeholder with empty object should return empty dict
            class MockAttrs:
                pass

            result_asdict = attrs_asdict(MockAttrs())  # type: ignore[arg-type]
            assert isinstance(result_asdict, dict)

    def test_cattrs_placeholders_when_not_installed(self) -> None:
        """Test that cattrs placeholders work when cattrs is not installed."""
        with patch.dict("sys.modules", {"cattrs": None}):
            # Clear relevant modules from cache
            modules_to_clear = [mod for mod in sys.modules.keys() if "sqlspec" in mod and "typing" in mod]
            for mod in modules_to_clear:
                if mod in sys.modules:
                    del sys.modules[mod]

            from sqlspec.typing import CATTRS_INSTALLED, cattrs_structure, cattrs_unstructure

            # Should detect that cattrs is not installed
            assert not CATTRS_INSTALLED

            # Placeholder functions should be callable
            assert callable(cattrs_structure)
            assert callable(cattrs_unstructure)

            # Calling placeholder functions should not raise errors
            cattrs_structure({}, dict)
            # cattrs_structure placeholder can return anything, typically empty dict

            cattrs_unstructure({})
            # cattrs_unstructure placeholder can return anything, typically the input

    def test_pydantic_placeholders_when_not_installed(self) -> None:
        """Test that Pydantic placeholders work when Pydantic is not installed."""
        with patch.dict("sys.modules", {"pydantic": None}):
            modules_to_clear = [mod for mod in sys.modules.keys() if "sqlspec" in mod and "typing" in mod]
            for mod in modules_to_clear:
                if mod in sys.modules:
                    del sys.modules[mod]

            from sqlspec.typing import PYDANTIC_INSTALLED
            from sqlspec.utils.type_guards import is_pydantic_model

            # Should detect that Pydantic is not installed
            assert not PYDANTIC_INSTALLED

            # Type guard should return False for any input
            assert not is_pydantic_model({})
            assert not is_pydantic_model(dict)
            assert not is_pydantic_model("test")

    def test_msgspec_placeholders_when_not_installed(self) -> None:
        """Test that msgspec placeholders work when msgspec is not installed."""
        with patch.dict("sys.modules", {"msgspec": None}):
            modules_to_clear = [mod for mod in sys.modules.keys() if "sqlspec" in mod and "typing" in mod]
            for mod in modules_to_clear:
                if mod in sys.modules:
                    del sys.modules[mod]

            from sqlspec.typing import MSGSPEC_INSTALLED
            from sqlspec.utils.type_guards import is_msgspec_struct

            # Should detect that msgspec is not installed
            assert not MSGSPEC_INSTALLED

            # Type guard should return False for any input
            assert not is_msgspec_struct({})
            assert not is_msgspec_struct(dict)
            assert not is_msgspec_struct("test")

    def test_all_libraries_installed_in_current_environment(self) -> None:
        """Test that all optional libraries are actually installed in our test environment."""
        from sqlspec.typing import ATTRS_INSTALLED, CATTRS_INSTALLED, MSGSPEC_INSTALLED, PYDANTIC_INSTALLED

        # In our test environment, these should all be installed
        assert ATTRS_INSTALLED, "attrs should be installed for testing"
        assert CATTRS_INSTALLED, "cattrs should be installed for testing"
        assert MSGSPEC_INSTALLED, "msgspec should be installed for testing"
        assert PYDANTIC_INSTALLED, "pydantic should be installed for testing"
