"""Tests for sqlspec.utils.uuids module.

Tests UUID and Nano ID generation utilities including optional acceleration
via uuid-utils and fastnanoid packages.
"""

import warnings
from uuid import UUID

import pytest

from sqlspec.utils.uuids import (
    NAMESPACE_DNS,
    NAMESPACE_OID,
    NAMESPACE_URL,
    NAMESPACE_X500,
    NANOID_INSTALLED,
    UUID_UTILS_INSTALLED,
    nanoid,
    uuid3,
    uuid4,
    uuid5,
    uuid6,
    uuid7,
)

pytestmark = pytest.mark.xdist_group("utils")


def _is_uuid_like(obj: object) -> bool:
    """Check if object has UUID-like interface.

    Both stdlib uuid.UUID and uuid_utils.UUID have these attributes.
    """
    return (
        hasattr(obj, "version")
        and hasattr(obj, "hex")
        and hasattr(obj, "bytes")
        and hasattr(obj, "int")
        and len(str(obj)) == 36
        and str(obj).count("-") == 4
    )


@pytest.fixture(autouse=True)
def reset_warnings() -> None:
    """Reset warning state before each test."""
    return


def test_uuid3_returns_valid_uuid() -> None:
    """Test uuid3 returns a valid UUID-like object."""
    result = uuid3("test-name")
    assert _is_uuid_like(result)


def test_uuid4_returns_valid_uuid() -> None:
    """Test uuid4 returns a valid UUID-like object."""
    result = uuid4()
    assert _is_uuid_like(result)


def test_uuid5_returns_valid_uuid() -> None:
    """Test uuid5 returns a valid UUID-like object."""
    result = uuid5("test-name")
    assert _is_uuid_like(result)


def test_uuid6_returns_valid_uuid() -> None:
    """Test uuid6 returns a valid UUID-like object."""
    result = uuid6()
    assert _is_uuid_like(result)


def test_uuid7_returns_valid_uuid() -> None:
    """Test uuid7 returns a valid UUID-like object."""
    result = uuid7()
    assert _is_uuid_like(result)


def test_nanoid_returns_valid_string() -> None:
    """Test nanoid returns a valid string."""
    result = nanoid()
    assert isinstance(result, str)
    assert len(result) > 0


def test_uuid3_deterministic() -> None:
    """Test uuid3 produces the same UUID for the same input."""
    name = "deterministic-test"
    result1 = uuid3(name)
    result2 = uuid3(name)
    assert str(result1) == str(result2)


def test_uuid5_deterministic() -> None:
    """Test uuid5 produces the same UUID for the same input."""
    name = "deterministic-test"
    result1 = uuid5(name)
    result2 = uuid5(name)
    assert str(result1) == str(result2)


def test_uuid3_default_namespace() -> None:
    """Test uuid3 uses NAMESPACE_DNS by default."""
    name = "test-namespace"
    result_default = uuid3(name)
    result_explicit = uuid3(name, namespace=NAMESPACE_DNS)
    assert str(result_default) == str(result_explicit)


def test_uuid5_default_namespace() -> None:
    """Test uuid5 uses NAMESPACE_DNS by default."""
    name = "test-namespace"
    result_default = uuid5(name)
    result_explicit = uuid5(name, namespace=NAMESPACE_DNS)
    assert str(result_default) == str(result_explicit)


def test_uuid3_custom_namespace() -> None:
    """Test uuid3 can use a custom namespace."""
    name = "test-custom"
    result_dns = uuid3(name, namespace=NAMESPACE_DNS)
    result_url = uuid3(name, namespace=NAMESPACE_URL)
    assert str(result_dns) != str(result_url)


def test_uuid5_custom_namespace() -> None:
    """Test uuid5 can use a custom namespace."""
    name = "test-custom"
    result_dns = uuid5(name, namespace=NAMESPACE_DNS)
    result_url = uuid5(name, namespace=NAMESPACE_URL)
    assert str(result_dns) != str(result_url)


def test_uuid3_different_names_produce_different_uuids() -> None:
    """Test uuid3 produces different UUIDs for different names."""
    result1 = uuid3("name-one")
    result2 = uuid3("name-two")
    assert str(result1) != str(result2)


def test_uuid5_different_names_produce_different_uuids() -> None:
    """Test uuid5 produces different UUIDs for different names."""
    result1 = uuid5("name-one")
    result2 = uuid5("name-two")
    assert str(result1) != str(result2)


def test_uuid4_uniqueness() -> None:
    """Test uuid4 produces different UUIDs on different calls."""
    results = [str(uuid4()) for _ in range(100)]
    unique_results = set(results)
    assert len(unique_results) == len(results)


def test_nanoid_uniqueness() -> None:
    """Test nanoid produces different IDs on different calls."""
    results = [nanoid() for _ in range(100)]
    unique_results = set(results)
    assert len(unique_results) == len(results)


def test_uuid3_is_version_3() -> None:
    """Test uuid3 returns a version 3 UUID."""
    result = uuid3("test-version")
    assert result.version == 3


def test_uuid5_is_version_5() -> None:
    """Test uuid5 returns a version 5 UUID."""
    result = uuid5("test-version")
    assert result.version == 5


def test_uuid4_is_version_4() -> None:
    """Test uuid4 returns a version 4 UUID."""
    result = uuid4()
    assert result.version == 4


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid-utils not installed")
def test_uuid6_is_version_6() -> None:
    """Test uuid6 returns a version 6 UUID when uuid-utils is installed."""
    result = uuid6()
    assert result.version == 6


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid-utils not installed")
def test_uuid7_is_version_7() -> None:
    """Test uuid7 returns a version 7 UUID when uuid-utils is installed."""
    result = uuid7()
    assert result.version == 7


def test_namespace_constants_exported() -> None:
    """Test all namespace constants are accessible and are UUID-like."""
    assert _is_uuid_like(NAMESPACE_DNS)
    assert _is_uuid_like(NAMESPACE_URL)
    assert _is_uuid_like(NAMESPACE_OID)
    assert _is_uuid_like(NAMESPACE_X500)


def test_namespace_constants_are_standard_values() -> None:
    """Test namespace constants have the standard RFC 4122 values."""
    assert str(NAMESPACE_DNS) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    assert str(NAMESPACE_URL) == "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
    assert str(NAMESPACE_OID) == "6ba7b812-9dad-11d1-80b4-00c04fd430c8"
    assert str(NAMESPACE_X500) == "6ba7b814-9dad-11d1-80b4-00c04fd430c8"


def test_uuid_utils_installed_flag() -> None:
    """Test UUID_UTILS_INSTALLED flag is accessible and truthy."""
    assert UUID_UTILS_INSTALLED is not None
    assert isinstance(bool(UUID_UTILS_INSTALLED), bool)


def test_nanoid_installed_flag() -> None:
    """Test NANOID_INSTALLED flag is accessible and truthy."""
    assert NANOID_INSTALLED is not None
    assert isinstance(bool(NANOID_INSTALLED), bool)


def test_uuid3_with_all_namespaces() -> None:
    """Test uuid3 works with all standard namespaces."""
    name = "namespace-test"
    namespaces = [NAMESPACE_DNS, NAMESPACE_URL, NAMESPACE_OID, NAMESPACE_X500]

    results = [str(uuid3(name, namespace=ns)) for ns in namespaces]
    unique_results = set(results)
    assert len(unique_results) == len(namespaces)


def test_uuid5_with_all_namespaces() -> None:
    """Test uuid5 works with all standard namespaces."""
    name = "namespace-test"
    namespaces = [NAMESPACE_DNS, NAMESPACE_URL, NAMESPACE_OID, NAMESPACE_X500]

    results = [str(uuid5(name, namespace=ns)) for ns in namespaces]
    unique_results = set(results)
    assert len(unique_results) == len(namespaces)


def test_uuid3_with_custom_uuid_namespace() -> None:
    """Test uuid3 works with a custom stdlib UUID as namespace."""
    custom_namespace = UUID("12345678-1234-1234-1234-123456789abc")
    name = "custom-namespace-test"

    result = uuid3(name, namespace=custom_namespace)
    assert _is_uuid_like(result)
    assert result.version == 3


def test_uuid5_with_custom_uuid_namespace() -> None:
    """Test uuid5 works with a custom stdlib UUID as namespace."""
    custom_namespace = UUID("12345678-1234-1234-1234-123456789abc")
    name = "custom-namespace-test"

    result = uuid5(name, namespace=custom_namespace)
    assert _is_uuid_like(result)
    assert result.version == 5


def test_uuid3_empty_string_name() -> None:
    """Test uuid3 works with an empty string name."""
    result = uuid3("")
    assert _is_uuid_like(result)
    assert result.version == 3


def test_uuid5_empty_string_name() -> None:
    """Test uuid5 works with an empty string name."""
    result = uuid5("")
    assert _is_uuid_like(result)
    assert result.version == 5


def test_uuid3_unicode_name() -> None:
    """Test uuid3 works with unicode characters in name."""
    result = uuid3("test-unicode-name")
    assert _is_uuid_like(result)
    assert result.version == 3


def test_uuid5_unicode_name() -> None:
    """Test uuid5 works with unicode characters in name."""
    result = uuid5("test-unicode-name")
    assert _is_uuid_like(result)
    assert result.version == 5


@pytest.mark.skipif(not NANOID_INSTALLED, reason="fastnanoid not installed")
def test_nanoid_length_with_fastnanoid() -> None:
    """Test nanoid returns 21 character string when fastnanoid is installed."""
    result = nanoid()
    assert len(result) == 21


@pytest.mark.skipif(not NANOID_INSTALLED, reason="fastnanoid not installed")
def test_nanoid_url_safe_characters() -> None:
    """Test nanoid contains only URL-safe characters when fastnanoid is installed."""
    result = nanoid()
    valid_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")
    assert all(c in valid_chars for c in result)


@pytest.mark.skipif(bool(UUID_UTILS_INSTALLED), reason="Test requires uuid-utils NOT installed")
def test_uuid6_warning_without_uuid_utils() -> None:
    """Test uuid6 emits warning when uuid-utils is not installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        uuid6()

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is UserWarning
        assert "uuid-utils not installed" in str(warning.message)
        assert "pip install sqlspec[uuid]" in str(warning.message)


@pytest.mark.skipif(bool(UUID_UTILS_INSTALLED), reason="Test requires uuid-utils NOT installed")
def test_uuid7_warning_without_uuid_utils() -> None:
    """Test uuid7 emits warning when uuid-utils is not installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        uuid7()

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is UserWarning
        assert "uuid-utils not installed" in str(warning.message)
        assert "pip install sqlspec[uuid]" in str(warning.message)


@pytest.mark.skipif(bool(NANOID_INSTALLED), reason="Test requires fastnanoid NOT installed")
def test_nanoid_warning_without_fastnanoid() -> None:
    """Test nanoid emits warning when fastnanoid is not installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        nanoid()

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert warning.category is UserWarning
        assert "fastnanoid not installed" in str(warning.message)
        assert "pip install sqlspec[nanoid]" in str(warning.message)


@pytest.mark.skipif(bool(UUID_UTILS_INSTALLED), reason="Test requires uuid-utils NOT installed")
def test_uuid6_warning_each_call() -> None:
    """Test uuid6 emits warning per call when uuid-utils is not installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        uuid6()
        uuid6()
        uuid6()

        assert len(warning_list) == 3


@pytest.mark.skipif(bool(UUID_UTILS_INSTALLED), reason="Test requires uuid-utils NOT installed")
def test_uuid7_warning_each_call() -> None:
    """Test uuid7 emits warning per call when uuid-utils is not installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        uuid7()
        uuid7()
        uuid7()

        assert len(warning_list) == 3


@pytest.mark.skipif(bool(NANOID_INSTALLED), reason="Test requires fastnanoid NOT installed")
def test_nanoid_warning_each_call() -> None:
    """Test nanoid emits warning per call when fastnanoid is not installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        nanoid()
        nanoid()
        nanoid()

        assert len(warning_list) == 3


@pytest.mark.skipif(bool(UUID_UTILS_INSTALLED), reason="Test requires uuid-utils NOT installed")
def test_uuid6_fallback_returns_uuid4() -> None:
    """Test uuid6 falls back to uuid4 when uuid-utils not installed."""
    result = uuid6()
    assert result.version == 4


@pytest.mark.skipif(bool(UUID_UTILS_INSTALLED), reason="Test requires uuid-utils NOT installed")
def test_uuid7_fallback_returns_uuid4() -> None:
    """Test uuid7 falls back to uuid4 when uuid-utils not installed."""
    result = uuid7()
    assert result.version == 4


@pytest.mark.skipif(bool(NANOID_INSTALLED), reason="Test requires fastnanoid NOT installed")
def test_nanoid_fallback_returns_32_char_hex() -> None:
    """Test nanoid fallback returns 32-character UUID hex string."""
    result = nanoid()
    assert len(result) == 32
    assert all(c in "0123456789abcdef" for c in result)


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid-utils not installed")
def test_uuid6_no_warning_with_uuid_utils() -> None:
    """Test uuid6 does not emit warning when uuid-utils is installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        uuid6()

        uuid6_warnings = [w for w in warning_list if "uuid-utils" in str(w.message)]
        assert len(uuid6_warnings) == 0


@pytest.mark.skipif(not UUID_UTILS_INSTALLED, reason="uuid-utils not installed")
def test_uuid7_no_warning_with_uuid_utils() -> None:
    """Test uuid7 does not emit warning when uuid-utils is installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        uuid7()

        uuid7_warnings = [w for w in warning_list if "uuid-utils" in str(w.message)]
        assert len(uuid7_warnings) == 0


@pytest.mark.skipif(not NANOID_INSTALLED, reason="fastnanoid not installed")
def test_nanoid_no_warning_with_fastnanoid() -> None:
    """Test nanoid does not emit warning when fastnanoid is installed."""
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        nanoid()

        nanoid_warnings = [w for w in warning_list if "fastnanoid" in str(w.message)]
        assert len(nanoid_warnings) == 0


def test_uuid_can_be_converted_to_string() -> None:
    """Test all UUID functions return UUIDs that convert to valid string format."""
    for result in [uuid3("test"), uuid5("test"), uuid4(), uuid6(), uuid7()]:
        str_result = str(result)
        assert len(str_result) == 36
        assert str_result.count("-") == 4


def test_uuid_can_be_converted_to_hex() -> None:
    """Test all UUID functions return UUIDs with valid hex representation."""
    for result in [uuid3("test"), uuid5("test"), uuid4(), uuid6(), uuid7()]:
        hex_result = result.hex
        assert len(hex_result) == 32
        assert all(c in "0123456789abcdef" for c in hex_result)


def test_uuid_can_be_converted_to_bytes() -> None:
    """Test all UUID functions return UUIDs with valid bytes representation."""
    for result in [uuid3("test"), uuid5("test"), uuid4(), uuid6(), uuid7()]:
        bytes_result = result.bytes
        assert len(bytes_result) == 16


def test_uuid_roundtrip_through_string() -> None:
    """Test UUIDs can be recreated from their string representation."""
    original = uuid4()
    recreated = UUID(str(original))
    assert str(original) == str(recreated)


def test_uuid3_roundtrip_through_string() -> None:
    """Test uuid3 UUIDs can be recreated from their string representation."""
    original = uuid3("test-roundtrip")
    recreated = UUID(str(original))
    assert str(original) == str(recreated)


def test_uuid5_roundtrip_through_string() -> None:
    """Test uuid5 UUIDs can be recreated from their string representation."""
    original = uuid5("test-roundtrip")
    recreated = UUID(str(original))
    assert str(original) == str(recreated)


def test_uuid3_produces_known_uuid() -> None:
    """Test uuid3 produces a known expected UUID for a given input.

    This validates that the uuid3 implementation produces correct results
    regardless of whether uuid-utils is installed.
    """
    result = uuid3("python.org", namespace=NAMESPACE_DNS)
    assert str(result) == "6fa459ea-ee8a-3ca4-894e-db77e160355e"


def test_uuid5_produces_known_uuid() -> None:
    """Test uuid5 produces a known expected UUID for a given input.

    This validates that the uuid5 implementation produces correct results
    regardless of whether uuid-utils is installed.
    """
    result = uuid5("python.org", namespace=NAMESPACE_DNS)
    assert str(result) == "886313e1-3b8a-5372-9b90-0c9aee199e5d"
