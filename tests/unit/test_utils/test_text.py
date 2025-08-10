"""Tests for sqlspec.utils.text module.

Tests text processing utilities including email validation, slugification,
camelCase conversion, and snake_case conversion.
"""

import pytest

from sqlspec.utils.text import camelize, check_email, slugify, snake_case


def test_check_email_valid() -> None:
    """Test check_email with valid email addresses."""
    assert check_email("test@example.com") == "test@example.com"
    assert check_email("USER@DOMAIN.COM") == "user@domain.com"
    assert check_email("complex.email+test@sub.domain.co.uk") == "complex.email+test@sub.domain.co.uk"


def test_check_email_invalid() -> None:
    """Test check_email with invalid email addresses."""
    with pytest.raises(ValueError, match="Invalid email!"):
        check_email("invalid_email")

    with pytest.raises(ValueError, match="Invalid email!"):
        check_email("")

    with pytest.raises(ValueError, match="Invalid email!"):
        check_email("no_at_symbol")


def test_check_email_unicode_domains() -> None:
    """Test check_email with Unicode domain names."""
    unicode_email = "test@тест.com"
    result = check_email(unicode_email)
    assert result == "test@тест.com"


def test_check_email_special_local_parts() -> None:
    """Test check_email with special characters in local part."""
    special_emails = ["user.name@example.com", "user+tag@example.com", "user_name@example.com", "user-name@example.com"]

    for email in special_emails:
        result = check_email(email)
        assert result == email.lower()


def test_slugify_basic() -> None:
    """Test basic slugify functionality."""
    assert slugify("Hello World") == "hello-world"
    assert slugify("Test String") == "test-string"
    assert slugify("Multiple   Spaces") == "multiple-spaces"


def test_slugify_special_characters() -> None:
    """Test slugify with special characters."""
    assert slugify("Hello, World!") == "hello-world"
    assert slugify("Test@#$%String") == "test-string"
    assert slugify("Special---Characters") == "special-characters"


def test_slugify_unicode() -> None:
    """Test slugify with unicode characters."""
    assert slugify("Héllo Wörld", allow_unicode=False) == "hello-world"
    assert slugify("Héllo Wörld", allow_unicode=True) == "héllo-wörld"


def test_slugify_custom_separator() -> None:
    """Test slugify with custom separator."""
    assert slugify("Hello World", separator="_") == "hello_world"
    assert slugify("Test String", separator=".") == "test.string"
    assert slugify("Multiple   Spaces", separator="") == "multiplespaces"


def test_slugify_edge_cases() -> None:
    """Test slugify with edge cases."""
    assert slugify("") == ""
    assert slugify("---") == ""
    assert slugify("   ") == ""
    assert slugify("test---string") == "test-string"


def test_slugify_comprehensive_edge_cases() -> None:
    """Test slugify with comprehensive edge cases."""
    # Only special characters
    assert slugify("!@#$%^&*()") == ""

    # Mixed numbers and text
    assert slugify("Version 2.0.1") == "version-2-0-1"

    # Leading/trailing spaces
    assert slugify("  spaced text  ") == "spaced-text"

    # Multiple consecutive separators after processing
    assert slugify("word!!!word") == "word-word"

    # Unicode handling
    assert slugify("Åccénted Tëxt", allow_unicode=False) == "accented-text"
    assert slugify("Unicode 世界 Text", allow_unicode=False) == "unicode-text"


def test_camelize_basic() -> None:
    """Test basic camelize functionality."""
    assert camelize("hello_world") == "helloWorld"
    assert camelize("test_string") == "testString"
    assert camelize("single") == "single"


def test_camelize_edge_cases() -> None:
    """Test camelize with edge cases."""
    assert camelize("") == ""
    assert camelize("_") == ""
    assert camelize("multiple_underscore_words") == "multipleUnderscoreWords"


def test_camelize_caching() -> None:
    """Test that camelize uses LRU cache."""
    # Clear cache if it exists
    camelize.cache_clear()

    # First call should populate cache
    result1 = camelize("test_string")
    assert result1 == "testString"

    # Second call should use cache
    result2 = camelize("test_string")
    assert result2 == "testString"

    # Check cache info
    cache_info = camelize.cache_info()
    assert cache_info.hits >= 1


def test_snake_case_basic() -> None:
    """Test basic snake_case functionality."""
    assert snake_case("HelloWorld") == "hello_world"
    assert snake_case("testString") == "test_string"
    assert snake_case("single") == "single"


def test_snake_case_camel_and_pascal() -> None:
    """Test snake_case with CamelCase and PascalCase."""
    assert snake_case("CamelCaseString") == "camel_case_string"
    assert snake_case("PascalCaseExample") == "pascal_case_example"
    assert snake_case("mixedCaseString") == "mixed_case_string"


def test_snake_case_acronyms() -> None:
    """Test snake_case correctly handles acronyms."""
    assert snake_case("HTTPRequest") == "http_request"
    assert snake_case("XMLParser") == "xml_parser"
    assert snake_case("URLPath") == "url_path"
    assert snake_case("HTTPSConnection") == "https_connection"


def test_snake_case_with_numbers() -> None:
    """Test snake_case with numbers."""
    assert snake_case("Python3IsGreat") == "python3_is_great"
    assert snake_case("Version2Update") == "version2_update"
    assert snake_case("Test123String") == "test123_string"


def test_snake_case_separators() -> None:
    """Test snake_case with different separators."""
    assert snake_case("hello-world") == "hello_world"
    assert snake_case("hello world") == "hello_world"
    assert snake_case("hello.world") == "hello_world"
    assert snake_case("hello@world") == "hello_world"


def test_snake_case_edge_cases() -> None:
    """Test snake_case with edge cases."""
    assert snake_case("") == ""
    assert snake_case("_") == ""
    assert snake_case("__") == ""
    assert snake_case("test__string") == "test_string"
    assert snake_case("_test_") == "test"


def test_snake_case_unicode_handling() -> None:
    """Test snake_case with Unicode characters."""
    assert snake_case("helloMörld") == "hello_mörld"
    assert snake_case("café") == "café"
    assert snake_case("naïveApproach") == "naïve_approach"


def test_snake_case_numbers_and_special_handling() -> None:
    """Test snake_case with numbers and special character combinations."""
    # Numbers at boundaries
    assert snake_case("version2Point0") == "version2_point0"
    assert snake_case("catch22Exception") == "catch22_exception"

    # Mixed special characters
    assert snake_case("file_name-v2.txt") == "file_name_v2_txt"
    assert snake_case("my@email.com") == "my_email_com"


def test_snake_case_comprehensive_edge_cases() -> None:
    """Test snake_case with comprehensive edge cases."""
    # Only special characters
    assert snake_case("!!!") == ""
    assert snake_case("...") == ""
    assert snake_case("___") == ""
    assert snake_case("---") == ""

    # Single characters
    assert snake_case("X") == "x"
    assert snake_case("1") == "1"
    assert snake_case("_") == ""

    # Whitespace only
    assert snake_case("   ") == ""


def test_snake_case_caching() -> None:
    """Test that snake_case uses LRU cache."""
    # Clear cache if it exists
    snake_case.cache_clear()

    # First call should populate cache
    result1 = snake_case("TestString")
    assert result1 == "test_string"

    # Second call should use cache
    result2 = snake_case("TestString")
    assert result2 == "test_string"

    # Check cache info
    cache_info = snake_case.cache_info()
    assert cache_info.hits >= 1


def test_text_utilities_integration() -> None:
    """Test integration of text utilities."""
    # Test chaining operations
    original = "Hello World Test"
    snake_cased = snake_case(original)
    assert snake_cased == "hello_world_test"

    camelized = camelize(snake_cased)
    assert camelized == "helloWorldTest"

    slugified = slugify(original)
    assert slugified == "hello-world-test"


@pytest.mark.parametrize(
    "input_string,expected_snake,expected_camel",
    [
        ("simple", "simple", "simple"),
        ("SimpleTest", "simple_test", "simpleTest"),
        ("HTTPSConnection", "https_connection", "httpsConnection"),
        ("XMLHttpRequest", "xml_http_request", "xmlHttpRequest"),
        ("test_string", "test_string", "testString"),
        ("", "", ""),
        ("A", "a", "a"),
        ("AB", "ab", "ab"),
        ("ABC", "abc", "abc"),
    ],
    ids=[
        "simple_word",
        "pascal_case",
        "https_acronym",
        "xml_acronym",
        "already_snake",
        "empty_string",
        "single_char",
        "two_chars",
        "three_chars",
    ],
)
def test_text_transformations_parametrized(input_string: str, expected_snake: str, expected_camel: str) -> None:
    """Test various text transformations with parametrized inputs."""
    assert snake_case(input_string) == expected_snake

    # For camelCase, we need to start with snake_case
    snake_version = snake_case(input_string) if "_" not in input_string else input_string
    assert camelize(snake_version) == expected_camel


def test_text_functions_boundary_conditions() -> None:
    """Test text functions with boundary conditions."""
    # Very long strings
    long_string = "a" * 1000
    assert len(slugify(long_string)) == len(long_string)
    assert len(snake_case("A" * 1000)) == 1000

    # Single character edge cases
    assert slugify("A") == "a"
    assert snake_case("A") == "a"
    assert camelize("a") == "a"

    # Numbers only
    assert slugify("123") == "123"
    assert snake_case("123") == "123"


def test_performance_text_utilities() -> None:
    """Test performance characteristics of text utilities."""
    # Test that caching works for repeated calls
    test_strings = ["TestString", "AnotherTest", "HTTPSConnection"] * 10

    # Multiple calls should benefit from caching
    for test_string in test_strings:
        snake_result = snake_case(test_string)
        camel_result = camelize(snake_result)

        # Verify results are consistent
        assert isinstance(snake_result, str)
        assert isinstance(camel_result, str)

    # Check cache statistics
    snake_cache_info = snake_case.cache_info()
    camel_cache_info = camelize.cache_info()

    assert snake_cache_info.hits > 0
    assert camel_cache_info.hits > 0
