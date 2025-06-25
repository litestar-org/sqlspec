"""Tests for sqlspec.utils.text module."""

from __future__ import annotations

import pytest

from sqlspec.utils.text import check_email, slugify, snake_case


@pytest.mark.parametrize(
    ("email", "expected"),
    [
        ("test@test.com", "test@test.com"),
        ("TEST@TEST.COM", "test@test.com"),
        ("User.Name@Example.Org", "user.name@example.org"),
        ("test+tag@example.com", "test+tag@example.com"),
        ("user@sub.domain.com", "user@sub.domain.com"),
        ("123@456.com", "123@456.com"),
    ],
    ids=[
        "lowercase_email",
        "uppercase_email",
        "mixed_case_email",
        "email_with_plus",
        "subdomain_email",
        "numeric_email",
    ],
)
def test_check_email_valid_emails(email: str, expected: str) -> None:
    """Test check_email with various valid email formats."""
    result = check_email(email)
    assert result == expected


def test_check_email_basic_functionality() -> None:
    """Test basic check_email functionality with simple cases."""
    valid_email = "test@test.com"
    valid_email_upper = "TEST@TEST.COM"

    assert check_email(valid_email) == valid_email
    assert check_email(valid_email_upper) == valid_email


def test_check_email_unicode_domains() -> None:
    """Test check_email with Unicode domain names."""
    unicode_email = "test@тест.com"
    result = check_email(unicode_email)
    assert result == "test@тест.com"


def test_check_email_special_characters() -> None:
    """Test check_email with special characters in local part."""
    special_emails = ["user.name@example.com", "user+tag@example.com", "user_name@example.com", "user-name@example.com"]

    for email in special_emails:
        result = check_email(email)
        assert result == email.lower()


@pytest.mark.parametrize(
    ("text", "expected", "separator"),
    [
        ("This is a Test!", "this-is-a-test", "-"),
        ("This is a Test!", "this_is_a_test", "_"),
        ("Hello World", "hello-world", "-"),
        ("Hello World", "hello_world", "_"),
        ("Multiple   Spaces", "multiple-spaces", "-"),
        ("CamelCaseText", "camelcasetext", "-"),
        ("", "", "-"),
        ("SingleWord", "singleword", "-"),
        ("123 Numbers", "123-numbers", "-"),
        ("Special!@#$%Characters", "special-characters", "-"),
        ("Åccénted Tëxt", "accented-text", "-"),
        ("Unicode 世界 Text", "unicode-text", "-"),
    ],
    ids=[
        "basic_dash",
        "basic_underscore",
        "simple_words_dash",
        "simple_words_underscore",
        "multiple_spaces",
        "camel_case",
        "empty_string",
        "single_word",
        "with_numbers",
        "special_chars",
        "accented_chars",
        "unicode_chars",
    ],
)
def test_slugify_comprehensive(text: str, expected: str, separator: str) -> None:
    """Test slugify with various text inputs and separators."""
    result = slugify(text, separator=separator)
    assert result == expected


def test_slugify_basic_functionality() -> None:
    """Test basic slugify functionality."""
    string = "This is a Test!"
    expected_slug = "this-is-a-test"
    assert slugify(string) == expected_slug
    assert slugify(string, separator="_") == "this_is_a_test"


def test_slugify_edge_cases() -> None:
    """Test slugify with edge cases."""
    # Only special characters
    assert slugify("!@#$%^&*()") == ""

    # Mixed numbers and text
    assert slugify("Version 2.0.1") == "version-2-0-1"

    # Leading/trailing spaces
    assert slugify("  spaced text  ") == "spaced-text"

    # Multiple consecutive separators after processing
    assert slugify("word!!!word") == "word-word"


def test_slugify_custom_separators() -> None:
    """Test slugify with different custom separators."""
    text = "Hello World Test"

    assert slugify(text, separator=".") == "hello.world.test"
    assert slugify(text, separator="__") == "hello__world__test"
    assert slugify(text, separator="") == "helloworldtest"


@pytest.mark.parametrize(
    ("input_str", "expected_output"),
    [
        ("simpleString", "simple_string"),
        ("SimpleString", "simple_string"),
        ("SimpleStringWithCAPS", "simple_string_with_caps"),
        ("HTTPRequest", "http_request"),
        ("anotherHTTPRequest", "another_http_request"),
        ("endsWithHTTPRequest", "ends_with_http_request"),
        ("SSLError", "ssl_error"),
        ("HTMLParser", "html_parser"),
        ("MyCoolAPI", "my_cool_api"),
        ("My_Cool_API", "my_cool_api"),
        ("my-cool-api", "my_cool_api"),
        ("my cool api", "my_cool_api"),
        ("my.cool.api", "my_cool_api"),
        ("  leading and trailing spaces  ", "leading_and_trailing_spaces"),
        ("__leading_and_trailing_underscores__", "leading_and_trailing_underscores"),
        ("--leading-and-trailing-hyphens--", "leading_and_trailing_hyphens"),
        ("with__multiple___underscores", "with_multiple_underscores"),
        ("with--multiple---hyphens", "with_multiple_hyphens"),
        ("with..multiple...dots", "with_multiple_dots"),
        ("stringWith1Number", "string_with1_number"),
        ("stringWith123Numbers", "string_with123_numbers"),
        ("123startsWithNumber", "123starts_with_number"),
        ("word", "word"),
        ("WORD", "word"),
        ("A", "a"),
        ("a", "a"),
        ("", ""),
        ("ComplexHTTPRequestWithNumber123AndMore", "complex_http_request_with_number123_and_more"),
        ("AnotherExample_ForYou-Sir.Yes", "another_example_for_you_sir_yes"),
        ("_Already_Snake_Case_", "already_snake_case"),
        ("Already_Snake_Case", "already_snake_case"),
        ("already_snake_case", "already_snake_case"),
    ],
    ids=[
        "simple_camel",
        "pascal_case",
        "mixed_caps",
        "consecutive_caps_start",
        "consecutive_caps_middle",
        "consecutive_caps_end",
        "ssl_error",
        "html_parser",
        "api_case",
        "mixed_separators",
        "hyphen_case",
        "space_case",
        "dot_case",
        "leading_trailing_spaces",
        "leading_trailing_underscores",
        "leading_trailing_hyphens",
        "multiple_underscores",
        "multiple_hyphens",
        "multiple_dots",
        "single_number",
        "multiple_numbers",
        "starts_with_number",
        "single_word_lower",
        "single_word_upper",
        "single_char_upper",
        "single_char_lower",
        "empty_string",
        "complex_case",
        "mixed_everything",
        "already_snake_with_underscores",
        "already_snake_clean",
        "already_snake_perfect",
    ],
)
def test_snake_case_comprehensive(input_str: str, expected_output: str) -> None:
    """Test snake_case with comprehensive test cases."""
    result = snake_case(input_str)
    assert result == expected_output, f"Input: '{input_str}' -> Expected: '{expected_output}' -> Got: '{result}'"


def test_snake_case_unicode_handling() -> None:
    """Test snake_case with Unicode characters."""
    # Unicode letters should be preserved
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


def test_snake_case_acronym_handling() -> None:
    """Test snake_case with various acronyms."""
    # Common tech acronyms
    assert snake_case("XMLHttpRequest") == "xml_http_request"
    assert snake_case("JSONParser") == "json_parser"
    assert snake_case("URLPath") == "url_path"
    assert snake_case("SQLDatabase") == "sql_database"
    assert snake_case("HTTPSConnection") == "https_connection"

    # Mixed acronyms and words
    assert snake_case("parseXMLToJSON") == "parse_xml_to_json"
    assert snake_case("HTTPSURLValidator") == "httpsurl_validator"


def test_snake_case_edge_cases() -> None:
    """Test snake_case with edge cases and boundary conditions."""
    # Only special characters
    assert snake_case("!!!") == ""
    assert snake_case("...") == ""
    assert snake_case("___") == ""
    assert snake_case("---") == ""

    # Single characters
    assert snake_case("X") == "x"
    assert snake_case("1") == "1"
    assert snake_case("_") == ""

    # Numbers only
    assert snake_case("123") == "123"
    assert snake_case("1.2.3") == "1_2_3"


def test_snake_case_whitespace_handling() -> None:
    """Test snake_case with various whitespace scenarios."""
    # Different types of whitespace
    assert snake_case("hello\tworld") == "hello_world"
    assert snake_case("hello\nworld") == "hello_world"
    assert snake_case("hello\r\nworld") == "hello_world"

    # Multiple whitespace types
    assert snake_case("  hello \t world \n test  ") == "hello_world_test"


def test_text_functions_with_very_long_strings() -> None:
    """Test text functions with very long input strings."""
    # Long string for snake_case
    long_camel = "this" + "AndThis" * 100
    result = snake_case(long_camel)
    assert result.startswith("this_and_this")
    assert result.count("_and_this") == 100

    # Long string for slugify
    long_text = "word " * 100
    slug_result = slugify(long_text.strip())
    assert slug_result == "-".join(["word"] * 100)

    # Long email
    long_local = "a" * 50
    long_email = f"{long_local}@example.com"
    email_result = check_email(long_email)
    assert email_result == long_email


def test_text_functions_preserve_original_on_no_change() -> None:
    """Test that functions return expected results when no changes needed."""
    # snake_case with already correct input
    perfect_snake = "already_perfect_snake_case"
    assert snake_case(perfect_snake) == perfect_snake

    # slugify with already slugified input
    perfect_slug = "already-perfect-slug"
    assert slugify(perfect_slug.replace("-", " ")) == perfect_slug

    # check_email with already lowercase
    perfect_email = "perfect@example.com"
    assert check_email(perfect_email) == perfect_email


def test_text_functions_empty_and_none_like_inputs() -> None:
    """Test text functions with empty and none-like inputs."""
    # Empty strings
    assert snake_case("") == ""
    assert slugify("") == ""
    with pytest.raises(ValueError):
        check_email("")

    # Whitespace only
    assert snake_case("   ") == ""
    assert slugify("   ") == ""
    with pytest.raises(ValueError):
        check_email("   ")
