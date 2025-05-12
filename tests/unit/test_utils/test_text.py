from sqlspec.utils.text import check_email, slugify, snake_case


def test_check_email() -> None:
    valid_email = "test@test.com"
    valid_email_upper = "TEST@TEST.COM"

    assert check_email(valid_email) == valid_email
    assert check_email(valid_email_upper) == valid_email


def test_slugify() -> None:
    string = "This is a Test!"
    expected_slug = "this-is-a-test"
    assert slugify(string) == expected_slug
    assert slugify(string, separator="_") == "this_is_a_test"


def test_snake_case() -> None:
    """Test the snake_case function."""
    test_cases = {
        "simpleString": "simple_string",
        "SimpleString": "simple_string",
        "SimpleStringWithCAPS": "simple_string_with_caps",
        "HTTPRequest": "http_request",
        "anotherHTTPRequest": "another_http_request",
        "endsWithHTTPRequest": "ends_with_http_request",
        "SSLError": "ssl_error",
        "HTMLParser": "html_parser",
        "MyCoolAPI": "my_cool_api",
        "My_Cool_API": "my_cool_api",
        "my-cool-api": "my_cool_api",
        "my cool api": "my_cool_api",
        "my.cool.api": "my_cool_api",
        "  leading and trailing spaces  ": "leading_and_trailing_spaces",
        "__leading_and_trailing_underscores__": "leading_and_trailing_underscores",
        "--leading-and-trailing-hyphens--": "leading_and_trailing_hyphens",
        "with__multiple___underscores": "with_multiple_underscores",
        "with--multiple---hyphens": "with_multiple_hyphens",
        "with..multiple...dots": "with_multiple_dots",
        "stringWith1Number": "string_with1_number",
        "stringWith123Numbers": "string_with123_numbers",
        "123startsWithNumber": "123starts_with_number",
        "word": "word",
        "WORD": "word",
        "A": "a",
        "a": "a",
        "": "",
        "ComplexHTTPRequestWithNumber123AndMore": "complex_http_request_with_number123_and_more",
        "AnotherExample_ForYou-Sir.Yes": "another_example_for_you_sir_yes",
        "_Already_Snake_Case_": "already_snake_case",  # existing underscores should be handled gracefully
        "Already_Snake_Case": "already_snake_case",
        "already_snake_case": "already_snake_case",
    }

    for input_str, expected_output in test_cases.items():
        assert snake_case(input_str) == expected_output, f"Input: '{input_str}'"
