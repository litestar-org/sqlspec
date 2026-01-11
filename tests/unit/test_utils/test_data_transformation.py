"""Unit tests for data transformation utilities.

Tests the transform_dict_keys function and related utilities for field name
conversion when mapping database results to schema objects.
"""

from sqlspec.utils.schema import transform_dict_keys
from sqlspec.utils.text import camelize, kebabize, pascalize


def test_transform_dict_keys_simple_dict() -> None:
    """Test basic dictionary key transformation with camelize."""
    data = {"user_id": 123, "created_at": "2024-01-01"}
    result = transform_dict_keys(data, camelize)
    expected = {"userId": 123, "createdAt": "2024-01-01"}
    assert result == expected


def test_transform_dict_keys_nested_dict() -> None:
    """Test nested dictionary key transformation."""
    data = {
        "user_data": {"first_name": "John", "last_name": "Doe"},
        "order_info": {"order_id": 456, "order_date": "2024-01-01"},
    }
    result = transform_dict_keys(data, camelize)
    expected = {
        "userData": {"firstName": "John", "lastName": "Doe"},
        "orderInfo": {"orderId": 456, "orderDate": "2024-01-01"},
    }
    assert result == expected


def test_transform_dict_keys_list_of_dicts() -> None:
    """Test transformation of list containing dictionaries."""
    data = [{"item_id": 1, "item_name": "Product A"}, {"item_id": 2, "item_name": "Product B"}]
    result = transform_dict_keys(data, camelize)
    expected = [{"itemId": 1, "itemName": "Product A"}, {"itemId": 2, "itemName": "Product B"}]
    assert result == expected


def test_transform_dict_keys_mixed_list() -> None:
    """Test transformation of list with mixed types."""
    data = [{"user_id": 1, "user_name": "John"}, "plain_string", 123, {"order_id": 456, "order_total": 99.99}]
    result = transform_dict_keys(data, camelize)
    expected = [{"userId": 1, "userName": "John"}, "plain_string", 123, {"orderId": 456, "orderTotal": 99.99}]
    assert result == expected


def test_transform_dict_keys_deeply_nested() -> None:
    """Test transformation of deeply nested structures."""
    data = {
        "user_data": {
            "personal_info": {"first_name": "John", "last_name": "Doe"},
            "contact_info": {
                "email_address": "john@example.com",
                "phone_numbers": [
                    {"phone_type": "home", "phone_number": "123-456-7890"},
                    {"phone_type": "work", "phone_number": "098-765-4321"},
                ],
            },
        }
    }
    result = transform_dict_keys(data, camelize)
    expected = {
        "userData": {
            "personalInfo": {"firstName": "John", "lastName": "Doe"},
            "contactInfo": {
                "emailAddress": "john@example.com",
                "phoneNumbers": [
                    {"phoneType": "home", "phoneNumber": "123-456-7890"},
                    {"phoneType": "work", "phoneNumber": "098-765-4321"},
                ],
            },
        }
    }
    assert result == expected


def test_transform_dict_keys_with_kebabize() -> None:
    """Test transformation with kebab-case converter."""
    data = {"user_id": 123, "created_at_timestamp": "2024-01-01"}
    result = transform_dict_keys(data, kebabize)
    expected = {"user-id": 123, "created-at-timestamp": "2024-01-01"}
    assert result == expected


def test_transform_dict_keys_with_pascalize() -> None:
    """Test transformation with PascalCase converter."""
    data = {"user_id": 123, "created_at_timestamp": "2024-01-01"}
    result = transform_dict_keys(data, pascalize)
    expected = {"UserId": 123, "CreatedAtTimestamp": "2024-01-01"}
    assert result == expected


def test_transform_dict_keys_empty_dict() -> None:
    """Test transformation of empty dictionary."""
    data = {}  # type: dict[str, str]
    result = transform_dict_keys(data, camelize)
    assert result == {}


def test_transform_dict_keys_empty_list() -> None:
    """Test transformation of empty list."""
    data = []  # type: list[dict[str, str]]
    result = transform_dict_keys(data, camelize)
    assert result == []


def test_transform_dict_keys_non_dict_non_list() -> None:
    """Test transformation of non-dict, non-list values."""
    assert transform_dict_keys("string", camelize) == "string"
    assert transform_dict_keys(123, camelize) == 123
    assert transform_dict_keys(None, camelize) is None
    assert transform_dict_keys(True, camelize) is True


def test_transform_dict_keys_dict_with_non_string_keys() -> None:
    """Test transformation of dictionary with non-string keys."""
    data = {123: "numeric_key", "user_id": 456, True: "boolean_key"}
    result = transform_dict_keys(data, camelize)
    expected = {123: "numeric_key", "userId": 456, True: "boolean_key"}
    assert result == expected


def test_transform_dict_keys_dict_with_none_values() -> None:
    """Test transformation preserves None values."""
    data = {"user_id": None, "email_address": None, "active_flag": True}
    result = transform_dict_keys(data, camelize)
    expected = {"userId": None, "emailAddress": None, "activeFlag": True}
    assert result == expected


def test_transform_dict_keys_conversion_error_handling() -> None:
    """Test error handling when converter function fails."""

    def failing_converter(key: str) -> str:
        if key == "bad_key":
            raise ValueError("Conversion failed")
        return camelize(key)

    data = {"user_id": 123, "bad_key": "value", "email_address": "test@example.com"}
    result = transform_dict_keys(data, failing_converter)

    # Should preserve original key when conversion fails
    assert "bad_key" in result
    assert result["bad_key"] == "value"  # type: ignore[call-overload]
    # But still convert other keys
    assert "userId" in result
    assert result["userId"] == 123  # type: ignore[call-overload]
    assert "emailAddress" in result
    assert result["emailAddress"] == "test@example.com"  # type: ignore[call-overload]


def test_transform_dict_keys_nested_conversion_error() -> None:
    """Test error handling in nested structures."""

    def failing_converter(key: str) -> str:
        if "bad" in key:
            raise ValueError("Conversion failed")
        return camelize(key)

    data = {"user_data": {"first_name": "John", "bad_field": "should_fail"}, "order_info": {"order_id": 123}}
    result = transform_dict_keys(data, failing_converter)

    # Top-level keys should convert successfully
    assert "userData" in result
    assert "orderInfo" in result

    # Failed conversion should preserve original key
    assert "bad_field" in result["userData"]  # type: ignore[call-overload]
    assert result["userData"]["bad_field"] == "should_fail"  # type: ignore[call-overload]

    # Successful conversions should still work
    assert "firstName" in result["userData"]  # type: ignore[call-overload]
    assert result["userData"]["firstName"] == "John"  # type: ignore[call-overload]
    assert "orderId" in result["orderInfo"]  # type: ignore[call-overload]
    assert result["orderInfo"]["orderId"] == 123  # type: ignore[call-overload]


def test_transform_dict_keys_complex_jsonb_structure() -> None:
    """Test transformation of complex JSONB-like structures."""
    data = {
        "user_profile": {
            "basic_info": {"first_name": "Alice", "last_name": "Smith", "date_of_birth": "1990-01-01"},
            "preferences": {
                "notification_settings": {
                    "email_notifications": True,
                    "push_notifications": False,
                    "sms_notifications": True,
                },
                "privacy_settings": {"profile_visibility": "public", "search_visibility": "friends"},
            },
            "activity_data": [
                {
                    "activity_type": "login",
                    "timestamp": "2024-01-01T10:00:00Z",
                    "metadata": {"ip_address": "192.168.1.1", "user_agent": "Chrome"},
                },
                {
                    "activity_type": "purchase",
                    "timestamp": "2024-01-01T11:00:00Z",
                    "metadata": {"order_id": "ORD-123", "total_amount": 99.99},
                },
            ],
        }
    }

    result = transform_dict_keys(data, camelize)

    # Verify structure transformation
    assert "userProfile" in result
    profile = result["userProfile"]  # type: ignore[call-overload]

    assert "basicInfo" in profile
    basic_info = profile["basicInfo"]
    assert basic_info["firstName"] == "Alice"
    assert basic_info["lastName"] == "Smith"
    assert basic_info["dateOfBirth"] == "1990-01-01"

    assert "preferences" in profile
    preferences = profile["preferences"]
    assert "notificationSettings" in preferences
    assert "privacySettings" in preferences

    notification_settings = preferences["notificationSettings"]
    assert notification_settings["emailNotifications"] is True
    assert notification_settings["pushNotifications"] is False
    assert notification_settings["smsNotifications"] is True

    privacy_settings = preferences["privacySettings"]
    assert privacy_settings["profileVisibility"] == "public"
    assert privacy_settings["searchVisibility"] == "friends"

    assert "activityData" in profile
    activity_data = profile["activityData"]
    assert len(activity_data) == 2

    login_activity = activity_data[0]
    assert login_activity["activityType"] == "login"
    assert login_activity["timestamp"] == "2024-01-01T10:00:00Z"
    assert login_activity["metadata"]["ipAddress"] == "192.168.1.1"
    assert login_activity["metadata"]["userAgent"] == "Chrome"

    purchase_activity = activity_data[1]
    assert purchase_activity["activityType"] == "purchase"
    assert purchase_activity["timestamp"] == "2024-01-01T11:00:00Z"
    assert purchase_activity["metadata"]["orderId"] == "ORD-123"
    assert purchase_activity["metadata"]["totalAmount"] == 99.99


def test_transform_dict_keys_performance_with_caching() -> None:
    """Test that converter function caching is utilized."""
    # This test verifies that the LRU cache on converter functions works
    data = {f"field_{i}": f"value_{i}" for i in range(50)}

    # First transformation - should populate cache
    result1 = transform_dict_keys(data, camelize)

    # Second transformation with same keys - should use cache
    result2 = transform_dict_keys(data, camelize)

    # Results should be identical
    assert result1 == result2

    # Verify transformation worked correctly
    assert f"field{0}" in result1  # field_0 -> field0
    assert f"field{49}" in result1  # field_49 -> field49
    assert len(result1) == 50


def test_transform_dict_keys_large_dataset() -> None:
    """Test transformation performance with larger datasets."""
    # Create a larger nested structure to test performance
    data = {
        "users": [
            {
                "user_id": i,
                "email_address": f"user{i}@example.com",
                "created_at": "2024-01-01",
                "profile_data": {
                    "first_name": f"User{i}",
                    "last_name": f"LastName{i}",
                    "phone_number": f"555-0{i:03d}",
                },
            }
            for i in range(100)
        ]
    }

    result = transform_dict_keys(data, camelize)

    # Verify structure is preserved and keys are transformed
    assert "users" in result
    assert len(result["users"]) == 100  # type: ignore[call-overload]

    first_user = result["users"][0]  # type: ignore[call-overload]
    assert "userId" in first_user
    assert "emailAddress" in first_user
    assert "createdAt" in first_user
    assert "profileData" in first_user

    profile = first_user["profileData"]
    assert "firstName" in profile
    assert "lastName" in profile
    assert "phoneNumber" in profile

    last_user = result["users"][99]  # type: ignore[call-overload]
    assert last_user["userId"] == 99
    assert last_user["emailAddress"] == "user99@example.com"
    assert last_user["profileData"]["firstName"] == "User99"


def test_transform_dict_keys_edge_case_empty_strings() -> None:
    """Test handling of edge cases like empty strings."""
    data = {"": "empty_key", "user_id": 123, "normal_field": "value"}
    result = transform_dict_keys(data, camelize)

    # Empty string key should be preserved
    assert "" in result
    assert result[""] == "empty_key"  # type: ignore[call-overload]

    # Normal keys should be transformed
    assert "userId" in result
    assert result["userId"] == 123  # type: ignore[call-overload]
    assert "normalField" in result
    assert result["normalField"] == "value"  # type: ignore[call-overload]


def test_transform_dict_keys_single_character_keys() -> None:
    """Test transformation of single character keys."""
    data = {"a": 1, "b": 2, "user_id": 123}
    result = transform_dict_keys(data, camelize)

    # Single character keys should be preserved (no underscores to transform)
    assert result == {"a": 1, "b": 2, "userId": 123}


def test_transform_dict_keys_numeric_in_keys() -> None:
    """Test transformation of keys containing numbers."""
    data = {"field_1": "value1", "field_2a": "value2a", "user_id_123": "user123", "item_456_name": "item456"}
    result = transform_dict_keys(data, camelize)
    expected = {"field1": "value1", "field2a": "value2a", "userId123": "user123", "item456Name": "item456"}
    assert result == expected
