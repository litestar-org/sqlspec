---
orphan: true
---

# MySQL/MariaDB Adapters Guide

This guide covers `asyncmy`.

## Key Information

-   **Driver:** `asyncmy`
-   **Parameter Style:** `pyformat` (e.g., `%s`)

## Parameter Profile

-   **Registry Key:** `"asyncmy"`
-   **JSON Strategy:** `helper` (uses shared JSON serializers for dict/list/tuple)
-   **Extras:** None (native list expansion remains disabled)

## Driver Features

While MySQL and MariaDB handle JSON natively, `sqlspec` allows you to provide custom JSON serializers for performance or specialized encoding requirements via the `AsyncmyDriverFeatures` TypedDict.

### AsyncmyDriverFeatures TypedDict

```python
class AsyncmyDriverFeatures(TypedDict):
    """Asyncmy driver feature flags.

    MySQL/MariaDB handle JSON natively, but custom serializers can be provided
    for specialized use cases (e.g., orjson for performance, msgspec for type safety).

    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
        Use for performance (orjson) or custom encoding.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
        Use for performance (orjson) or custom decoding.
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
```

### Example: Custom JSON Serializer

You can use a high-performance library like `orjson` for JSON serialization.

```python
import orjson
from sqlspec.adapters.asyncmy import AsyncmyConfig

def orjson_serializer(obj: Any) -> str:
    return orjson.dumps(obj).decode("utf-8")

config = AsyncmyConfig(
    pool_config={"host": "localhost", "user": "user", "password": "password", "db": "testdb"},
    driver_features={
        "json_serializer": orjson_serializer,
    },
)
```

## Best Practices

-   **Character Set:** Always ensure the connection character set is `utf8mb4` to support a full range of Unicode characters, including emojis.
-   **`sql_mode`:** Be aware of the server's `sql_mode`. It can affect how MySQL handles invalid data, dates, and other constraints.

## Common Issues

-   **`PyMySQL.err.OperationalError: (1366, ...)`**: Incorrect string value for a column. This is often due to character set issues. Ensure your connection and tables are using `utf8mb4`.
-   **Authentication Errors:** MySQL 8.0 and later use a different default authentication plugin (`caching_sha2_password`). If you have trouble connecting, you may need to configure the user account to use the older `mysql_native_password` plugin, though this is less secure.
