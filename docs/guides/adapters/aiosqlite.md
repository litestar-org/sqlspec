---
orphan: true
---

# aiosqlite Adapter Guide

This guide provides specific instructions for the `aiosqlite` adapter.

## Key Information

- **Driver:** `aiosqlite`
- **Parameter Style:** `qmark` (e.g., `?`)

## Parameter Profile

- **Registry Key:** `"aiosqlite"`
- **JSON Strategy:** `helper` (shared serializer handles dict/list/tuple inputs)
- **Extras:** None (profile applies bool→int and ISO datetime coercions automatically)

## Driver Features

The `aiosqlite` adapter's behavior can be customized through the `driver_features` configuration, which is defined by the `AiosqliteDriverFeatures` TypedDict.

### AiosqliteDriverFeatures TypedDict

```python
class AiosqliteDriverFeatures(TypedDict):
    """Aiosqlite driver feature configuration.

    Controls optional type handling and serialization features for SQLite connections.

    enable_custom_adapters: Enable custom type adapters for JSON/UUID/datetime conversion.
        Defaults to True for enhanced Python type support.
        Set to False only if you need pure SQLite behavior without type conversions.
    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
    """

    enable_custom_adapters: NotRequired[bool]
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
```

### Configuration and Defaults

-   **Custom Adapters (`enable_custom_adapters`)**: This feature is **enabled by default** (`True`). It automatically registers custom type adapters to handle common Python types that SQLite doesn't natively support, including JSON, UUIDs, and datetimes.

### Example: Disabling Custom Adapters

If you need to disable the custom type handling to work with raw SQLite types, you can configure it as follows:

```python
from sqlspec.adapters.aiosqlite import AiosqliteConfig

config = AiosqliteConfig(
    pool_config={"database": "app.db"},
    driver_features={
        "enable_custom_adapters": False,  # Disable all custom type handling
    },
)
```

## Best Practices

- **Async Only:** This is an asynchronous driver for SQLite. Use it in `asyncio` applications.
- **Concurrency:** While `aiosqlite` provides async access, SQLite itself has limitations on concurrent writes. For highly concurrent applications, consider a different database like PostgreSQL.

## Common Issues

- **`sqlite3.OperationalError: database is locked`**: Same as the sync `sqlite3` driver, this occurs when multiple writers conflict. Ensure your application design avoids simultaneous writes.
