---
orphan: true
---

# SQLite Adapters Guide

This guide covers `sqlite3` (sync) and `aiosqlite` (async).

## Key Information

-   **Driver:** `sqlite3` (built-in), `aiosqlite`
-   **Parameter Style:** `qmark` (e.g., `?`)

## Parameter Profile

-   **Registry Keys:** `"sqlite"` (sync), `"aiosqlite"` (async)
-   **JSON Strategy:** `helper` for both drivers (shared serializer handles dict/list/tuple parameters)
-   **Extras:** None (profiles apply ISO formatting for datetime/date and convert Decimal to string)

## Driver Features

The `sqlite` adapter's behavior can be customized through the `driver_features` configuration, which is defined by the `SqliteDriverFeatures` TypedDict.

### SqliteDriverFeatures TypedDict

```python
class SqliteDriverFeatures(TypedDict):
    """SQLite driver feature configuration.

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

-   **Custom Adapters (`enable_custom_adapters`)**: This feature is **enabled by default** (`True`). It automatically registers custom type adapters to handle common Python types that SQLite doesn't natively support, including:
    -   `dict`, `list` -> `TEXT` (as JSON)
    -   `UUID` -> `TEXT`
    -   `datetime` -> `TEXT` (ISO 8601 format)
    -   `date` -> `TEXT` (ISO 8601 format)

### Example: Customizing Type Handling

If you need to disable the custom adapters or provide a high-performance JSON serializer, you can do so in the configuration.

```python
import msgspec
from sqlspec.adapters.sqlite import SqliteConfig

# A faster JSON serializer
def custom_json_serializer(obj: Any) -> str:
    return msgspec.json.encode(obj).decode("utf-8")

config = SqliteConfig(
    pool_config={"database": "app.db"},
    driver_features={
        "enable_custom_adapters": False,  # Disable all custom type handling
        "json_serializer": custom_json_serializer,
    },
)
```

## Best Practices

-   **Use Cases:** Ideal for testing, local development, and embedded applications. Not suitable for high-concurrency production workloads.
-   **In-Memory Databases:** For tests, use `:memory:` for the database name to create a fast, temporary database.
-   **Foreign Keys:** Remember to enable foreign key support with `PRAGMA foreign_keys = ON;` if you need it, as it's off by default.

## Common Issues

-   **`sqlite3.OperationalError: database is locked`**: This occurs when multiple threads/processes try to write to the same database file simultaneously. For testing, use separate database files or in-memory databases for each test process.
