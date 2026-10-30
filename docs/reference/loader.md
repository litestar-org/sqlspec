# SQL File Loader

```python
from sqlspec.loader import NamedStatement, SlotDeclaration, SQLFile, SQLFileCacheEntry, SQLFileLoader
```

Load, cache, and compose named SQL queries (`-- name:`), reusable fragments (`-- fragment:` / `/* include: */`), and dynamic slots (`-- slot:` / `/* slot: */`) from local files or remote object storage. See the [SQL Files Guide](../learn/sql-files.md) for syntax and usage examples.

## File Loader & Containers

::: sqlspec.loader.SQLFileLoader
    options:
      heading_level: 3
      members:
        - load_sql
        - add_named_sql
        - add_fragment
        - has_fragment
        - list_fragments
        - get_fragment_text
        - get_query_slots
        - get_query_parameters
        - get_file
        - get_file_for_query
        - list_queries
        - list_files
        - has_query
        - clear_cache
        - clear_file_cache
        - get_query_text
        - get_sql

::: sqlspec.loader.SQLFile
    options:
      heading_level: 3
      members: false

## Statement & Slot Metadata

::: sqlspec.loader.NamedStatement
    options:
      heading_level: 3
      members: false

::: sqlspec.loader.SlotDeclaration
    options:
      heading_level: 3
      members: false

::: sqlspec.loader.SQLFileCacheEntry
    options:
      heading_level: 3
      members: false
