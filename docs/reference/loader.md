# SQL File Loader

```python
from sqlspec.core import (
    ParameterDeclaration,
    ParamTypeMatcher,
    matches_param_type,
    register_param_type,
    resolve_param_type,
)
from sqlspec.loader import (
    NamedStatement,
    SlotDeclaration,
    SQLFile,
    SQLFileCacheEntry,
    SQLFileLoader,
)
```

<span id="composition-rules"></span>Load, cache, and compose named SQL queries (`-- name:`), declared parameters (`-- param:`), reusable fragments (`-- fragment:` / `/* include: */`), and dynamic slots (`-- slot:` / `/* slot: */`) from local files or remote object storage. See [SQL Files — Fragments, Slots, and Composition Rules](../learn/sql-files.md#fragments-and-slots) for directive syntax, slot validation rules, and the query composition decision matrix.

## File Loader & Containers

::: sqlspec.loader.SQLFileLoader
    options:
      heading_level: 3
      members:
        - __init__
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
        - set_observability_runtime

::: sqlspec.loader.SQLFile
    options:
      heading_level: 3
      members: false

## Statement, Slot & Parameter Metadata

::: sqlspec.loader.NamedStatement
    options:
      heading_level: 3
      members: false

::: sqlspec.loader.SlotDeclaration
    options:
      heading_level: 3
      members: false

::: sqlspec.core.parameters.ParameterDeclaration
    options:
      heading_level: 3
      members: false

::: sqlspec.loader.SQLFileCacheEntry
    options:
      heading_level: 4
      members: false

::: sqlspec.core.parameters.ParamTypeMatcher
    options:
      heading_level: 4

::: sqlspec.core.parameters.register_param_type
    options:
      heading_level: 4

::: sqlspec.core.parameters.resolve_param_type
    options:
      heading_level: 4

::: sqlspec.core.parameters.matches_param_type
    options:
      heading_level: 4
