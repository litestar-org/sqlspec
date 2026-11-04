# SQL File Loader

```python
from sqlspec.loader import SQLFileLoader
```

Load, cache, and compose named SQL queries (`-- name:`), reusable fragments (`-- fragment:` / `/* include: */`), and dynamic slots (`-- slot:` / `/* slot: */`) from local files or remote object storage. See [Fragments and Slots](../learn/sql-files.md#fragments-and-slots) for syntax and a runnable example.

## Composition Rules

### Declarations and Values

| Syntax or value | Behavior |
| --- | --- |
| `-- fragment: name` | Starts a reusable section that ends at the next fragment or named-query header. |
| `/* include: name */` | Expands a fragment, including nested fragment references. |
| `/* slot: name */` | Declares a required fill point unless the named query supplies a default. |
| `-- slot: name = SQL` | Supplies a default in the named query's leading header block. The default extends to the end of the line. |
| `str` slot value | Inserts trusted SQL text verbatim. Bind user data separately. |
| SQLGlot expression | Renders SQL in the named query's dialect. |
| `SQL` slot value | Inserts SQL text and carries named bindings into the returned statement. Positional bindings and attached statement filters are rejected. |

Fragments may contain include and slot markers, but cannot declare dialect, parameter, or slot headers. When a query contains fragments or slots, declared parameters are checked against the completed SQL at `get_sql()` time.

Bound parameter names from a `SQL` slot must not overlap the outer query's placeholders. Two slots may share a named binding only when their values are equal. Slot values do not change the stored query template.

Markers inside single-quoted strings, double-quoted identifiers, dollar-quoted bodies, or other comments are left unchanged. Leading `-- slot:` headers are case-insensitive; avoid that spelling for ordinary prose comments.

### Namespaces and Inspection

Directory namespaces apply to fragments and named queries. Include lookup first tries the literal registered name, then the namespace of the containing query or fragment. Use a qualified name such as `common.team_source` to reference another namespace explicitly.

All referenced fragments must be loaded before retrieving a query. The order in which their files are loaded does not matter.

Use `spec.loader` to access the inspection and registration methods documented below.

### Caching and Errors

`get_sql(name)` without explicit fills reuses its cached `SQL` object. Calls that supply slot values create fresh statements. Adding or loading fragments clears resolved query caches so later retrievals see their current definitions.

| Error | Cause and recovery |
| --- | --- |
| `SQLSlotError` | Fill missing slots, remove unknown names, use named bindings, and resolve conflicting parameter names or values. Apply statement filters after retrieving the completed query. |
| `SQLFragmentNotFoundError` | Load the referenced fragment or correct its name or namespace. |
| `SQLFileParseError` | Correct an include cycle, duplicate fragment, forbidden fragment header, misplaced slot header, or default without a matching marker. It also reports declared-parameter or compilation errors. |
| `TypeError` | Supply a string, SQLGlot expression, or `SQL` object as the slot value. |

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
