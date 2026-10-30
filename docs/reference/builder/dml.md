# DML Builders

```python
from sqlspec.builder import (
    ConflictBuilder,
    Delete,
    DeleteFromClauseMixin,
    Insert,
    InsertFromSelectMixin,
    InsertIntoClauseMixin,
    InsertValuesMixin,
    Merge,
    ReturningClauseMixin,
    Update,
    UpdateFromClauseMixin,
    UpdateSetClauseMixin,
    UpdateTableClauseMixin,
    Values,
)
```

Fluent builders for `INSERT`, `ON CONFLICT` upserts, `UPDATE`, `DELETE`, `MERGE`, `VALUES`, and `RETURNING` clauses. See the [Query Builder Guide](../../learn/query-builder.md) for usage examples.

## Insert, Update, Delete & Merge

::: sqlspec.builder._insert.Insert
    options:
      heading_level: 3

::: sqlspec.builder._insert.ConflictBuilder
    options:
      heading_level: 3

::: sqlspec.builder._update.Update
    options:
      heading_level: 3

::: sqlspec.builder._delete.Delete
    options:
      heading_level: 3

::: sqlspec.builder._merge.Merge
    options:
      heading_level: 3

::: sqlspec.builder._values.Values
    options:
      heading_level: 3
      members: false

## DML Clause Mixins

::: sqlspec.builder._dml.ReturningClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.InsertIntoClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.InsertValuesMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.InsertFromSelectMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.UpdateTableClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.UpdateSetClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.UpdateFromClauseMixin
    options:
      heading_level: 4
      members: false

::: sqlspec.builder._dml.DeleteFromClauseMixin
    options:
      heading_level: 4
      members: false
