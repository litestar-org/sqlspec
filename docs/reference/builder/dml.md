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

Fluent builders for `INSERT`, `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` upserts, `UPDATE`, `DELETE`, `MERGE`, `VALUES`, and `RETURNING` clauses. See [DML, Upserts, MERGE and DDL Schema Builders](../../learn/query-builder-dml-ddl.md) for usage examples.

## Insert, Update, Delete and Merge

::: sqlspec.builder._insert.Insert
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._insert.ConflictBuilder
    options:
      heading_level: 3
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._update.Update
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._delete.Delete
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._merge.Merge
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._values.Values
    options:
      heading_level: 3
      inherited_members: false
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false
      summary:
        attributes: true

## DML Clause Mixins

::: sqlspec.builder._dml.ReturningClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.InsertIntoClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.InsertValuesMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.InsertFromSelectMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.UpdateTableClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.UpdateSetClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.UpdateFromClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false

::: sqlspec.builder._dml.DeleteFromClauseMixin
    options:
      heading_level: 4
      separate_signature: false
      show_symbol_type_heading: false
      show_docstring_examples: false
