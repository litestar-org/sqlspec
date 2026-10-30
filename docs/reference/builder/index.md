# Query Builders

```python
from sqlspec.builder import (
    AlterTable,
    Column,
    CreateTable,
    Delete,
    DropTable,
    Insert,
    Merge,
    QueryBuilder,
    Select,
    SQLFactory,
    Update,
    sql,
)
```

Fluent, parameterized SQL query builders backed by SQLGlot AST nodes. See the [Query Builder Guide](../../learn/query-builder.md) for usage examples.

## Reference Pages

| Page | Primary Symbols | Description |
| --- | --- | --- |
| [SQL Factory (`sql`)](factory.md) | `sql`, `SQLFactory`, `QueryBuilder`, `BuiltQuery`, `Explain`, `build_copy_statement` | Entry-point `sql` factory instance, base `QueryBuilder`, `EXPLAIN` builder, and `COPY` helpers. |
| [`Select` & Clauses](select.md) | `Select`, `JoinBuilder`, `CaseBuilder`, `WindowFunctionBuilder`, `SubqueryBuilder` | `SELECT`, `JOIN`, CTE (`WITH`), `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `PIVOT/UNPIVOT`, and set operations. |
| [DML (`Insert`, `Update`, `Delete`, `Merge`)](dml.md) | `Insert`, `Update`, `Delete`, `Merge`, `ConflictBuilder`, `Values` | Parameterized `INSERT`, `ON CONFLICT` upserts, `UPDATE`, `DELETE`, `MERGE`, and `RETURNING` clauses. |
| [DDL (`CreateTable`, `AlterTable`, `DropTable`)](ddl.md) | `CreateTable`, `AlterTable`, `DropTable`, `CreateIndex`, `CreateView`, `Truncate` | Schema, table, index, view, materialized view, and temporal table DDL builders. |
| [Columns & Expressions](expressions.md) | `Column`, `ColumnExpression`, `FunctionColumn`, `AggregateExpression`, `VectorDistance` | Typed column descriptors, aggregate/math/string/conversion expression wrappers, and vector distance expressions. |
