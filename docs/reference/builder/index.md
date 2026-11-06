# Query Builders

```python
from sqlspec.builder import (
    AlterTable,
    BuiltQuery,
    Column,
    CreateTable,
    Delete,
    DropTable,
    Explain,
    Insert,
    Merge,
    QueryBuilder,
    Select,
    SQLFactory,
    Update,
    Values,
    VectorDistance,
    sql,
)
```

Fluent, parameterized SQL query builders backed by SQLGlot AST nodes. For hands-on tutorials and compiled SQL examples, see:

- [Query Builder: SELECT, Filtering and Joins](../../learn/query-builder.md)
- [Expressions, Window Functions and Vector Search](../../learn/query-builder-expressions.md)
- [DML, Upserts, MERGE and DDL Schema Builders](../../learn/query-builder-dml-ddl.md)

## Reference Pages

| Page | Primary Symbols | Description |
| --- | --- | --- |
| [SQL Factory and Base Builder](factory.md) | `sql`, `SQLFactory`, `QueryBuilder`, `BuiltQuery`, `ExpressionBuilder`, `Explain`, `ExplainMixin`, `build_copy_statement` | Entry-point `sql` factory, base `QueryBuilder`, `EXPLAIN` plan builders, and `COPY` statement helpers. |
| [Select and Clause Builders](select.md) | `Select`, `JoinBuilder`, `WindowFunctionBuilder`, `CaseBuilder`, `Case`, `SubqueryBuilder`, `WhereClauseMixin`, `JoinClauseMixin` | `SELECT`, `JOIN`, CTE (`WITH`), `WHERE` / `OR WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `PIVOT/UNPIVOT`, and set operations. |
| [DML Builders](dml.md) | `Insert`, `ConflictBuilder`, `Update`, `Delete`, `Merge`, `Values`, `ReturningClauseMixin` | Parameterized `INSERT`, `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` upserts, `UPDATE`, `DELETE`, `MERGE`, `VALUES`, and `RETURNING` clauses. |
| [DDL Builders](ddl.md) | `DDLBuilder`, `CreateTable`, `CreateTableAsSelect`, `AlterTable`, `CreateIndex`, `CreateSequence`, `CreateView`, `CreateMaterializedView`, `CreateSchema`, `DropTable`, `RenameTable`, `Truncate`, `CommentOn` | Schema, table, sequence, index, view, materialized view, and catalog/Arrow/record schema-inference DDL builders. |
| [Columns and Expressions](expressions.md) | `Column`, `ColumnExpression`, `FunctionColumn`, `AggregateExpression`, `VectorDistance`, `build_oracle_hnsw_subquery`, `build_oracle_hybrid_rrf_sql` | Operator-overloaded column descriptors, scalar/aggregate/math/string/conversion wrappers, cross-dialect vector distance, and Oracle 26ai Hybrid RRF helpers. |
