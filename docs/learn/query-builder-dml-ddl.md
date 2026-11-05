# DML, Upserts, MERGE and DDL Schema Builders

Construct parameterized `INSERT`, `UPDATE`, `DELETE`, `VALUES`, `COPY`, `MERGE`, upsert, and portable DDL statements with automatic schema-model binding and catalog/Arrow type inference.

## INSERT, UPDATE, DELETE and RETURNING

=== "INSERT"

    ```{ .python title="docs/examples/builder/insert_query.py" }
    --8<-- "docs/examples/builder/insert_query.py:example"
    ```

=== "UPDATE"

    ```{ .python title="docs/examples/builder/update_query.py" }
    --8<-- "docs/examples/builder/update_query.py:example"
    ```

=== "DELETE"

    ```{ .python title="docs/examples/builder/delete_query.py" }
    --8<-- "docs/examples/builder/delete_query.py:example"
    ```

Combine `.values(...)`, `.add_values(...)`, `Insert.from_select(...)`, multi-table `Update.from_(...)` / `Update.join(...)`, `sql.values(...)`, `sql.copy(...)` (`copy_from`, `copy_to`), and `.returning(...)`. The `# SQL:` comments omit identifier quoting and table qualification, as on the [SELECT page](query-builder.md#inspecting-and-executing-built-queries):

```python
from sqlspec import sql

src = sql.select("id", "email").from_("staging_users").where_eq("verified", True)
ins_sel = (
    sql
    .insert("users")
    .columns("id", "email")
    .from_select(src)
    .returning("id", "created_at")
)
# SQL: INSERT INTO users (id, email) SELECT id, email FROM staging_users
# SQL: WHERE verified = :verified RETURNING id, created_at
# Parameters: {'verified': True}

upd_join = (
    sql
    .update("users")
    .set("tier", "vip")
    .from_("staging_users", alias="s")
    .where("users.id = s.id")
)
# SQL: UPDATE users SET tier = :tier FROM staging_users AS s WHERE users.id = s.id
# Parameters: {'tier': 'vip'}

vals = sql.values([("a", 1), ("b", 2)], alias="v", columns=["code", "qty"])
# SQL: (VALUES (:code, :qty), (:code_1, :qty_1)) AS v(code, qty)
# Parameters: {'code': 'a', 'qty': 1, 'code_1': 'b', 'qty_1': 2}

copy_stmt = sql.copy(
    "events", source="/tmp/events.csv", options={"format": "csv", "header": True}
)
# SQL: COPY events FROM '/tmp/events.csv' WITH (FORMAT 'csv', HEADER TRUE)
```

## Schema Model Binding

Bind `msgspec.Struct`, `pydantic.BaseModel`, `dataclass`, `attrs`, or `dict` payloads directly via `Insert.values_from(data)`, `Insert.values_from_many(items)`, and `Update.set_from(data)`. Model binding uses `schema_dump(..., wire_format=False)` so SQL columns always match Python attribute names. `exclude_unset=True` (the default) drops only fields the model reports as unset, such as msgspec `UNSET` values or fields missing from Pydantic's `model_fields_set`; msgspec defaults are still written:

```python
import msgspec

from sqlspec import sql


class UserCreate(msgspec.Struct):
    email: str
    role: str = "member"


batch_ins = sql.insert("users").values_from_many([
    UserCreate(email="ada@example.com", role="admin"),
    UserCreate(email="grace@example.com"),
])
# SQL: INSERT INTO users (email, role)
# SQL: VALUES (:email, :role), (:email_1, :role_1)
# Parameters: {'email': 'ada@example.com', 'role': 'admin',
# Parameters:  'email_1': 'grace@example.com', 'role_1': 'member'}

patch_upd = (
    sql
    .update("users")
    .set_from({"email": "ada@new.io"})
    .where_eq("id", 1)
)
# SQL: UPDATE users SET email = :email WHERE id = :id
# Parameters: {'email': 'ada@new.io', 'id': 1}
```

## Dialect-Aware Upserts and MERGE

```{ .python title="docs/examples/builder/upsert.py" }
--8<-- "docs/examples/builder/upsert.py:example"
```

| Upsert API | Target Dialects | Behavior |
| --- | --- | --- |
| `.on_conflict(*cols).do_update(...)` / `.do_nothing()` | `postgres`, `cockroachdb`, `sqlite`, `duckdb` | Compiles `ON CONFLICT (...) DO UPDATE SET ...` or `DO NOTHING` |
| `.on_duplicate_key_update(**kwargs)` | `mysql`, `mariadb` | Compiles `ON DUPLICATE KEY UPDATE col = :val` |
| `sql.merge(table, dialect=...)` | `postgres` (15+), `oracle`, `bigquery`, `db2`, `tsql` | Full `MERGE INTO` with `WHEN MATCHED` / `NOT MATCHED` clauses; raises `DialectNotSupportedError` on `sqlite` and `mysql` |
| `sql.upsert(table, dialect=...)` | All dialects | Returns `Merge` on `postgres`, `oracle`, `bigquery`, and `db2`, and `Insert` on every other dialect |

When `Merge.using(rows, alias="src")` receives a Python `dict` or `list[dict]`, SQLSpec expands it into a dialect-native row source: `jsonb_to_recordset(CAST(:json_data AS JSONB))` bound to the row list on PostgreSQL, `JSON_TABLE(:json_payload, '$[*]' COLUMNS (...))` bound to a JSON string on Oracle, and typed `SELECT CAST(:id AS BIGINT) ... FROM SYSIBM.SYSDUMMY1` rows on Db2:

```python
from sqlspec import sql

merge_q = (
    sql
    .merge("products", dialect="postgres")
    .using([{"id": 1, "name": "Ada"}], alias="src")
    .on("products.id = src.id")
    .when_matched_then_update(name="src.name")
    .when_not_matched_then_insert(id="src.id", name="src.name")
    .when_not_matched_by_source_then_delete()
)
# SQL: MERGE INTO products USING (SELECT id, name FROM
# SQL: JSONB_TO_RECORDSET(CAST(%(json_data)s AS JSONB))
# SQL: AS src_data(id INT, name TEXT)) AS src(id, name)
# SQL: ON (products.id = src.id) WHEN MATCHED THEN UPDATE ...
# Parameters: {'json_data': [{'id': 1, 'name': 'Ada'}]}
```

## DDL Builders and Schema Inference

```{ .python title="docs/examples/builder/ddl_create_table.py" }
--8<-- "docs/examples/builder/ddl_create_table.py:example"
```

| Factory Method | Builder Class | SQL Statement & Key Methods |
| --- | --- | --- |
| `sql.create_table(name)` | `CreateTable` | `CREATE TABLE`: `.column(..., identity=..., generated_expression=..., persisted=True)`, `.primary_key_constraint`, `.foreign_key_constraint`, `.unique_constraint`, `.check_constraint`, `.partition_by`, `.in_schema`, `.if_not_exists` |
| `sql.create_table_as_select()` | `CreateTableAsSelect` | `CREATE TABLE ... AS SELECT`: `.name(n)`, `.as_select(query)`, `.columns(...)`, `.if_not_exists()` |
| `sql.create_index(name)` | `CreateIndex` | `CREATE INDEX`: `.on_table(t)`, `.columns(...)`, `.include(...)`, `.unique()`, `.where(...)`, `.using(...)`, `.clustered()`, `.if_not_exists()` |
| `sql.create_sequence(name)` | `CreateSequence` | `CREATE SEQUENCE`: `.start_with(n)`, `.increment_by(n)`, `.min_value(n)`, `.max_value(n)`, `.cache(n)`, `.cycle()`, `.no_cycle()` |
| `sql.create_view(name)` / `sql.create_materialized_view(name)` | `CreateView` / `CreateMaterializedView` | `CREATE [MATERIALIZED] VIEW`: `.as_select(query)`, `.columns(...)`, `.if_not_exists()` |
| `sql.create_schema(name)` | `CreateSchema` | `CREATE SCHEMA`: `.if_not_exists()`, `.authorization(role)` |
| `sql.alter_table(name)` | `AlterTable` | `ALTER TABLE`: `.add_column`, `.drop_column`, `.alter_column_type`, `.rename_column`, `.add_constraint`, `.drop_constraint`, `.set_not_null`, `.drop_not_null`, `.set_column_default`, `.drop_column_default` |
| `sql.drop_table(name)` / `sql.drop_index(name)` | `DropTable` / `DropIndex` | `DROP TABLE / INDEX`: `.if_exists()`, `.cascade()`, `.restrict()` |
| `sql.drop_sequence(name)` / `sql.drop_view(name)` / `sql.drop_materialized_view(name)` / `sql.drop_schema(name)` | `DropSequence` / `DropView` / `DropMaterializedView` / `DropSchema` | `DROP SEQUENCE / [MATERIALIZED] VIEW / SCHEMA`: `.if_exists()`, `.cascade()` |
| `sql.rename_table(old)` / `sql.truncate(name)` / `sql.comment_on()` | `RenameTable` / `Truncate` / `CommentOn` | `ALTER TABLE ... RENAME TO` (`.to(new)`), `TRUNCATE` (`.restart_identity()`, `.continue_identity()`, `.cascade()`), `COMMENT ON` (`.on_table(t)`, `.on_column(t, c)`, `.is_(text)`) |

`create_temporal_table()` in `sqlspec.builder` builds system-versioned tables. Synthesize DDL directly from PyArrow schemas (`CreateTable.from_arrow_schema()`), Python dicts or structs (`CreateTable.from_records()`), or `DataDictionary` catalog metadata (`CreateTable.from_metadata()`, `CreateIndex.from_metadata()`, and `compile_table_ddl_results()`).
