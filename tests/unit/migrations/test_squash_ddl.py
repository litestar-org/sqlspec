"""Execution-based tests for SQLGlot DDL folding in sqlspec.migrations.squash.

Each equivalence test applies the source migrations in order to a fresh in-memory database,
applies the folded ``up`` statements to a second fresh database, compares normalized catalog
snapshots and table contents, then applies the folded ``down`` statements and checks that the
database is back to its pre-range state.
"""

import sqlite3
from collections.abc import Sequence
from pathlib import Path

import duckdb
import pytest

from sqlspec.exceptions import SquashValidationError
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.squash import DDLStateFolder, MigrationSquasher, fold_migration_ddl


def test_fold_columns_add_drop_rename_matches_sequential_sqlite() -> None:
    """ADD, DROP, and RENAME COLUMN fold into one CREATE TABLE that matches sequential application."""
    up_sql, down_sql = _assert_sqlite_equivalent([
        "CREATE TABLE users (id INTEGER PRIMARY KEY, temp_col TEXT, bio TEXT);",
        "ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT '';",
        "ALTER TABLE users DROP COLUMN temp_col;",
        "ALTER TABLE users RENAME COLUMN email TO primary_email;",
    ])

    assert up_sql == ["CREATE TABLE users (id INTEGER PRIMARY KEY, bio TEXT, primary_email TEXT NOT NULL DEFAULT '');"]
    assert down_sql == ["DROP TABLE IF EXISTS users;"]


def test_fold_rename_table_with_foreign_key_child_matches_sequential_sqlite() -> None:
    """Renaming a parent table and its key column rewrites child foreign key references."""
    up_sql, _ = _assert_sqlite_equivalent([
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT);",
        "CREATE TABLE invoices (id INTEGER PRIMARY KEY, account_id INTEGER REFERENCES accounts (id));",
        "ALTER TABLE accounts RENAME TO customers;",
        "ALTER TABLE customers RENAME COLUMN id TO customer_id;",
    ])

    assert up_sql[1] == (
        "CREATE TABLE invoices (id INTEGER PRIMARY KEY, account_id INTEGER REFERENCES customers (customer_id));"
    )


def test_fold_indexes_follow_column_and_table_renames_sqlite() -> None:
    """Indexes are re-targeted when their table or indexed column is renamed."""
    up_sql, _ = _assert_sqlite_equivalent([
        "CREATE TABLE profiles (id INTEGER PRIMARY KEY, username TEXT NOT NULL, obsolete TEXT);",
        "CREATE UNIQUE INDEX idx_profiles_username ON profiles (username);",
        "ALTER TABLE profiles RENAME COLUMN username TO handle;",
        "ALTER TABLE profiles DROP COLUMN obsolete;",
        "ALTER TABLE profiles RENAME TO user_profiles;",
    ])

    assert up_sql == [
        "CREATE TABLE user_profiles (id INTEGER PRIMARY KEY, handle TEXT NOT NULL);",
        "CREATE UNIQUE INDEX idx_profiles_username ON user_profiles(handle);",
    ]


def test_fold_views_depending_on_views_matches_sequential_sqlite() -> None:
    """Views are emitted after the relations they read and dropped before them."""
    up_sql, down_sql = _assert_sqlite_equivalent(
        [
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL, status TEXT);",
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open';",
            "CREATE VIEW open_order_totals AS SELECT SUM(total) AS total FROM open_orders;",
            "CREATE INDEX idx_orders_status ON orders (status);",
        ],
        base=["CREATE TABLE audit (id INTEGER PRIMARY KEY);"],
    )

    assert up_sql[1:3] == [
        "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open';",
        "CREATE VIEW open_order_totals AS SELECT SUM(total) AS total FROM open_orders;",
    ]
    assert down_sql == [
        "DROP VIEW IF EXISTS open_order_totals;",
        "DROP VIEW IF EXISTS open_orders;",
        "DROP TABLE IF EXISTS orders;",
    ]


def test_fold_drop_and_recreate_table_matches_sequential_sqlite() -> None:
    """A table dropped and recreated inside the range folds to its final definition only."""
    up_sql, down_sql = _assert_sqlite_equivalent([
        "CREATE TABLE staging (id INTEGER, payload TEXT);",
        "CREATE INDEX idx_staging_id ON staging (id);",
        "DROP TABLE staging;",
        "CREATE TABLE staging (id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
    ])

    assert up_sql == ["CREATE TABLE staging (id INTEGER PRIMARY KEY, body TEXT NOT NULL);"]
    assert down_sql == ["DROP TABLE IF EXISTS staging;"]


def test_fold_preserves_dml_on_final_shape_tables_sqlite() -> None:
    """DML against tables whose shape is final is kept verbatim, in order, after all CREATEs."""
    up_sql, _ = _assert_sqlite_equivalent([
        "CREATE TABLE roles (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        "ALTER TABLE roles ADD COLUMN description TEXT;",
        "INSERT INTO roles VALUES (1, 'admin', 'Administrators');\nINSERT INTO roles (id, name) VALUES (2, 'user');",
        "CREATE TABLE grants (role_id INTEGER REFERENCES roles (id), scope TEXT);",
        "UPDATE roles SET description = 'Everyone' WHERE id = 2;",
        "INSERT INTO grants (role_id, scope) SELECT id, 'read' FROM roles;",
    ])

    assert up_sql[2:] == [
        "INSERT INTO roles VALUES (1, 'admin', 'Administrators');",
        "INSERT INTO roles (id, name) VALUES (2, 'user');",
        "UPDATE roles SET description = 'Everyone' WHERE id = 2;",
        "INSERT INTO grants (role_id, scope) SELECT id, 'read' FROM roles;",
    ]


def test_fold_drop_table_cascade_removes_child_foreign_keys_and_views_postgres() -> None:
    """DROP TABLE ... CASCADE strips referencing foreign keys and drops dependent views."""
    up_sql, down_sql = fold_migration_ddl(
        [
            "CREATE TABLE parents (id INT PRIMARY KEY);",
            (
                "CREATE TABLE children (id INT, parent_id INT REFERENCES parents (id), "
                "CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parents (id));"
            ),
            "CREATE VIEW parent_ids AS SELECT id FROM parents;",
            "DROP TABLE parents CASCADE;",
        ],
        dialect="postgres",
    )

    assert up_sql == ["CREATE TABLE children (id INT, parent_id INT);"]
    assert down_sql == ["DROP TABLE IF EXISTS children;"]


def test_fold_drop_column_removes_multi_column_constraints_and_indexes_postgres() -> None:
    """Dropping a column removes every constraint and index that references it."""
    up_sql, _ = fold_migration_ddl(
        [
            (
                "CREATE TABLE memberships (org_id INT, user_id INT, role TEXT, "
                "CONSTRAINT uq_member UNIQUE (org_id, user_id), CHECK (role <> '' OR org_id > 0));"
            ),
            "CREATE INDEX idx_member_org_role ON memberships (org_id, role);",
            "CREATE INDEX idx_member_role ON memberships (role);",
            "ALTER TABLE memberships DROP COLUMN org_id;",
        ],
        dialect="postgres",
    )

    assert up_sql == [
        "CREATE TABLE memberships (user_id INT, role TEXT);",
        "CREATE INDEX idx_member_role ON memberships (role);",
    ]


def test_fold_mysql_modify_and_change_column() -> None:
    """MySQL MODIFY and CHANGE COLUMN replace the folded column definition."""
    up_sql, _ = fold_migration_ddl(
        [
            "CREATE TABLE items (id INT PRIMARY KEY, qty INT, label VARCHAR(10));",
            "CREATE INDEX idx_items_label ON items (label);",
            "ALTER TABLE items MODIFY COLUMN qty BIGINT NOT NULL;",
            "ALTER TABLE items CHANGE COLUMN label title VARCHAR(80);",
        ],
        dialect="mysql",
    )

    assert up_sql == [
        "CREATE TABLE items (id INT PRIMARY KEY, qty BIGINT NOT NULL, title VARCHAR(80));",
        "CREATE INDEX idx_items_label ON items(title);",
    ]


def test_fold_varchar_columns_keep_declared_types_sqlite() -> None:
    """VARCHAR(n) columns fold through ADD COLUMN and CREATE INDEX and keep their declared type text."""
    up_sql, _ = _assert_sqlite_equivalent([
        "CREATE TABLE contacts (id INTEGER PRIMARY KEY, name VARCHAR(80) NOT NULL);",
        "ALTER TABLE contacts ADD COLUMN email VARCHAR(255);",
        "CREATE INDEX idx_contacts_email ON contacts (email);",
    ])

    assert up_sql == [
        "CREATE TABLE contacts (id INTEGER PRIMARY KEY, name VARCHAR(80) NOT NULL, email VARCHAR(255));",
        "CREATE INDEX idx_contacts_email ON contacts (email);",
    ]


def test_fold_varchar_columns_keep_declared_types_duckdb() -> None:
    """DuckDB VARCHAR(n) and multi-word types fold and match sequential application."""
    up_sql, _ = _assert_duckdb_equivalent([
        "CREATE TABLE contacts (id INTEGER PRIMARY KEY, name VARCHAR(80) NOT NULL, seen TIMESTAMP WITH TIME ZONE);",
        "ALTER TABLE contacts ADD COLUMN email VARCHAR(255);",
        "CREATE INDEX idx_contacts_email ON contacts (email);",
    ])

    assert up_sql[0] == (
        "CREATE TABLE contacts (id INTEGER PRIMARY KEY, name VARCHAR(80) NOT NULL, "
        "seen TIMESTAMP WITH TIME ZONE, email VARCHAR(255));"
    )


def test_fold_keeps_sequence_used_by_column_default_duckdb() -> None:
    """A sequence referenced by a column default is created before the table and dropped after it."""
    up_sql, down_sql = _assert_duckdb_equivalent([
        "CREATE SEQUENCE order_ids START 10;",
        "CREATE TABLE orders (id INTEGER DEFAULT nextval('order_ids'), note VARCHAR);",
        "ALTER TABLE orders ADD COLUMN total DOUBLE;",
        "INSERT INTO orders (note, total) VALUES ('first', 1.5);",
    ])

    assert up_sql[:2] == [
        "CREATE SEQUENCE order_ids START 10;",
        "CREATE TABLE orders (id INTEGER DEFAULT NEXTVAL('order_ids'), note VARCHAR, total DOUBLE);",
    ]
    assert down_sql == ["DROP TABLE IF EXISTS orders;", "DROP SEQUENCE IF EXISTS order_ids;"]


def test_fold_rewrites_postgres_types_with_original_spelling() -> None:
    """Rewritten Postgres tables keep multi-word and array type spellings exactly as written."""
    up_sql, _ = fold_migration_ddl(
        [
            "CREATE TABLE events (id int, name character varying(20), at timestamp with time zone, tags text[]);",
            "ALTER TABLE events ADD COLUMN amount numeric(10, 2);",
        ],
        dialect="postgres",
    )

    assert up_sql == [
        (
            "CREATE TABLE events (id int, name character varying(20), at timestamp with time zone, tags text[], "
            "amount numeric(10, 2));"
        )
    ]


def test_fold_mysql_drop_primary_key_keeps_columns_not_null() -> None:
    """MySQL keeps former primary key columns NOT NULL after DROP PRIMARY KEY."""
    up_sql, _ = fold_migration_ddl(
        [
            "CREATE TABLE pairs (a INT, b INT, label VARCHAR(10), PRIMARY KEY (a, b));",
            "CREATE TABLE singles (id INT PRIMARY KEY, label VARCHAR(10));",
            "ALTER TABLE pairs DROP PRIMARY KEY;",
            "ALTER TABLE singles DROP PRIMARY KEY;",
        ],
        dialect="mysql",
    )

    assert up_sql == [
        "CREATE TABLE pairs (a INT NOT NULL, b INT NOT NULL, label VARCHAR(10));",
        "CREATE TABLE singles (id INT NOT NULL, label VARCHAR(10));",
    ]


def test_fold_respects_quoted_identifier_case_postgres() -> None:
    """Quoted mixed-case names are distinct objects from unquoted names."""
    up_sql, down_sql = fold_migration_ddl(
        [
            'CREATE TABLE "Users" (id INT);',
            "CREATE TABLE users (id INT);",
            'ALTER TABLE "Users" ADD COLUMN email TEXT;',
        ],
        dialect="postgres",
    )

    assert up_sql == ['CREATE TABLE "Users" (id INT, email TEXT);', "CREATE TABLE users (id INT);"]
    assert down_sql == ["DROP TABLE IF EXISTS users;", 'DROP TABLE IF EXISTS "Users";']


def test_fold_keeps_unnamed_indexes_and_drops_materialized_views_postgres() -> None:
    """Unnamed indexes survive folding and materialized views drop as materialized views."""
    up_sql, down_sql = fold_migration_ddl(
        [
            "CREATE TABLE events (id INT, kind TEXT);",
            "CREATE INDEX ON events (kind);",
            "CREATE MATERIALIZED VIEW event_kinds AS SELECT kind, COUNT(*) AS n FROM events GROUP BY kind;",
            "CREATE INDEX idx_event_kinds ON event_kinds (kind);",
        ],
        dialect="postgres",
    )

    assert up_sql == [
        "CREATE TABLE events (id INT, kind TEXT);",
        "CREATE MATERIALIZED VIEW event_kinds AS SELECT kind, COUNT(*) AS n FROM events GROUP BY kind;",
        "CREATE INDEX ON events (kind);",
        "CREATE INDEX idx_event_kinds ON event_kinds (kind);",
    ]
    assert down_sql == ["DROP MATERIALIZED VIEW IF EXISTS event_kinds;", "DROP TABLE IF EXISTS events;"]


def test_fold_duplicate_create_if_not_exists_keeps_first_definition_postgres() -> None:
    """A later CREATE TABLE IF NOT EXISTS for a table created in range is a no-op."""
    up_sql, _ = fold_migration_ddl(
        ["CREATE TABLE notes (id INT);", "CREATE TABLE IF NOT EXISTS notes (id INT, body TEXT);"], dialect="postgres"
    )

    assert up_sql == ["CREATE TABLE notes (id INT);"]


def test_fold_rename_column_and_drop_column_matches_sequential_duckdb() -> None:
    """Column renames and drops fold to the same DuckDB catalog as sequential application."""
    _assert_duckdb_equivalent([
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, first_name VARCHAR, legacy VARCHAR);",
        "ALTER TABLE customers RENAME COLUMN first_name TO given_name;",
        "ALTER TABLE customers DROP COLUMN legacy;",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS email VARCHAR;",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS email VARCHAR;",
        "INSERT INTO customers VALUES (1, 'Ada', 'ada@example.com');",
    ])


def test_fold_rename_table_matches_sequential_duckdb() -> None:
    """Table renames fold to the same DuckDB catalog as sequential application."""
    up_sql, down_sql = _assert_duckdb_equivalent(
        [
            "CREATE TABLE draft_items (id INTEGER, label VARCHAR);",
            "ALTER TABLE draft_items RENAME TO items;",
            "ALTER TABLE items ALTER COLUMN label SET DATA TYPE TEXT;",
            "ALTER TABLE items ALTER COLUMN label SET NOT NULL;",
        ],
        base=["CREATE TABLE settings (key VARCHAR PRIMARY KEY);"],
    )

    assert up_sql == ["CREATE TABLE items (id INTEGER, label TEXT NOT NULL);"]
    assert down_sql == ["DROP TABLE IF EXISTS items;"]


def test_fold_replaced_view_is_ordered_after_its_new_dependency_duckdb() -> None:
    """A view replaced to read a later view is emitted after that view."""
    up_sql, down_sql = _assert_duckdb_equivalent([
        "CREATE TABLE metrics (id INTEGER, value DOUBLE);",
        "CREATE VIEW summary AS SELECT id FROM metrics;",
        "CREATE VIEW positive_metrics AS SELECT id, value FROM metrics WHERE value > 0;",
        "CREATE OR REPLACE VIEW summary AS SELECT id FROM positive_metrics;",
    ])

    assert up_sql == [
        "CREATE TABLE metrics (id INTEGER, value DOUBLE);",
        "CREATE VIEW positive_metrics AS SELECT id, value FROM metrics WHERE value > 0;",
        "CREATE OR REPLACE VIEW summary AS SELECT id FROM positive_metrics;",
    ]
    assert down_sql == [
        "DROP VIEW IF EXISTS summary;",
        "DROP VIEW IF EXISTS positive_metrics;",
        "DROP TABLE IF EXISTS metrics;",
    ]


_REFUSALS: list[tuple[str, str, list[str], str]] = [
    (
        "insert_before_column_rename",
        "sqlite",
        [
            "CREATE TABLE users (name TEXT);",
            "INSERT INTO users (name) VALUES ('a');",
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        ],
        "after DML in migration 0002",
    ),
    (
        "insert_before_table_rename",
        "sqlite",
        ["CREATE TABLE users (name TEXT);", "INSERT INTO users VALUES ('a');", "ALTER TABLE users RENAME TO people;"],
        "after DML in migration 0002",
    ),
    (
        "positional_insert_before_add_column",
        "sqlite",
        ["CREATE TABLE users (name TEXT);", "INSERT INTO users VALUES ('a');", "ALTER TABLE users ADD COLUMN age INT;"],
        "after DML in migration 0002",
    ),
    (
        "insert_before_drop_table",
        "sqlite",
        ["CREATE TABLE staging (id INT);", "INSERT INTO staging VALUES (1);", "DROP TABLE staging;"],
        "after DML in migration 0002",
    ),
    (
        "ctas_after_source_insert",
        "sqlite",
        ["CREATE TABLE src (id INT);", "INSERT INTO src VALUES (1);", "CREATE TABLE snapshot AS SELECT id FROM src;"],
        "CREATE TABLE AS SELECT reads src",
    ),
    (
        "ctas_source_altered_later",
        "sqlite",
        [
            "CREATE TABLE src (id INT, note TEXT);",
            "CREATE TABLE copy AS SELECT * FROM src;",
            "ALTER TABLE src DROP COLUMN note;",
        ],
        "after CREATE TABLE AS SELECT for copy reads it",
    ),
    (
        "insert_select_source_altered_later",
        "sqlite",
        [
            "CREATE TABLE src (id INT, note TEXT);",
            "CREATE TABLE dst (id INT);",
            "INSERT INTO dst SELECT id FROM src;",
            "ALTER TABLE src RENAME COLUMN note TO body;",
        ],
        "after DML in migration 0003",
    ),
    ("alter_out_of_range_table", "sqlite", ["ALTER TABLE existing ADD COLUMN c INT;"], "not a table created within"),
    ("index_on_out_of_range_table", "sqlite", ["CREATE INDEX idx_existing ON existing (id);"], "not created within"),
    ("dml_on_out_of_range_table", "sqlite", ["INSERT INTO existing VALUES (1);"], "not a table created within"),
    ("drop_out_of_range_table", "sqlite", ["DROP TABLE existing;"], "not a table created within"),
    (
        "drop_and_recreate_out_of_range_table",
        "sqlite",
        ["DROP TABLE IF EXISTS existing;", "CREATE TABLE existing (id INT);"],
        "not a table created within",
    ),
    ("create_if_not_exists", "sqlite", ["CREATE TABLE IF NOT EXISTS existing (id INT);"], "IF NOT EXISTS"),
    ("rename_out_of_range_table", "sqlite", ["ALTER TABLE existing RENAME TO renamed;"], "not a table created within"),
    (
        "view_reads_renamed_table",
        "sqlite",
        ["CREATE TABLE t (a INT);", "CREATE VIEW v AS SELECT a FROM t;", "ALTER TABLE t RENAME TO u;"],
        "would have to rewrite view v",
    ),
    (
        "view_reads_renamed_column",
        "sqlite",
        ["CREATE TABLE t (a INT);", "CREATE VIEW v AS SELECT a FROM t;", "ALTER TABLE t RENAME COLUMN a TO b;"],
        "would have to rewrite view v",
    ),
    (
        "add_column_to_star_view_table",
        "sqlite",
        ["CREATE TABLE t (a INT);", "CREATE VIEW v AS SELECT * FROM t;", "ALTER TABLE t ADD COLUMN b INT;"],
        "changes columns that view v selects",
    ),
    (
        "sqlite_trigger",
        "sqlite",
        [
            "CREATE TABLE t (a INT);\nCREATE TABLE log (a INT);",
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.a); END;",
        ],
        "opaque command",
    ),
    (
        "drop_sequence_used_by_default",
        "duckdb",
        [
            "CREATE SEQUENCE order_ids;",
            "CREATE TABLE orders (id INTEGER DEFAULT nextval('order_ids'));",
            "DROP SEQUENCE order_ids CASCADE;",
        ],
        "DROP SEQUENCE order_ids while orders still reference it",
    ),
    (
        "drop_sequence_used_by_regclass_default",
        "postgres",
        [
            "CREATE SEQUENCE order_ids;",
            "CREATE TABLE orders (id INTEGER DEFAULT nextval('public.order_ids'::regclass));",
            "DROP SEQUENCE order_ids;",
        ],
        "DROP SEQUENCE order_ids while orders still reference it",
    ),
    (
        "mysql_drop_foreign_key",
        "mysql",
        [
            "CREATE TABLE p (id INT PRIMARY KEY);",
            "CREATE TABLE c (pid INT, CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES p (id));",
            "ALTER TABLE c DROP FOREIGN KEY fk_parent;",
        ],
        "leaves behind the index MySQL created",
    ),
    (
        "mysql_drop_foreign_key_constraint",
        "mysql",
        [
            "CREATE TABLE p (id INT PRIMARY KEY);",
            "CREATE TABLE c (pid INT, CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES p (id));",
            "ALTER TABLE c DROP CONSTRAINT fk_parent;",
        ],
        "leaves behind the index MySQL created",
    ),
    (
        "drop_named_primary_key_constraint",
        "postgres",
        ["CREATE TABLE t (id INT, CONSTRAINT t_pk PRIMARY KEY (id));", "ALTER TABLE t DROP CONSTRAINT t_pk;"],
        "dropping primary key constraint t_pk",
    ),
    ("create_type", "postgres", ["CREATE TYPE mood AS ENUM ('happy', 'sad');"], "CREATE TYPE cannot be folded"),
    (
        "create_function",
        "postgres",
        ["CREATE FUNCTION one() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;"],
        "CREATE FUNCTION cannot be folded",
    ),
    ("create_extension", "postgres", ["CREATE EXTENSION pgcrypto;"], "opaque command"),
    (
        "unknown_alter_action",
        "postgres",
        ["CREATE TABLE t (a INT);", "ALTER TABLE t ENABLE ROW LEVEL SECURITY;"],
        "opaque command",
    ),
    (
        "drop_primary_key_unsupported_dialect",
        "postgres",
        ["CREATE TABLE t (a INT PRIMARY KEY);", "ALTER TABLE t DROP PRIMARY KEY;"],
        "opaque command",
    ),
    (
        "drop_unknown_constraint",
        "postgres",
        ["CREATE TABLE t (a INT UNIQUE);", "ALTER TABLE t DROP CONSTRAINT t_a_key;"],
        "constraint t_a_key is not defined",
    ),
    (
        "serial_rewritten",
        "postgres",
        ["CREATE TABLE t (id SERIAL PRIMARY KEY);", "ALTER TABLE t ADD COLUMN label TEXT;"],
        "does not reproduce faithfully",
    ),
    (
        "drop_referenced_column",
        "postgres",
        [
            "CREATE TABLE p (id INT PRIMARY KEY, code INT UNIQUE);",
            "CREATE TABLE c (code INT REFERENCES p (code));",
            "ALTER TABLE p DROP COLUMN code;",
        ],
        "referenced by a foreign key on c",
    ),
    (
        "drop_referenced_table_without_cascade",
        "postgres",
        ["CREATE TABLE p (id INT PRIMARY KEY);", "CREATE TABLE c (pid INT REFERENCES p (id));", "DROP TABLE p;"],
        "without CASCADE",
    ),
    (
        "foreign_key_cycle",
        "postgres",
        [
            "CREATE TABLE a (id INT PRIMARY KEY, b_id INT);",
            "CREATE TABLE b (id INT PRIMARY KEY, a_id INT REFERENCES a (id));",
            "ALTER TABLE a ADD CONSTRAINT fk_b FOREIGN KEY (b_id) REFERENCES b (id);",
        ],
        "depend on each other",
    ),
    ("comment_on_out_of_range_table", "postgres", ["COMMENT ON TABLE existing IS 'x';"], "not created within"),
    ("grant", "postgres", ["CREATE TABLE t (a INT);", "GRANT SELECT ON t TO reader;"], "Grant statements"),
]


@pytest.mark.parametrize(("case", "dialect", "migrations", "reason"), _REFUSALS, ids=[case[0] for case in _REFUSALS])
def test_fold_refuses_unfaithful_input(case: str, dialect: str, migrations: list[str], reason: str) -> None:
    """The ddl strategy refuses with an actionable message naming the migration and suggesting concat."""
    with pytest.raises(SquashValidationError) as exc_info:
        _fold(migrations, dialect)

    message = str(exc_info.value)
    assert reason in message
    assert "--strategy concat" in message
    assert f"migration {len(migrations):04d}" in message or "migration 000" in message


@pytest.mark.parametrize(("case", "dialect", "migrations", "reason"), _REFUSALS, ids=[case[0] for case in _REFUSALS])
def test_apply_squash_refusal_leaves_migration_files_untouched(
    tmp_path: Path, case: str, dialect: str, migrations: list[str], reason: str
) -> None:
    """A ddl refusal raised through apply_squash writes and deletes nothing."""
    for position, sql in enumerate(migrations, start=1):
        (tmp_path / f"{position:04d}_step.sql").write_text(
            f"-- name: migrate-{position:04d}-up\n{sql}\n", encoding="utf-8"
        )
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}

    runner = SyncMigrationRunner(tmp_path, context=MigrationContext(dialect=dialect))
    squasher = MigrationSquasher(tmp_path, runner, strategy="ddl")
    plans = squasher.plan_squash("0001", f"{len(migrations):04d}", "release")

    with pytest.raises(SquashValidationError, match=reason.replace("(", r"\(").replace(")", r"\)")):
        squasher.apply_squash(plans)

    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


def test_fold_refusal_names_the_offending_migration() -> None:
    """Refusal messages name the migration version and the offending statement text."""
    with pytest.raises(SquashValidationError) as exc_info:
        fold_migration_ddl(
            ["CREATE TABLE t (a INT);", "ALTER TABLE existing ADD COLUMN c INT;"],
            dialect="sqlite",
            origins=["0007", "0008"],
        )

    assert str(exc_info.value).startswith(
        "Cannot fold migration 0008 statement 'ALTER TABLE existing ADD COLUMN c INT' with the ddl squash strategy"
    )


def test_folder_emits_unmodified_statements_verbatim() -> None:
    """Statements never rewritten by later migrations keep their original text."""
    folder = DDLStateFolder(dialect="postgres")
    folder.feed("create table Orders (id serial primary key, created_at timestamptz default now());", origin="0001")
    folder.feed("create index idx_orders_created on Orders using BRIN (created_at);", origin="0002")

    up_sql, down_sql = folder.to_statements()

    assert up_sql == [
        "create table Orders (id serial primary key, created_at timestamptz default now());",
        "create index idx_orders_created on Orders using BRIN (created_at);",
    ]
    assert down_sql == ["DROP TABLE IF EXISTS Orders;"]


def test_sqlite_add_column_keeps_columns_before_table_constraints() -> None:
    """Added columns are inserted before table-level constraints so SQLite accepts the folded DDL."""
    up_sql, _ = _assert_sqlite_equivalent([
        "CREATE TABLE parents (id INTEGER PRIMARY KEY);",
        (
            "CREATE TABLE items ("
            "id INTEGER NOT NULL, "
            "parent_id INTEGER, "
            "code VARCHAR(20) NOT NULL, "
            "PRIMARY KEY (id, code), "
            "UNIQUE (code), "
            "CHECK (id > 0), "
            "FOREIGN KEY (parent_id) REFERENCES parents (id)"
            ");"
        ),
        "ALTER TABLE items ADD COLUMN note VARCHAR(40) DEFAULT 'ok';",
        "INSERT INTO parents (id) VALUES (1);",
        "INSERT INTO items (id, parent_id, code, note) VALUES (1, 1, 'A', 'first');",
    ])
    assert "note VARCHAR(40) DEFAULT 'ok', PRIMARY KEY (id, code)" in up_sql[1]


def test_fold_refuses_when_dialect_rewrites_cast_type_in_expression() -> None:
    """Non-column CAST types that SQLGlot rewrites to a different affinity are refused on rewrite."""
    with pytest.raises(SquashValidationError, match="does not reproduce faithfully"):
        fold_migration_ddl(
            [
                "CREATE TABLE metrics (id INTEGER PRIMARY KEY, amount DEFAULT (CAST('1' AS DECIMAL(10, 2))));",
                "ALTER TABLE metrics ADD COLUMN label TEXT;",
            ],
            dialect="sqlite",
            origins=["0001", "0002"],
        )


def test_alter_column_set_not_null_replaces_explicit_null_constraint() -> None:
    """ALTER COLUMN ... SET NOT NULL removes any prior explicit NULL constraint."""
    up_sql, _ = _fold(
        [
            "CREATE TABLE accounts (id INT PRIMARY KEY, email VARCHAR(120) NULL);",
            "ALTER TABLE accounts ALTER COLUMN email SET NOT NULL;",
        ],
        "postgres",
    )
    assert up_sql == ["CREATE TABLE accounts (id INT PRIMARY KEY, email VARCHAR(120) NOT NULL);"]


def test_drop_column_refuses_primary_key_columns() -> None:
    """Dropping a column participating in a primary key is refused during DDL folding."""
    with pytest.raises(SquashValidationError, match="dropping primary key column"):
        _fold(
            [
                "CREATE TABLE memberships (tenant_id INT, user_id INT, role TEXT, PRIMARY KEY (tenant_id, user_id));",
                "ALTER TABLE memberships DROP COLUMN user_id;",
            ],
            "postgres",
        )
    with pytest.raises(SquashValidationError, match="dropping primary key column"):
        _fold(
            ["CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);", "ALTER TABLE items DROP COLUMN id;"], "sqlite"
        )


def test_mysql_modify_column_preserves_inline_primary_key_and_unique() -> None:
    """MySQL MODIFY COLUMN preserves existing inline PRIMARY KEY and UNIQUE constraints."""
    up_sql, _ = _fold(
        [
            "CREATE TABLE users (id INT PRIMARY KEY, code VARCHAR(16) UNIQUE);",
            "ALTER TABLE users MODIFY COLUMN id BIGINT NOT NULL;",
            "ALTER TABLE users MODIFY COLUMN code VARCHAR(32) NOT NULL;",
        ],
        "mysql",
    )
    assert up_sql == ["CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL UNIQUE);"]


def test_cross_schema_index_and_sequence_operations_stay_isolated() -> None:
    """Schema-qualified indexes and sequences do not collide across schemas."""
    up_sql, _ = _fold(
        [
            "CREATE SEQUENCE s1.order_seq START 1;",
            "CREATE SEQUENCE s2.order_seq START 10;",
            "CREATE TABLE s1.orders (id INT DEFAULT nextval('s1.order_seq'), code TEXT);",
            "CREATE TABLE s2.orders (id INT, code TEXT);",
            "CREATE INDEX idx_orders_code ON s1.orders (code);",
            "CREATE INDEX idx_orders_code ON s2.orders (code);",
            "DROP INDEX s2.idx_orders_code;",
            "DROP SEQUENCE s2.order_seq;",
        ],
        "postgres",
    )
    joined = "\n".join(up_sql)
    assert "CREATE SEQUENCE s1.order_seq" in joined
    assert "CREATE SEQUENCE s2.order_seq" not in joined
    assert "CREATE INDEX idx_orders_code ON s1.orders" in joined
    assert "ON s2.orders" not in joined


def _fold(migrations: Sequence[str], dialect: str) -> tuple[list[str], list[str]]:
    return fold_migration_ddl(
        migrations, dialect=dialect, origins=[f"{position:04d}" for position in range(1, len(migrations) + 1)]
    )


def _assert_sqlite_equivalent(migrations: Sequence[str], *, base: Sequence[str] = ()) -> tuple[list[str], list[str]]:
    sequential = sqlite3.connect(":memory:")
    for statement in base:
        sequential.executescript(statement)
    pre_range = _sqlite_snapshot(sequential)
    for migration in migrations:
        sequential.executescript(migration)
    expected = _sqlite_snapshot(sequential)

    up_sql, down_sql = _fold(migrations, "sqlite")

    folded = sqlite3.connect(":memory:")
    for statement in base:
        folded.executescript(statement)
    folded.executescript("\n".join(up_sql))
    assert _sqlite_snapshot(folded) == expected
    folded.executescript("\n".join(down_sql))
    assert _sqlite_snapshot(folded) == pre_range
    return up_sql, down_sql


def _sqlite_snapshot(connection: sqlite3.Connection) -> dict[str, object]:
    objects = connection.execute(
        "SELECT type, name, tbl_name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    relations: dict[str, object] = {}
    for object_type, name, _ in objects:
        if object_type not in {"table", "view"}:
            continue
        columns = list(connection.execute(f'PRAGMA table_info("{name}")'))
        foreign_keys = sorted(
            (row[2], row[3], row[4], row[5], row[6]) for row in connection.execute(f'PRAGMA foreign_key_list("{name}")')
        )
        indexes = sorted(
            (row[1], row[2], row[3], tuple(info[2] for info in connection.execute(f'PRAGMA index_info("{row[1]}")')))
            for row in connection.execute(f'PRAGMA index_list("{name}")')
        )
        rows = sorted(connection.execute(f'SELECT * FROM "{name}"').fetchall(), key=repr)
        relations[name] = (columns, foreign_keys, indexes, rows)
    return {"objects": objects, "relations": relations}


def _assert_duckdb_equivalent(migrations: Sequence[str], *, base: Sequence[str] = ()) -> tuple[list[str], list[str]]:
    sequential = duckdb.connect(":memory:")
    for statement in base:
        sequential.execute(statement)
    pre_range = _duckdb_snapshot(sequential)
    for migration in migrations:
        sequential.execute(migration)
    expected = _duckdb_snapshot(sequential)

    up_sql, down_sql = _fold(migrations, "duckdb")

    folded = duckdb.connect(":memory:")
    for statement in base:
        folded.execute(statement)
    for statement in up_sql:
        folded.execute(statement)
    assert _duckdb_snapshot(folded) == expected
    for statement in down_sql:
        folded.execute(statement)
    assert _duckdb_snapshot(folded) == pre_range
    return up_sql, down_sql


def _duckdb_snapshot(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    tables = connection.execute("SELECT table_name FROM duckdb_tables() WHERE NOT internal ORDER BY 1").fetchall()
    views = connection.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1").fetchall()
    sequences = connection.execute(
        "SELECT sequence_name, start_value, increment_by FROM duckdb_sequences() ORDER BY 1"
    ).fetchall()
    columns = connection.execute(
        "SELECT table_name, column_index, column_name, data_type, is_nullable, column_default "
        "FROM duckdb_columns() WHERE NOT internal ORDER BY table_name, column_index"
    ).fetchall()
    constraints = connection.execute(
        "SELECT table_name, constraint_type, constraint_column_names, referenced_table, referenced_column_names "
        "FROM duckdb_constraints() ORDER BY ALL"
    ).fetchall()
    indexes = connection.execute(
        "SELECT index_name, table_name, is_unique, expressions FROM duckdb_indexes() ORDER BY ALL"
    ).fetchall()
    rows = {
        name: sorted(connection.execute(f'SELECT * FROM "{name}"').fetchall(), key=repr)
        for (name,) in [*tables, *views]
    }
    return {
        "tables": tables,
        "views": views,
        "sequences": sequences,
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
        "rows": rows,
    }
