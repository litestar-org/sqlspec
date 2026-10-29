"""Unit tests for SQLGlot AST DDL state folding in sqlspec.migrations.squash."""

from sqlspec.migrations.squash import DDLStateFolder, fold_migration_ddl


def test_fold_create_and_alter_table_columns() -> None:
    """DDLStateFolder should fold ADD, DROP, RENAME, and ALTER COLUMN operations into CREATE TABLE."""
    statements = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, temp_col TEXT, bio VARCHAR(100));",
        "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL;",
        "ALTER TABLE users DROP COLUMN temp_col;",
        "ALTER TABLE users RENAME COLUMN email TO primary_email;",
        "ALTER TABLE users ALTER COLUMN bio SET DATA TYPE TEXT;",
        "ALTER TABLE users ALTER COLUMN bio SET NOT NULL;",
        "ALTER TABLE users ALTER COLUMN bio SET DEFAULT 'n/a';",
    ]

    up_sql, down_sql = fold_migration_ddl(statements, dialect="postgres")

    assert len(up_sql) == 1
    folded = up_sql[0]
    assert "CREATE TABLE" in folded
    assert "users" in folded
    assert "temp_col" not in folded
    assert "primary_email VARCHAR(255) NOT NULL" in folded
    assert "bio TEXT" in folded
    assert "'n/a'" in folded
    assert len(down_sql) == 1
    assert "DROP TABLE IF EXISTS" in down_sql[0]
    assert "users" in down_sql[0]


def test_fold_create_then_drop_table_elides_table_indexes_and_dml() -> None:
    """Dropping a table later in the range should elide its CREATE TABLE, indexes, and DML."""
    statements = [
        "CREATE TABLE staging_import (id INT, payload TEXT);",
        "CREATE INDEX idx_staging_id ON staging_import (id);",
        "INSERT INTO staging_import (id, payload) VALUES (1, 'a');",
        "CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT);",
        "INSERT INTO accounts (id, name) VALUES (1, 'System');",
        "DROP TABLE staging_import;",
    ]

    up_sql, down_sql = fold_migration_ddl(statements, dialect="postgres")

    combined_up = "\n".join(up_sql)
    assert "staging_import" not in combined_up
    assert "idx_staging_id" not in combined_up
    assert "CREATE TABLE accounts" in combined_up
    assert "INSERT INTO accounts" in combined_up
    assert len(down_sql) == 1
    assert "accounts" in down_sql[0]
    assert "staging_import" not in down_sql[0]


def test_fold_rename_table_updates_table_and_attached_indexes() -> None:
    """Renaming a table should re-key the table and update attached index targets."""
    statements = [
        "CREATE TABLE account_items (id INT PRIMARY KEY, code VARCHAR(32));",
        "CREATE INDEX idx_account_items_code ON account_items (code);",
        "ALTER TABLE account_items RENAME TO billing_items;",
    ]

    up_sql, down_sql = fold_migration_ddl(statements, dialect="postgres")

    assert len(up_sql) == 2
    assert "CREATE TABLE billing_items" in up_sql[0]
    assert "account_items" not in up_sql[0]
    assert "ON billing_items" in up_sql[1]
    assert len(down_sql) == 1
    assert "DROP TABLE IF EXISTS billing_items" in down_sql[0]


def test_fold_constraints_add_and_drop() -> None:
    """ADD CONSTRAINT and DROP CONSTRAINT should mutate the table's constraint expressions."""
    statements = [
        "CREATE TABLE invoices (id INT PRIMARY KEY, code VARCHAR(50), amount INT, CONSTRAINT chk_old CHECK (amount > -10));",
        "ALTER TABLE invoices DROP CONSTRAINT chk_old;",
        "ALTER TABLE invoices ADD CONSTRAINT uq_invoices_code UNIQUE (code);",
        "ALTER TABLE invoices ADD CONSTRAINT chk_positive CHECK (amount >= 0);",
    ]

    up_sql, _ = fold_migration_ddl(statements, dialect="postgres")

    assert len(up_sql) == 1
    folded = up_sql[0]
    assert "chk_old" not in folded
    assert "uq_invoices_code" in folded
    assert "chk_positive" in folded


def test_fold_foreign_key_topological_ordering_and_reverse_drop() -> None:
    """Tables should be emitted in topological FK order for UP and reverse order for DOWN."""
    statements = [
        "CREATE TABLE order_lines (id INT PRIMARY KEY, order_id INT REFERENCES orders (id));",
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customers (id));",
        "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100));",
        "CREATE VIEW active_orders AS SELECT id FROM orders;",
    ]

    folder = DDLStateFolder(dialect="postgres")
    for stmt in statements:
        folder.feed(stmt)
    up_sql, down_sql = folder.to_statements()

    assert len(up_sql) == 4
    assert "customers" in up_sql[0]
    assert "orders" in up_sql[1]
    assert "order_lines" in up_sql[2]
    assert "CREATE VIEW active_orders" in up_sql[3]

    assert len(down_sql) == 4
    assert "DROP VIEW IF EXISTS active_orders" in down_sql[0]
    assert "DROP TABLE IF EXISTS order_lines" in down_sql[1]
    assert "DROP TABLE IF EXISTS orders" in down_sql[2]
    assert "DROP TABLE IF EXISTS customers" in down_sql[3]


def test_fold_sequences_schemas_views_and_comments() -> None:
    """Schemas, sequences, views, and comments should fold deterministically."""
    statements = [
        "CREATE SCHEMA billing;",
        "CREATE SEQUENCE billing.invoice_seq;",
        "CREATE TABLE billing.invoices (id BIGINT PRIMARY KEY, total NUMERIC(10, 2));",
        "CREATE VIEW billing.invoice_summary AS SELECT id, total FROM billing.invoices;",
        "CREATE OR REPLACE VIEW billing.invoice_summary AS SELECT id FROM billing.invoices;",
        "COMMENT ON TABLE billing.invoices IS 'Billing invoices';",
    ]

    up_sql, down_sql = fold_migration_ddl(statements, dialect="postgres")

    assert len(up_sql) == 5
    assert up_sql[0].startswith("CREATE SCHEMA")
    assert up_sql[1].startswith("CREATE SEQUENCE")
    assert up_sql[2].startswith("CREATE TABLE")
    assert "SELECT id FROM billing.invoices" in up_sql[3]
    assert "total" not in up_sql[3]
    assert up_sql[4].startswith("COMMENT ON TABLE")

    assert len(down_sql) == 4
    assert down_sql[0].startswith("DROP VIEW IF EXISTS")
    assert down_sql[1].startswith("DROP TABLE IF EXISTS")
    assert down_sql[2].startswith("DROP SEQUENCE IF EXISTS")
    assert down_sql[3].startswith("DROP SCHEMA IF EXISTS")


def test_fold_rename_column_updates_indexes_and_constraints_and_drop_column_drops_index() -> None:
    """RENAME COLUMN should update index/constraint column references and DROP COLUMN should drop single-column indexes."""
    statements = [
        "CREATE TABLE profiles (id INT PRIMARY KEY, username VARCHAR(64) NOT NULL, obsolete VARCHAR(32), CONSTRAINT uq_profiles_username UNIQUE (username));",
        "CREATE INDEX idx_profiles_username ON profiles (username);",
        "CREATE INDEX idx_profiles_obsolete ON profiles (obsolete);",
        "ALTER TABLE profiles RENAME COLUMN username TO handle;",
        "ALTER TABLE profiles ALTER COLUMN handle DROP NOT NULL;",
        "ALTER TABLE profiles DROP COLUMN obsolete;",
        "RENAME TABLE profiles TO user_profiles;",
    ]

    up_sql, down_sql = fold_migration_ddl(statements, dialect="postgres")

    assert len(up_sql) == 2
    assert "CREATE TABLE user_profiles" in up_sql[0]
    assert "handle VARCHAR(64)" in up_sql[0]
    assert "NOT NULL" not in up_sql[0].split("handle VARCHAR(64)")[1].split(",")[0]
    assert "UNIQUE (handle)" in up_sql[0]
    assert "obsolete" not in up_sql[0]
    assert "ON user_profiles(handle)" in up_sql[1] or "ON user_profiles (handle)" in up_sql[1]
    assert len(down_sql) == 1
    assert "DROP TABLE IF EXISTS user_profiles" in down_sql[0]
