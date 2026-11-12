-- name: by_schema
-- dialect: snowflake
SELECT
    tc.constraint_catalog AS "catalog_name",
    tc.constraint_schema AS "schema_name",
    tc.table_name AS "table_name",
    tc.constraint_name AS "constraint_name",
    tc.constraint_type AS "constraint_type",
    tc.is_deferrable AS "is_deferrable",
    tc.initially_deferred AS "initially_deferred",
    tc.enforced AS "enforced",
    rc.unique_constraint_schema AS "referenced_schema",
    rc.unique_constraint_name AS "referenced_constraint_name",
    rc.update_rule AS "update_rule",
    rc.delete_rule AS "delete_rule",
    tc.comment AS "comment"
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.referential_constraints rc
    ON tc.constraint_catalog = rc.constraint_catalog
   AND tc.constraint_schema = rc.constraint_schema
   AND tc.constraint_name = rc.constraint_name
WHERE tc.table_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
ORDER BY tc.table_name, tc.constraint_name;

-- name: by_table
-- dialect: snowflake
SELECT
    tc.constraint_catalog AS "catalog_name",
    tc.constraint_schema AS "schema_name",
    tc.table_name AS "table_name",
    tc.constraint_name AS "constraint_name",
    tc.constraint_type AS "constraint_type",
    tc.is_deferrable AS "is_deferrable",
    tc.initially_deferred AS "initially_deferred",
    tc.enforced AS "enforced",
    rc.unique_constraint_schema AS "referenced_schema",
    rc.unique_constraint_name AS "referenced_constraint_name",
    rc.update_rule AS "update_rule",
    rc.delete_rule AS "delete_rule",
    tc.comment AS "comment"
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.referential_constraints rc
    ON tc.constraint_catalog = rc.constraint_catalog
   AND tc.constraint_schema = rc.constraint_schema
   AND tc.constraint_name = rc.constraint_name
WHERE tc.table_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
  AND tc.table_name = :table_name
ORDER BY tc.constraint_name;

-- name: primary_keys_by_schema
-- dialect: snowflake
SHOW PRIMARY KEYS {scope_clause};

-- name: primary_keys_by_table
-- dialect: snowflake
SHOW PRIMARY KEYS {scope_clause};

-- name: unique_keys_by_schema
-- dialect: snowflake
SHOW UNIQUE KEYS {scope_clause};

-- name: unique_keys_by_table
-- dialect: snowflake
SHOW UNIQUE KEYS {scope_clause};
