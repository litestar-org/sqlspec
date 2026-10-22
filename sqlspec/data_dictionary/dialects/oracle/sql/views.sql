-- name: by_schema
-- dialect: oracle
SELECT
    owner AS schema_name,
    view_name,
    text_length,
    text,
    type_text_length,
    type_text,
    oid_text_length,
    oid_text,
    view_type_owner,
    view_type,
    superview_name,
    editioning_view,
    read_only,
    container_data,
    bequeath,
    origin_con_id
FROM all_views
WHERE owner = COALESCE(:schema_name, USER)
  AND (:view_name IS NULL OR view_name = :view_name)
ORDER BY owner, view_name;

-- name: json_duality_views
-- dialect: oracle
SELECT
    view_name AS "view_name",
    root_table_name AS "root_table_name",
    root_table_owner AS "root_table_owner",
    allow_insert AS "allow_insert",
    allow_update AS "allow_update",
    allow_delete AS "allow_delete"
FROM user_json_duality_views
ORDER BY view_name;

-- name: json_duality_views_by_schema
-- dialect: oracle
SELECT
    view_name AS "view_name",
    root_table_name AS "root_table_name",
    root_table_owner AS "root_table_owner",
    allow_insert AS "allow_insert",
    allow_update AS "allow_update",
    allow_delete AS "allow_delete"
FROM all_json_duality_views
WHERE (:schema_name IS NULL AND view_owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))
   OR (:schema_name IS NOT NULL AND view_owner = :schema_name)
ORDER BY view_name;
