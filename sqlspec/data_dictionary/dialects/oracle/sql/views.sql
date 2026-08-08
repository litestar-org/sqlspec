-- name: by_owner
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
