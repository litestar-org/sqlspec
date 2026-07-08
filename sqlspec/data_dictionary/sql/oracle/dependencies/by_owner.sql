-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    name AS object_name,
    type AS object_type,
    referenced_owner,
    referenced_name,
    referenced_type,
    referenced_link_name,
    dependency_type,
    origin_con_id
FROM all_dependencies
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR name = :object_name)
ORDER BY owner, name, referenced_owner, referenced_name, referenced_type;
