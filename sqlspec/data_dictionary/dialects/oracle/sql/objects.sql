-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    object_name,
    object_type,
    status,
    created,
    last_ddl_time,
    temporary,
    generated,
    secondary,
    editionable,
    oracle_maintained
FROM all_objects
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR object_name = :object_name)
ORDER BY owner, object_type, object_name;
