-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    object_name,
    procedure_name,
    object_id,
    subprogram_id,
    object_type,
    aggregate,
    pipelined,
    impltypeowner,
    impltypename,
    parallel,
    interface,
    deterministic,
    authid
FROM all_procedures
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR object_name = :object_name)
ORDER BY owner, object_name, subprogram_id, procedure_name;
