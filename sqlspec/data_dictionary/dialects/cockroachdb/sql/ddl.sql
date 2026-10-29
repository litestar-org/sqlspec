-- name: by_object
-- dialect: cockroachdb
SELECT
    :schema_name::text AS schema_name,
    :object_name::text AS object_name,
    :object_type::text AS object_type,
    NULL::text AS ddl,
    'lossy'::text AS fidelity,
    'SHOW CREATE requires a safely quoted identifier and is not emitted by the bind-only query pack'::text AS warning
WHERE :object_name::text IS NOT NULL;
