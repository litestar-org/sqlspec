-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    object_name,
    package_name,
    procedure_name,
    argument_name,
    position,
    sequence,
    data_level,
    data_type,
    defaulted,
    default_value,
    in_out,
    data_length,
    data_precision,
    data_scale,
    type_owner,
    type_name,
    type_subname,
    pls_type,
    char_length,
    char_used,
    origin_con_id
FROM all_arguments
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR object_name = :object_name)
ORDER BY owner, object_name, package_name, subprogram_id, sequence;
