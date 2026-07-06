-- name: by_schema
-- dialect: mssql
SELECT
    OBJECT_SCHEMA_NAME(dep.referencing_id) AS referencing_schema_name,
    OBJECT_NAME(dep.referencing_id) AS referencing_object_name,
    dep.referencing_class_desc,
    dep.referenced_server_name,
    dep.referenced_database_name,
    dep.referenced_schema_name,
    dep.referenced_entity_name,
    dep.referenced_class_desc,
    CAST(dep.is_schema_bound_reference AS BIT) AS is_schema_bound_reference,
    CAST(dep.is_caller_dependent AS BIT) AS is_caller_dependent,
    CAST(dep.is_ambiguous AS BIT) AS is_ambiguous
FROM sys.sql_expression_dependencies AS dep
WHERE (:schema_name IS NULL OR OBJECT_SCHEMA_NAME(dep.referencing_id) = :schema_name)
ORDER BY OBJECT_SCHEMA_NAME(dep.referencing_id), OBJECT_NAME(dep.referencing_id), dep.referenced_entity_name;
