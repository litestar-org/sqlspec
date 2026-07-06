-- name: by_schema
-- dialect: duckdb
WITH object_inventory AS (
    SELECT
        database_name,
        schema_name,
        table_oid AS object_oid,
        table_name AS object_name,
        'table' AS object_type
    FROM duckdb_tables()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
    UNION ALL
    SELECT
        database_name,
        schema_name,
        index_oid AS object_oid,
        index_name AS object_name,
        'index' AS object_type
    FROM duckdb_indexes()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
    UNION ALL
    SELECT
        database_name,
        schema_name,
        view_oid AS object_oid,
        view_name AS object_name,
        'view' AS object_type
    FROM duckdb_views()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
)
SELECT
    dependent.database_name,
    dependent.schema_name,
    dependent.object_name,
    dependent.object_type,
    referenced.object_name AS referenced_object_name,
    referenced.object_type AS referenced_object_type,
    dep.classid,
    dep.objid,
    dep.objsubid,
    dep.refclassid,
    dep.refobjid,
    dep.refobjsubid,
    dep.deptype
FROM duckdb_dependencies() AS dep
LEFT JOIN object_inventory AS dependent ON dependent.object_oid = dep.objid
LEFT JOIN object_inventory AS referenced ON referenced.object_oid = dep.refobjid
WHERE dependent.object_oid IS NOT NULL
   OR referenced.object_oid IS NOT NULL
ORDER BY dependent.object_type, dependent.object_name, referenced.object_type, referenced.object_name;
