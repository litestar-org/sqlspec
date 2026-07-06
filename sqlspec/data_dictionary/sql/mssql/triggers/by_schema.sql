-- name: by_schema
-- dialect: mssql
SELECT
    trigger_schema.name AS schema_name,
    tr.name AS trigger_name,
    parent_schema.name AS parent_schema_name,
    parent_object.name AS parent_object_name,
    tr.type_desc,
    sm.definition,
    CAST(tr.is_disabled AS BIT) AS is_disabled,
    CAST(tr.is_instead_of_trigger AS BIT) AS is_instead_of_trigger,
    tr.create_date,
    tr.modify_date
FROM sys.triggers AS tr
LEFT JOIN sys.objects AS parent_object ON tr.parent_id = parent_object.object_id
LEFT JOIN sys.schemas AS parent_schema ON parent_object.schema_id = parent_schema.schema_id
LEFT JOIN sys.schemas AS trigger_schema ON parent_object.schema_id = trigger_schema.schema_id
LEFT JOIN sys.sql_modules AS sm ON tr.object_id = sm.object_id
WHERE (:schema_name IS NULL OR parent_schema.name = :schema_name)
  AND tr.is_ms_shipped = 0
ORDER BY parent_schema.name, tr.name;
