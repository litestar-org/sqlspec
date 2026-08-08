-- name: by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    ds.name AS data_space_name,
    ds.type_desc AS data_space_type,
    ps.name AS partition_scheme_name,
    pf.name AS partition_function_name,
    pf.type_desc AS partition_function_type,
    CAST(p.partition_number AS INT) AS partition_number,
    p.rows AS row_count
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
LEFT JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
LEFT JOIN sys.partition_schemes AS ps ON ds.data_space_id = ps.data_space_id
LEFT JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, t.name, i.name, p.partition_number;
