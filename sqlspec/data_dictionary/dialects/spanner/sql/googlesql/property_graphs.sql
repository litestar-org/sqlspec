-- name: list
-- dialect: spanner
SELECT
    PROPERTY_GRAPH_CATALOG AS property_graph_catalog,
    PROPERTY_GRAPH_SCHEMA AS property_graph_schema,
    PROPERTY_GRAPH_NAME AS property_graph_name,
    PROPERTY_GRAPH_DEFINITION AS property_graph_definition
FROM INFORMATION_SCHEMA.PROPERTY_GRAPHS
WHERE (CAST(:schema_name AS STRING) IS NULL OR PROPERTY_GRAPH_SCHEMA = :schema_name)
  AND (CAST(:property_graph_name AS STRING) IS NULL OR PROPERTY_GRAPH_NAME = :property_graph_name)
ORDER BY PROPERTY_GRAPH_SCHEMA, PROPERTY_GRAPH_NAME;
