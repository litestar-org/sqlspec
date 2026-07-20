-- name: version
-- dialect: oracle
SELECT
    product AS "product",
    version AS "version",
    status AS "status"
FROM product_component_version
WHERE product LIKE 'Oracle%'
ORDER BY TO_NUMBER(REGEXP_SUBSTR(version, '^[0-9]+')) DESC, version DESC
FETCH FIRST 1 ROWS ONLY;

-- name: autonomous_service
-- dialect: oracle
SELECT sys_context('USERENV', 'CLOUD_SERVICE') AS "service" FROM dual;

-- name: compatible
-- dialect: oracle
SELECT value AS compatible
FROM v$parameter
WHERE name = 'compatible';

-- name: storage_options
-- dialect: oracle
SELECT parameter AS "parameter", value AS "value"
FROM v$option
WHERE parameter IN ('Basic Compression', 'Advanced Compression', 'Partitioning', 'In-Memory Column Store');
