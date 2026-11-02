-- name: current
-- dialect: oracle
SELECT
    p.*
FROM product_component_version p
WHERE p.product LIKE 'Oracle%'
ORDER BY TO_NUMBER(REGEXP_SUBSTR(p.version, '^[0-9]+')) DESC, p.version DESC
FETCH FIRST 1 ROWS ONLY;

-- name: autonomous_service
-- dialect: oracle
SELECT sys_context('USERENV', 'CLOUD_SERVICE') AS "service" FROM dual;

-- name: compatible
-- dialect: oracle
SELECT value AS compatible
FROM v$parameter
WHERE name = 'compatible';

-- name: apex_release
-- dialect: oracle
SELECT
    version_no AS "version_no",
    api_compatibility AS "api_compatibility"
FROM apex_release;
