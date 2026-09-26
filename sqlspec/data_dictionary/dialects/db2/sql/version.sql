-- name: current
-- dialect: db2
SELECT
    SERVICE_LEVEL AS service_level,
    BLD_LEVEL AS bld_level,
    FIXPACK_NUM AS fixpack_num,
    INST_NAME AS inst_name
FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS T;
