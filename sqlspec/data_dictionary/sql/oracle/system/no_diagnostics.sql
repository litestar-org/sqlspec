-- name: no_diagnostics
-- dialect: oracle
SELECT
    0 AS diagnostics_enabled,
    'disabled' AS diagnostics_mode,
    'Oracle diagnostics, AWR/ASH/ADDM, Statspack, and DBA_HIST queries are disabled by default.' AS warning
FROM dual;
