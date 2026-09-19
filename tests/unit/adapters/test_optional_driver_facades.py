"""Adapter imports must not require optional cloud connector or storage extras."""

import subprocess
import sys
import textwrap


def test_adapter_configs_import_without_optional_cloud_extras() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import importlib.abc
                import sys

                class MissingCloudExtras(importlib.abc.MetaPathFinder):
                    def find_spec(self, fullname, path=None, target=None):
                        if fullname.startswith((
                            "google.cloud.sql.connector",
                            "google.cloud.alloydb.connector",
                            "google.cloud.bigquery_storage",
                        )):
                            raise ModuleNotFoundError(fullname)
                        return None

                sys.meta_path.insert(0, MissingCloudExtras())

                from sqlspec.adapters.asyncpg import AsyncpgConfig
                from sqlspec.adapters.bigquery import BigQueryConfig
                from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
                from sqlspec.adapters.pymysql import PyMysqlConfig

                for config in (
                    AsyncpgConfig, BigQueryConfig, PsycopgAsyncConfig,
                    PsycopgSyncConfig, PyMysqlConfig,
                ):
                    config()
                """
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
