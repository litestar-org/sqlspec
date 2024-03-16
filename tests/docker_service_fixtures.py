# SPDX-FileCopyrightText: 2023-present Cody Fincher <codyfincher@google.com>
#
# SPDX-License-Identifier: MIT
from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
import timeit
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import aioodbc
import asyncmy
import asyncpg
import oracledb
import pytest

from tests.helpers import wrap_sync

if TYPE_CHECKING:
    from collections.abc import Awaitable, Generator


async def wait_until_responsive(
    check: Callable[..., Awaitable],
    timeout: float,
    pause: float,
    **kwargs: Any,
) -> None:
    """Wait until a service is responsive.

    Args:
        check: Coroutine, return truthy value when waiting should stop.
        timeout: Maximum seconds to wait.
        pause: Seconds to wait between calls to `check`.
        **kwargs: Given as kwargs to `check`.
    """
    ref = timeit.default_timer()
    now = ref
    while (now - ref) < timeout:  # sourcery skip
        if await check(**kwargs):
            return
        await asyncio.sleep(pause)
        now = timeit.default_timer()

    msg = "Timeout reached while waiting on service!"
    raise RuntimeError(msg)


SKIP_DOCKER_COMPOSE: bool = bool(os.environ.get("SKIP_DOCKER_COMPOSE", False))
USE_LEGACY_DOCKER_COMPOSE: bool = bool(
    os.environ.get("USE_LEGACY_DOCKER_COMPOSE", os.getenv("GITHUB_ACTIONS") != "true")
)


class DockerServiceRegistry:
    def __init__(self, worker_id: str) -> None:
        self._running_services: set[str] = set()
        self.docker_ip = self._get_docker_ip()
        self._base_command = ["docker-compose"] if USE_LEGACY_DOCKER_COMPOSE else ["docker", "compose"]
        self._base_command.extend(
            [
                f"--file={Path(__file__).parent / 'docker-compose.yml'}",
                f"--project-name=dma-{worker_id}",
            ],
        )

    @staticmethod
    def _get_docker_ip() -> str:
        docker_host = os.environ.get("DOCKER_HOST", "").strip()
        if not docker_host or docker_host.startswith("unix://"):
            return "127.0.0.1"

        if match := re.match(r"^tcp://(.+?):\d+$", docker_host):
            return match[1]

        msg = f'Invalid value for DOCKER_HOST: "{docker_host}".'
        raise ValueError(msg)

    def run_command(self, *args: str) -> None:
        command = [*self._base_command, *args]
        subprocess.run(command, check=True, capture_output=True)

    async def start(
        self,
        name: str,
        *,
        check: Callable[..., Any],
        timeout: float = 30,
        pause: float = 0.1,
        **kwargs: Any,
    ) -> None:
        if SKIP_DOCKER_COMPOSE:
            self._running_services.add(name)
        if name not in self._running_services:
            self.run_command("up", "-d", name)
            self._running_services.add(name)

        await wait_until_responsive(
            check=wrap_sync(check),
            timeout=timeout,
            pause=pause,
            host=self.docker_ip,
            **kwargs,
        )

    def stop(self, name: str) -> None:
        pass

    def down(self) -> None:
        if not SKIP_DOCKER_COMPOSE:
            self.run_command("down", "--remove-orphans", "--volumes", "-t", "10")


@pytest.fixture(scope="session")
def docker_services(worker_id: str) -> Generator[DockerServiceRegistry, None, None]:
    if os.getenv("GITHUB_ACTIONS") == "true" and sys.platform != "linux":
        pytest.skip("Docker not available on this platform")

    registry = DockerServiceRegistry(worker_id)
    try:
        yield registry
    finally:
        registry.down()


@pytest.fixture(scope="session")
def docker_ip(docker_services: DockerServiceRegistry) -> str:
    return docker_services.docker_ip


async def mysql8_responsive(host: str) -> bool:
    try:
        conn = await asyncmy.connect(
            host=host,
            port=3360,
            user="app",
            database="db",
            password="super-secret",
        )
        async with conn.cursor() as cursor:
            await cursor.execute("select 1 as is_available")
            resp = await cursor.fetchone()
        return resp[0] == 1
    except asyncmy.errors.OperationalError:
        return False


async def mysql56_responsive(host: str) -> bool:
    try:
        conn = await asyncmy.connect(
            host=host,
            port=3362,
            user="app",
            database="db",
            password="super-secret",
        )
        async with conn.cursor() as cursor:
            await cursor.execute("select 1 as is_available")
            resp = await cursor.fetchone()
        return resp[0] == 1
    except asyncmy.errors.OperationalError:
        return False


async def mysql57_responsive(host: str) -> bool:
    try:
        conn = await asyncmy.connect(
            host=host,
            port=3363,
            user="app",
            database="db",
            password="super-secret",
        )
        async with conn.cursor() as cursor:
            await cursor.execute("select 1 as is_available")
            resp = await cursor.fetchone()
        return resp[0] == 1
    except asyncmy.errors.OperationalError:
        return False


@pytest.fixture()
async def mysql8_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("mysql8", timeout=45, pause=1, check=mysql8_responsive)


@pytest.fixture()
async def mysql57_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("mysql57", timeout=45, pause=1, check=mysql57_responsive)


@pytest.fixture()
async def mysql56_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("mysql56", timeout=45, pause=1, check=mysql56_responsive)


async def postgres16_responsive(host: str) -> bool:
    try:
        conn = await asyncpg.connect(
            host=host,
            port=5427,
            user="postgres",
            database="postgres",
            password="super-secret",
        )
    except Exception:  # noqa: BLE001
        return False

    try:
        db_open = await conn.fetchrow("SELECT 1")
        return bool(db_open is not None and db_open[0] == 1)
    finally:
        await conn.close()


async def postgres15_responsive(host: str) -> bool:
    try:
        conn = await asyncpg.connect(
            host=host,
            port=5426,
            user="postgres",
            database="postgres",
            password="super-secret",
        )
    except Exception:  # noqa: BLE001
        return False

    try:
        db_open = await conn.fetchrow("SELECT 1")
        return bool(db_open is not None and db_open[0] == 1)
    finally:
        await conn.close()


async def postgres14_responsive(host: str) -> bool:
    try:
        conn = await asyncpg.connect(
            host=host,
            port=5425,
            user="postgres",
            database="postgres",
            password="super-secret",
        )
    except Exception:  # noqa: BLE001
        return False

    try:
        db_open = await conn.fetchrow("SELECT 1")
        return bool(db_open is not None and db_open[0] == 1)
    finally:
        await conn.close()


async def postgres13_responsive(host: str) -> bool:
    try:
        conn = await asyncpg.connect(
            host=host,
            port=5424,
            user="postgres",
            database="postgres",
            password="super-secret",
        )
    except Exception:  # noqa: BLE001
        return False

    try:
        db_open = await conn.fetchrow("SELECT 1")
        return bool(db_open is not None and db_open[0] == 1)
    finally:
        await conn.close()


async def postgres12_responsive(host: str) -> bool:
    try:
        conn = await asyncpg.connect(
            host=host,
            port=5423,
            user="postgres",
            database="postgres",
            password="super-secret",
        )
    except Exception:  # noqa: BLE001
        return False

    try:
        db_open = await conn.fetchrow("SELECT 1")
        return bool(db_open is not None and db_open[0] == 1)
    finally:
        await conn.close()


@pytest.fixture()
async def postgres12_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("postgres12", timeout=45, pause=1, check=postgres12_responsive)


@pytest.fixture()
async def postgres13_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("postgres13", timeout=45, pause=1, check=postgres13_responsive)


@pytest.fixture()
async def postgres14_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("postgres14", timeout=45, pause=1, check=postgres14_responsive)


@pytest.fixture()
async def postgres15_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("postgres15", timeout=45, pause=1, check=postgres15_responsive)


@pytest.fixture()
async def postgres16_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("postgres16", timeout=45, pause=1, check=postgres16_responsive)


def oracle23c_responsive(host: str) -> bool:
    try:
        conn = oracledb.connect(
            host=host,
            port=1513,
            user="app",
            service_name="FREEPDB1",
            password="super-secret",
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM dual")
            resp = cursor.fetchone()
        return resp[0] == 1 if resp is not None else False
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
async def oracle23c_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("oracle23c", check=oracle23c_responsive, timeout=120)


def oracle18c_responsive(host: str) -> bool:
    try:
        conn = oracledb.connect(
            host=host,
            port=1512,
            user="app",
            service_name="xepdb1",
            password="super-secret",
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM dual")
            resp = cursor.fetchone()
        return resp[0] == 1 if resp is not None else False
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
async def oracle18c_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("oracle18c", timeout=120, pause=1, check=oracle18c_responsive)


async def mssql_responsive(host: str) -> bool:
    await asyncio.sleep(1)
    try:
        port = 1344
        user = "sa"
        database = "master"
        conn = await aioodbc.connect(
            connstring=f"encrypt=no; TrustServerCertificate=yes; driver={{ODBC Driver 18 for SQL Server}}; server={host},{port}; database={database}; UID={user}; PWD=Super-secret1",
            timeout=2,
        )
        async with conn.cursor() as cursor:
            await cursor.execute("select 1 as is_available")
            resp = cursor.fetchone()
            return resp[0] == 1 if resp is not None else False
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
async def mssql_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("mssql", timeout=60, pause=1, check=mssql_responsive)
