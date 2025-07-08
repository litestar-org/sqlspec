"""Docker container management for benchmarks."""

import socket
import subprocess
import time
from typing import Optional

from rich.console import Console

from tools.benchmark.config import DockerConfig


class ContainerManager:
    """Manages Docker containers for database benchmarks."""

    def __init__(self, console: Optional[Console] = None) -> None:
        self.console = console or Console()
        self.docker_config = DockerConfig()

    def is_docker_running(self) -> bool:
        """Check if Docker daemon is running."""
        try:
            subprocess.run(["docker", "info"], check=True, capture_output=True, text=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def is_container_running(self, container_name: str) -> bool:
        """Check if a specific container is running."""
        try:
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stdout.strip() == "true"
        except subprocess.CalledProcessError:
            return False

    def find_available_port(self, default_port: int) -> int:
        """Find an available port, starting from the default."""
        port = default_port
        max_attempts = 10

        for _ in range(max_attempts):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("", port))
                sock.close()
                return port
            except OSError:
                port += 1

        raise RuntimeError(f"Could not find available port after {max_attempts} attempts")

    def start_postgres(self, keep_container: bool = False) -> tuple[str, int]:
        """Start PostgreSQL container."""
        container_name = self.docker_config.POSTGRES_CONTAINER_NAME

        if self.is_container_running(container_name):
            self.console.print(f"[yellow]Container '{container_name}' already running[/yellow]")
            return "localhost", self.docker_config.POSTGRES_DEFAULT_PORT

        # Find available port
        port = self.find_available_port(self.docker_config.POSTGRES_DEFAULT_PORT)

        # Remove existing container if it exists
        subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True, text=True)

        # Start container
        cmd = [
            "docker",
            "run",
            "--name",
            container_name,
            "-e",
            f"POSTGRES_PASSWORD={self.docker_config.POSTGRES_DEFAULT_PASSWORD}",
            "-e",
            f"POSTGRES_USER={self.docker_config.POSTGRES_DEFAULT_USER}",
            "-e",
            f"POSTGRES_DB={self.docker_config.POSTGRES_DEFAULT_DB}",
            "-p",
            f"{port}:5432",
        ]

        if not keep_container:
            cmd.append("--rm")

        cmd.extend(["-d", self.docker_config.POSTGRES_IMAGE])

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.console.print(f"[green]Started PostgreSQL container on port {port}[/green]")

            # Wait for container to be ready
            self._wait_for_postgres("localhost", port)

            return "localhost", port
        except subprocess.CalledProcessError as e:
            self.console.print(f"[red]Failed to start PostgreSQL: {e}[/red]")
            raise

    def start_oracle(self, keep_container: bool = False) -> tuple[str, int]:
        """Start Oracle container."""
        container_name = self.docker_config.ORACLE_CONTAINER_NAME

        if self.is_container_running(container_name):
            self.console.print(f"[yellow]Container '{container_name}' already running[/yellow]")
            return "localhost", self.docker_config.ORACLE_DEFAULT_PORT

        # Find available port
        port = self.find_available_port(self.docker_config.ORACLE_DEFAULT_PORT)

        # Remove existing container if it exists
        subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True, text=True)

        # Start container
        cmd = [
            "docker",
            "run",
            "--name",
            container_name,
            "-e",
            f"ORACLE_PASSWORD={self.docker_config.ORACLE_DEFAULT_PASSWORD}",
            "-p",
            f"{port}:1521",
        ]

        if not keep_container:
            cmd.append("--rm")

        cmd.extend(["-d", self.docker_config.ORACLE_IMAGE])

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.console.print(f"[green]Started Oracle container on port {port}[/green]")

            # Oracle takes longer to start
            self._wait_for_oracle("localhost", port)

            return "localhost", port
        except subprocess.CalledProcessError as e:
            self.console.print(f"[red]Failed to start Oracle: {e}[/red]")
            raise

    def stop_container(self, container_name: str) -> None:
        """Stop and remove a container."""
        try:
            subprocess.run(["docker", "stop", container_name], check=True, capture_output=True, text=True)
            subprocess.run(["docker", "rm", container_name], check=True, capture_output=True, text=True)
            self.console.print(f"[green]Stopped container '{container_name}'[/green]")
        except subprocess.CalledProcessError:
            pass  # Container might not exist

    def cleanup_containers(self) -> None:
        """Clean up all benchmark containers."""
        containers = [
            self.docker_config.POSTGRES_CONTAINER_NAME,
            self.docker_config.ORACLE_CONTAINER_NAME,
            self.docker_config.MYSQL_CONTAINER_NAME,
        ]

        for container in containers:
            self.stop_container(container)

    def _wait_for_postgres(self, host: str, port: int, timeout: int = 30) -> bool:
        """Wait for PostgreSQL to be ready."""
        import psycopg

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                conn_str = (
                    f"host={host} port={port} "
                    f"user={self.docker_config.POSTGRES_DEFAULT_USER} "
                    f"password={self.docker_config.POSTGRES_DEFAULT_PASSWORD} "
                    f"dbname={self.docker_config.POSTGRES_DEFAULT_DB}"
                )
                with psycopg.connect(conn_str) as conn, conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
            except Exception:
                time.sleep(1)

        return False

    def _wait_for_oracle(self, host: str, port: int, timeout: int = 60) -> bool:
        """Wait for Oracle to be ready."""
        # Oracle takes much longer to start
        self.console.print("[yellow]Waiting for Oracle to start (this may take a minute)...[/yellow]")

        start_time = time.time()

        while time.time() - start_time < timeout:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((host, port))
                sock.close()

                # Additional wait for Oracle to fully initialize
                time.sleep(5)
                return True
            except OSError:
                time.sleep(2)

        return False
