from __future__ import annotations

import asyncio
import contextlib
import datetime
import os
import shutil
import typing

import msgpack
import orjson

from foundation.types import ModuleType
from modules.abc import Result, Module
from modules.typescript.uds import get_uds_pool
from modules.typescript.utils import get_uds_path
from shared.constants import (
    GUILD_ID,
    SOCKET_HEADER_SIZE,
    TOKEN,
    SOCKET_MAX_MESSAGE_SIZE,
    SOCKET_DIR,
)
from shared.logger import logger

if typing.TYPE_CHECKING:
    import pathlib

    import hikari


class TypeScriptModule(Module):
    __slots__ = ("_log_task", "_process", "_socket_path")

    def __init__(self, name: str, path: pathlib.Path) -> None:
        super().__init__(name, path)
        self._socket_path: pathlib.Path = get_uds_path(name)
        self._process: asyncio.subprocess.Process | None = None
        self._log_task: asyncio.Task[None] | None = None

    @property
    def module_type(self) -> ModuleType:
        return ModuleType.TYPESCRIPT

    @property
    def is_loaded(self) -> bool:
        return (
            self._is_loaded
            and self._process is not None
            and self._process.returncode is None
        )

    def get_info(self) -> dict[str, typing.Any]:
        pkg_file = self.path / "package.json"
        if not pkg_file.is_file():
            return {
                "name": self.name,
                "type": self.module_type.value,
                "has_package_json": False,
            }
        try:
            content = pkg_file.read_text(encoding="utf-8")
            info = orjson.loads(content)
            info["type"] = self.module_type.value
            info["name"] = self.name
        except (OSError, orjson.JSONDecodeError):
            return {
                "name": self.name,
                "type": self.module_type.value,
                "has_package_json": True,
                "valid": False,
            }
        return info

    def _is_bun_available(self) -> bool:
        return shutil.which("bun") is not None

    def _requires_build(self) -> bool:
        dist_dir = self.path / "dist"
        src_dir = self.path / "src"

        if not dist_dir.exists():
            return True

        if not src_dir.exists():
            return False

        try:
            newest_src = max(
                (f.stat().st_mtime for f in src_dir.rglob("*.ts") if f.is_file()),
                default=0,
            )
        except (OSError, ValueError):
            newest_src = 0

        try:
            newest_dist = max(
                (f.stat().st_mtime for f in dist_dir.rglob("*.js") if f.is_file()),
                default=0,
            )
        except (OSError, ValueError):
            newest_dist = 0

        return newest_src > newest_dist

    async def _install_dependencies(self) -> tuple[bool, str]:
        node_modules_dir = self.path / "node_modules"
        if node_modules_dir.exists():
            return True, "Dependencies already installed"

        if not self._is_bun_available():
            return False, "Bun not available for dependency installation"

        try:
            process = await asyncio.create_subprocess_exec(
                "bun",
                "install",
                cwd=str(self.path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                return False, f"bun install failed: {stderr.decode()}"

            return True, "Installed dependencies"
        except Exception as e:
            return False, f"Failed to install dependencies: {e}"

    async def _build_module(self) -> tuple[bool, str]:
        if not self._is_bun_available():
            return False, "Bun not available for building"

        if not self._requires_build():
            return True, "Built module is current"

        logger.info("Building TypeScript module %s", self.name)

        try:
            process = await asyncio.create_subprocess_exec(
                "bun",
                "run",
                "build",
                cwd=str(self.path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                return False, f"Build failed: {stderr.decode()}"

            logger.info("Built TypeScript module %s", self.name)
            return True, "Built module"
        except Exception as e:
            return False, f"Failed to build module: {e}"

    async def _terminate_process(self, timeout: float = 5.0) -> None:
        if self._process is None or self._process.returncode is not None:
            return
        self._process.terminate()
        try:
            await asyncio.wait_for(self._process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self._process.kill()
            await self._process.wait()

    async def _cleanup_socket(self) -> None:
        if self._socket_path.exists():
            with contextlib.suppress(OSError):
                self._socket_path.unlink()

    async def load(
        self,
        bot: hikari.GatewayBot,
        *,
        is_reload: bool = False,
    ) -> Result:
        if is_reload and self._process:
            await self.unload()
            await asyncio.sleep(0.5)

        pkg_info = self.get_info()
        if not pkg_info.get("valid", True):
            return Result.failure("Invalid package.json")

        success, message = await self._install_dependencies()
        if not success:
            logger.info(f"Failed to install dependencies for {self.name}: {message}")

        success, message = await self._build_module()
        if not success:
            return Result.failure(f"Failed to build module: {message}")

        if not (self.path / "dist" / "index.js").is_file():
            return Result.failure("Failed to build module - missing dist/index.js")

        SOCKET_DIR.mkdir(parents=True, exist_ok=True)
        await self._cleanup_socket()

        env = os.environ.copy()
        env["MODULE_NAME"] = self.name
        env["UDS_PATH"] = str(self._socket_path)
        env["DISCORD_TOKEN"] = TOKEN or ""
        env["GUILD_ID"] = str(GUILD_ID)

        try:
            self._process = await asyncio.create_subprocess_exec(
                "node",
                "dist/index.js",
                cwd=str(self.path),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            for _ in range(50):
                await asyncio.sleep(0.1)
                if self._socket_path.exists():
                    break
                if self._process.returncode is not None:
                    _, stderr = await self._process.communicate()
                    return Result.failure(
                        f"Failed to start module process: {stderr.decode()}"
                    )

            if not self._socket_path.exists():
                await self._terminate_process()
                return Result.failure("Failed to create UDS socket within timeout")

            try:
                reader, writer = await get_uds_pool().acquire(
                    self.name,
                    self._socket_path,
                )
                ping_data: bytes = (
                    msgpack.packb({"method": "ping", "payload": {}}) or b""
                )
                writer.write(
                    len(ping_data).to_bytes(SOCKET_HEADER_SIZE, "big") + ping_data
                )
                await writer.drain()

                size_bytes = await asyncio.wait_for(
                    reader.read(SOCKET_HEADER_SIZE),
                    timeout=2.0,
                )
                if len(size_bytes) == SOCKET_HEADER_SIZE:
                    size = int.from_bytes(size_bytes, "big")
                    response_data = await asyncio.wait_for(
                        reader.read(size),
                        timeout=2.0,
                    )
                    msgpack.unpackb(response_data)

                await get_uds_pool().release(
                    self.name,
                    reader,
                    writer,
                    healthy=True,
                )
            except Exception as e:
                await self._terminate_process()
                await self._cleanup_socket()
                return Result.failure(f"Failed to establish UDS connection: {e}")

            self._set_loaded(True)
            self._log_task = asyncio.create_task(self._monitor_logs())
            return Result.ok(f"Started module with UDS at {self._socket_path}")

        except Exception as e:
            await self._cleanup_socket()
            return Result.failure(f"Failed to start module: {e}")

    async def unload(self) -> Result:
        await self._terminate_process()
        await self._cleanup_socket()
        self._set_loaded(False)
        self._process = None
        return Result.ok(f"Stopped module {self.name}")

    async def call_method(self, method: str, payload: dict) -> dict | None:
        if not self.is_loaded:
            return None

        reader = None
        writer = None
        try:
            reader, writer = await get_uds_pool().acquire(
                self.name,
                self._socket_path,
            )

            request = {
                "method": method,
                "payload": payload,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            request_data: bytes = msgpack.packb(request) or b""

            writer.write(
                len(request_data).to_bytes(SOCKET_HEADER_SIZE, "big") + request_data
            )
            await writer.drain()

            size_bytes = await asyncio.wait_for(
                reader.read(SOCKET_HEADER_SIZE), timeout=30.0
            )
            if len(size_bytes) != SOCKET_HEADER_SIZE:
                msg = "Failed to read response size from socket"
                raise ConnectionError(msg)

            size = int.from_bytes(size_bytes, "big")
            if size > SOCKET_MAX_MESSAGE_SIZE:
                msg = f"Failed to process oversized response: {size} bytes"
                raise ValueError(msg)

            response_data = await asyncio.wait_for(reader.read(size), timeout=30.0)
            response = msgpack.unpackb(response_data)

            await get_uds_pool().release(self.name, reader, writer, healthy=True)
            return response

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("Failed to call method '%s' on module %s", method, self.name)
            if reader is not None and writer is not None:
                await get_uds_pool().release(
                    self.name,
                    reader,
                    writer,
                    healthy=False,
                )
            return None

    async def _monitor_logs(self) -> None:
        if self._process is None:
            return

        stdout = self._process.stdout
        stderr = self._process.stderr
        if stdout is None or stderr is None:
            return

        async def read_stream(stream: asyncio.StreamReader, level: str) -> None:
            try:
                async for line in stream:
                    if not line:
                        break
                    message = line.decode("utf-8", errors="ignore").rstrip()
                    if message:
                        if level == "ERROR":
                            logger.error("[%s] %s", self.name, message)
                        else:
                            logger.info("[%s] %s", self.name, message)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

        try:
            await asyncio.gather(
                read_stream(stdout, "INFO"),
                read_stream(stderr, "ERROR"),
            )
        except asyncio.CancelledError:
            pass

        logger.info("Terminated TypeScript module '%s' process", self.name)
        self._set_loaded(False)
        self._process = None
        await self._cleanup_socket()
