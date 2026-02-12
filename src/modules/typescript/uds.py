from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import typing

if typing.TYPE_CHECKING:
    import pathlib

MAX_CONNECTIONS: int = 10


@dataclasses.dataclass(slots=True)
class UDS:
    _connection_semaphores: dict[str, asyncio.Semaphore] = dataclasses.field(
        default_factory=dict
    )

    def _get_semaphore(self, module_name: str) -> asyncio.Semaphore:
        return self._connection_semaphores.setdefault(
            module_name, asyncio.Semaphore(MAX_CONNECTIONS)
        )

    async def acquire(
        self,
        module_name: str,
        socket_path: pathlib.Path,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        async with self._get_semaphore(module_name):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(socket_path),
                    timeout=5.0,
                )
            except OSError as e:
                msg = f"Failed to establish UDS connection to {module_name}: {e}"
                raise ConnectionError(msg) from e
            return reader, writer

    async def release(
        self,
        module_name: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        healthy: bool = True,
    ) -> None:
        del module_name, reader, healthy
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


_uds_pool: UDS | None = None


def get_uds_pool() -> UDS:
    global _uds_pool
    if _uds_pool is None:
        _uds_pool = UDS()
    return _uds_pool
