from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import threading
import typing

if typing.TYPE_CHECKING:
    import arc
    import hikari
    import miru


@dataclasses.dataclass(slots=True, kw_only=True)
class Container:
    hikari_client: hikari.GatewayBot
    arc_client: arc.GatewayClient
    miru_client: miru.Client
    _shutdown_event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    _shutdown_reason: str | None = None
    _is_initialized: bool = False

    def mark_initialized(self) -> None:
        self._is_initialized = True

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    @property
    def me(self) -> hikari.OwnUser | None:
        return self.hikari_client.get_me()

    async def shutdown(self, reason: str = "Shutdown requested") -> None:
        if self._shutdown_event.is_set():
            return
        self._shutdown_reason = reason
        self._shutdown_event.set()
        if self.hikari_client.is_alive:
            with contextlib.suppress(Exception):
                await self.hikari_client.close()

    def is_shutting_down(self) -> bool:
        return self._shutdown_event.is_set()

    async def wait_shutdown(self) -> None:
        await self._shutdown_event.wait()


_container: Container | None = None
_container_lock = threading.RLock()


def init_app(
    hikari_client: hikari.GatewayBot,
    arc_client: arc.GatewayClient,
    miru_client: miru.Client,
) -> Container:
    global _container
    with _container_lock:
        _container = Container(
            hikari_client=hikari_client,
            arc_client=arc_client,
            miru_client=miru_client,
        )
        _container.mark_initialized()
        return _container


def get_app() -> Container:
    with _container_lock:
        if _container is None:
            msg = "Application container not initialized"
            raise RuntimeError(msg)
        return _container


def get_hikari() -> hikari.GatewayBot:
    return get_app().hikari_client


def get_arc() -> arc.GatewayClient:
    return get_app().arc_client


def get_miru() -> miru.Client:
    return get_app().miru_client
