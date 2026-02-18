from __future__ import annotations

import asyncio
import typing

from src.modules.python.module import PythonModule
from src.modules.utils import delete_module, detect_module_type
from src.shared.constants import EXTENSIONS_DIR
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import Color, reply_embed

if typing.TYPE_CHECKING:
    import pathlib

    import hikari

    from src.modules.abc import Module


class Registry:
    __slots__ = ("_lock", "_modules")

    _RELOAD_DELAY_SECONDS: typing.Final[float] = 0.5

    def __init__(self) -> None:
        self._modules: dict[str, Module] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def detect_module_type(module_path: pathlib.Path) -> type[Module] | None:
        detected_type = detect_module_type(module_path)
        if detected_type is None:
            return None
        return PythonModule

    @staticmethod
    def _module_path(module_name: str) -> pathlib.Path:
        return EXTENSIONS_DIR / module_name

    @staticmethod
    async def _notify_load_failure(
        hikari_client: hikari.GatewayBot,
        module: Module,
        result_message: str,
    ) -> None:
        try:
            embed = await reply_embed(
                hikari_client,
                f"{module.module_type.value.title()} Module Load Failed",
                f"Failed to load {module.module_type.value} module `{module.name}`:\n{result_message}",
                Color.ERROR,
            )
            await dm_role_members(embeds=[embed])
        except Exception:
            logger.exception("Failed to notify role members about module failure")

    async def _create_module(
        self,
        module_name: str,
    ) -> Module | None:
        module_disk_path = self._module_path(module_name)
        if not module_disk_path.is_dir():
            return None
        module_class = self.detect_module_type(module_disk_path)
        if module_class is None:
            logger.exception("Failed to determine module type for %s", module_name)
            return None
        return module_class(name=module_name, path=module_disk_path)

    async def _replace_loaded_module(
        self,
        module_name: str,
        module: Module,
    ) -> None:
        async with self._lock:
            self._modules[module_name] = module

    async def _pop_module(self, module_name: str) -> Module | None:
        async with self._lock:
            return self._modules.pop(module_name, None)

    async def _get_module(self, module_name: str) -> Module | None:
        async with self._lock:
            return self._modules.get(module_name)

    async def _iter_module_names(self) -> tuple[str, ...]:
        async with self._lock:
            return tuple(self._modules.keys())

    def get_module(self, module_name: str) -> Module | None:
        return self._modules.get(module_name)

    def is_module_loaded(self, module_name: str) -> bool:
        module = self._modules.get(module_name)
        return module is not None and module.is_loaded

    async def load_module(
        self,
        hikari_client: hikari.GatewayBot,
        module_name: str,
        *,
        is_reload: bool = False,
    ) -> bool:
        module = await self._create_module(module_name)
        if module is None:
            return False

        old_module: Module | None = None
        async with self._lock:
            old_module = self._modules.get(module_name)
            if old_module is not None and not is_reload:
                logger.info("Loaded module %s", module_name)
                return False
            if is_reload and old_module is not None:
                del self._modules[module_name]

        if is_reload and old_module is not None:
            await old_module.unload()
            await asyncio.sleep(self._RELOAD_DELAY_SECONDS)

        result = await module.load(hikari_client, is_reload=is_reload)
        if not result.success:
            logger.exception(
                "Failed to load %s module %s: %s",
                module.module_type.value,
                module_name,
                result.message,
            )
            if not is_reload:
                await self._notify_load_failure(
                    hikari_client,
                    module,
                    result.message,
                )
                if isinstance(module, PythonModule):
                    deleted = await asyncio.to_thread(delete_module, module_name)
                    if not deleted:
                        logger.info("Failed to delete invalid module '%s'", module_name)
            if old_module is not None and is_reload:
                await self._replace_loaded_module(module_name, old_module)
            return False

        await self._replace_loaded_module(module_name, module)
        logger.info("Loaded %s", result.message)
        return True

    async def unload_module(self, module_name: str) -> bool:
        module = await self._pop_module(module_name)
        if module is None:
            return False

        result = await module.unload()
        if not result.success:
            logger.exception(
                "Failed to unload module %s: %s",
                module_name,
                result.message,
            )
            await self._replace_loaded_module(module_name, module)
            return False

        logger.info("Unloaded %s", result.message)
        return True

    async def reload_module(
        self,
        hikari_client: hikari.GatewayBot,
        module_name: str,
    ) -> bool:
        if await self._get_module(module_name) is None:
            logger.info("Failed to reload module %s", module_name)
            return False

        return await self.load_module(hikari_client, module_name, is_reload=True)

    async def call_method(
        self,
        module_name: str,
        method: str,
        payload: dict,
    ) -> dict | None:
        module = await self._get_module(module_name)
        if module is None:
            return None
        return await module.call_method(method, payload)

    async def unload_all(self) -> None:
        modules = await self._iter_module_names()
        for module_name in modules:
            await self.unload_module(module_name)


registry = Registry()
