from __future__ import annotations

import asyncio
import pathlib
import typing

from foundation.types import ModuleType
from modules.abc import Module
from modules.python.module import PythonModule
from modules.typescript.module import TypeScriptModule
from modules.utils import delete_module, detect_module_type
from shared.constants import EXTENSIONS_DIR, SOCKET_DIR
from shared.logger import logger
from shared.utils.member import dm_role_members
from shared.utils.view import Color, reply_embed

if typing.TYPE_CHECKING:
    import hikari


class Registry:
    __slots__ = ("_modules",)

    def __init__(self) -> None:
        self._modules: dict[str, Module] = {}

    @staticmethod
    def detect_module_type(module_path: pathlib.Path) -> type[Module] | None:
        detected_type = detect_module_type(module_path)
        if detected_type is None:
            return None
        return (
            TypeScriptModule if detected_type is ModuleType.TYPESCRIPT else PythonModule
        )

    def get_module(self, module_name: str) -> Module | None:
        return self._modules.get(module_name)

    def is_module_loaded(self, module_name: str) -> bool:
        module = self._modules.get(module_name)
        return module is not None and module.is_loaded

    async def load_module(
        self,
        bot: hikari.GatewayBot,
        module_name: str,
        *,
        is_reload: bool = False,
    ) -> bool:
        module_disk_path = EXTENSIONS_DIR / module_name
        if not module_disk_path.is_dir():
            return False

        module_class = self.detect_module_type(module_disk_path)
        if module_class is None:
            logger.error("Failed to determine module type for %s", module_name)
            return False

        if module_name in self._modules and not is_reload:
            logger.info("Loaded module %s", module_name)
            return False

        if is_reload and (old_module := self._modules.get(module_name)):
            await old_module.unload()
            del self._modules[module_name]
            await asyncio.sleep(0.5)

        module = module_class(name=module_name, path=module_disk_path)
        result = await module.load(bot, is_reload=is_reload)

        if not result.success:
            logger.error(
                "Failed to load %s module %s: %s",
                module.module_type.value,
                module_name,
                result.message,
            )
            if not is_reload:
                await dm_role_members(
                    embeds=[
                        await reply_embed(
                            bot,
                            f"{module.module_type.value.title()} Module Load Failed",
                            f"Failed to load {module.module_type.value} module `{module_name}`:\n{result.message}",
                            Color.ERROR,
                        ),
                    ],
                )
                if isinstance(module, PythonModule):
                    await asyncio.to_thread(delete_module, module_name)
            return False

        self._modules[module_name] = module
        logger.info("Loaded %s", result.message)
        return True

    async def unload_module(self, module_name: str) -> bool:
        module = self._modules.get(module_name)
        if module is None:
            return False

        result = await module.unload()
        if not result.success:
            logger.error("Failed to unload module %s: %s", module_name, result.message)
            return False

        del self._modules[module_name]
        logger.info("Unloaded %s", result.message)
        return True

    async def reload_module(self, bot: hikari.GatewayBot, module_name: str) -> bool:
        if module_name not in self._modules:
            logger.info("Cannot reload module %s: not currently loaded", module_name)
            return False

        return await self.load_module(bot, module_name, is_reload=True)

    async def call_method(
        self,
        module_name: str,
        method: str,
        payload: dict,
    ) -> dict | None:
        module = self._modules.get(module_name)
        if module is None:
            return None
        return await module.call_method(method, payload)

    def get_loaded_modules(self, module_type: ModuleType) -> list[str]:
        module_class = (
            TypeScriptModule if module_type is ModuleType.TYPESCRIPT else PythonModule
        )
        return [
            name
            for name, mod in self._modules.items()
            if mod.is_loaded and isinstance(mod, module_class)
        ]

    async def unload_all(self) -> None:
        modules = list(self._modules.keys())
        for module_name in modules:
            await self.unload_module(module_name)

    async def stop_all_ts_modules(self) -> None:
        ts_modules = self.get_loaded_modules(ModuleType.TYPESCRIPT)
        for module_name in ts_modules:
            await self.unload_module(module_name)
        if SOCKET_DIR.exists():
            for sock in SOCKET_DIR.glob("*.sock"):
                sock.unlink()


registry = Registry()
