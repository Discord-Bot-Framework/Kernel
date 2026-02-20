from __future__ import annotations

import asyncio
import contextlib
import sys
import typing
import inspect

from src.container.app import get_arc
from src.container.types import ModuleType
from src.modules.abc import Module, Result
from src.modules.utils import check_local_module, delete_module
from src.shared.logger import logger

if typing.TYPE_CHECKING:
    import pathlib
    import types
    from collections.abc import Iterable

    import arc
    import hikari


class PythonModule(Module):
    __slots__ = ("_backup_modules", "_module_full_name", "_state_lock")

    def __init__(self, name: str, path: pathlib.Path) -> None:
        super().__init__(name, path)
        self._module_full_name: str = f"extensions.{name}.main"
        self._backup_modules: dict[str, types.ModuleType] = {}
        self._state_lock = asyncio.Lock()

    @property
    def module_type(self) -> ModuleType:
        return ModuleType.PYTHON

    def get_info(self) -> dict[str, object]:
        info: dict[str, object] = {
            "name": self.name,
            "type": self.module_type.value,
            "module_full_name": self._module_full_name,
        }
        reqs_file = self.path / "requirements.txt"
        info["has_requirements"] = reqs_file.is_file()
        return info

    def _submodule_prefix(self) -> str:
        return f"{self._module_full_name}."

    def _is_owned_module_name(self, module_name: str) -> bool:
        return module_name == self._module_full_name or module_name.startswith(
            self._submodule_prefix(),
        )

    def _iter_owned_module_names(self) -> Iterable[str]:
        return (
            module_name
            for module_name in tuple(sys.modules.keys())
            if self._is_owned_module_name(module_name)
        )

    def _capture_loaded_modules(self) -> dict[str, types.ModuleType]:
        return {
            module_name: module_obj
            for module_name, module_obj in sys.modules.items()
            if self._is_owned_module_name(module_name)
        }

    def _purge_loaded_modules(self) -> None:
        for module_name in self._iter_owned_module_names():
            sys.modules.pop(module_name, None)

    @staticmethod
    async def _resync_commands_if_started(arc_client: arc.GatewayClient) -> None:
        if arc_client.is_started:
            await arc_client.resync_commands()

    async def _load_extension(self, *, resync: bool = True) -> None:
        arc_client = get_arc()
        arc_client.load_extension(self._module_full_name)
        if resync:
            await self._resync_commands_if_started(arc_client)

    async def _unload_extension(self, *, resync: bool = True) -> None:
        arc_client = get_arc()
        arc_client.unload_extension(self._module_full_name)
        if resync:
            await self._resync_commands_if_started(arc_client)

    async def _validate(self) -> Result | None:
        if not self.path.is_dir():
            return Result.failure(f"Module path not found: {self.path}")

        is_valid, error_msg, missing_deps = await check_local_module(
            str(self.path),
            self.name,
        )
        if is_valid:
            return None

        message = f"Validation failed:\n{error_msg}"
        if missing_deps:
            missing_list = "\n- ".join(missing_deps)
            message += f"\n\n**Missing:**\n- {missing_list}"
        return Result.failure(message)

    async def _load_fresh(self) -> Result:
        try:
            await self._load_extension(resync=True)
            self._set_loaded(True)
            return Result.ok(f"Loaded Python module {self.name}")
        except Exception as exc:
            deleted = await asyncio.to_thread(delete_module, self.name)
            if not deleted:
                logger.info("Failed to delete invalid module '%s'", self.name)
            return Result.failure(f"Failed to load: {exc}")

    async def _reload_extension(self) -> Result:
        arc_client = get_arc()
        backup_submodules = self._capture_loaded_modules()
        self._backup_modules = backup_submodules

        try:
            with contextlib.suppress(Exception):
                arc_client.unload_extension(self._module_full_name)
            self._purge_loaded_modules()
            await self._load_extension(resync=True)
            self._set_loaded(True)
            logger.info("Reloaded extension '%s'", self._module_full_name)
            return Result.ok(f"Reloaded Python module {self.name}")
        except Exception as reload_exc:
            logger.exception("Failed to reload extension '%s'", self._module_full_name)
            with contextlib.suppress(Exception):
                arc_client.unload_extension(self._module_full_name)
            self._purge_loaded_modules()
            for module_name, module_obj in backup_submodules.items():
                sys.modules[module_name] = module_obj

            try:
                await self._load_extension(resync=True)
            except Exception:
                logger.exception(
                    "Failed to roll back extension '%s'",
                    self._module_full_name,
                )
                self._set_loaded(False)
                return Result.failure(
                    f"Reload failed and rollback also failed: {reload_exc}",
                )

            logger.info(
                "Rolled back extension '%s' to previous state",
                self._module_full_name,
            )
            self._set_loaded(True)
            return Result.failure(f"Reload failed, rolled back: {reload_exc}")

    async def load(
        self,
        hikari_client: hikari.GatewayBot,
        *,
        is_reload: bool = False,
    ) -> Result:
        del hikari_client
        async with self._state_lock:
            validation_error = await self._validate()
            if validation_error is not None:
                return validation_error
            if is_reload:
                return await self._reload_extension()
            return await self._load_fresh()

    async def unload(self) -> Result:
        try:
            async with self._state_lock:
                await self._unload_extension(resync=False)
                logger.info("Unloaded extension '%s'", self._module_full_name)

                try:
                    await self._resync_commands_if_started(get_arc())
                    logger.info("Resynced commands after extension unload")
                except Exception:
                    logger.exception("Failed to resync commands after unload")

                self._set_loaded(False)
                self._purge_loaded_modules()
                return Result.ok(f"Unloaded Python module {self.name}")
        except Exception as exc:
            logger.exception("Failed to unload extension '%s'", self._module_full_name)
            return Result.failure(f"Failed to unload: {exc}")

    async def call_method(self, method: str, payload: dict) -> dict | None:
        try:
            module = sys.modules.get(self._module_full_name)
            if module is None:
                return {"error": "Module not loaded in sys.modules"}

            callable_obj = getattr(module, method, None)
            if callable_obj is None:
                return {"error": f"Method '{method}' not found in module"}

            if inspect.iscoroutinefunction(callable_obj):
                result = await callable_obj(**payload)
            else:
                result = callable_obj(**payload)
            return {"result": result}
        except Exception as exc:
            logger.exception(
                "Failed to call method %s on Python module %s",
                method,
                self.name,
            )
            return {"error": str(exc)}
