from __future__ import annotations

import abc
import dataclasses
import typing

if typing.TYPE_CHECKING:
    import pathlib

    import hikari

    from src.container.types import ModuleType


@dataclasses.dataclass(slots=True, frozen=True)
class Result:
    success: bool
    message: str

    @classmethod
    def ok(cls, message: str) -> Result:
        return cls(success=True, message=message)

    @classmethod
    def failure(cls, message: str) -> Result:
        return cls(success=False, message=message)


class Module(abc.ABC):
    __slots__ = ("_is_loaded", "name", "path")

    def __init__(self, name: str, path: pathlib.Path) -> None:
        self.name: str = name
        self.path: pathlib.Path = path
        self._is_loaded: bool = False

    @property
    def is_loaded(self) -> bool:
        return self._is_loaded

    @property
    @abc.abstractmethod
    def module_type(self) -> ModuleType: ...

    @abc.abstractmethod
    async def load(
        self,
        hikari_client: hikari.GatewayBot,
        *,
        is_reload: bool = False,
    ) -> Result: ...

    @abc.abstractmethod
    async def unload(self) -> Result: ...

    @abc.abstractmethod
    def get_info(self) -> dict[str, object]: ...

    @abc.abstractmethod
    async def call_method(self, method: str, payload: dict) -> dict | None: ...

    def _set_loaded(self, loaded: bool) -> None:
        self._is_loaded = loaded
