from __future__ import annotations

import asyncio
import contextlib
import pathlib
import threading
import typing
from dataclasses import dataclass
from typing import final

import jurigged as _jurigged
from jurigged import codetools as _codetools
from jurigged import live as _live

from src.container.app import get_arc, get_hikari, get_miru
from src.modules.registry import registry
from src.shared.constants import EXTENSIONS_DIR
from src.shared.logger import logger

if typing.TYPE_CHECKING:
    from collections.abc import Callable

    import arc
    import hikari
    import miru

    class _JuriggedSignal(typing.Protocol):
        def register(self, callback: Callable[..., object]) -> object: ...

    class _JuriggedWatcher(typing.Protocol):
        prerun: _JuriggedSignal
        postrun: _JuriggedSignal

        def stop(self) -> object: ...


def _is_instance(value: object, klass: type[object] | None) -> typing.TypeIs[object]:
    return klass is not None and isinstance(value, klass)


_watch_fn: Callable[..., object] | None = None
_watch_operation_type: type[object] | None = None
_add_operation_type: type[object] | None = None
_delete_operation_type: type[object] | None = None
_line_definition_type: type[object] | None = None
_update_operation_type: type[object] | None = None

if _jurigged is not None and callable(getattr(_jurigged, "watch", None)):
    _watch_fn = _jurigged.watch

if _live is not None:
    _watch_operation_type = (
        watch_operation
        if isinstance(watch_operation := getattr(_live, "WatchOperation", None), type)
        else None
    )

if _codetools is not None:
    _add_operation_type = (
        add_op
        if isinstance(add_op := getattr(_codetools, "AddOperation", None), type)
        else None
    )
    _delete_operation_type = (
        del_op
        if isinstance(del_op := getattr(_codetools, "DeleteOperation", None), type)
        else None
    )
    _line_definition_type = (
        line_def
        if isinstance(line_def := getattr(_codetools, "LineDefinition", None), type)
        else None
    )
    _update_operation_type = (
        upd_op
        if isinstance(upd_op := getattr(_codetools, "UpdateOperation", None), type)
        else None
    )

_JURIGGED_AVAILABLE = _watch_fn is not None


__all__ = ("Jurigged", "setup")


@dataclass(frozen=True, slots=True, kw_only=True)
@final
class _ReloadTarget:
    kind: typing.Literal["module", "extension"]
    name: str


@final
class Jurigged:
    __slots__ = (
        "_arc_client",
        "_debounce_seconds",
        "_drain_task",
        "_hikari_client",
        "_loop",
        "_miru_client",
        "_pending",
        "_pending_lock",
        "_poll",
        "_watcher",
    )

    def __init__(
        self,
        hikari_client: hikari.GatewayBot,
        arc_client: arc.GatewayClient,
        miru_client: miru.Client,
        *,
        poll: bool = False,
        debounce_seconds: float = 0.35,
    ) -> None:
        self._hikari_client = hikari_client
        self._arc_client = arc_client
        self._miru_client = miru_client
        self._poll = poll
        self._debounce_seconds = debounce_seconds

        self._loop: asyncio.AbstractEventLoop | None = None
        self._watcher: _JuriggedWatcher | None = None
        self._pending: set[_ReloadTarget] = set()
        self._pending_lock = threading.RLock()
        self._drain_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if not _JURIGGED_AVAILABLE:
            logger.warning(
                "Failed to start jurigged watcher; jurigged is not installed",
            )
            return

        if self._watcher is not None:
            logger.debug("Started jurigged watcher")
            return

        self._loop = asyncio.get_running_loop()
        pattern = str((EXTENSIONS_DIR / "**" / "[!.]*.py").resolve())

        logger.info("Starting jurigged watcher on '%s' (poll=%s)", pattern, self._poll)

        if _watch_fn is None:
            logger.warning(
                "Failed to start jurigged watcher; watch function unavailable"
            )
            return

        raw_watcher = _watch_fn(pattern, logger=self._log_event, poll=self._poll)
        if not hasattr(raw_watcher, "prerun") or not hasattr(raw_watcher, "postrun"):
            logger.warning(
                "Failed to start jurigged watcher; returned invalid watcher object"
            )
            return

        self._watcher = typing.cast("_JuriggedWatcher", raw_watcher)
        self._watcher.prerun.register(self._on_prerun)
        self._watcher.postrun.register(self._on_postrun)

    async def stop(self) -> None:
        watcher = self._watcher
        self._watcher = None

        if watcher is None:
            return

        logger.info("Stopping jurigged watcher")
        with contextlib.suppress(Exception):
            if callable(stop_fn := getattr(watcher, "stop", None)):
                await asyncio.to_thread(stop_fn)

        drain_task = self._drain_task
        self._drain_task = None
        if drain_task is not None and not drain_task.done():
            drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task

    def _log_event(self, event: object) -> None:
        if _is_instance(event, _watch_operation_type):
            logger.debug(
                "Processing jurigged watch event: %s",
                getattr(event, "filename", "<unknown>"),
            )
            return

        if isinstance(event, (SyntaxError, Exception)):
            logger.exception("Failed to process jurigged event", exc_info=event)
            return

        action: str | None = None
        dotpath = "<unknown>"
        lineno = 0
        extra = ""

        definition = getattr(event, "defn", None)
        if definition is not None:
            stashed = getattr(definition, "stashed", None)
            lineno = int(getattr(stashed, "lineno", 0) or 0)
            if hasattr(definition, "dotpath"):
                with contextlib.suppress(Exception):
                    dotpath = definition.dotpath()

            if _is_instance(definition, _line_definition_type):
                parent = getattr(definition, "parent", None)
                if parent is not None and hasattr(parent, "dotpath"):
                    with contextlib.suppress(Exception):
                        dotpath = parent.dotpath()
                extra = f" | {getattr(definition, 'text', '')}".rstrip()

        if _is_instance(event, _add_operation_type):
            action = "Run" if _is_instance(definition, _line_definition_type) else "Add"
        elif _is_instance(event, _update_operation_type):
            action = "Update"
        elif _is_instance(event, _delete_operation_type):
            action = "Delete"

        if action is None:
            logger.debug("Processing jurigged event: %s", event)
            return

        logger.debug("Executing %s %s:%s%s", action, dotpath, lineno, extra)

    def _on_prerun(self, _path: str, cf: object) -> None:
        logger.debug(
            "Processing jurigged prerun for module '%s'",
            getattr(cf, "module_name", "<unknown>"),
        )

    def _on_postrun(self, path: str, cf: object) -> None:
        target = self._target_from_change(path, cf)
        if target is None:
            return

        logger.debug("Queued hot-reload target %s:%s", target.kind, target.name)

        loop = self._loop
        if loop is None:
            logger.warning(
                "Failed to queue reload for '%s'; jurigged loop unavailable",
                target.name,
            )
            return

        loop.call_soon_threadsafe(self._enqueue_target, target)

    def _enqueue_target(self, target: _ReloadTarget) -> None:
        with self._pending_lock:
            self._pending.add(target)
            if self._drain_task is None or self._drain_task.done():
                self._drain_task = asyncio.create_task(self._drain_targets())

    async def _drain_targets(self) -> None:
        await asyncio.sleep(self._debounce_seconds)

        with self._pending_lock:
            targets = tuple(self._pending)
            self._pending.clear()

        for target in targets:
            with contextlib.suppress(Exception):
                await self._reload_target(target)

    async def _reload_target(self, target: _ReloadTarget) -> None:
        if target.kind == "module":
            reloaded = await registry.reload_module(self._hikari_client, target.name)
            if reloaded:
                logger.info("Hot-reloaded module '%s'", target.name)
            else:
                logger.warning("Failed to hot-reload module '%s'", target.name)
            return

        full_name = target.name
        logger.info("Hot-reloading extension '%s'", full_name)

        with contextlib.suppress(Exception):
            self._arc_client.unload_extension(full_name)

        self._arc_client.load_extension(full_name)

        if self._arc_client.is_started:
            await self._arc_client.resync_commands()

    @staticmethod
    def _target_from_change(path: str, cf: object) -> _ReloadTarget | None:
        changed_path = pathlib.Path(path).resolve()

        with contextlib.suppress(ValueError):
            rel = changed_path.relative_to(EXTENSIONS_DIR.resolve())
            parts = rel.parts
            if not parts:
                return None

            if len(parts) == 1 and changed_path.suffix == ".py":
                return _ReloadTarget(
                    kind="extension", name=f"extensions.{changed_path.stem}"
                )

            module_name = parts[0]
            if not module_name.startswith("."):
                return _ReloadTarget(kind="module", name=module_name)

        module_name = getattr(cf, "module_name", "")
        if module_name.startswith("extensions."):
            parts = module_name.split(".")
            if parts[-1] == "main" and len(parts) >= 2:
                return _ReloadTarget(kind="module", name=parts[-2])
            return _ReloadTarget(kind="extension", name=module_name)

        return None


def setup(
    hikari_client: hikari.GatewayBot | None = None,
    arc_client: arc.GatewayClient | None = None,
    miru_client: miru.Client | None = None,
    *,
    poll: bool = False,
) -> Jurigged:
    service = Jurigged(
        hikari_client or get_hikari(),
        arc_client or get_arc(),
        miru_client or get_miru(),
        poll=poll,
    )
    service.start()
    return service
