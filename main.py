from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import signal
import sys
import typing

import arc
import hikari
import miru
import uvloop

from src.commands.app.cmd import (
    autocomplete_app_cmd,
    cmd_app_delete,
    cmd_app_info,
    cmd_app_scope,
    cmd_app_search,
)
from src.commands.app.exec import cmd_app_exec
from src.commands.debug.download import cmd_debug_download
from src.commands.debug.export import autocomplete_debug_export, cmd_debug_export
from src.commands.debug.info import cmd_debug_info
from src.commands.debug.restart import cmd_debug_restart
from src.commands.kernel.info import cmd_kernel_info
from src.commands.kernel.update import cmd_kernel_update
from src.commands.module.autocomplete import autocomplete_module
from src.commands.module.info import cmd_module_info
from src.commands.module.list import cmd_module_list
from src.commands.module.load import cmd_module_load
from src.commands.module.unload import cmd_module_unload
from src.commands.module.update import cmd_module_update
from src.commands.utils import hook_cmd_group, hook_cmd_subgroup
from src.container.app import get_arc, init_app
from src.container.types import ModuleType
from src.modules.registry import registry
from src.shared.constants import EXTENSIONS_DIR, SHUTDOWN_EVENT, TOKEN
from src.shared.error import error_handler
from src.shared.logger import logger
from src.shared.utils.client import make_arc_client, make_hikari_client
from src.shared.utils.jurigged import setup as setup_jurigged
from src.shared.utils.package import import_package

uvloop.install()

if typing.TYPE_CHECKING:
    from src.shared.utils.jurigged import Jurigged


if not TOKEN:
    logger.info("Failed to load bot token")
    sys.exit(1)


hikari_client: hikari.GatewayBot = make_hikari_client(TOKEN)
arc_client: arc.GatewayClient = make_arc_client(hikari_client)
miru_client: miru.Client = miru.Client.from_arc(arc_client)
jurigged_service: Jurigged | None = None

init_app(hikari_client, arc_client, miru_client)

os.environ.pop("TOKEN", None)
with contextlib.suppress(OSError):
    pathlib.Path(".env").unlink(missing_ok=True)


import_package("src")


# --- Error ---


@get_arc().set_error_handler
async def error_handler_wrapper(
    ctx: arc.GatewayContext,
    error: Exception,
) -> None:
    await error_handler(ctx, error)


# --- Events ---


@hikari_client.listen()
async def on_hikari_starting(event: hikari.StartingEvent) -> None:
    logger.info("Processing starting hikari event for %s", type(event.app).__name__)
    logger.info("Starting hikari client %s", event.app)


@hikari_client.listen()
async def on_hikari_started(event: hikari.StartedEvent) -> None:
    logger.info("Processing started hikari event for %s", type(event.app).__name__)
    logger.info("Started hikari client %s", event.app)
    try:
        me = hikari_client.get_me()
        status = hikari.Status.ONLINE if me else hikari.Status.DO_NOT_DISTURB
        activity = hikari.Activity(
            name="with hikari",
            type=hikari.ActivityType.LISTENING,
        )
        await hikari_client.update_presence(status=status, activity=activity)
        if me:
            logger.info("Authenticated as bot %s (%s)", me.username, me.id)
        else:
            logger.error("Failed to retrieve bot user object")
    except Exception:
        logger.exception("Failed to complete bot startup")


@arc_client.add_startup_hook
async def on_arc_starting(client: arc.GatewayClient) -> None:
    global jurigged_service

    discovered_modules: set[str] = set()
    try:
        with os.scandir(EXTENSIONS_DIR) as entries:
            for entry in entries:
                entry_name = entry.name
                if entry.is_file() and entry_name.endswith(".py") and not entry_name.startswith("_"):
                    discovered_modules.add(f"extensions.{entry_name[:-3]}")
                elif entry.is_dir() and entry_name != "__pycache__":
                    entry_path = EXTENSIONS_DIR / entry_name
                    if (entry_path / ModuleType.PYTHON.entry_file).is_file():
                        discovered_modules.add(f"extensions.{entry_name}.main")
    except OSError:
        logger.exception("Failed to discover extensions directory")

    extension_modules = tuple(sorted(discovered_modules, key=str.casefold))
    logger.info("Discovered %d modules", len(extension_modules))

    if extension_modules:
        logger.info("Loading modules: %s", list(extension_modules))
        loaded: set[str] = set()
        failed: set[str] = set()

        for extension_module in extension_modules:
            module_parts = extension_module.rsplit(".", 2)
            module_name = module_parts[-2] if module_parts[-1] == "main" else module_parts[-1]

            try:
                if extension_module.endswith(".main"):
                    if await registry.load_module(hikari_client, module_name):
                        loaded.add(extension_module)
                    else:
                        failed.add(extension_module)
                else:
                    client.load_extension(extension_module)
                    logger.info("Loaded module '%s'", extension_module)
                    loaded.add(extension_module)
            except Exception:
                logger.exception("Failed to load module '%s'", extension_module)
                failed.add(extension_module)

        logger.info("Loaded %d modules", len(loaded))
        logger.info("Failed to load %d modules", len(failed))
        if failed:
            logger.info("Failed to load modules: %s", failed)

    try:
        await client.resync_commands()
    except Exception:
        logger.exception("Failed to resync application commands")

    if jurigged_service is None:
        try:
            jurigged_service = setup_jurigged(hikari_client, arc_client, miru_client)
            logger.info("Initialized jurigged hot-reload service")
        except Exception:
            logger.exception("Failed to initialize jurigged hot-reload service")


@arc_client.listen()
async def on_arc_started(event: arc.StartedEvent) -> None:
    logger.info("Processing started arc event for %s", type(event.client).__name__)
    logger.info("Started arc client %s", event.client)


# --- Commands ---

cmd_group = get_arc().include_slash_group("bot", "Bot commands")
hook_cmd_group(cmd_group)

cmd_module = cmd_group.include_subgroup("module", "Module commands")
cmd_kernel = cmd_group.include_subgroup("kernel", "Kernel commands")
cmd_debug = cmd_group.include_subgroup("debug", "Debug commands")
cmd_app = cmd_group.include_subgroup("app", "App commands")
hook_cmd_subgroup(cmd_module)
hook_cmd_subgroup(cmd_kernel)
hook_cmd_subgroup(cmd_debug)
hook_cmd_subgroup(cmd_app)


# Debug commands
@cmd_debug.include()
@arc.slash_subcommand(
    name="download",
    description="Download current code",
)
async def cmd_debug_download_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_debug_download(ctx)


@cmd_debug.include()
@arc.slash_subcommand(
    name="export",
    description="Export files",
)
async def cmd_debug_export_wrapper(
    ctx: arc.GatewayContext,
    path: arc.Option[
        str,
        arc.StrParams(
            name="path",
            description="Relative path to export",
            autocomplete_with=autocomplete_debug_export,
        ),
    ],
) -> None:
    await cmd_debug_export(ctx, path)


@cmd_debug.include()
@arc.slash_subcommand(
    name="info",
    description="Show debugging information",
)
async def cmd_debug_info_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_debug_info(ctx)


@cmd_debug.include()
@arc.slash_subcommand(name="restart", description="Restart the bot")
async def cmd_debug_restart_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_debug_restart(ctx)


# App commands
@cmd_app.include()
@arc.slash_subcommand(
    name="exec",
    description="Run arbitrary code",
)
async def cmd_app_exec_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_app_exec(ctx)


@cmd_app.include()
@arc.slash_subcommand(
    name="info",
    description="Get information about registered app commands",
)
async def cmd_app_info_wrapper(
    ctx: arc.GatewayContext,
) -> None:
    await cmd_app_info(ctx)


@cmd_app.include()
@arc.slash_subcommand(
    name="search",
    description="Search for an application command and export its JSON",
)
async def cmd_app_search_wrapper(
    ctx: arc.GatewayContext,
    cmd_id: arc.Option[
        str,
        arc.StrParams(
            name="cmd",
            description="Application command ID",
            autocomplete_with=autocomplete_app_cmd,
        ),
    ],
    scope: arc.Option[
        str,
        arc.StrParams(
            name="scope",
            description="Scope ID (0 for global)",
        ),
    ] = "0",
    remote: arc.Option[
        bool,
        arc.BoolParams(
            name="remote",
            description="Search from Discord API instead of local cache",
        ),
    ] = False,
) -> None:
    await cmd_app_search(
        ctx,
        cmd_id=cmd_id,
        scope=scope,
        remote=remote,
    )


@cmd_app.include()
@arc.slash_subcommand(
    name="scope",
    description="List commands in a scope",
)
async def cmd_app_scope_wrapper(
    ctx: arc.GatewayContext,
    scope: arc.Option[
        str,
        arc.StrParams(
            name="scope",
            description="Scope ID (0 for global)",
        ),
    ] = "0",
) -> None:
    await cmd_app_scope(ctx, scope=scope)


@cmd_app.include()
@arc.slash_subcommand(
    name="delete",
    description="Delete command(s) in a scope",
)
async def cmd_app_delete_wrapper(
    ctx: arc.GatewayContext,
    scope: arc.Option[
        str,
        arc.StrParams(
            name="scope",
            description="Scope ID (0 for global)",
        ),
    ] = "0",
    cmd_id: arc.Option[
        str,
        arc.StrParams(
            name="cmd_id",
            description="Application command ID (leave empty when deleting all)",
            autocomplete_with=autocomplete_app_cmd,
        ),
    ] = "",
    delete_all: arc.Option[
        bool,
        arc.BoolParams(
            name="all",
            description="Delete all commands in the selected scope",
        ),
    ] = False,
) -> None:
    await cmd_app_delete(
        ctx,
        scope=scope,
        cmd_id=cmd_id,
        delete_all=delete_all,
    )


# Kernel commands
@cmd_kernel.include()
@arc.slash_subcommand(
    name="info",
    description="Show information about the Kernel",
)
async def cmd_kernel_info_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_kernel_info(ctx)


@cmd_kernel.include()
@arc.slash_subcommand(
    name="update",
    description="Update the kernel to the latest version",
)
async def cmd_kernel_update_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_kernel_update(ctx)


# Module commands
@cmd_module.include()
@arc.slash_subcommand(name="info", description="Show information about a loaded module")
async def cmd_module_info_wrapper(
    ctx: arc.GatewayContext,
    module: arc.Option[
        str,
        arc.StrParams(
            name="module",
            description="Module name",
            autocomplete_with=autocomplete_module,
        ),
    ],
) -> None:
    await cmd_module_info(ctx, module)


@cmd_module.include()
@arc.slash_subcommand(name="list", description="List all loaded modules")
async def cmd_module_list_wrapper(ctx: arc.GatewayContext) -> None:
    await cmd_module_list(ctx)


@cmd_module.include()
@arc.slash_subcommand(name="load", description="Load module from Git URL")
async def cmd_module_load_wrapper(
    ctx: arc.GatewayContext,
    url: arc.Option[
        str,
        arc.StrParams(
            name="url",
            description="Git repo URL (e.g., https://github.com/user/repo.git)",
        ),
    ],
) -> None:
    await cmd_module_load(ctx, url)


@cmd_module.include()
@arc.slash_subcommand(name="unload", description="Unload and delete a module")
async def cmd_module_unload_wrapper(
    ctx: arc.GatewayContext,
    module: arc.Option[
        str,
        arc.StrParams(
            name="module",
            description="Module name",
            autocomplete_with=autocomplete_module,
        ),
    ],
) -> None:
    await cmd_module_unload(ctx, module)


@cmd_module.include()
@arc.slash_subcommand(
    name="update",
    description="Update a module to the latest version",
)
async def cmd_module_update_wrapper(
    ctx: arc.GatewayContext,
    module: arc.Option[
        str,
        arc.StrParams(
            name="module",
            description="Module name",
            autocomplete_with=autocomplete_module,
        ),
    ],
) -> None:
    await cmd_module_update(ctx, module)


# --- Main Execution ---

_shutdown_started: bool = False


async def _request_shutdown(reason: str) -> None:
    global _shutdown_started
    if _shutdown_started:
        return
    _shutdown_started = True
    logger.info("Shutdown requested: %s", reason)
    SHUTDOWN_EVENT.set()
    with contextlib.suppress(Exception):
        if hikari_client.is_alive:
            logger.info("Closing bot connection")
            await hikari_client.close()
            logger.info("Closed bot connection")


async def main() -> None:
    global jurigged_service

    event_loop = asyncio.get_running_loop()

    def exception_handler(
        _event_loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        exception = context.get("exception")
        if isinstance(exception, BaseException):
            logger.exception(
                "Unhandled event loop exception: %s",
                exception,
                exc_info=exception,
            )
            return
        message = context.get("message")
        logger.exception("Unhandled event loop error: %s", message)

    event_loop.set_exception_handler(exception_handler)

    def dispatch_shutdown(signal_type: signal.Signals) -> None:
        asyncio.create_task(_request_shutdown(reason=f"Received {signal_type.name}"))

    for signal_type in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            event_loop.add_signal_handler(
                signal_type,
                dispatch_shutdown,
                signal_type,
            )

    try:
        await hikari_client.start()

        shutdown_wait_task = asyncio.create_task(SHUTDOWN_EVENT.wait())
        join_task = asyncio.create_task(hikari_client.join())
        orchestration_tasks: tuple[asyncio.Task[object], ...] = (
            shutdown_wait_task,
            join_task,
        )

        done, pending = await asyncio.wait(
            orchestration_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if join_task in done:
            join_error = join_task.exception()
            if join_error is not None:
                logger.exception("Bot join exited with exception", exc_info=join_error)
            await _request_shutdown("Bot connection closed")

        if shutdown_wait_task in pending:
            shutdown_wait_task.cancel()
            await asyncio.gather(shutdown_wait_task, return_exceptions=True)
    except Exception:
        logger.exception("Failed during bot runtime")
        await _request_shutdown("Runtime failure")
    finally:
        if jurigged_service is not None:
            with contextlib.suppress(Exception):
                await jurigged_service.stop()
            jurigged_service = None

        current = asyncio.current_task()
        excluded = {current} if current is not None else set()
        pending_tasks = {task for task in asyncio.all_tasks() if task not in excluded and not task.done()}
        if pending_tasks:
            logger.info("Cancelling %d outstanding tasks", len(pending_tasks))
            original_limit = sys.getrecursionlimit()
            try:
                sys.setrecursionlimit(
                    max(original_limit, len(pending_tasks) * 100 + 1000),
                )
                for task in pending_tasks:
                    task.cancel()
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            finally:
                sys.setrecursionlimit(original_limit)


def entrypoint() -> None:
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Failed to execute main application")
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
