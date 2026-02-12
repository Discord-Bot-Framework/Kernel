from __future__ import annotations

import asyncio
import collections
import contextlib
import importlib
import os
import pathlib
import pkgutil
import signal
import sys

import arc
import hikari
import miru
import orjson
import uvloop

from foundation.app import init_app
from foundation.types import ModuleType
from modules.registry import registry
from shared.constants import EXTENSIONS_DIR, GUILD_ID, TOKEN
from shared.logger import logger

uvloop.install()


def _import_modules(package_name: str) -> None:
    queue: collections.deque[str] = collections.deque([package_name])

    while queue:
        current_name = queue.popleft()

        if current_name in sys.modules:
            package = sys.modules[current_name]
        else:
            try:
                package = importlib.import_module(current_name)
            except ImportError:
                continue

        package_path = getattr(package, "__path__", None)
        if package_path is None:
            continue

        for _, module_name, _ in pkgutil.iter_modules(package_path):
            full_module_name = f"{current_name}.{module_name}"
            if full_module_name not in sys.modules:
                queue.append(full_module_name)


_import_modules("src")

if not TOKEN:
    logger.critical("Failed to load bot token")
    sys.exit(1)

bot: hikari.GatewayBot = hikari.GatewayBot(
    token=TOKEN,
    banner=None,
    dumps=orjson.dumps,
    loads=orjson.loads,
    logs=None,
    intents=hikari.Intents.ALL,
)

arc_client: arc.GatewayClient = arc.GatewayClient(
    bot,
    default_enabled_guilds=(hikari.snowflakes.Snowflake(GUILD_ID),)
    if GUILD_ID
    else hikari.undefined.UNDEFINED,
)

miru_client: miru.Client = miru.Client.from_arc(arc_client)

init_app(bot, arc_client, miru_client)

os.environ.pop("TOKEN", None)
with contextlib.suppress(OSError):
    pathlib.Path(".env").unlink(missing_ok=True)


@bot.listen()
async def on_starting(event: hikari.events.StartingEvent) -> None:
    logger.info("Processing starting event for %s", type(event.app).__name__)
    logger.info("Starting bot application %s", event.app)


@bot.listen()
async def on_started(event: hikari.events.StartedEvent) -> None:
    logger.info("Processing started event for %s", type(event.app).__name__)
    logger.info("Bot application started %s", event.app)
    try:
        me = bot.get_me()
        status = hikari.Status.ONLINE if me else hikari.Status.DO_NOT_DISTURB
        activity = hikari.Activity(
            name="with hikari",
            type=hikari.ActivityType.LISTENING,
        )
        await bot.update_presence(status=status, activity=activity)
        if me:
            logger.info("Authenticated as bot %s (%s)", me.username, me.id)
        else:
            logger.error("Failed to retrieve bot user object")
    except Exception:
        logger.error("Failed to complete bot startup")


@arc_client.add_startup_hook
async def on_arc_starting(client: arc.GatewayClient) -> None:
    try:
        await client.resync_commands()
    except Exception:
        logger.error("Failed to resync application commands")


@arc_client.listen()
async def on_arc_started(event: arc.events.StartedEvent) -> None:
    logger.info("Processing started event for %s", type(event.client).__name__)
    logger.info("Arc client started %s", event.client)


async def _shutdown_handler(
    shutdown_event: asyncio.Event,
    signal_type: signal.Signals,
) -> None:
    logger.info("Received %s signal, initiating graceful shutdown", signal_type.name)
    shutdown_event.set()

    current_task = asyncio.current_task()
    pending_tasks: set[asyncio.Task[object]] = {
        task for task in asyncio.all_tasks() if task is not current_task
    }

    with contextlib.suppress(Exception):
        logger.info("Stopping TypeScript modules")
        await registry.stop_all_ts_modules()
        logger.info("Stopped TypeScript modules")

    with contextlib.suppress(Exception):
        if bot.is_alive:
            logger.warning("Closing bot connection")
            await bot.close()
            logger.warning("Closed bot connection")

    if not pending_tasks:
        logger.warning("Found no outstanding tasks to cancel")
        return

    logger.info("Cancelling %d outstanding tasks", len(pending_tasks))
    for task in pending_tasks:
        task.cancel()

    try:
        completed, _ = await asyncio.wait(
            pending_tasks,
            return_when=asyncio.ALL_COMPLETED,
        )
        logger.info("Cancelled %d tasks", len(completed))
    except Exception:
        logger.error("Failed to wait for task cancellation")


def _discover_modules() -> tuple[frozenset[str], frozenset[str]]:
    discovered_python_modules: set[str] = set()
    discovered_ts_modules: set[str] = set()

    try:
        with os.scandir(EXTENSIONS_DIR) as entries:
            for entry in entries:
                entry_name = entry.name
                if (
                    entry.is_file()
                    and entry_name.endswith(".py")
                    and not entry_name.startswith("_")
                ):
                    discovered_python_modules.add(f"extensions.{entry_name[:-3]}")
                elif entry.is_dir() and entry_name != "__pycache__":
                    entry_path = EXTENSIONS_DIR / entry_name
                    if (entry_path / ModuleType.PYTHON.config_file).is_file():
                        discovered_python_modules.add(f"extensions.{entry_name}.main")
                    elif (entry_path / ModuleType.TYPESCRIPT.config_file).is_file():
                        discovered_ts_modules.add(entry_name)
    except OSError:
        logger.error("Failed to discover extensions directory")

    return frozenset(discovered_python_modules), frozenset(discovered_ts_modules)


async def _load_python_modules(extension_modules: frozenset[str]) -> None:
    if not extension_modules:
        return

    logger.info("Loading Python modules: %s", sorted(extension_modules))
    loaded: set[str] = set()
    failed: set[str] = set()

    for extension_module in sorted(extension_modules, key=str.casefold):
        module_parts = extension_module.rsplit(".", 2)
        module_name = (
            module_parts[-2] if module_parts[-1] == "main" else module_parts[-1]
        )

        try:
            if extension_module.endswith(".main"):
                if await registry.load_module(bot, module_name):
                    loaded.add(extension_module)
                else:
                    failed.add(extension_module)
            else:
                arc_client.load_extension(extension_module)
                logger.info("Loaded Python extension '%s'", extension_module)
                loaded.add(extension_module)
        except Exception:
            logger.error("Failed to load Python extension '%s'", extension_module)
            failed.add(extension_module)

    logger.info("Loaded %d Python modules", len(loaded))
    logger.info("Failed to load %d Python modules", len(failed))
    if failed:
        logger.warning("Failed to load Python modules: %s", failed)


async def _load_typescript_modules(ts_modules: frozenset[str]) -> None:
    if not ts_modules:
        return

    logger.info("Loading TypeScript modules: %s", sorted(ts_modules))
    loaded: set[str] = set()
    failed: set[str] = set()

    for module_name in sorted(ts_modules, key=str.casefold):
        try:
            if await registry.load_module(bot, module_name):
                logger.info("Loaded TypeScript module '%s'", module_name)
                loaded.add(module_name)
            else:
                failed.add(module_name)
        except Exception:
            logger.error("Failed to load TypeScript module '%s'", module_name)
            failed.add(module_name)

    logger.info("Loaded %d TypeScript modules", len(loaded))
    logger.info("Failed to load %d TypeScript modules", len(failed))
    if failed:
        logger.warning("Failed to load TypeScript modules: %s", failed)


async def main() -> None:
    def exception_handler(
        event_loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        exception = context.get("exception")
        if isinstance(exception, BaseException):
            logger.error(
                "Failed to handle unhandled event loop exception: %s",
                exception,
                exc_info=exception,
            )
        else:
            logger.error(
                "Failed to process event loop error: %s",
                context.get("message"),
            )

    shutdown_event = asyncio.Event()
    event_loop = asyncio.get_running_loop()
    event_loop.set_exception_handler(exception_handler)

    for signal_type in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(
            signal_type,
            lambda s=signal_type: asyncio.create_task(
                _shutdown_handler(shutdown_event, s),
            ),
        )

    extension_modules, ts_modules = _discover_modules()
    logger.info("Discovered %d Python modules", len(extension_modules))
    logger.info("Discovered %d TypeScript modules", len(ts_modules))

    await _load_python_modules(extension_modules)
    await _load_typescript_modules(ts_modules)

    try:
        await asyncio.gather(
            bot.start(),
            shutdown_event.wait(),
            return_exceptions=True,
        )
    except Exception:
        logger.error("Failed to start bot client")


def entrypoint() -> None:
    try:
        asyncio.run(main())
    except Exception:
        logger.error("Failed to execute main application")
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
