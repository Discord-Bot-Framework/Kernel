from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

from miru.ext import nav

from src.commands.module.list import get_loadable_modules
from src.container.app import get_hikari, get_miru
from src.shared.constants import BASE_DIR, LOG_FILE, Color
from src.shared.logger import logger
from src.shared.utils.view import (
    defer,
    reply_embed,
    reply_err,
    respond_with_builder_and_bind_view,
)

if TYPE_CHECKING:
    import pathlib

    import arc
    import hikari

_MAX_LOG_FILES = 5


def _log_path_display() -> str:
    try:
        return str(LOG_FILE.relative_to(BASE_DIR))
    except Exception:
        return str(LOG_FILE)


def _latency_ms(hikari_client: hikari.GatewayBot) -> str:
    latency = hikari_client.heartbeat_latency
    if latency is None:
        return "N/A"
    return f"{latency * 1000:.2f} ms"


def _recent_logs(log_dir: pathlib.Path) -> list[str]:
    if not log_dir.is_dir():
        return []
    try:
        files = sorted(
            (path for path in log_dir.glob("*.log*") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[:_MAX_LOG_FILES]
    except Exception:
        logger.exception("Failed to enumerate recent log files")
        return []

    entries: list[str] = []
    for file_path in files:
        try:
            size_kb = file_path.stat().st_size // 1024
        except Exception:
            size_kb = 0
        entries.append(f"- `{file_path.name}` ({size_kb} KB)")
    return entries


async def cmd_debug_info(ctx: arc.GatewayContext) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    miru_client = get_miru()

    me = hikari_client.get_me()
    embed = await reply_embed(
        hikari_client,
        "System Status",
        "Runtime diagnostics and bot information.",
        Color.INFO,
    )

    system_field = (
        f"- Python: `{sys.version.split()[0]}`\n"
        f"- Platform: `{sys.platform}`\n"
        f"- PID: `{os.getpid()}`\n"
        f"- CWD: `{BASE_DIR}`\n"
        f"- Log: `{_log_path_display()}`"
    )
    bot_field = (
        f"- User: `{me.username if me else 'N/A'}` ({me.id if me else 'N/A'})\n"
        f"- Guilds: `{len(hikari_client.cache.get_guilds_view())}`\n"
        f"- Modules: `{len(get_loadable_modules())}`\n"
        f"- Latency: `{_latency_ms(hikari_client)}`"
    )

    embed.add_field(name="System", value=system_field, inline=True)
    embed.add_field(name="Bot", value=bot_field, inline=True)

    recent_logs = _recent_logs(LOG_FILE.parent)
    if recent_logs:
        embed.add_field(name="Recent Logs", value="\n".join(recent_logs), inline=False)

    buttons: list[nav.NavItem] = [
        nav.PrevButton(),
        nav.StopButton(),
        nav.NextButton(),
    ]
    try:
        navigator = nav.navigator.NavigatorView(
            pages=[embed],
            items=buttons,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client)
        await respond_with_builder_and_bind_view(
            ctx=ctx,
            builder=builder,
            miru_client=miru_client,
            view=navigator,
        )
    except Exception as exc:
        logger.exception("Failed to send system status")
        await reply_err(hikari_client, ctx, f"Failed to display system status: {exc}.")
