from __future__ import annotations

import asyncio
import contextlib
import datetime
import os
import sys
from typing import TYPE_CHECKING

from src.container.app import get_hikari
from src.shared.constants import FLAG_DIR, SHUTDOWN_EVENT, Color
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import defer, reply_embed, reply_err, reply_ok

if TYPE_CHECKING:
    import arc
    import hikari


def _write_restart_flag(executor_id: hikari.Snowflake) -> None:
    FLAG_DIR.mkdir(exist_ok=True)
    restart_flag = FLAG_DIR / "restart"
    restart_flag.write_text(
        "Restart triggered by debug command at "
        f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} by {executor_id}",
    )
    logger.info("Wrote restart flag to %s", restart_flag)


async def _announce_restart(
    ctx: arc.GatewayContext,
) -> None:
    hikari_client = get_hikari()
    embed = await reply_embed(
        hikari_client,
        "Initiating Restart",
        f"{ctx.user.mention} initiated bot restart.",
        Color.WARNING,
    )
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])


async def cmd_debug_restart(ctx: arc.GatewayContext) -> None:
    await defer(ctx)
    hikari_client = get_hikari()

    try:
        await _announce_restart(ctx)
        await reply_ok(hikari_client, ctx, "Initiated bot restart sequence.")
        _write_restart_flag(ctx.user.id)
        logger.info("Attempting graceful restart")
        shutdown_func = None
        main_mod = sys.modules.get("__main__") or sys.modules.get("main")
        if main_mod is not None:
            shutdown_func = getattr(main_mod, "request_shutdown", None)
        if callable(shutdown_func):
            shutdown_result = shutdown_func("Restart requested")
            if asyncio.iscoroutine(shutdown_result):
                await shutdown_result
        else:
            SHUTDOWN_EVENT.set()
            if hikari_client.is_alive:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(hikari_client.close(), timeout=5.0)
        await asyncio.sleep(0.1)
        logger.info("Forced restart exit")
        os._exit(0)
    except Exception as exc:
        logger.exception("Failed to execute restart command")
        await reply_err(hikari_client, ctx, f"Failed to restart bot: {exc}")
