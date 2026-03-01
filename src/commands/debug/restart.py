from __future__ import annotations

import asyncio
import datetime
import os
from typing import TYPE_CHECKING

from src.container.app import get_hikari
from src.shared.constants import FLAG_DIR, Color
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import defer, reply_embed, reply_err, reply_ok
from main import request_shutdown

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
        await request_shutdown("Restart requested")
        await asyncio.sleep(0.1)
        logger.info("Forced restart exit")
        os._exit(0)
    except Exception as exc:
        logger.exception("Failed to execute restart command")
        await reply_err(hikari_client, ctx, f"Failed to restart bot: {exc}")
