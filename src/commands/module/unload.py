from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from src.container.app import get_hikari
from src.git.utils import get_module_info, is_valid_repo
from src.modules.registry import registry
from src.modules.utils import delete_module
from src.shared.constants import EXTENSIONS_DIR, Color
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import defer, reply_embed, reply_err, reply_ok

if TYPE_CHECKING:
    import arc


def _module_metadata(module: str) -> tuple[str, str]:
    info, valid_info = get_module_info(module)
    if not valid_info or info is None:
        return "Unknown", "Unknown"
    commit_id = str(info.local_commit.id)[:7] if info.local_commit else "Unknown"
    remote_url = info.url or "Unknown"
    return commit_id, remote_url


async def _announce_unload_start(
    ctx: arc.GatewayContext,
    module: str,
    commit_id: str,
    remote_url: str,
) -> None:
    hikari_client = get_hikari()
    embed = await reply_embed(
        hikari_client,
        "Unloading Module",
        f"{ctx.user.mention} is unloading `{module}`.",
        Color.WARNING,
    )
    embed.add_field(name="Current Commit", value=f"`{commit_id}`", inline=True)
    embed.add_field(name="Remote URL", value=remote_url, inline=True)
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])


async def cmd_module_unload(
    ctx: arc.GatewayContext,
    module: str,
) -> None:
    await defer(ctx)
    executor = ctx.user
    hikari_client = get_hikari()

    logger.info(
        "User %s (%s) requested unload of module: %s",
        executor.username,
        executor.id,
        module,
    )

    module_path = EXTENSIONS_DIR / module
    if not module_path.is_dir():
        await reply_err(hikari_client, ctx, f"Directory `{module}` not found.")
        return

    if not is_valid_repo(module):
        await reply_err(
            hikari_client,
            ctx,
            f"`{module}` is not a valid module repository.",
        )
        return

    commit_id, remote_url = _module_metadata(module)
    await _announce_unload_start(ctx, module, commit_id, remote_url)

    unload_success = True
    if registry.is_module_loaded(module):
        unload_success = await registry.unload_module(module)
        if not unload_success:
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to unload module `{module}`. Attempting cleanup.",
            )

    delete_success = await asyncio.to_thread(delete_module, module)
    if not delete_success:
        logger.exception("Failed to delete module directory for '%s'", module)
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to delete directory for module `{module}`. Manual cleanup required.",
        )
        return

    final_message = f"Unloaded module `{module}` and removed its directory."
    if not unload_success:
        final_message += (
            " (Extension unload encountered issues, but cleanup completed.)"
        )
    await reply_ok(hikari_client, ctx, final_message)

    completion_embed = await reply_embed(
        hikari_client,
        "Module Unloaded",
        f"`{module}` unloaded and deleted by {executor.mention}.",
    )
    await dm_role_members(embeds=[completion_embed])
