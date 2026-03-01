from __future__ import annotations

import asyncio
import contextlib
import datetime
import os
from typing import TYPE_CHECKING

from src.container.app import get_hikari
from src.git.utils import get_kernel_info
from src.modules.python.pip import run_pip
from src.modules.utils import pull_kernel
from src.shared.constants import BASE_DIR, FLAG_DIR, Color
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import defer, reply_embed, reply_err, reply_ok, response

if TYPE_CHECKING:
    import arc
    import hikari


def _commit_id(commit: object | None) -> str:
    commit_id = getattr(commit, "id", None)
    if commit_id is None:
        return "Unknown"
    return str(commit_id)


def _pull_error_message(code: int) -> str:
    error_reasons = {
        1: "Repository misidentified as kernel",
        2: "Failed to fetch or apply remote changes",
        3: "Master branch checkout failed",
    }
    return error_reasons.get(code, "Git pull failed with unknown error")


async def _progress(
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
    message: str,
) -> None:
    with contextlib.suppress(Exception):
        await reply_ok(hikari_client, ctx, message, title=None)


async def _announce_update_start(
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
    repo_url: str,
    current_commit_id: str,
    target_commit_id: str,
    uncommitted_changes: int,
) -> None:
    embed = await reply_embed(
        hikari_client,
        "Updating Kernel",
        f"{ctx.user.mention} is updating kernel.",
        Color.WARNING,
    )
    embed.url = repo_url
    embed.add_field(name="Current Commit", value=f"`{current_commit_id}`", inline=True)
    embed.add_field(name="Target Commit", value=f"`{target_commit_id}`", inline=True)
    if uncommitted_changes > 0:
        embed.add_field(
            name="Warning",
            value="Local modifications detected and will be overwritten.",
        )
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    try:
        await dm_role_members(ctx, embeds=[embed])
    except Exception:
        logger.exception("Failed to send kernel update start notification")


def _write_restart_flag(executor_id: hikari.Snowflake) -> None:
    FLAG_DIR.mkdir(exist_ok=True)
    reboot_flag = FLAG_DIR / "restart"
    reboot_flag.write_text(
        "Restart triggered post-kernel-update at "
        f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} by {executor_id}",
    )
    logger.info("Set restart flag at %s", reboot_flag)


async def cmd_kernel_update(ctx: arc.GatewayContext) -> None:
    await defer(ctx)
    executor = ctx.user
    hikari_client = get_hikari()

    logger.info(
        "User %s (%s) initiated kernel update",
        executor.username,
        executor.id,
    )

    info = get_kernel_info()
    if not info:
        await reply_err(
            hikari_client,
            ctx,
            "Failed to retrieve kernel repository information.",
        )
        return

    if (
        info.local_commit
        and info.remote_commit
        and info.local_commit.id == info.remote_commit.id
        and info.uncommitted_changes == 0
    ):
        await reply_ok(
            hikari_client,
            ctx,
            "Kernel already up-to-date (no local modifications).",
        )
        return

    current_commit_id = _commit_id(info.local_commit)
    target_commit_id = _commit_id(info.remote_commit)

    logger.info(
        "Initiated kernel update: cur=%s target=%s uncommitted_changes=%d",
        current_commit_id,
        target_commit_id,
        info.uncommitted_changes,
    )

    await _announce_update_start(
        hikari_client,
        ctx,
        info.url,
        current_commit_id,
        target_commit_id,
        info.uncommitted_changes,
    )

    await _progress(hikari_client, ctx, "Pulling kernel updates.")
    pull_result = await asyncio.to_thread(pull_kernel)

    if pull_result != 0:
        error_msg = _pull_error_message(pull_result)
        logger.exception(
            "Failed to pull kernel updates: %s (%s)",
            error_msg,
            pull_result,
        )
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to update kernel: {error_msg}.",
        )
        return

    await _progress(hikari_client, ctx, "Updating kernel dependencies.")
    pip_success = await asyncio.to_thread(
        run_pip,
        str(BASE_DIR / "requirements.txt"),
        install=True,
    )

    if not pip_success:
        logger.exception("Failed to update kernel dependencies")
        await reply_err(
            hikari_client,
            ctx,
            "Kernel updated but dependency update failed. Bot may be unstable. Check logs and requirements.txt.",
        )
    else:
        logger.info("Updated kernel dependencies")

    await _progress(
        hikari_client,
        ctx,
        "Kernel update complete. Signaling restart now.",
    )

    try:
        result_embed = await reply_embed(
            hikari_client,
            "Kernel Updated",
            f"Updated to `{target_commit_id[:7]}`. Restarting.",
        )
        await response(ctx, embeds=[result_embed])
    except Exception:
        logger.exception("Failed to send kernel update completion response")

    try:
        _write_restart_flag(executor.id)
        reboot_notice_embed = await reply_embed(
            hikari_client,
            "Restarting",
            "Kernel updated. Bot restarting.",
        )
        await dm_role_members(embeds=[reboot_notice_embed])
        logger.info("Scheduling process exit for kernel restart")
        asyncio.get_running_loop().call_later(0.5, os._exit, 0)
    except Exception:
        logger.exception("Failed to signal restart after kernel update")
        await reply_err(
            hikari_client,
            ctx,
            "Kernel updated but restart signaling failed. Manual restart required.",
        )
