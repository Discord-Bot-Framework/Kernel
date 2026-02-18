from __future__ import annotations

from typing import TYPE_CHECKING

from miru.ext import nav

from src.container.app import get_hikari, get_miru
from src.git.utils import RepoInfo, get_kernel_info
from src.shared.constants import Color
from src.shared.logger import logger
from src.shared.utils.view import (
    defer,
    reply_embed,
    reply_err,
    respond_with_builder_and_bind_view,
)

if TYPE_CHECKING:
    import arc


def _commit_id(commit: object | None) -> str:
    commit_id = getattr(commit, "id", None)
    if commit_id is None:
        return "N/A"
    return str(commit_id)[:10]


def _status_text(info: RepoInfo) -> str:
    if info.uncommitted_changes > 0:
        return f"{info.uncommitted_changes} local modification(s) detected"
    return "No local modifications"


def _status_color(info: RepoInfo) -> Color:
    return Color.WARNING if info.uncommitted_changes > 0 else Color.INFO


async def cmd_kernel_info(ctx: arc.GatewayContext) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    miru_client = get_miru()

    info = get_kernel_info()
    if info is None:
        await reply_err(
            hikari_client,
            ctx,
            "Failed to retrieve kernel repository information.",
        )
        return

    try:
        embed = await reply_embed(
            hikari_client,
            "Kernel",
            f"Repository: {info.url}",
            _status_color(info),
        )
        embed.url = info.url
        embed.add_field(name="Status", value=_status_text(info), inline=False)
        embed.add_field(
            name="Local Commit",
            value=(
                f"ID: `{_commit_id(info.local_commit)}`\nTime: `{info.local_commit_time_utc or 'N/A'}`"
            ),
            inline=True,
        )
        embed.add_field(
            name="Remote Commit",
            value=(
                f"ID: `{_commit_id(info.remote_commit)}`\nTime: `{info.remote_commit_time_utc or 'N/A'}`"
            ),
            inline=True,
        )

        buttons: list[nav.NavItem] = [
            nav.PrevButton(),
            nav.StopButton(),
            nav.NextButton(),
        ]
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
        logger.exception("Failed to send kernel information")
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to display kernel information: {exc}.",
        )
