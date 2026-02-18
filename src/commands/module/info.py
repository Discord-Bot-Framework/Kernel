from __future__ import annotations

from typing import TYPE_CHECKING

from miru.ext import nav

from src.container.app import get_hikari, get_miru
from src.git.utils import RepoInfo, get_module_info
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
    import hikari

_MAX_CHANGELOG_FIELD_LEN = 1000


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


def _changelog_chunks(changelog: str) -> list[str]:
    text = changelog.strip() or "No changelog information available."
    return [
        text[index : index + _MAX_CHANGELOG_FIELD_LEN]
        for index in range(0, len(text), _MAX_CHANGELOG_FIELD_LEN)
    ]


async def _build_module_info_embed(module: str, info: RepoInfo) -> hikari.Embed:
    hikari_client = get_hikari()
    result_embed = await reply_embed(
        hikari_client,
        f"Module: `{module}`",
        f"Repository: {info.url}",
        _status_color(info),
    )
    result_embed.url = info.url

    result_embed.add_field(name="Status", value=_status_text(info), inline=False)
    result_embed.add_field(
        name="Local Commit",
        value=(
            f"ID: `{_commit_id(info.local_commit)}`\nTime: `{info.local_commit_time_utc or 'N/A'}`"
        ),
        inline=True,
    )
    result_embed.add_field(
        name="Remote Commit",
        value=(
            f"ID: `{_commit_id(info.remote_commit)}`\nTime: `{info.remote_commit_time_utc or 'N/A'}`"
        ),
        inline=True,
    )

    chunks = _changelog_chunks(info.changelog)
    for index, chunk in enumerate(chunks):
        field_name = "Recent Changes"
        if len(chunks) > 1:
            field_name = f"Recent Changes (Part {index + 1}/{len(chunks)})"
        result_embed.add_field(
            name=field_name,
            value=f"```md\n{chunk}\n```",
            inline=False,
        )

    return result_embed


async def cmd_module_info(
    ctx: arc.GatewayContext,
    module: str,
) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    miru_client = get_miru()

    info, valid = get_module_info(module)
    if not valid or info is None:
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to locate module `{module}`: not found or invalid repository. Use `/kernel module list`.",
        )
        return

    buttons: list[nav.NavItem] = [
        nav.PrevButton(),
        nav.StopButton(),
        nav.NextButton(),
    ]
    try:
        result_embed = await _build_module_info_embed(module, info)
        navigator = nav.navigator.NavigatorView(
            pages=[result_embed],
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
        logger.exception("Failed to send module info for '%s'", module)
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to display {module} information: {exc}.",
        )
