from __future__ import annotations

import os
from typing import TYPE_CHECKING

from miru.ext import nav

from src.container.app import get_hikari, get_miru
from src.git.utils import get_module_info, is_valid_repo
from src.shared.constants import EXTENSIONS_DIR
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


_MAX_FIELDS = 25


def _display_name(module_name: str) -> str:
    return (
        module_name.rsplit("__", maxsplit=1)[-1]
        .replace("_s_", "/")
        .replace("_u_", "_")
        .replace("_d_", ".")
        .replace("_h_", "-")
    )


def get_loadable_modules() -> list[str]:
    try:
        with os.scandir(EXTENSIONS_DIR) as entries:
            modules = [
                entry.name
                for entry in entries
                if entry.is_dir()
                and entry.name != "__pycache__"
                and is_valid_repo(entry.name)
            ]
    except Exception:
        logger.exception("Failed to enumerate loadable modules")
        return []
    return sorted(modules, key=str.casefold)


async def _build_module_list_embed(modules_list: list[str]) -> hikari.Embed:
    hikari_client = get_hikari()
    embed = await reply_embed(
        hikari_client,
        "Module List",
        f"Discovered {len(modules_list)} loadable modules:",
    )

    for index, module_name in enumerate(modules_list):
        if index >= _MAX_FIELDS:
            embed.set_footer(text=f"Displaying first {_MAX_FIELDS} modules.")
            break

        try:
            info, valid = get_module_info(module_name)
            if valid and info is not None:
                commit_id = (
                    str(info.local_commit.id)[:7] if info.local_commit else "N/A"
                )
                status = "WARNING " if info.uncommitted_changes > 0 else ""
                embed.add_field(
                    name=f"{status}{_display_name(module_name)}",
                    value=f"`{module_name}`\nCommit: `{commit_id}`",
                    inline=True,
                )
            else:
                embed.add_field(
                    name=module_name,
                    value="*Error fetching info*",
                    inline=True,
                )
        except Exception:
            logger.exception("Failed to process module '%s' for list", module_name)
            embed.add_field(name=module_name, value="*Error*", inline=True)

    return embed


async def cmd_module_list(ctx: arc.GatewayContext) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    miru_client = get_miru()

    modules_list = get_loadable_modules()
    if not modules_list:
        await reply_err(
            hikari_client,
            ctx,
            "Failed to locate any modules: extensions directory empty. Use `/kernel module load` to add modules.",
        )
        return

    embed = await _build_module_list_embed(modules_list)
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
        logger.exception("Failed to send module list")
        await reply_err(hikari_client, ctx, f"Failed to display module list: {exc}.")
