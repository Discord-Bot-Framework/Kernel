from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from src.container.app import get_hikari
from src.git.utils import clone_repo, parse_repo_url
from src.modules.python.pip import run_pip
from src.modules.registry import registry
from src.modules.utils import check_remote_module, delete_module
from src.shared.constants import EXTENSIONS_DIR
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import defer, reply_embed, reply_err, reply_ok

if TYPE_CHECKING:
    import arc
    import hikari


async def _progress(
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
    message: str,
) -> None:
    with contextlib.suppress(Exception):
        await reply_ok(hikari_client, ctx, message, title=None)


async def _announce_load_start(
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
    module_name: str,
) -> None:
    await dm_role_members(
        ctx,
        embeds=[
            await reply_embed(
                hikari_client,
                "Loading Module",
                f"{ctx.user.mention} is loading `{module_name}`.",
            ),
        ],
    )


async def _install_requirements_if_present(
    module_name: str,
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
) -> bool:
    requirements_path = EXTENSIONS_DIR / module_name / "requirements.txt"
    if not requirements_path.is_file():
        return True

    await _progress(hikari_client, ctx, "Installing dependencies.")
    pip_success = await asyncio.to_thread(run_pip, str(requirements_path), install=True)
    if pip_success:
        return True

    deleted = await asyncio.to_thread(delete_module, module_name)
    if deleted:
        await reply_err(
            hikari_client,
            ctx,
            "Failed to install dependencies: module removed.",
        )
    else:
        await reply_err(
            hikari_client,
            ctx,
            "Failed to install dependencies and cleanup failed. Manual intervention required.",
        )
    return False


async def cmd_module_load(
    ctx: arc.GatewayContext,
    url: str,
) -> None:
    await defer(ctx)
    hikari_client = get_hikari()

    git_url, parsed_name, validated_url = parse_repo_url(url)
    if not validated_url or not parsed_name:
        await reply_err(hikari_client, ctx, "Failed to parse Git URL: invalid format.")
        return

    if (EXTENSIONS_DIR / parsed_name).is_dir():
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to load module: `{parsed_name}` already exists.",
        )
        return

    await _progress(hikari_client, ctx, f"Validating `{parsed_name}`")
    remote_valid, remote_error_msg, _ = await check_remote_module(git_url)
    if not remote_valid:
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to validate module: {remote_error_msg}",
        )
        return

    await _announce_load_start(hikari_client, ctx, parsed_name)

    await _progress(hikari_client, ctx, f"Cloning `{parsed_name}`.")
    cloned_name, clone_success = await asyncio.to_thread(clone_repo, git_url)
    if not clone_success or not cloned_name:
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to clone repository `{parsed_name}`.",
        )
        return

    logger.info("Cloned module '%s' from '%s'", cloned_name, git_url)

    deps_ok = await _install_requirements_if_present(cloned_name, hikari_client, ctx)
    if not deps_ok:
        return

    await _progress(hikari_client, ctx, f"Loading `{cloned_name}`")
    loaded = await registry.load_module(hikari_client, cloned_name)
    if not loaded:
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to load module `{cloned_name}` after cloning.",
        )
        return

    await reply_ok(hikari_client, ctx, f"Loaded module `{cloned_name}`.")
    await dm_role_members(
        embeds=[
            await reply_embed(
                hikari_client,
                "Module Loaded",
                f"`{cloned_name}` loaded by {ctx.user.mention}.",
            ),
        ],
    )
