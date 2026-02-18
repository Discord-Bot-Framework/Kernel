from __future__ import annotations

import asyncio
import contextlib
import shutil
from typing import TYPE_CHECKING

import aioshutil
import anyio
import pygit2
from miru.ext import nav

from src.container.app import get_hikari, get_miru
from src.git.utils import get_module_info
from src.modules.python.pip import run_pip
from src.modules.registry import registry
from src.modules.utils import pull_module
from src.shared.constants import BACKUP_DIR, EXTENSIONS_DIR, Color
from src.shared.logger import logger
from src.shared.utils.member import dm_role_members
from src.shared.utils.view import (
    defer,
    reply_embed,
    reply_err,
    reply_ok,
    respond_with_builder_and_bind_view,
)

if TYPE_CHECKING:
    import pathlib

    import arc
    import hikari


def _commit_id(commit: pygit2.Commit | None) -> str:
    if commit is None:
        return "Unknown"
    return str(commit.id)


async def _progress(
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
    message: str,
) -> None:
    with contextlib.suppress(Exception):
        await reply_ok(hikari_client, ctx, message, title=None)


async def _create_backup(
    module: str,
    module_dir: pathlib.Path,
    backup_base: pathlib.Path,
) -> tuple[bool, str | None, bool, pygit2.Oid | None]:
    patch_path: str | None = None
    has_local_changes = False
    original_commit_id: pygit2.Oid | None = None
    backup_base_async = anyio.Path(backup_base)

    try:
        BACKUP_DIR.mkdir(exist_ok=True)
        if await backup_base_async.exists():
            await asyncio.to_thread(shutil.rmtree, backup_base, ignore_errors=False)

        await asyncio.to_thread(shutil.copytree, module_dir, backup_base, symlinks=True)
        logger.info("Created backup of module '%s' at '%s'", module, backup_base)

        repo = pygit2.Repository(str(module_dir))
        if not repo.head_is_unborn and isinstance(repo.head.target, pygit2.Oid):
            original_commit_id = repo.head.target
        diff = repo.diff()
        if diff.patch:
            has_local_changes = True
            patch_candidate = backup_base_async / "local_changes.patch"
            await patch_candidate.write_text(diff.patch, encoding="utf-8")
            patch_path = str(patch_candidate)
            logger.info(
                "Saved local changes patch for '%s' to '%s'",
                module,
                patch_path,
            )
    except Exception:
        logger.exception("Failed to create backup for module '%s'", module)
        return False, None, False, None

    return True, patch_path, has_local_changes, original_commit_id


async def _restore_backup(
    module: str,
    module_dir: pathlib.Path,
    backup_base: pathlib.Path,
    original_commit_id: pygit2.Oid | None,
) -> bool:
    module_dir_async = anyio.Path(module_dir)
    backup_base_async = anyio.Path(backup_base)

    logger.info(
        "Attempting to restore module '%s' from backup '%s'",
        module,
        backup_base,
    )
    if not await backup_base_async.exists():
        logger.exception(
            "Failed to restore: backup directory '%s' missing",
            backup_base,
        )
        return False

    try:
        if await module_dir_async.exists():
            await aioshutil.rmtree(module_dir_async)
        await aioshutil.copytree(backup_base_async, module_dir_async, symlinks=True)

        if original_commit_id is not None:
            try:
                repo = pygit2.Repository(str(module_dir))
                from pygit2.enums import ResetMode

                repo.reset(original_commit_id, ResetMode.HARD)
                logger.info(
                    "Reset module '%s' to original commit %s",
                    module,
                    original_commit_id,
                )
            except Exception:
                logger.exception("Failed to reset module '%s' after restore", module)

        logger.info("Restored module '%s' from backup", module)
        return True
    except Exception:
        logger.exception("Failed to restore module '%s' from backup", module)
        return False


async def _cleanup_backup(backup_base: pathlib.Path) -> None:
    backup_base_async = anyio.Path(backup_base)
    with contextlib.suppress(Exception):
        if await backup_base_async.exists():
            await aioshutil.rmtree(backup_base_async)
            logger.info("Cleaned up backup '%s'", backup_base)


async def _apply_local_patch(
    hikari_client: hikari.GatewayBot,
    ctx: arc.GatewayContext,
    module: str,
    module_dir: pathlib.Path,
    patch_path: str,
) -> None:
    logger.info("Reapplying local changes from '%s'", patch_path)
    try:
        process = await asyncio.create_subprocess_exec(
            "git",
            "apply",
            "--reject",
            "--whitespace=fix",
            patch_path,
            cwd=str(module_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        stdout_str = stdout.decode(errors="ignore").strip()
        stderr_str = stderr.decode(errors="ignore").strip()

        if process.returncode == 0:
            logger.info("Reapplied local changes for '%s'", module)
            if stdout_str:
                logger.info("Git apply stdout: %s", stdout_str)
            if stderr_str:
                logger.info("Git apply stderr: %s", stderr_str)
            await reply_ok(hikari_client, ctx, "Reapplied local changes.", title=None)
            return

        logger.exception(
            "Failed to apply patch for '%s' (exit %d)",
            module,
            process.returncode,
        )
        if stdout_str:
            logger.exception("Git apply stdout: %s", stdout_str)
        if stderr_str:
            logger.exception("Git apply stderr: %s", stderr_str)
        await reply_err(
            hikari_client,
            ctx,
            f"Update completed but failed to reapply local changes. Changes preserved in `{patch_path}`.",
            ephemeral=False,
        )
    except Exception:
        logger.exception("Failed to reapply patch for '%s'", module)
        await reply_err(
            hikari_client,
            ctx,
            "Update completed but failed to reapply local changes.",
        )


def _changelog_text(module_dir: pathlib.Path) -> str:
    changelog_path = module_dir / "CHANGELOG"
    if not changelog_path.is_file():
        return "No changelog provided."
    with contextlib.suppress(Exception):
        return changelog_path.read_text(encoding="utf-8", errors="ignore")
    return "No changelog provided."


async def cmd_module_update(
    ctx: arc.GatewayContext,
    module: str,
) -> None:
    await defer(ctx)
    executor = ctx.user
    hikari_client = get_hikari()
    miru_client = get_miru()
    module_dir = EXTENSIONS_DIR / module
    backup_base = BACKUP_DIR / module

    logger.info(
        "User %s (%s) requested update of module: %s",
        executor.username,
        executor.id,
        module,
    )

    if not module_dir.is_dir():
        await reply_err(hikari_client, ctx, f"Directory `{module}` not found.")
        return

    info, valid_info = get_module_info(module)
    if not valid_info or not info:
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to retrieve repository info for module `{module}`.",
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
            f"Module `{module}` already up-to-date (no local modifications).",
        )
        return

    current_commit_id = _commit_id(info.local_commit)
    target_commit_id = _commit_id(info.remote_commit)

    logger.info(
        "Initiated update for module %s: cur=%s target=%s uncommitted_changes=%d",
        module,
        current_commit_id,
        target_commit_id,
        info.uncommitted_changes,
    )

    embed = await reply_embed(
        hikari_client,
        "Updating Module",
        f"{executor.mention} is updating `{module}`.",
        Color.WARNING,
    )
    embed.url = info.url
    embed.add_field(name="Current Commit", value=f"`{current_commit_id}`", inline=True)
    embed.add_field(name="Target Commit", value=f"`{target_commit_id}`", inline=True)
    if info.uncommitted_changes > 0:
        embed.add_field(name="Warning", value="Local modifications detected!")
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    try:
        await dm_role_members(ctx, embeds=[embed])
    except Exception:
        logger.exception("Failed to send module update start notification")

    await _progress(hikari_client, ctx, f"Backing up module `{module}`.")
    backup_ok, patch_path, has_local_changes, original_commit_id = await _create_backup(
        module,
        module_dir,
        backup_base,
    )
    if not backup_ok:
        await reply_err(
            hikari_client,
            ctx,
            f"Failed to create backup for {module}. Update aborted.",
        )
        return

    await _progress(hikari_client, ctx, f"Pulling updates for module `{module}`.")
    pull_result = await asyncio.to_thread(pull_module, module)

    if pull_result != 0:
        error_reasons = {
            1: "Cannot update the main repo using this command.",
            2: "Failed to fetch or apply remote changes.",
            3: "Master branch not found or checkout failed.",
        }
        error_msg = error_reasons.get(pull_result, "Git pull failed with unknown error")
        logger.exception(
            "Failed to pull updates for module '%s': %s (%s)",
            module,
            error_msg,
            pull_result,
        )
        if await _restore_backup(module, module_dir, backup_base, original_commit_id):
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to update module: {error_msg}. Restored from backup.",
            )
        else:
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to update module: {error_msg}. CRITICAL: Backup restoration failed. Manual intervention required.",
            )
        return

    await _progress(hikari_client, ctx, f"Updating dependencies for `{module}`.")
    pip_success = await asyncio.to_thread(
        run_pip,
        str(module_dir / "requirements.txt"),
        install=True,
    )

    if not pip_success:
        logger.exception("Failed to update dependencies for module '%s'", module)
        if await _restore_backup(module, module_dir, backup_base, original_commit_id):
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to update dependencies for `{module}`. Restored from backup.",
            )
        else:
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to update dependencies for `{module}`. CRITICAL: Backup restoration failed. Manual intervention required.",
            )
        return

    await _progress(hikari_client, ctx, f"Validating and reloading module `{module}`.")
    reload_success = await registry.load_module(hikari_client, module, is_reload=True)

    if not reload_success:
        logger.exception("Failed to reload module '%s' after update", module)
        if await _restore_backup(module, module_dir, backup_base, original_commit_id):
            logger.info("Loading restored version of module '%s'", module)
            final_load_success = await registry.load_module(
                hikari_client,
                module,
                is_reload=False,
            )

            if final_load_success:
                await reply_err(
                    hikari_client,
                    ctx,
                    f"Failed to reload updated module `{module}`: previous version restored and loaded.",
                )
            else:
                await reply_err(
                    hikari_client,
                    ctx,
                    f"Failed to reload updated module `{module}`: restored from backup but reload failed. Manual intervention required.",
                )
        else:
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to reload updated module `{module}`. CRITICAL: Backup restoration failed. Manual intervention required.",
            )
        await _cleanup_backup(backup_base)
        return

    if has_local_changes and patch_path and anyio.Path(patch_path).exists():
        await _apply_local_patch(hikari_client, ctx, module, module_dir, patch_path)

    try:
        changelog_content = _changelog_text(module_dir)

        result_embed = await reply_embed(
            hikari_client,
            "Module Updated",
            f"Updated module `{module}` to commit `{target_commit_id[:7]}`.",
        )

        max_field_len = 1000
        changelog_chunks = [
            changelog_content[i : i + max_field_len]
            for i in range(0, len(changelog_content), max_field_len)
        ]

        for i, chunk in enumerate(changelog_chunks):
            field_name = (
                "CHANGELOG"
                if len(changelog_chunks) == 1
                else f"CHANGELOG (Part {i + 1}/{len(changelog_chunks)})"
            )
            result_embed.add_field(
                name=field_name,
                value=f"```md\n{chunk.strip() or 'No CHANGELOG available.'}\n```",
            )

        buttons: list[nav.NavItem] = [
            nav.PrevButton(),
            nav.StopButton(),
            nav.NextButton(),
        ]
        pages = [result_embed]
        navigator = nav.navigator.NavigatorView(
            pages=pages,
            items=buttons,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client, ephemeral=True)
        await respond_with_builder_and_bind_view(
            ctx=ctx,
            builder=builder,
            miru_client=miru_client,
            view=navigator,
        )
    except Exception:
        logger.exception("Failed to send update confirmation")
        await reply_ok(
            hikari_client,
            ctx,
            f"Updated `{module}` but failed to display details.",
        )
    else:
        try:
            completion_embed = await reply_embed(
                hikari_client,
                "Module Updated",
                f"`{module}` updated by {executor.mention}.",
            )
            await dm_role_members(embeds=[completion_embed])
        except Exception:
            logger.exception("Failed to send module update completion notification")

    await _cleanup_backup(backup_base)
