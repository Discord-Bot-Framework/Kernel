from __future__ import annotations

import asyncio
import os
import pathlib
import tarfile
import tempfile
from collections.abc import Sequence

import arc
import compression.zstd
import hikari

from src.container.app import get_hikari
from src.shared.constants import BASE_DIR
from src.shared.logger import logger
from src.shared.utils.view import defer, reply_err


def _resolve_export_target(user_path: str) -> tuple[pathlib.Path | None, str | None]:
    try:
        candidate = BASE_DIR.joinpath(user_path).resolve()
    except Exception:
        return None, "Invalid path format."

    if BASE_DIR not in candidate.parents and candidate != BASE_DIR:
        return None, "Path outside allowed directory."
    return candidate, None


def _build_archive(source: pathlib.Path, archive_path: pathlib.Path) -> None:
    with compression.zstd.ZstdFile(archive_path, mode="wb", level=6) as zstd_out:
        with tarfile.open(mode="w|", fileobj=zstd_out) as tar_file:
            tar_file.add(source, arcname=source.name)


async def autocomplete_debug_export(
    _ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Sequence[str]:
    choices: list[str] = ["all"]

    try:
        with os.scandir(BASE_DIR) as entries:
            files = [
                entry.name
                for entry in entries
                if entry.is_file() and not entry.name.startswith(".")
            ]
        choices.extend(sorted(files, key=str.casefold))
    except Exception:
        logger.exception("Failed to autocomplete export path")
        choices = ["error"]

    return choices[:25]


async def cmd_debug_export(
    ctx: arc.GatewayContext,
    path: str,
) -> None:
    await defer(ctx)
    hikari_client = get_hikari()

    target_path, path_error = _resolve_export_target(path)
    if target_path is None:
        await reply_err(hikari_client, ctx, path_error or "Invalid path format.")
        return

    if not target_path.exists():
        await reply_err(hikari_client, ctx, f"Path `{path}` does not exist.")
        return

    if not (target_path.is_file() or target_path.is_dir()):
        await reply_err(
            hikari_client,
            ctx,
            f"Path `{path}` is neither file nor directory.",
        )
        return

    try:
        with tempfile.TemporaryDirectory(prefix="export_") as temp_dir:
            archive_name = f"{target_path.name}.tar.zst"
            archive_path = pathlib.Path(temp_dir) / archive_name
            await asyncio.to_thread(_build_archive, target_path, archive_path)

            if not archive_path.is_file() or archive_path.stat().st_size == 0:
                await reply_err(
                    hikari_client,
                    ctx,
                    "Failed to create archive: output file missing.",
                )
                return

            await ctx.respond(
                f"Exported `{path}`:",
                attachments=[hikari.File(str(archive_path), filename=archive_name)],
            )
    except Exception:
        logger.exception("Failed to create export archive for '%s'", path)
        await reply_err(hikari_client, ctx, "Failed to create archive.")
