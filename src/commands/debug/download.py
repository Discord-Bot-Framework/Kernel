from __future__ import annotations

import asyncio
import contextlib
import pathlib
import tarfile
import tempfile
from typing import TYPE_CHECKING

import compression.zstd
import hikari

from src.container.app import get_hikari
from src.shared.constants import BASE_DIR
from src.shared.logger import logger
from src.shared.utils.view import defer, reply_err

if TYPE_CHECKING:
    import arc

_dl_lock = asyncio.Lock()
_EXCLUDED_NAMES = frozenset({".git", "venv", "__pycache__", ".env", ".bak", "flag"})
_EXCLUDED_PATTERNS = frozenset({"*.pyc", "*.log"})


def _should_exclude(tar_name: str) -> bool:
    path = pathlib.PurePosixPath(tar_name)
    for part in path.parts:
        if part in _EXCLUDED_NAMES:
            return True
        if any(
            pathlib.PurePosixPath(part).match(pattern) for pattern in _EXCLUDED_PATTERNS
        ):
            return True
    return False


def _build_archive(destination: pathlib.Path, source_path: pathlib.Path) -> None:
    def tar_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
        if _should_exclude(tarinfo.name):
            logger.info("Excluding '%s' from archive", tarinfo.name)
            return None
        return tarinfo

    with compression.zstd.ZstdFile(destination, mode="wb", level=6) as zstd_out:
        with tarfile.open(mode="w|", fileobj=zstd_out) as tar:
            tar.add(source_path, arcname=".", filter=tar_filter)
    logger.info("Compressed code to '%s'", destination)


async def cmd_debug_download(ctx: arc.GatewayContext) -> None:
    if _dl_lock.locked():
        await ctx.respond(
            "Download already in progress.",
            flags=hikari.MessageFlag.EPHEMERAL,
        )
        return

    async with _dl_lock:
        await defer(ctx)
        hikari_client = get_hikari()
        logger.info(
            "User %s (%s) requested code download",
            ctx.user.username,
            ctx.user.id,
        )

        temp_file_path: pathlib.Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                suffix=".tar.zst",
                prefix="Discord-Bot-Framework_",
                delete=False,
            ) as tmp:
                temp_file_path = pathlib.Path(tmp.name)

            await asyncio.to_thread(_build_archive, temp_file_path, BASE_DIR)

            file_size = temp_file_path.stat().st_size
            if file_size <= 0:
                msg = "Archive is empty"
                raise RuntimeError(msg)

            logger.info(
                "Sending archive '%s' (%d bytes)",
                temp_file_path,
                file_size,
            )
            await ctx.respond(
                "Code archive attached:",
                attachments=[
                    hikari.File(str(temp_file_path), filename="client_code.tar.zst"),
                ],
            )

        except Exception as exc:
            logger.exception("Failed to complete code download")
            await reply_err(
                hikari_client,
                ctx,
                f"Failed to complete download: {exc}.",
            )
        finally:
            if temp_file_path is not None:
                with contextlib.suppress(Exception):
                    temp_file_path.unlink(missing_ok=True)
                    logger.info("Cleaned up temporary file: %s", temp_file_path)
