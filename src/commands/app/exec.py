from __future__ import annotations

import io
import textwrap
import traceback
import typing
from collections.abc import Iterable
from contextlib import redirect_stdout

import hikari
import miru

from src.container.app import get_arc, get_hikari, get_miru
from src.shared.constants import TOKEN
from src.shared.logger import logger
from src.shared.utils.view import reply_err

if typing.TYPE_CHECKING:
    import arc

__all__ = ("cmd_app_exec",)

_MAX_MESSAGE_LEN = 2000
_CODE_BLOCK_OVERHEAD = len("```py\n\n```")
_MAX_CODE_PAYLOAD = _MAX_MESSAGE_LEN - _CODE_BLOCK_OVERHEAD

_exec_cache: dict[int, str] = {}


def _strip_code_block(body: str) -> str:
    cleaned = body.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 2:
            return "\n".join(lines[1:-1])
    return cleaned.strip("` \n")


def _split_chunks(value: str, max_len: int) -> list[str]:
    if not value:
        return [""]
    return [value[i : i + max_len] for i in range(0, len(value), max_len)]


def _sanitize_output(value: str) -> str:
    if TOKEN:
        return value.replace(TOKEN, "[REDACTED TOKEN]")
    return value


async def _respond_code_chunks(
    ctx: miru.ModalContext,
    source: str,
) -> None:
    chunks = _split_chunks(source, _MAX_CODE_PAYLOAD)
    for chunk in chunks:
        await ctx.respond(
            f"```py\n{chunk}\n```",
            flags=hikari.MessageFlag.EPHEMERAL,
        )


async def _respond_embed_pages(
    ctx: miru.ModalContext,
    embeds: list[hikari.Embed],
) -> None:
    if not embeds:
        return
    for i in range(0, len(embeds), 10):
        await ctx.respond(
            embeds=embeds[i : i + 10],
            flags=hikari.MessageFlag.EPHEMERAL,
        )


class _DebugExecModal(miru.Modal):
    def __init__(self, command_ctx: arc.GatewayContext) -> None:
        super().__init__(title="Debug-Exec", custom_id="kernel:debug:exec")
        self._command_ctx = command_ctx
        self._author_id = int(command_ctx.user.id)

        last_code = _exec_cache.get(self._author_id)
        self.body = miru.TextInput(
            label="Code to run",
            custom_id="body",
            style=hikari.TextInputStyle.PARAGRAPH,
            value=last_code,
            placeholder="Write your code here!",
            required=True,
        )
        self.add_item(self.body)

    async def callback(self, ctx: miru.ModalContext) -> None:
        if int(ctx.user.id) != self._author_id:
            await ctx.respond(
                "This modal is not for you.",
                flags=hikari.MessageFlag.EPHEMERAL,
            )
            return

        raw_body = self.body.value or ""
        _exec_cache[self._author_id] = raw_body
        body = _strip_code_block(raw_body)

        env: dict[str, object] = {
            "hikari_client": get_hikari(),
            "arc_client": get_arc(),
            "miru_client": get_miru(),
            "ctx": ctx,
            "command_ctx": self._command_ctx,
            "channel": getattr(ctx, "channel", None),
            "author": ctx.user,
            "guild": getattr(ctx, "guild", None),
            "bot": get_hikari(),
            "__name__": "__debug_exec__",
        }
        env.update(globals())

        stdout = io.StringIO()
        to_compile = "async def __debug_exec_func__():\n{}".format(
            textwrap.indent(
                body,
                "  ",
            ),
        )

        try:
            exec(to_compile, env)
        except SyntaxError:
            await _respond_code_chunks(ctx, traceback.format_exc())
            return

        func_obj = env.get("__debug_exec_func__")
        if not callable(func_obj):
            await ctx.respond(
                "Failed to compile executable function.",
                flags=hikari.MessageFlag.EPHEMERAL,
            )
            return

        try:
            with redirect_stdout(stdout):
                result = await typing.cast("typing.Awaitable[object]", func_obj())
        except Exception:
            await _respond_code_chunks(
                ctx,
                f"{stdout.getvalue()}{traceback.format_exc()}",
            )
            return

        await self._handle_exec_result(
            ctx=ctx,
            result=result,
            stdout_value=stdout.getvalue(),
            body=body,
        )

    async def _handle_exec_result(
        self,
        *,
        ctx: miru.ModalContext,
        result: object,
        stdout_value: str,
        body: str,
    ) -> None:
        await _respond_code_chunks(ctx, body)

        if result is None:
            result = stdout_value or "No Output!"

        elif isinstance(result, hikari.Message):
            jump_url = getattr(result, "jump_url", None)
            if jump_url:
                await ctx.respond(
                    str(jump_url),
                    flags=hikari.MessageFlag.EPHEMERAL,
                )
                return
            result = result.content or "No Output!"

        elif isinstance(result, hikari.Embed):
            await ctx.respond(
                embeds=[result],
                flags=hikari.MessageFlag.EPHEMERAL,
            )
            return

        elif isinstance(result, (hikari.File, hikari.Bytes)):
            await ctx.respond(
                attachments=[result],
                flags=hikari.MessageFlag.EPHEMERAL,
            )
            return

        elif isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
            items = list(result)
            if items and all(isinstance(item, hikari.Embed) for item in items):
                await _respond_embed_pages(
                    ctx, typing.cast("list[hikari.Embed]", items)
                )
                return
            result = repr(items)

        if not isinstance(result, str):
            result = repr(result)

        await _respond_code_chunks(ctx, _sanitize_output(result))


async def cmd_app_exec(ctx: arc.GatewayContext) -> None:
    try:
        miru_client = get_miru()
        modal = _DebugExecModal(ctx)
        builder = modal.build_response(miru_client)
        await ctx.respond_with_builder(builder)
        miru_client.start_modal(modal)
    except Exception as exc:
        logger.exception("Failed to open debug exec modal")
        await reply_err(get_hikari(), ctx, f"Failed to open debug exec modal: {exc}")
