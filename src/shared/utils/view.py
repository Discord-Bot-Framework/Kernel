from __future__ import annotations

import contextlib
import datetime
import typing
from collections.abc import Sequence

import hikari

from src.shared.constants import Color
from src.shared.logger import logger

if typing.TYPE_CHECKING:
    import arc
    import miru


def _normalize_embeds(
    embeds: hikari.Embed | Sequence[hikari.Embed] | None,
) -> Sequence[hikari.Embed] | hikari.UndefinedType:
    if embeds is None:
        return hikari.UNDEFINED
    if isinstance(embeds, hikari.Embed):
        return [embeds]
    if not embeds:
        return hikari.UNDEFINED
    return embeds


def _message_flags(ephemeral: bool) -> hikari.MessageFlag:
    if ephemeral:
        return hikari.MessageFlag.EPHEMERAL
    return hikari.MessageFlag.NONE


def _has_response(
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context,
) -> bool:
    return bool(getattr(ctx, "issued_response", False))


async def _respond(
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context,
    *,
    content: str,
    embeds: Sequence[hikari.Embed] | hikari.UndefinedType,
    components: Sequence[hikari.api.ComponentBuilder] | hikari.UndefinedType,
    flags: hikari.MessageFlag | hikari.UndefinedType,
) -> object:
    return await ctx.respond(
        content=content,
        embeds=embeds,
        components=components,
        flags=flags,
    )


async def _edit_response(
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context,
    *,
    content: str | hikari.UndefinedType,
    embeds: Sequence[hikari.Embed] | hikari.UndefinedType,
    components: Sequence[hikari.api.ComponentBuilder] | hikari.UndefinedType,
) -> None:
    if hasattr(ctx, "edit_initial_response"):
        gateway_ctx = typing.cast("arc.GatewayContext", ctx)
        await gateway_ctx.edit_initial_response(
            content=content,
            embeds=embeds,
            components=components,
        )
        return
    if hasattr(ctx, "edit_response"):
        miru_ctx = typing.cast("miru.abc.Context", ctx)
        await miru_ctx.edit_response(
            content=content,
            embeds=embeds,
            components=components,
        )
        return
    response_obj = await _respond(
        ctx,
        content=content if isinstance(content, str) else "",
        embeds=embeds,
        components=components,
        flags=hikari.UNDEFINED,
    )
    if response_obj is not None and hasattr(response_obj, "edit"):
        editable = typing.cast("arc.InteractionResponse", response_obj)
        await editable.edit(
            content=content,
            embeds=embeds,
            components=components,
        )


async def response(
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context,
    *,
    content: str = "",
    embeds: hikari.Embed | Sequence[hikari.Embed] | None = None,
    components: Sequence[hikari.api.ComponentBuilder]
    | hikari.UndefinedType = hikari.UNDEFINED,
    ephemeral: bool = True,
) -> None:
    normalized_embeds = _normalize_embeds(embeds)
    response_content = content or hikari.UNDEFINED
    try:
        if _has_response(ctx):
            await _edit_response(
                ctx,
                content=response_content,
                embeds=normalized_embeds,
                components=components,
            )
            return
        await _respond(
            ctx,
            content=content,
            embeds=normalized_embeds,
            components=components,
            flags=_message_flags(ephemeral),
        )
    except Exception:
        logger.exception("Failed to send or edit response")


async def reply_embed(
    hikari_client: hikari.GatewayBot | hikari.GatewayBotAware | arc.GatewayClient,
    title: str | None,
    description: str = "",
    color: Color = Color.INFO,
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context | None = None,
    fields: list[hikari.EmbedField] | None = None,
) -> hikari.Embed:
    me: hikari.OwnUser | None = getattr(hikari_client, "get_me", lambda: None)()

    embed = hikari.Embed(
        description=description,
        color=int(color),
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )

    if title is not None:
        embed.title = title

    if ctx and (user := getattr(ctx, "user", None)) and not user.is_bot:
        embed.set_author(name=user.username, icon=user.display_avatar_url)
    elif me:
        embed.set_author(name=me.username, icon=me.display_avatar_url)

    if fields:
        for field in fields:
            embed.add_field(
                name=field.name,
                value=field.value,
                inline=field.is_inline,
            )

    guild_id = getattr(ctx, "guild_id", None) if ctx else None
    if guild_id:
        try:
            guild = await hikari_client.rest.fetch_guild(guild_id)
            if guild:
                embed.set_footer(
                    text=guild.name,
                    icon=guild.make_icon_url() if guild.icon_hash else None,
                )
        except Exception:
            if me:
                embed.set_footer(text=me.username, icon=me.display_avatar_url)
    elif me:
        embed.set_footer(text=me.username, icon=me.display_avatar_url)

    return embed


async def reply_err(
    hikari_client: hikari.GatewayBot | hikari.GatewayBotAware | arc.GatewayClient,
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context | None,
    message: str,
    *,
    ephemeral: bool = True,
) -> None:
    if ctx is None:
        return
    embed = await reply_embed(hikari_client, "Exception", message[:1900], Color.ERROR)
    await response(
        ctx,
        embeds=embed,
        ephemeral=ephemeral,
    )


async def reply_ok(
    hikari_client: hikari.GatewayBot | hikari.GatewayBotAware | arc.GatewayClient,
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context | None,
    message: str,
    *,
    title: str | None = "Completion",
    ephemeral: bool = True,
) -> None:
    if ctx is None:
        return
    embed = await reply_embed(hikari_client, title, message)
    await response(
        ctx,
        embeds=embed,
        ephemeral=ephemeral,
    )


async def defer(
    ctx: arc.GatewayContext | arc.Context | miru.abc.Context | None,
) -> None:
    if ctx is None:
        return
    if _has_response(ctx):
        return
    try:
        await ctx.defer(flags=hikari.MessageFlag.EPHEMERAL)
    except Exception:
        logger.exception("Failed to defer response")
        with contextlib.suppress(Exception):
            await ctx.respond(
                "Failed to defer.",
                flags=hikari.MessageFlag.EPHEMERAL,
            )


async def bind_view_to_response(
    *,
    response_obj: object | None,
    miru_client: miru.Client,
    view: miru.View,
) -> hikari.Message | None:
    if response_obj is None or not hasattr(response_obj, "retrieve_message"):
        return None
    try:
        interaction_response = typing.cast("arc.InteractionResponse", response_obj)
        message = await interaction_response.retrieve_message()
    except Exception:
        logger.exception("Failed to retrieve response message for miru view binding")
        return None

    try:
        miru_client.start_view(view, bind_to=message)
    except Exception:
        logger.exception("Failed to bind miru view to response message")
        return None
    return message


async def respond_with_builder_and_bind_view(
    *,
    ctx: arc.GatewayContext,
    builder: hikari.api.InteractionMessageBuilder
    | hikari.api.InteractionDeferredBuilder,
    miru_client: miru.Client,
    view: miru.View,
) -> hikari.Message | None:
    response_obj = await ctx.respond_with_builder(builder)
    return await bind_view_to_response(
        response_obj=response_obj,
        miru_client=miru_client,
        view=view,
    )
