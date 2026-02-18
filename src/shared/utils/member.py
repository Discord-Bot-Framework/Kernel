from __future__ import annotations

import asyncio
import typing
from collections.abc import Sequence

import hikari

from src.container.app import get_hikari
from src.shared.constants import GUILD_ID, ROLE_ID
from src.shared.logger import logger

if typing.TYPE_CHECKING:
    import arc


def _resolve_guild_id(
    ctx: arc.GatewayContext | None,
) -> hikari.Snowflake:
    if ctx is not None and ctx.guild_id:
        return hikari.Snowflake(ctx.guild_id)
    return hikari.Snowflake(GUILD_ID)


def _has_target_role(member: hikari.Member) -> bool:
    role_ids = getattr(member, "role_ids", None)
    if role_ids is not None:
        return any(int(role_id) == ROLE_ID for role_id in role_ids)
    return any(int(role.id) == ROLE_ID for role in member.get_roles())


async def _collect_role_members(
    hikari_client: hikari.GatewayBot,
    guild_id: hikari.Snowflake,
) -> list[hikari.Member]:
    members_view = hikari_client.cache.get_members_view_for_guild(guild_id)
    if members_view:
        return [member for member in members_view.values() if _has_target_role(member)]
    if not hikari_client.is_alive:
        return []
    try:
        return [
            member
            async for member in hikari_client.rest.fetch_members(guild_id)
            if _has_target_role(member)
        ]
    except hikari.ComponentStateConflictError:
        return []


async def _send_dm(
    member: hikari.Member | hikari.User,
    *,
    me_id: hikari.Snowflake | None,
    msg: str | None,
    embeds: Sequence[hikari.Embed] | hikari.UndefinedType,
    components: Sequence[hikari.api.ComponentBuilder] | hikari.UndefinedType,
    semaphore: asyncio.Semaphore,
) -> hikari.Message | None:
    if me_id is not None and member.id == me_id:
        return None
    async with semaphore:
        try:
            return await member.send(content=msg, embeds=embeds, components=components)
        except Exception:
            logger.exception("Failed to DM member %s", member.id)
            return None


async def dm_role_members(
    ctx: arc.GatewayContext | None = None,
    msg: str | None = None,
    *,
    embeds: Sequence[hikari.Embed] | None = None,
    components: Sequence[hikari.api.ComponentBuilder]
    | hikari.UndefinedType = hikari.UNDEFINED,
) -> list[hikari.Message]:
    if not (ROLE_ID and GUILD_ID):
        return []

    hikari_client = get_hikari()
    me = hikari_client.get_me()
    guild_id = _resolve_guild_id(ctx)
    members = await _collect_role_members(hikari_client, guild_id)
    if not members:
        return []

    embeds_payload: Sequence[hikari.Embed] | hikari.UndefinedType
    embeds_payload = list(embeds) if embeds else hikari.UNDEFINED

    unique_members: dict[hikari.Snowflake, hikari.Member | hikari.User] = {
        member.id: member for member in members
    }
    semaphore = asyncio.Semaphore(10)
    results = await asyncio.gather(
        *(
            _send_dm(
                member,
                me_id=me.id if me is not None else None,
                msg=msg,
                embeds=embeds_payload,
                components=components,
                semaphore=semaphore,
            )
            for member in unique_members.values()
        ),
    )
    return [message for message in results if message is not None]
