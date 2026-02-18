from __future__ import annotations

import arc
import hikari
import orjson

from src.shared.constants import GUILD_ID


def make_hikari_client(token: str) -> hikari.GatewayBot:
    return hikari.GatewayBot(
        token=token,
        banner=None,
        dumps=orjson.dumps,
        loads=orjson.loads,
        logs=None,
        intents=hikari.Intents.ALL,
    )


def make_arc_client(hikari_client: hikari.GatewayBot) -> arc.GatewayClient:
    default_guilds: tuple[hikari.Snowflake, ...] | hikari.UndefinedType
    default_guilds = (hikari.Snowflake(GUILD_ID),) if GUILD_ID else hikari.UNDEFINED
    return arc.GatewayClient(hikari_client, default_enabled_guilds=default_guilds)
