from __future__ import annotations

import pprint
from collections.abc import Mapping, Sequence, Sized
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import hikari

from src.container.app import get_arc, get_hikari
from src.shared.logger import logger
from src.shared.utils.view import defer, reply_embed, reply_err

if TYPE_CHECKING:
    import arc

try:
    from thefuzz import process as fuzz_process
except ImportError:
    fuzz_process = None


_MAX_CHOICES = 25


@runtime_checkable
class _SupportsToDict(Protocol):
    def to_dict(self) -> object: ...


def _parse_int(value: str, field: str) -> int:
    stripped = value.strip()
    if not stripped:
        msg = f"{field} is empty."
        raise ValueError(msg)
    return int(stripped)


def _parse_scope(scope: str) -> hikari.Snowflake | hikari.UndefinedType:
    scope_int = _parse_int(scope, "scope")
    if scope_int == 0:
        return hikari.UNDEFINED
    return hikari.Snowflake(scope_int)


def _scope_label(guild_scope: hikari.Snowflake | hikari.UndefinedType) -> str:
    if guild_scope is hikari.UNDEFINED:
        return "global"
    return str(int(guild_scope))


def _get_application_id(hikari_client: hikari.GatewayBot) -> hikari.Snowflake:
    me = hikari_client.get_me()
    if me is None:
        msg = "Bot identity unavailable."
        raise RuntimeError(msg)
    return me.id


def _safe_len(value: object | None) -> int:
    if isinstance(value, Sized):
        return len(value)
    return 0


def _collect_local_command_objects(arc_client: arc.GatewayClient) -> list[object]:
    commands: list[object] = []
    seen_ids: set[int] = set()

    for attr_name in (
        "_slash_commands",
        "_message_commands",
        "_user_commands",
        "_commands",
    ):
        raw = getattr(arc_client, attr_name, None)
        if isinstance(raw, Mapping):
            values = list(raw.values())
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            values = list(raw)
        else:
            continue

        for entry in values:
            entry_id = id(entry)
            if entry_id in seen_ids:
                continue
            seen_ids.add(entry_id)
            commands.append(entry)
    return commands


def _serialize_obj(value: object) -> object:
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, Mapping):
        return {str(k): _serialize_obj(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_serialize_obj(v) for v in value]
    if isinstance(value, _SupportsToDict):
        try:
            return _serialize_obj(value.to_dict())
        except Exception:
            return repr(value)
    if hasattr(value, "__dict__"):
        raw_dict = vars(value)
        result: dict[str, object] = {}
        for key, item in raw_dict.items():
            if key.startswith("_"):
                continue
            try:
                result[key] = _serialize_obj(item)
            except Exception:
                result[key] = repr(item)
        if result:
            return result
    return repr(value)


def _filename_stem(name: str) -> str:
    safe = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name.strip()
    )
    return safe or "command"


async def _respond_json_attachment(
    ctx: arc.GatewayContext,
    payload: object,
    *,
    name: str,
) -> None:
    content = pprint.pformat(payload, indent=2, sort_dicts=True, width=120)
    attachment = hikari.Bytes(content.encode("utf-8"), f"{_filename_stem(name)}.json")
    await ctx.respond(
        attachments=[attachment],
        flags=hikari.MessageFlag.EPHEMERAL,
    )


async def _fetch_remote_commands(
    *,
    hikari_client: hikari.GatewayBot,
    guild_scope: hikari.Snowflake | hikari.UndefinedType,
) -> Sequence[hikari.PartialCommand]:
    app_id = _get_application_id(hikari_client)
    return await hikari_client.rest.fetch_application_commands(
        application=app_id,
        guild=guild_scope,
    )


def _scope_from_autocomplete(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> str | None:
    options = getattr(ctx, "options", None)
    if isinstance(options, Mapping):
        value = options.get("scope")
        if value is not None:
            return str(value)
    kwargs = getattr(ctx, "kwargs", None)
    if isinstance(kwargs, Mapping):
        value = kwargs.get("scope")
        if value is not None:
            return str(value)
    return None


def _rank_choices(query: str, names: list[str]) -> list[str]:
    if not query:
        return names[:_MAX_CHOICES]

    if fuzz_process is not None:
        mapping = {name: name for name in names}
        ranked = fuzz_process.extract(query, mapping, limit=_MAX_CHOICES)
        return [match[0] for match in ranked if match[1] >= 80]

    query_cf = query.casefold()
    prefix = [name for name in names if name.casefold().startswith(query_cf)]
    contains = [
        name for name in names if query_cf in name.casefold() and name not in prefix
    ]
    return (prefix + contains)[:_MAX_CHOICES]


async def cmd_app_info(ctx: arc.GatewayContext) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    arc_client = get_arc()

    local_commands = _collect_local_command_objects(arc_client)
    component_callbacks = 0
    client_obj = getattr(arc_client, "client", None)
    if hasattr(client_obj, "_component_callbacks"):
        component_callbacks = _safe_len(
            getattr(client_obj, "_component_callbacks", None),
        )
    elif hasattr(arc_client, "_component_callbacks"):
        component_callbacks = _safe_len(
            getattr(arc_client, "_component_callbacks", None),
        )

    tracked_scopes = _safe_len(getattr(arc_client, "_guild_commands", None))
    if tracked_scopes == 0:
        tracked_scopes = _safe_len(getattr(arc_client, "_commands", None))

    embed = await reply_embed(
        hikari_client,
        "Application-Commands Cache",
        "Internal application command diagnostics.",
    )
    embed.add_field("Local application cmds", str(len(local_commands)), inline=True)
    embed.add_field("Component callbacks", str(component_callbacks), inline=True)
    embed.add_field("Tracked scopes", str(tracked_scopes), inline=True)
    await ctx.respond(embeds=[embed], flags=hikari.MessageFlag.EPHEMERAL)


async def cmd_app_search(
    ctx: arc.GatewayContext,
    cmd_id: str,
    scope: str,
    *,
    remote: bool = False,
) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    arc_client = get_arc()

    try:
        target_id = _parse_int(cmd_id, "cmd_id")
        guild_scope = _parse_scope(scope)
    except ValueError as exc:
        await reply_err(hikari_client, ctx, str(exc))
        return

    scope_display = _scope_label(guild_scope)

    try:
        if not remote:
            local_commands = _collect_local_command_objects(arc_client)
            local_match = next(
                (
                    command_obj
                    for command_obj in local_commands
                    if str(getattr(command_obj, "id", "")) == str(target_id)
                ),
                None,
            )
            if local_match is not None:
                await _respond_json_attachment(
                    ctx,
                    _serialize_obj(local_match),
                    name=str(getattr(local_match, "name", target_id)),
                )
                return

        remote_commands = await _fetch_remote_commands(
            hikari_client=hikari_client,
            guild_scope=guild_scope,
        )
        remote_match = next(
            (cmd for cmd in remote_commands if int(cmd.id) == target_id),
            None,
        )
        if remote_match is not None:
            await _respond_json_attachment(
                ctx,
                _serialize_obj(remote_match),
                name=str(remote_match.name),
            )
            return
    except Exception:
        logger.exception(
            "Failed to lookup application command %s in scope %s (remote=%s)",
            target_id,
            scope_display,
            remote,
        )

    await reply_err(
        hikari_client,
        ctx,
        f"Unable to locate command `{target_id}` in scope `{scope_display}`.",
    )


async def cmd_app_scope(
    ctx: arc.GatewayContext,
    scope: str,
) -> None:
    await defer(ctx)
    hikari_client = get_hikari()

    try:
        guild_scope = _parse_scope(scope)
        commands = await _fetch_remote_commands(
            hikari_client=hikari_client,
            guild_scope=guild_scope,
        )
    except Exception:
        logger.exception("Failed to list application commands in scope '%s'", scope)
        await reply_err(hikari_client, ctx, f"No commands found in `{scope.strip()}`.")
        return

    if not commands:
        await reply_err(hikari_client, ctx, f"No commands found in `{scope.strip()}`.")
        return

    lines = [f"`{int(command.id)}` : `{command.name}`" for command in commands]
    description = f"**Listing Commands Registered in {_scope_label(guild_scope)}**\n\n"
    description += "\n".join(lines)
    if len(description) > 3800:
        description = f"{description[:3797]}..."

    embed = await reply_embed(
        hikari_client,
        "Application Command Information",
        description,
    )
    await ctx.respond(embeds=[embed], flags=hikari.MessageFlag.EPHEMERAL)


async def cmd_app_delete(
    ctx: arc.GatewayContext,
    scope: str,
    *,
    cmd_id: str = "",
    delete_all: bool = False,
) -> None:
    await defer(ctx)
    hikari_client = get_hikari()
    scope_label = scope

    try:
        guild_scope = _parse_scope(scope)
        scope_label = _scope_label(guild_scope)
    except ValueError:
        await reply_err(hikari_client, ctx, f"`{scope}` is not a valid scope ID.")
        return

    try:
        app_id = _get_application_id(hikari_client)

        if delete_all:
            await hikari_client.rest.set_application_commands(
                application=app_id,
                commands=[],
                guild=guild_scope,
            )
            await ctx.respond(
                f"Successfully deleted all commands in scope `{scope_label}`.",
                flags=hikari.MessageFlag.EPHEMERAL,
            )
            return

        target_id = _parse_int(cmd_id, "cmd_id")
        await hikari_client.rest.delete_application_command(
            application=app_id,
            command=target_id,
            guild=guild_scope,
        )
        await ctx.respond(
            f"Successfully deleted command with ID `{target_id}` in scope `{scope_label}`.",
            flags=hikari.MessageFlag.EPHEMERAL,
        )
        return
    except ValueError as exc:
        await reply_err(hikari_client, ctx, str(exc))
        return
    except Exception:
        if delete_all:
            logger.exception(
                "Failed to delete all application commands in scope '%s'",
                scope_label,
            )
            await reply_err(
                hikari_client,
                ctx,
                f"Unable to delete all commands in scope `{scope_label}`.",
            )
            return
        logger.exception(
            "Failed to delete application command %s in scope %s",
            cmd_id,
            scope_label,
        )
        await reply_err(
            hikari_client,
            ctx,
            f"Unable to delete command with ID `{cmd_id}` in scope `{scope_label}`.",
        )
        return


async def autocomplete_app_cmd(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Sequence[str]:
    hikari_client = get_hikari()
    scope_value = _scope_from_autocomplete(ctx)
    if scope_value is None:
        return []

    try:
        guild_scope = _parse_scope(scope_value)
        commands = await _fetch_remote_commands(
            hikari_client=hikari_client,
            guild_scope=guild_scope,
        )
    except Exception:
        return []

    ids = [str(int(command.id)) for command in commands]
    query = (ctx.focused_value or "").strip()
    ranked = _rank_choices(query, ids)
    return ranked[:_MAX_CHOICES]
