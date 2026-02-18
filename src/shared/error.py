from __future__ import annotations

import arc
import miru
from hikari.errors import (
    BadRequestError,
    BulkDeleteError,
    ComponentStateConflictError,
    ForbiddenError,
    GatewayConnectionError,
    GatewayServerClosedConnectionError,
    GatewayTransportError,
    HikariError,
    InternalServerError,
    MissingIntentError,
    NotFoundError,
    RateLimitTooLongError,
    UnauthorizedError,
    UnrecognisedEntityError,
    VoiceError,
)
from miru.exceptions import NoResponseIssuedError

from src.container.app import get_hikari
from src.shared.logger import logger
from src.shared.utils.view import reply_err


def _command_name(ctx: arc.GatewayContext) -> str:
    command = getattr(ctx, "command", None)
    return getattr(command, "name", "unknown")


def _user_id(ctx: arc.GatewayContext) -> str:
    user = getattr(ctx, "user", None)
    user_id = getattr(user, "id", None)
    return str(user_id) if user_id is not None else "unknown"


async def _reply_and_log(
    ctx: arc.GatewayContext,
    *,
    level: str,
    log_message: str,
    log_args: tuple[object, ...],
    user_message: str,
) -> None:
    log_func = logger.exception if level == "error" else logger.info
    log_func(log_message, *log_args)
    await reply_err(get_hikari(), ctx, user_message, ephemeral=True)


async def error_handler(ctx: arc.GatewayContext, error: Exception) -> None:
    command_name = _command_name(ctx)
    user_id = _user_id(ctx)

    if isinstance(error, arc.errors.GuildOnlyError):
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Failed to invoke command '%s' outside guild for user %s",
            log_args=(command_name, user_id),
            user_message="Command restricted to guild channels.",
        )
        return
    if isinstance(error, arc.errors.DMOnlyError):
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Failed to invoke command '%s' outside DM for user %s",
            log_args=(command_name, user_id),
            user_message="Command restricted to DM channels.",
        )
        return
    if isinstance(error, arc.errors.NotOwnerError):
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Failed to invoke owner-only command '%s' for user %s",
            log_args=(command_name, user_id),
            user_message="Command restricted to bot owners.",
        )
        return
    if isinstance(error, arc.errors.InvokerMissingPermissionsError):
        missing_perms = error.missing_permissions
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Failed to invoke command '%s' for user %s: missing permissions %s",
            log_args=(command_name, user_id, missing_perms),
            user_message=f"Missing required permissions: {missing_perms}",
        )
        return
    if isinstance(error, arc.errors.BotMissingPermissionsError):
        missing_perms = error.missing_permissions
        guild_id = getattr(ctx, "guild_id", None) or "unknown"
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to execute command '%s' in guild %s: bot missing permissions %s",
            log_args=(command_name, guild_id, missing_perms),
            user_message=f"Bot missing required permissions: {missing_perms}",
        )
        return
    if isinstance(error, arc.errors.UnderCooldownError):
        retry_after = error.retry_after
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Rate limiting command '%s' for user %s: retry in %.2fs",
            log_args=(command_name, user_id, retry_after),
            user_message=f"Rate limited. Retry in {retry_after:.1f}s.",
        )
        return
    if isinstance(error, arc.errors.MaxConcurrencyReachedError):
        max_concurrency = error.max_concurrency
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Blocking concurrent invocation of command '%s' for user %s: maximum %d instances running",
            log_args=(command_name, user_id, max_concurrency),
            user_message=f"Reached maximum concurrent instances ({max_concurrency}).",
        )
        return
    if isinstance(error, arc.utils.ratelimiter.RateLimiterExhaustedError):
        retry_after = error.retry_after
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Exhausted rate limit for command '%s' by user %s: retry in %.2fs",
            log_args=(command_name, user_id, retry_after),
            user_message=f"Rate limit exhausted. Retry in {retry_after:.1f}s.",
        )
        return
    if isinstance(error, (arc.errors.NoResponseIssuedError, NoResponseIssuedError)):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to issue response for command '%s' by user %s: interaction timeout",
            log_args=(command_name, user_id),
            user_message="Interaction timed out.",
        )
        return
    if isinstance(error, arc.errors.ResponseAlreadyIssuedError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to issue duplicate response for command '%s' by user %s",
            log_args=(command_name, user_id),
            user_message="Attempted duplicate response.",
        )
        return
    if isinstance(error, arc.errors.CommandInvokeError):
        root_error = error.__cause__ or error
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to invoke command '%s' for user %s: %s",
            log_args=(command_name, user_id, str(root_error)),
            user_message="Command failed during invocation.",
        )
        return
    if isinstance(error, arc.errors.AutocompleteError):
        root_error = error.__cause__ or error
        logger.info(
            "Failed to complete autocomplete for command '%s' by user %s: %s",
            command_name,
            user_id,
            str(root_error),
        )
        return
    if isinstance(error, arc.errors.OptionConverterFailureError):
        failed_option = error.option
        failed_value = error.value
        option_name = getattr(failed_option, "name", "unknown")
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Failed to convert option for command '%s' by user %s: option '%s' with value '%s'",
            log_args=(command_name, user_id, option_name, failed_value),
            user_message=f"Option '{option_name}' rejected value '{failed_value}'.",
        )
        return
    if isinstance(error, arc.errors.ExtensionLoadError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to load extension: %s",
            log_args=(str(error.__cause__ or error),),
            user_message="Extension failed to load.",
        )
        return
    if isinstance(error, arc.errors.ExtensionUnloadError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to unload extension: %s",
            log_args=(str(error.__cause__ or error),),
            user_message="Extension failed to unload.",
        )
        return
    if isinstance(error, arc.errors.CommandPublishFailedError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to publish commands: %s",
            log_args=(str(error.__cause__ or error),),
            user_message="Command publishing failed.",
        )
        return
    if isinstance(error, arc.errors.GlobalCommandPublishFailedError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to publish global commands: %s",
            log_args=(str(error.__cause__ or error),),
            user_message="Global command publishing failed.",
        )
        return
    if isinstance(error, arc.errors.GuildCommandPublishFailedError):
        guild_id = error.guild_id
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to publish guild commands for guild %s: %s",
            log_args=(guild_id, str(error.__cause__ or error)),
            user_message=f"Guild command publishing failed for guild {guild_id}.",
        )
        return
    if isinstance(error, arc.errors.HookAbortError):
        await _reply_and_log(
            ctx,
            level="info",
            log_message="Aborted command '%s' for user %s by hook: %s",
            log_args=(command_name, user_id, str(error.__cause__ or error)),
            user_message="Command aborted by hook.",
        )
        return
    if isinstance(error, arc.errors.ArcError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to execute command '%s' for user %s: %s",
            log_args=(command_name, user_id, str(error.__cause__ or error)),
            user_message="Internal framework error.",
        )
        return
    if isinstance(error, BadRequestError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to send request for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Sent invalid request to Discord.",
        )
        return
    if isinstance(error, UnauthorizedError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to authorize for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Bot authorization failed.",
        )
        return
    if isinstance(error, ForbiddenError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to access forbidden resource for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Bot lacks required permissions.",
        )
        return
    if isinstance(error, NotFoundError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to locate resource for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Requested resource not found.",
        )
        return
    if isinstance(error, RateLimitTooLongError):
        retry_after = getattr(error, "retry_after", 0.0)
        route = getattr(error, "route", "unknown")
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to execute command '%s' for user %s: rate limit exceeded, retry after %.1fs (route %s)",
            log_args=(command_name, user_id, retry_after, route),
            user_message=f"Rate limit exceeded. Retry after {retry_after:.1f}s.",
        )
        return
    if isinstance(error, InternalServerError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to execute command '%s' for user %s: Discord internal server error",
            log_args=(command_name, user_id),
            user_message="Discord internal server error.",
        )
        return
    if isinstance(error, GatewayConnectionError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to connect gateway for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Gateway connection failed.",
        )
        return
    if isinstance(error, GatewayTransportError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to transport gateway for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Gateway transport failed.",
        )
        return
    if isinstance(error, GatewayServerClosedConnectionError):
        code = getattr(error, "code", "unknown")
        can_reconnect = getattr(error, "can_reconnect", False)
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to maintain gateway connection for command '%s' by user %s: server closed connection (code %s, can_reconnect %s)",
            log_args=(command_name, user_id, code, can_reconnect),
            user_message="Gateway connection closed by server.",
        )
        return
    if isinstance(error, ComponentStateConflictError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to resolve component state for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Component state conflict.",
        )
        return
    if isinstance(error, UnrecognisedEntityError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to recognise entity for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Unrecognised entity encountered.",
        )
        return
    if isinstance(error, BulkDeleteError):
        deleted_count = len(getattr(error, "deleted_messages", []))
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to bulk delete for command '%s' by user %s: partially completed (%d messages deleted)",
            log_args=(command_name, user_id, deleted_count),
            user_message=f"Bulk delete partially completed ({deleted_count} messages deleted).",
        )
        return
    if isinstance(error, VoiceError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to process voice for command '%s' by user %s: %s",
            log_args=(command_name, user_id, str(error)),
            user_message="Voice subsystem error.",
        )
        return
    if isinstance(error, MissingIntentError):
        missing_intents = getattr(error, "intents", "unknown")
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to execute command '%s' for user %s: missing intents %s",
            log_args=(command_name, user_id, missing_intents),
            user_message="Missing required bot intents.",
        )
        return
    if isinstance(error, HikariError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to execute command '%s' for user %s: library error",
            log_args=(command_name, user_id),
            user_message="Internal library error.",
        )
        return
    if isinstance(error, miru.RowFullError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to add UI component for command '%s' by user %s: row is full",
            log_args=(command_name, user_id),
            user_message="UI row is full.",
        )
        return
    if isinstance(error, miru.HandlerFullError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to add UI handler for command '%s' by user %s: handler is full",
            log_args=(command_name, user_id),
            user_message="UI handler is full.",
        )
        return
    if isinstance(error, miru.ItemAlreadyAttachedError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to attach UI component for command '%s' by user %s: component already attached",
            log_args=(command_name, user_id),
            user_message="UI component already attached.",
        )
        return
    if isinstance(error, miru.MiruError):
        await _reply_and_log(
            ctx,
            level="error",
            log_message="Failed to process UI for command '%s' by user %s: framework error",
            log_args=(command_name, user_id),
            user_message="UI framework error.",
        )
        return

    raise error
