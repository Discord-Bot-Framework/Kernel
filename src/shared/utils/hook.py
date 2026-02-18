from __future__ import annotations

from typing import TYPE_CHECKING

import arc

from src.container.app import get_hikari
from src.shared.constants import ROLE_ID
from src.shared.logger import logger
from src.shared.utils.view import reply_err

if TYPE_CHECKING:
    import hikari


def _member_has_role(member: hikari.Member, role_id: int) -> bool:
    role_ids = getattr(member, "role_ids", None)
    if role_ids is not None:
        return any(int(member_role_id) == role_id for member_role_id in role_ids)
    return any(int(role.id) == role_id for role in member.get_roles())


async def is_privileged(ctx: arc.GatewayContext) -> arc.HookResult:
    user_id = ctx.user.id
    member = ctx.member
    if not ROLE_ID:
        logger.info(
            "Permission verification failed for user %s",
            user_id,
        )
        await reply_err(get_hikari(), ctx, "`ROLE_ID` not configured.", ephemeral=True)
        return arc.HookResult(abort=True)

    if member is None:
        logger.info(
            "Permission verification failed for user %s",
            user_id,
        )
        await reply_err(get_hikari(), ctx, "Member context missing.", ephemeral=True)
        return arc.HookResult(abort=True)

    if not _member_has_role(member, ROLE_ID):
        logger.info(
            "Denied access to user %s for missing required role %s",
            user_id,
            ROLE_ID,
        )
        await reply_err(
            get_hikari(),
            ctx,
            f"Required role <@&{ROLE_ID}> missing.",
            ephemeral=True,
        )
        return arc.HookResult(abort=True)

    return arc.HookResult(abort=False)
