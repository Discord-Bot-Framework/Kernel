from __future__ import annotations

import arc

from src.shared.utils.hook import is_privileged


def hook_cmd_group(group: arc.SlashGroup) -> None:
    group.add_hook(is_privileged)
    group.add_hook(arc.guild_only)


def hook_cmd_subgroup(group: arc.SlashSubGroup) -> None:
    group.add_hook(arc.utils.hooks.limiters.guild_limiter(10.0, 1))
    group.set_concurrency_limiter(arc.guild_concurrency(2))
