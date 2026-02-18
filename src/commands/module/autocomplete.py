from __future__ import annotations

import os
from collections.abc import Sequence

import arc
from src.git.utils import is_valid_repo
from src.shared.constants import EXTENSIONS_DIR
from src.shared.logger import logger

_MAX_CHOICES = 25


def _discover_valid_modules() -> list[str]:
    with os.scandir(EXTENSIONS_DIR) as entries:
        modules = [
            entry.name
            for entry in entries
            if entry.is_dir()
            and entry.name != "__pycache__"
            and is_valid_repo(entry.name)
        ]
    return sorted(modules, key=str.casefold)


def _rank_matches(modules: list[str], query: str) -> list[str]:
    if not query:
        return modules[:_MAX_CHOICES]
    query_cf = query.casefold()
    prefix_matches = [name for name in modules if name.casefold().startswith(query_cf)]
    contains_matches = [
        name
        for name in modules
        if query_cf in name.casefold() and name not in prefix_matches
    ]
    return (prefix_matches + contains_matches)[:_MAX_CHOICES]


async def autocomplete_module(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Sequence[str]:
    query = (ctx.focused_value or "").strip()

    try:
        valid_modules = _discover_valid_modules()
        if not valid_modules:
            return ["none"]
        choices = _rank_matches(valid_modules, query)
        if query and not choices:
            return ["no_match"]
        return choices
    except Exception:
        logger.exception("Failed to autocomplete module choices")
        return ["error"]
