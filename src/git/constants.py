from __future__ import annotations

import typing

import pygit2

from src.shared.constants import BASE_DIR


def _discover_main_repo_path() -> str | None:
    try:
        return pygit2.discover_repository(str(BASE_DIR))
    except Exception:
        return None


GIT_URL_TRANS_MAP: typing.Final[dict[int, str]] = str.maketrans(
    {"_": "_u_", "/": "_s_", ".": "_d_", "-": "_h_"},
)
MAIN_REPO_PATH: typing.Final[str | None] = _discover_main_repo_path()
