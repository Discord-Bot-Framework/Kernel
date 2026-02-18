from __future__ import annotations

import asyncio
import enum
import os
import pathlib
import typing

from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int = 0) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value.strip())
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return default
    return parsed


BASE_DIR: typing.Final[pathlib.Path] = pathlib.Path(
    __file__,
).parent.parent.parent.resolve()
LOG_FILE: typing.Final[pathlib.Path] = BASE_DIR / "main.log"
BACKUP_DIR: typing.Final[pathlib.Path] = BASE_DIR / ".bak"
EXTENSIONS_DIR: typing.Final[pathlib.Path] = BASE_DIR / "extensions"
FLAG_DIR: typing.Final[pathlib.Path] = BASE_DIR / "flag"

GUILD_ID: typing.Final[int] = _env_int("GUILD_ID")
ROLE_ID: typing.Final[int] = _env_int("ROLE_ID")
TOKEN: typing.Final[str | None] = os.getenv("TOKEN")

SHUTDOWN_EVENT: asyncio.Event = asyncio.Event()


class Color(enum.IntEnum):
    ERROR = 0xE81123
    WARNING = 0xFFB900
    INFO = 0x0078D7
    SUCCESS = 0x107C10
