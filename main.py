from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import enum
import functools
import logging
import os
import pathlib
import shutil
import signal
import sys
import tarfile
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING, Final, TypeVar, cast

import aiofiles
import aioshutil
import arc
import compression.zstd
import hikari
import miru
import orjson
import pygit2
from dotenv import load_dotenv
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
from miru.ext import nav

if TYPE_CHECKING:
    from collections.abc import Sequence

load_dotenv()


BASE_DIR: Final[pathlib.Path] = pathlib.Path(__file__).parent.resolve()
LOG_FILE: Final[pathlib.Path] = BASE_DIR / "main.log"
BACKUP_DIR: Final[pathlib.Path] = BASE_DIR / ".bak"
EXTENSIONS_DIR: Final[pathlib.Path] = BASE_DIR / "extensions"
FLAG_DIR: Final[pathlib.Path] = BASE_DIR / "flag"
GUILD_ID: Final[int] = int(os.environ.get("GUILD_ID", "0"))
ROLE_ID: Final[int] = int(os.environ.get("ROLE_ID", "0"))
TOKEN: Final[str | None] = os.getenv("TOKEN")


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False
log_formatter = logging.Formatter(
    "%(asctime)s | %(process)d - %(processName)s | %(thread)d - %(threadName)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(pathname)s | %(message)s",
    "%Y-%m-%d %H:%M:%S,%f %z",
)
log_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf-8",
)
log_handler.setFormatter(log_formatter)
if not any(
    isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", None) == str(LOG_FILE)
    for handler in logger.handlers
):
    logger.addHandler(log_handler)


T = TypeVar("T")


# --- Client ---


if not TOKEN:
    logger.critical("Failed to load TOKEN")
    sys.exit(1)

assert TOKEN is not None

bot = hikari.GatewayBot(
    token=TOKEN,
    banner=None,
    dumps=orjson.dumps,
    loads=orjson.loads,
    logs=None,
    intents=hikari.Intents.ALL,
)

arc_client = arc.client.GatewayClient(
    bot,
    default_enabled_guilds=[hikari.snowflakes.Snowflake(GUILD_ID)] if GUILD_ID else hikari.undefined.UNDEFINED,
)

miru_client = miru.Client.from_arc(arc_client)

os.environ.pop("TOKEN", None)
with contextlib.suppress(Exception):
    pathlib.Path(".env").unlink(missing_ok=True)


@bot.listen()
async def on_starting(event: hikari.events.StartingEvent) -> None:
    logger.info("Event app type: %s", type(event.app).__name__)
    logger.info("Event app: %s", event.app)


@bot.listen()
async def on_started(event: hikari.events.StartedEvent) -> None:
    logger.info("Event app type: %s", type(event.app).__name__)
    logger.info("Event app: %s", event.app)
    try:
        me = bot.get_me()
        status = hikari.Status.ONLINE if me else hikari.Status.DO_NOT_DISTURB
        activity = hikari.Activity(
            name="with hikari",
            type=hikari.ActivityType.LISTENING,
        )
        await bot.update_presence(status=status, activity=activity)
        if me:
            logger.info("Authenticated as %s (%s)", me.username, me.id)
        else:
            logger.exception("Failed to retrieve bot user object")
    except Exception:
        logger.exception("Failed to complete startup sequence")


@arc_client.add_startup_hook
async def on_arc_startup(client: arc.GatewayClient) -> None:
    try:
        await client.resync_commands()
    except Exception:
        logger.exception("Failed to resync commands")


@arc_client.listen()
async def on_arc(event: arc.events.StartedEvent) -> None:
    logger.info("Event app type: %s", type(event.client).__name__)
    logger.info("Event app: %s", event.client)


# --- Git ---


GIT_URL_TRANS_MAP = str.maketrans({"_": "_u_", "/": "_s_", ".": "_d_", "-": "_h_"})
MAIN_REPO_PATH: Final[str | None] = pygit2.discover_repository(str(BASE_DIR))


def resolve_remote_ref(repo: pygit2.Repository) -> pygit2.Reference | None:
    ref_names = (
        "refs/remotes/origin/HEAD",
        "refs/remotes/origin/main",
        "refs/remotes/origin/master",
    )
    for ref_name in ref_names:
        try:
            ref = repo.lookup_reference(ref_name)
        except Exception:
            continue
        if ref_name.endswith("/HEAD"):
            target = ref.target
            if isinstance(target, str):
                with contextlib.suppress(Exception):
                    return repo.lookup_reference(target)
        else:
            return ref
    return None


def resolve_local_ref(
    repo: pygit2.Repository,
    remote_ref: pygit2.Reference,
) -> pygit2.Reference | None:
    if not remote_ref.name.startswith("refs/remotes/origin/"):
        return None
    local_name = remote_ref.name.replace("refs/remotes/origin/", "refs/heads/", 1)
    with contextlib.suppress(Exception):
        return repo.lookup_reference(local_name)
    with contextlib.suppress(Exception):
        return repo.create_reference(local_name, remote_ref.target, force=True)
    return None


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Repo:
    mods: int
    url: str
    cur_commit: pygit2.Commit | None = None
    rmt_commit: pygit2.Commit | None = None
    changelog: str = ""

    def __post_init__(self) -> None:
        if self.mods < 0:
            msg = f"mods must be non-negative, got {self.mods}"
            raise ValueError(msg)

    @staticmethod
    @functools.lru_cache(maxsize=1024)
    def _ts(commit_time: int, offset: int) -> float:
        return commit_time + offset * 60

    @classmethod
    def _fmt(cls, commit: pygit2.Commit | None) -> str | None:
        if commit is None:
            return None
        return datetime.datetime.fromtimestamp(
            cls._ts(commit.commit_time, commit.committer.offset),
            datetime.timezone.utc,
        ).isoformat()

    @property
    def cur_time_utc(self) -> str | None:
        return self._fmt(self.cur_commit)

    @property
    def rmt_time_utc(self) -> str | None:
        return self._fmt(self.rmt_commit)


def parse_repo_url(url: str) -> tuple[str, str, bool]:
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc or not parsed.path.endswith(".git") or "." not in parsed.netloc:
        return url, "", False
    netloc_parts = parsed.netloc.split(".")
    relevant_netloc = ".".join(p for p in netloc_parts if p not in frozenset({"www", "com"}))
    if not relevant_netloc:
        return url, "", False
    path_part = parsed.path.removesuffix(".git").removeprefix("/")
    if not path_part:
        return url, "", False
    return (
        url,
        f"{relevant_netloc.translate(GIT_URL_TRANS_MAP)}__{path_part.translate(GIT_URL_TRANS_MAP)}",
        True,
    )


def clone_repo(url: str) -> tuple[str, bool]:
    _, reponame, is_valid = parse_repo_url(url)
    if not is_valid:
        return "", False
    repo_path = EXTENSIONS_DIR / reponame
    EXTENSIONS_DIR.mkdir(exist_ok=True)
    try:
        pygit2.clone_repository(url, str(repo_path))
        return reponame, True
    except Exception:
        shutil.rmtree(repo_path, ignore_errors=True)
        return "", False


def pull_repo(repo_path_str: str) -> int:
    try:
        repo = pygit2.Repository(repo_path_str)
        origin = repo.remotes["origin"]
    except Exception:
        return 2
    with contextlib.suppress(Exception):
        origin.fetch()
    remote_ref = resolve_remote_ref(repo)
    if remote_ref is None:
        return 3
    local_ref = resolve_local_ref(repo, remote_ref)
    if local_ref is None:
        return 3
    remote_oid = remote_ref.target
    if local_ref.target == remote_oid:
        return 0
    try:
        repo.checkout_tree(repo.get(remote_oid))
        local_ref.set_target(remote_oid)
        repo.head.set_target(remote_oid)
    except Exception:
        return 2
    return 0


def is_valid_repo(name: str) -> bool:
    module_repo_path = pygit2.discover_repository(str(EXTENSIONS_DIR / name))
    return bool(module_repo_path and module_repo_path != MAIN_REPO_PATH)


def get_repo_commits(
    repo_path_str: str,
) -> tuple[pygit2.Commit | None, pygit2.Commit | None, int] | None:
    try:
        repo = pygit2.Repository(repo_path_str)
        origin = repo.remotes["origin"]
    except Exception:
        return None

    with contextlib.suppress(Exception):
        origin.fetch()

    remote_ref = resolve_remote_ref(repo)
    if remote_ref is None:
        return None

    remote_commit_obj = repo.get(remote_ref.target)
    head_commit_obj = repo.get(repo.head.target)

    remote_commit: pygit2.Commit | None = remote_commit_obj if isinstance(remote_commit_obj, pygit2.Commit) else None
    head_commit: pygit2.Commit | None = head_commit_obj if isinstance(head_commit_obj, pygit2.Commit) else None

    modifications = (
        -1
        if remote_commit is None or head_commit is None
        else repo.diff(head_commit.id, remote_commit.id).stats.files_changed
    )

    return head_commit, remote_commit, modifications


def get_module_info(name: str) -> tuple[Repo | None, bool]:
    module_path = EXTENSIONS_DIR / name
    repo_path_str = pygit2.discover_repository(str(module_path))
    if not repo_path_str or repo_path_str == MAIN_REPO_PATH:
        return None, False

    commits = get_repo_commits(repo_path_str)
    if commits is None:
        return None, False

    head_commit, remote_commit, modifications = commits

    changelog = ""
    if (changelog_path := module_path / "CHANGELOG").is_file():
        with contextlib.suppress(Exception):
            changelog = changelog_path.read_text(encoding="utf-8", errors="ignore")

    try:
        repo = pygit2.Repository(repo_path_str)
        origin = repo.remotes["origin"]
        origin_url: str | None = origin.url
    except Exception:
        return None, False

    if origin_url is None:
        return None, False

    return Repo(
        mods=modifications,
        url=origin_url,
        cur_commit=head_commit,
        rmt_commit=remote_commit,
        changelog=changelog,
    ), True


def get_kernel_info() -> Repo | None:
    if not MAIN_REPO_PATH:
        return None

    commits = get_repo_commits(MAIN_REPO_PATH)
    if commits is None:
        return None

    head_commit, remote_commit, modifications = commits

    try:
        repo = pygit2.Repository(MAIN_REPO_PATH)
        origin = repo.remotes["origin"]
        origin_url: str | None = origin.url
    except Exception:
        return None

    if origin_url is None:
        return None

    return Repo(
        mods=modifications,
        url=origin_url,
        cur_commit=head_commit,
        rmt_commit=remote_commit,
    )


# --- Module ---


def pull_kernel() -> int:
    return pull_repo(MAIN_REPO_PATH) if MAIN_REPO_PATH else 2


def pull_module(name: str) -> int:
    if not (repo_path := pygit2.discover_repository(str(EXTENSIONS_DIR / name))):
        return 2
    if repo_path == MAIN_REPO_PATH:
        return 1
    return pull_repo(repo_path)


def delete_module(name: str) -> bool:
    module_path = EXTENSIONS_DIR / name
    if not module_path.is_dir() or not is_valid_repo(name):
        return False
    try:
        shutil.rmtree(module_path)
        return True
    except Exception:
        return False


async def load_module(
    bot: hikari.GatewayBot,
    module_name: str,
    *,
    is_reload: bool = False,
) -> bool:
    module_full_name = f"extensions.{module_name}.main"
    module_disk_path = EXTENSIONS_DIR / module_name
    if not module_disk_path.is_dir():
        return False
    is_valid, error_msg, missing_deps = await check_local_module(
        str(module_disk_path),
        module_name,
    )
    if not is_valid:
        if not is_reload:
            msg = f"Failed to load `{module_name}`:\n{error_msg}"
            if missing_deps:
                msg += f"\n\n**Missing:**\n- {'\n- '.join(missing_deps)}"
            await dm_role_members(
                embeds=[await reply_embed(bot, "Validation Failed", msg, Color.ERROR)],
            )
        return False
    if not is_reload:
        try:
            arc_client.load_extension(module_full_name)
            if arc_client.is_started:
                await arc_client.resync_commands()
            return True
        except Exception as e:
            await dm_role_members(
                embeds=[
                    await reply_embed(
                        bot,
                        "Load Failed",
                        f"Failed to load `{module_name}`:\n```py\n{e!s}\n```",
                        Color.ERROR,
                    ),
                ],
            )
            await asyncio.to_thread(delete_module, module_name)
            return False
    backup_submodules = {
        name: mod
        for name, mod in sys.modules.items()
        if name == module_full_name or name.startswith(f"{module_full_name}.")
    }
    try:
        with contextlib.suppress(Exception):
            arc_client.unload_extension(module_full_name)

        for name in list(sys.modules.keys()):
            if name == module_full_name or name.startswith(f"{module_full_name}."):
                del sys.modules[name]

        arc_client.load_extension(module_full_name)
        if arc_client.is_started:
            await arc_client.resync_commands()
        logger.info("Reloaded extension '%s'", module_full_name)
        return True

    except Exception as e:
        logger.exception("Failed to reload extension '%s': %s", module_full_name)

        with contextlib.suppress(Exception):
            arc_client.unload_extension(module_full_name)

        for name in list(sys.modules.keys()):
            if name == module_full_name or name.startswith(f"{module_full_name}."):
                del sys.modules[name]

        for name, mod in backup_submodules.items():
            sys.modules[name] = mod

        try:
            arc_client.load_extension(module_full_name)
            if arc_client.is_started:
                await arc_client.resync_commands()
            logger.info(
                "Rolled back extension '%s' to previous state",
                module_full_name,
            )
            await dm_role_members(
                embeds=[
                    await reply_embed(
                        bot,
                        "Rollback Succeeded",
                        f"Failed to reload `{module_name}`:\n```py\n{e!s}\n```\n"
                        f"The previous version has been restored and is running.",
                        Color.ERROR,
                    ),
                ],
            )
        except Exception:
            logger.exception(
                "Failed to roll back extension '%s'",
                module_full_name,
            )
            await dm_role_members(
                embeds=[
                    await reply_embed(
                        bot,
                        "Rollback Failed",
                        f"Failed to reload `{module_name}`:\n```py\n{e!s}\n```\n"
                        f"Failed to restore the previous version. "
                        f"The extension is now in an inconsistent state. "
                        f"Manual intervention may be required.",
                        Color.ERROR,
                    ),
                ],
            )
        return False


async def check_local_module(
    module_path_str: str,
    module_name: str,
) -> tuple[bool, str, list[str]]:
    module_path = pathlib.Path(module_path_str)
    if not module_path.is_dir():
        return False, f"pathlib.Path not found: {module_path}", []

    valid_struct, struct_msg = await check_module_exec(str(module_path), module_name)
    if not valid_struct:
        return False, struct_msg, []

    valid_deps, missing_deps = await asyncio.to_thread(
        check_module_deps,
        str(module_path),
    )
    return (True, "", []) if valid_deps else (False, "Missing dependencies.", missing_deps)


async def check_remote_module(url: str) -> tuple[bool, str, list[str]]:
    parsed_url, module_name, is_valid_url = parse_repo_url(url)
    if not is_valid_url or not module_name:
        return False, f"Invalid Git URL: {url}", []

    async with aiofiles.tempfile.TemporaryDirectory(
        prefix=f"{module_name}_remote_validation_",
    ) as temp_dir:
        temp_path = pathlib.Path(temp_dir)
        try:
            await asyncio.to_thread(pygit2.clone_repository, parsed_url, str(temp_path))
        except Exception:
            return False, "Failed to clone", []

        valid_struct, struct_msg = await check_module_exec(str(temp_path), module_name)
        if not valid_struct:
            return False, struct_msg, []

        error_msg, reqs_preview = "", []
        if (requirements_file := temp_path / "requirements.txt").is_file():
            try:
                content = requirements_file.read_text(encoding="utf-8")
                from packaging.requirements import Requirement

                reqs_preview = [
                    line.split("#")[0].strip()
                    for line in content.splitlines()
                    if line.strip() and not line.startswith("#")
                ]
                for req_str in reqs_preview:
                    Requirement(req_str)
            except Exception as e:
                error_msg = f"Failed to read requirements.txt: {e}"

        return True, error_msg, reqs_preview


def check_module_deps(module_path_str: str) -> tuple[bool, list[str]]:
    requirements_file = pathlib.Path(module_path_str) / "requirements.txt"
    if not requirements_file.is_file():
        return True, []

    try:
        content = requirements_file.read_text(encoding="utf-8")
        requirements = [
            line.split("#")[0].strip() for line in content.splitlines() if line.strip() and not line.startswith("#")
        ]
    except Exception as e:
        return False, [f"Error reading requirements file: {e}"]

    if not requirements:
        return True, []

    from importlib.metadata import PackageNotFoundError, distribution

    from packaging.requirements import Requirement
    from packaging.version import Version

    missing: list[str] = []
    for req_str in requirements:
        try:
            req = Requirement(req_str)
            try:
                dist = distribution(req.name)
            except PackageNotFoundError as e:
                missing.append(f"{req_str} (Missing: {e})")
                continue
            if req.specifier and not req.specifier.contains(
                Version(dist.version),
                prereleases=True,
            ):
                missing.append(
                    f"{req_str} (Installed: {dist.version}, Required: {req.specifier})",
                )
        except Exception as e:
            missing.append(f"{req_str} (Error: {e})")

    return (False, missing) if missing else (True, [])


async def check_module_exec(module_path_str: str, module_name: str) -> tuple[bool, str]:
    main_file = pathlib.Path(module_path_str) / "main.py"
    if not main_file.is_file():
        return False, f"`main.py` not found in `{module_name}`."

    try:
        code = main_file.read_text(encoding="utf-8")
    except Exception as e:
        return False, f"Error reading `main.py`: {e}"

    try:
        import ast

        ast.parse(code, filename=str(main_file))
    except SyntaxError as e:
        pointer = " " * ((e.offset or 1) - 1) + "^"
        code_snippet = f"{e.text.strip()}\n{pointer}" if e.text else ""
        return (
            False,
            f"Syntax error in `main.py` (line {e.lineno}):\n```py\n{code_snippet}\n```",
        )

    try:
        compile(code, str(main_file), "exec", dont_inherit=True)
    except Exception as e:
        return False, f"Compilation error: {e}"

    return True, ""


# --- pip ---

import importlib

_pip_internal = importlib.import_module("pip._internal.cli.main")
_pip_module = importlib.import_module("pip")
pip_main = getattr(_pip_internal, "main", None) or getattr(_pip_module, "main", None)

if pip_main is None:
    logger.critical("Failed to import pip main function")


def run_pip(file_path: str, install: bool = True) -> bool:
    if not pip_main:
        logger.exception(
            "Failed to process requirements file: pip main function unavailable",
        )
        return False

    path = pathlib.Path(file_path)
    if not path.is_file():
        logger.exception("Failed to locate requirements file: %s", file_path)
        return False

    operation = ("install", "-U") if install else ("uninstall", "-y")
    command = [*operation, "-r", file_path]

    try:
        status_code = pip_main(command)
        if status_code == 0:
            logger.info(
                "Processed requirements file '%s' with pip %s",
                file_path,
                " ".join(operation),
            )
            return True
        logger.exception(
            "Failed to process requirements file '%s': pip exited with status %d",
            file_path,
            status_code,
        )
        return False
    except Exception:
        logger.exception("Failed to process requirements file '%s'", file_path)
        return False


# --- UI ---


class Color(enum.IntEnum):
    ERROR = 0xE81123
    WARN = 0xFFB900
    INFO = 0x0078D7


async def response(
    ctx: arc.client.GatewayContext | arc.context.base.Context | miru.abc.context.Context,
    *,
    content: str = "",
    embeds: hikari.Embed | Sequence[hikari.Embed] | None = None,
    components: Sequence[hikari.api.ComponentBuilder] | hikari.undefined.UndefinedType = hikari.undefined.UNDEFINED,
    ephemeral: bool = True,
) -> None:
    undefined = hikari.undefined.UNDEFINED
    embed_list: Sequence[hikari.Embed] | hikari.undefined.UndefinedType
    match embeds:
        case None:
            embed_list = undefined
        case hikari.Embed():
            embed_list = [embeds]
        case _:
            embed_list = embeds
    flags = hikari.MessageFlag.EPHEMERAL if ephemeral else hikari.MessageFlag.NONE

    if ctx.issued_response:
        try:
            if hasattr(ctx, "edit_initial_response"):
                gateway_ctx = cast("arc.client.GatewayContext", ctx)
                await gateway_ctx.edit_initial_response(
                    content=content or undefined,
                    embeds=embed_list or undefined,
                    components=components,
                )
            elif hasattr(ctx, "edit_response"):
                miru_ctx = cast("miru.abc.context.Context", ctx)
                await miru_ctx.edit_response(
                    content=content or undefined,
                    embeds=embed_list or undefined,
                    components=components,
                )
            elif hasattr(ctx, "respond"):
                response_obj = await ctx.respond(
                    content=content or undefined,
                    embeds=embed_list or undefined,
                    components=components,
                )
                if response_obj:
                    await response_obj.edit(
                        content=content or undefined,
                        embeds=embed_list or undefined,
                        components=components,
                    )
        except Exception:
            logger.exception("Failed to edit response")
    else:
        try:
            await ctx.respond(
                content=content,
                embeds=embed_list,
                components=components,
                flags=flags,
            )
        except Exception:
            logger.exception("Failed to send response")


async def reply_embed(
    bot: hikari.GatewayBot | hikari.GatewayBotAware,
    title: str | None,
    description: str = "",
    color: Color = Color.INFO,
) -> hikari.Embed:
    embed = hikari.Embed(
        description=description,
        color=int(color),
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )

    if title is not None:
        embed.title = title

    me = bot.get_me()
    if me:
        embed.set_author(name=me.username, icon=me.display_avatar_url)
        embed.set_footer(text=me.username, icon=me.display_avatar_url)

    return embed


async def reply_err(
    bot: hikari.GatewayBot | hikari.GatewayBotAware,
    ctx: arc.client.GatewayContext | arc.context.base.Context | miru.abc.context.Context | None,
    message: str,
    *,
    ephemeral: bool = True,
) -> None:
    if ctx is None:
        return
    embed = await reply_embed(bot, "Exception", message[:1900], Color.ERROR)
    await response(
        ctx,
        embeds=embed,
        ephemeral=ephemeral,
    )


async def reply_ok(
    bot: hikari.GatewayBot | hikari.GatewayBotAware,
    ctx: arc.client.GatewayContext | arc.context.base.Context | miru.abc.context.Context | None,
    message: str,
    *,
    title: str | None = "Completion",
    ephemeral: bool = True,
) -> None:
    if ctx is None:
        return
    embed = await reply_embed(bot, title, message)
    await response(
        ctx,
        embeds=embed,
        ephemeral=ephemeral,
    )


async def defer(
    ctx: arc.client.GatewayContext | arc.context.base.Context | miru.abc.context.Context | None,
) -> None:
    if ctx is None:
        return
    if ctx.issued_response:
        return
    try:
        await ctx.defer(flags=hikari.MessageFlag.EPHEMERAL)
    except Exception:
        with contextlib.suppress(Exception):
            await ctx.respond(
                "Failed to defer.",
                flags=hikari.MessageFlag.EPHEMERAL,
            )


# --- Members ---


async def dm_role_members(
    ctx: arc.client.GatewayContext | None = None,
    msg: str | None = None,
    *,
    embeds: Sequence[hikari.Embed] | None = None,
    components: Sequence[hikari.api.ComponentBuilder] | hikari.undefined.UndefinedType = hikari.undefined.UNDEFINED,
) -> list[hikari.Message]:
    if not (ROLE_ID and GUILD_ID):
        return []

    me = bot.get_me()

    guild_id = ctx.guild_id if ctx and ctx.guild_id else hikari.snowflakes.Snowflake(GUILD_ID)

    members_view = bot.cache.get_members_view_for_guild(guild_id)
    if members_view:
        members_set = {m for m in members_view.values() if any(r.id == ROLE_ID for r in m.get_roles())}
    else:
        members_set = {m async for m in bot.rest.fetch_members(guild_id) if any(r.id == ROLE_ID for r in m.get_roles())}

    if not members_set:
        return []

    processed_embeds = list(embeds) if embeds else hikari.undefined.UNDEFINED
    semaphore = asyncio.Semaphore(10)

    async def dm_member(
        member: hikari.Member | hikari.User,
    ) -> hikari.Message | None:
        if me and member.id == me.id:
            return None
        async with semaphore:
            with contextlib.suppress(Exception):
                return await member.send(
                    content=msg,
                    embeds=processed_embeds,
                    components=components,
                )
        return None

    return [r for r in await asyncio.gather(*(dm_member(m) for m in members_set)) if r]


# --- Hook ---


async def is_privileged(ctx: arc.client.GatewayContext) -> arc.HookResult:
    if ROLE_ID and ctx.member:
        has_role = any(r.id == ROLE_ID for r in ctx.member.get_roles())
        if not has_role:
            logger.info(
                "Denied access to user %s: missing required role %s",
                ctx.user.id,
                ROLE_ID,
            )
            await reply_err(
                bot,
                ctx,
                "Access denied: insufficient privileges.",
                ephemeral=True,
            )
            return arc.HookResult(abort=True)
        return arc.HookResult(abort=False)

    logger.warning(
        "Failed to verify permissions for user %s: ROLE_ID not set or ctx.member is None",
        ctx.user.id,
    )
    await reply_err(
        bot,
        ctx,
        "Access denied: permission verification failed.",
        ephemeral=True,
    )
    return arc.HookResult(abort=True)


# --- Commands ---


cmd_group = arc_client.include_slash_group("kernel", "Bot Framework Kernel Commands")
cmd_module = cmd_group.include_subgroup("module", "Module Commands")
cmd_review = cmd_group.include_subgroup("review", "Review Commands")
cmd_debug = cmd_group.include_subgroup("debug", "Debug commands")

cmd_group.add_hook(is_privileged)
cmd_group.add_hook(arc.guild_only)
cmd_group.add_hook(arc.utils.hooks.limiters.guild_limiter(60.0, 2))
cmd_group.set_concurrency_limiter(arc.guild_concurrency(1))


@arc_client.set_error_handler
async def error_handler(ctx: arc.client.GatewayContext, error: Exception) -> None:
    if isinstance(error, arc.errors.GuildOnlyError):
        logger.warning(
            "Failed to invoke command '%s' outside guild for user %s",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Command restricted to guild channels.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.DMOnlyError):
        logger.warning(
            "Failed to invoke command '%s' outside DM for user %s",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Command restricted to DM channels.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.NotOwnerError):
        logger.warning(
            "Failed to invoke owner-only command '%s' for user %s",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Command restricted to bot owners.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.InvokerMissingPermissionsError):
        missing_perms = error.missing_permissions
        logger.warning(
            "Failed to invoke command '%s' for user %s: missing permissions %s",
            ctx.command.name,
            ctx.user.id,
            missing_perms,
        )
        await reply_err(
            bot,
            ctx,
            f"Missing required permissions: {missing_perms}",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.BotMissingPermissionsError):
        missing_perms = error.missing_permissions
        logger.error(
            "Failed to execute command '%s' in guild %s: bot missing permissions %s",
            ctx.command.name,
            ctx.guild_id or "unknown",
            missing_perms,
        )
        await reply_err(
            bot,
            ctx,
            f"Bot missing required permissions: {missing_perms}",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.UnderCooldownError):
        retry_after = error.retry_after
        logger.info(
            "Rate limiting command '%s' for user %s: retry in %.2fs",
            ctx.command.name,
            ctx.user.id,
            retry_after,
        )
        await reply_err(
            bot,
            ctx,
            f"Rate limited. Retry in {retry_after:.1f}s.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.MaxConcurrencyReachedError):
        max_concurrency = error.max_concurrency
        logger.info(
            "Blocking concurrent invocation of command '%s' for user %s: maximum %d instances running",
            ctx.command.name,
            ctx.user.id,
            max_concurrency,
        )
        await reply_err(
            bot,
            ctx,
            f"Reached maximum concurrent instances ({max_concurrency}).",
            ephemeral=True,
        )
    elif isinstance(error, arc.utils.ratelimiter.RateLimiterExhaustedError):
        retry_after = error.retry_after
        logger.info(
            "Exhausted rate limit for command '%s' by user %s: retry in %.2fs",
            ctx.command.name,
            ctx.user.id,
            retry_after,
        )
        await reply_err(
            bot,
            ctx,
            f"Rate limit exhausted. Retry in {retry_after:.1f}s.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.NoResponseIssuedError):
        logger.error(
            "Failed to issue response for command '%s' by user %s: interaction timeout",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Interaction timed out.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.ResponseAlreadyIssuedError):
        logger.error(
            "Failed to issue duplicate response for command '%s' by user %s",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Attempted duplicate response.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.CommandInvokeError):
        logger.error(
            "Failed to invoke command '%s' for user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Command failed during invocation.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.AutocompleteError):
        logger.warning(
            "Failed to complete autocomplete for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error.__cause__ or error),
        )
    elif isinstance(error, arc.errors.OptionConverterFailureError):
        failed_option = error.option
        failed_value = error.value
        logger.warning(
            "Failed to convert option for command '%s' by user %s: option '%s' with value '%s'",
            ctx.command.name,
            ctx.user.id,
            getattr(failed_option, "name", "unknown"),
            failed_value,
        )
        await reply_err(
            bot,
            ctx,
            f"Option '{getattr(failed_option, 'name', 'unknown')}' rejected value '{failed_value}'.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.ExtensionLoadError):
        logger.error(
            "Failed to load extension: %s",
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Extension failed to load.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.ExtensionUnloadError):
        logger.error(
            "Failed to unload extension: %s",
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Extension failed to unload.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.CommandPublishFailedError):
        logger.error(
            "Failed to publish commands: %s",
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Command publishing failed.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.GlobalCommandPublishFailedError):
        logger.error(
            "Failed to publish global commands: %s",
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Global command publishing failed.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.GuildCommandPublishFailedError):
        guild_id = error.guild_id
        logger.error(
            "Failed to publish guild commands for guild %s: %s",
            guild_id,
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            f"Guild command publishing failed for guild {guild_id}.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.HookAbortError):
        logger.info(
            "Aborted command '%s' for user %s by hook: %s",
            ctx.command.name,
            ctx.user.id,
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Command aborted by hook.",
            ephemeral=True,
        )
    elif isinstance(error, arc.errors.ArcError):
        logger.error(
            "Failed to execute command '%s' for user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error.__cause__ or error),
        )
        await reply_err(
            bot,
            ctx,
            "Internal framework error.",
            ephemeral=True,
        )
    elif isinstance(error, BadRequestError):
        logger.error(
            "Failed to send request for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Sent invalid request to Discord.",
            ephemeral=True,
        )
    elif isinstance(error, UnauthorizedError):
        logger.error(
            "Failed to authorize for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Bot authorization failed.",
            ephemeral=True,
        )
    elif isinstance(error, ForbiddenError):
        logger.error(
            "Failed to access forbidden resource for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Bot lacks required permissions.",
            ephemeral=True,
        )
    elif isinstance(error, NotFoundError):
        logger.error(
            "Failed to locate resource for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Requested resource not found.",
            ephemeral=True,
        )
    elif isinstance(error, RateLimitTooLongError):
        retry_after = getattr(error, "retry_after", 0)
        route = getattr(error, "route", "unknown")
        logger.error(
            "Failed to execute command '%s' for user %s: rate limit exceeded, retry after %.1fs (route %s)",
            ctx.command.name,
            ctx.user.id,
            retry_after,
            route,
        )
        await reply_err(
            bot,
            ctx,
            f"Rate limit exceeded. Retry after {retry_after:.1f}s.",
            ephemeral=True,
        )
    elif isinstance(error, InternalServerError):
        logger.error(
            "Failed to execute command '%s' for user %s: Discord internal server error",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Discord internal server error.",
            ephemeral=True,
        )

    elif isinstance(error, GatewayConnectionError):
        logger.error(
            "Failed to connect gateway for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Gateway connection failed.",
            ephemeral=True,
        )
    elif isinstance(error, GatewayTransportError):
        logger.error(
            "Failed to transport gateway for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Gateway transport failed.",
            ephemeral=True,
        )
    elif isinstance(error, GatewayServerClosedConnectionError):
        code = getattr(error, "code", "unknown")
        can_reconnect = getattr(error, "can_reconnect", False)
        logger.error(
            "Failed to maintain gateway connection for command '%s' by user %s: server closed connection (code %s, can_reconnect %s)",
            ctx.command.name,
            ctx.user.id,
            code,
            can_reconnect,
        )
        await reply_err(
            bot,
            ctx,
            "Gateway connection closed by server.",
            ephemeral=True,
        )

    elif isinstance(error, ComponentStateConflictError):
        logger.error(
            "Failed to resolve component state for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Component state conflict.",
            ephemeral=True,
        )
    elif isinstance(error, UnrecognisedEntityError):
        logger.error(
            "Failed to recognise entity for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Unrecognised entity encountered.",
            ephemeral=True,
        )
    elif isinstance(error, BulkDeleteError):
        deleted_count = len(getattr(error, "deleted_messages", []))
        logger.error(
            "Failed to bulk delete for command '%s' by user %s: partially completed (%d messages deleted)",
            ctx.command.name,
            ctx.user.id,
            deleted_count,
        )
        await reply_err(
            bot,
            ctx,
            f"Bulk delete partially completed ({deleted_count} messages deleted).",
            ephemeral=True,
        )
    elif isinstance(error, VoiceError):
        logger.error(
            "Failed to process voice for command '%s' by user %s: %s",
            ctx.command.name,
            ctx.user.id,
            str(error),
        )
        await reply_err(
            bot,
            ctx,
            "Voice subsystem error.",
            ephemeral=True,
        )
    elif isinstance(error, MissingIntentError):
        missing_intents = getattr(error, "intents", "unknown")
        logger.error(
            "Failed to execute command '%s' for user %s: missing intents %s",
            ctx.command.name,
            ctx.user.id,
            missing_intents,
        )
        await reply_err(
            bot,
            ctx,
            "Missing required bot intents.",
            ephemeral=True,
        )

    elif isinstance(error, HikariError):
        logger.error(
            "Failed to execute command '%s' for user %s: library error",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Internal library error.",
            ephemeral=True,
        )
    elif isinstance(error, NoResponseIssuedError):
        logger.error(
            "Failed to issue response for command '%s' by user %s: interaction timeout",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "Interaction timed out.",
            ephemeral=True,
        )
    elif isinstance(error, miru.RowFullError):
        logger.error(
            "Failed to add UI component for command '%s' by user %s: row is full",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "UI row is full.",
            ephemeral=True,
        )
    elif isinstance(error, miru.HandlerFullError):
        logger.error(
            "Failed to add UI handler for command '%s' by user %s: handler is full",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "UI handler is full.",
            ephemeral=True,
        )
    elif isinstance(error, miru.ItemAlreadyAttachedError):
        logger.error(
            "Failed to attach UI component for command '%s' by user %s: component already attached",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "UI component already attached.",
            ephemeral=True,
        )
    elif isinstance(error, miru.MiruError):
        logger.error(
            "Failed to process UI for command '%s' by user %s: framework error",
            ctx.command.name,
            ctx.user.id,
        )
        await reply_err(
            bot,
            ctx,
            "UI framework error.",
            ephemeral=True,
        )
    else:
        raise error


# --- Command Export ---


async def export_path_autocomplete(
    ctx: arc.context.autocomplete.AutocompleteData[arc.client.GatewayClient, str],
) -> Sequence[str]:
    choices: list[str] = ["all"]

    try:
        with os.scandir(BASE_DIR) as entries:
            files = [entry.name for entry in entries if entry.is_file() and not entry.name.startswith(".")]
        choices.extend(sorted(files))
    except Exception:
        logger.exception("Failed to autocomplete")
        choices = ["error"]

    return choices[:25]


@cmd_debug.include()
@arc.slash_subcommand(name="export", description="Export files/directories")
async def cmd_export(
    ctx: arc.client.GatewayContext,
    path: arc.Option[
        str,
        arc.StrParams(
            name="path",
            description="Relative path to export",
            autocomplete_with=export_path_autocomplete,
        ),
    ],
) -> None:
    await defer(ctx)

    try:
        target_path = BASE_DIR.joinpath(path).resolve()
    except Exception:
        logger.exception("Failed to resolve export path")
        await reply_err(bot, ctx, "Invalid path format.")
        return

    if BASE_DIR not in target_path.parents and target_path != BASE_DIR:
        await reply_err(bot, ctx, "Path outside allowed directory.")
        return

    if not target_path.exists():
        await reply_err(bot, ctx, f"Path `{path}` does not exist.")
        return

    if not (target_path.is_file() or target_path.is_dir()):
        await reply_err(
            bot,
            ctx,
            f"Path `{path}` is neither file nor directory.",
        )
        return

    async with aiofiles.tempfile.TemporaryDirectory(prefix="export_") as temp_dir:
        base_archive_name = pathlib.Path(temp_dir) / target_path.name

        try:
            tar_filename = await asyncio.to_thread(
                shutil.make_archive,
                base_name=str(base_archive_name),
                format="tar",
                root_dir=str(target_path.parent),
                base_dir=target_path.name,
            )
            zst_filename = str(tar_filename) + ".zst"
            with open(tar_filename, "rb") as f_in, open(zst_filename, "wb") as f_out:
                f_out.write(compression.zstd.compress(f_in.read(), level=6))
            pathlib.Path(tar_filename).unlink()
            archive_filename = zst_filename
        except Exception:
            logger.exception("Failed to create archive")
            await reply_err(bot, ctx, "Failed to create archive.")
            return

        if not archive_filename or not pathlib.Path(archive_filename).exists():
            await reply_err(bot, ctx, "Failed to create archive: output file missing.")
            return

        await ctx.respond(
            f"Exported `{path}`:",
            attachments=[hikari.File(archive_filename)],
        )


# --- Restart Command ---


@cmd_debug.include()
@arc.slash_subcommand(name="restart", description="Restart the bot")
async def cmd_reboot(ctx: arc.client.GatewayContext) -> None:
    await defer(ctx)
    executor = ctx.user

    try:
        embed = await reply_embed(
            bot,
            "Restart Initiated",
            f"{executor.mention} initiated bot restart.",
            Color.WARN,
        )
        if ctx.member:
            embed.set_author(
                name=ctx.member.display_name,
                icon=ctx.member.display_avatar_url,
            )

        await dm_role_members(ctx, embeds=[embed])
        await reply_ok(bot, ctx, "Initiated bot restart sequence.")

        FLAG_DIR.mkdir(exist_ok=True)
        (FLAG_DIR / "restart").write_text(
            f"Restart at {datetime.datetime.now(datetime.timezone.utc).isoformat()} by {executor.id}",
        )
        await bot.close()
    except Exception as e:
        logger.exception("Failed to execute restart command: %s", e)
        await reply_err(bot, ctx, f"Failed to restart bot: {e}")


# --- Load Module Command ---


@cmd_module.include()
@arc.slash_subcommand(name="load", description="Load module from Git URL")
async def cmd_load_mod(
    ctx: arc.client.GatewayContext,
    url: arc.Option[
        str,
        arc.StrParams(
            name="url",
            description="Git repo URL (e.g., https://github.com/user/repo.git)",
        ),
    ],
) -> None:
    await defer(ctx)
    executor = ctx.user

    git_url, parsed_name, validated_url = parse_repo_url(url)
    if not validated_url or not parsed_name:
        await reply_err(bot, ctx, "Failed to parse Git URL: invalid format.")
        return

    if (EXTENSIONS_DIR / parsed_name).is_dir():
        await reply_err(
            bot,
            ctx,
            f"Failed to load module: `{parsed_name}` already exists.",
        )
        return

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, f"Validating `{parsed_name}`", title=None)

    remote_valid, remote_error_msg, _ = await check_remote_module(git_url)
    if not remote_valid:
        await reply_err(bot, ctx, f"Failed to validate module: {remote_error_msg}")
        return

    await dm_role_members(
        ctx,
        embeds=[
            await reply_embed(
                bot,
                "Loading Module",
                f"{executor.mention} is loading `{parsed_name}`.",
            ),
        ],
    )

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, f"Cloning `{parsed_name}`.", title=None)

    cloned_name, clone_success = await asyncio.to_thread(clone_repo, git_url)
    if not clone_success:
        await reply_err(bot, ctx, f"Failed to clone repository `{cloned_name}`.")
        return

    reqs_path = EXTENSIONS_DIR / cloned_name / "requirements.txt"
    if reqs_path.is_file():
        with contextlib.suppress(Exception):
            await reply_ok(bot, ctx, "Installing dependencies.", title=None)
        pip_success = await asyncio.to_thread(run_pip, str(reqs_path), True)
        if not pip_success:
            await asyncio.to_thread(delete_module, cloned_name)
            await reply_err(
                bot,
                ctx,
                "Failed to install dependencies: module removed.",
            )
            return

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, f"Loading `{cloned_name}`", title=None)

    if await load_module(bot, cloned_name):
        await reply_ok(bot, ctx, f"Loaded module `{cloned_name}`.")
        await dm_role_members(
            embeds=[
                await reply_embed(
                    bot,
                    "Module Loaded",
                    f"`{cloned_name}` loaded by {executor.mention}.",
                ),
            ],
        )


# --- Module Autocomplete ---


async def mod_autocomplete(
    ctx: arc.context.autocomplete.AutocompleteData[arc.client.GatewayClient, str],
) -> Sequence[str]:
    query = (ctx.focused_value or "").lower() if ctx.focused_value else ""
    choices: list[str]

    try:
        with os.scandir(EXTENSIONS_DIR) as entries:
            valid_modules = [
                entry.name
                for entry in entries
                if entry.is_dir() and entry.name != "__pycache__" and is_valid_repo(entry.name)
            ]

        matching = [m for m in valid_modules if query in m.lower()]
        choices = sorted(matching)[:25]

        if valid_modules and not matching and query:
            choices = ["no_match"]
        elif not valid_modules:
            choices = ["none"]
    except Exception:
        logger.exception("Failed to autocomplete")
        choices = ["error"]

    return choices


# --- Unload Module Command ---


@cmd_module.include()
@arc.slash_subcommand(name="unload", description="Unload and delete a module")
async def cmd_unload_mod(
    ctx: arc.client.GatewayContext,
    module: arc.Option[
        str,
        arc.StrParams(
            name="module",
            description="Module name",
            autocomplete_with=mod_autocomplete,
        ),
    ],
) -> None:
    await defer(ctx)
    executor = ctx.user

    logger.info(
        "User %s (%s) requested unload of module: %s",
        executor.username,
        executor.id,
        module,
    )

    module_path = EXTENSIONS_DIR / module
    if not module_path.is_dir():
        await reply_err(
            bot,
            ctx,
            f"Failed to locate module: directory `{module}` not found.",
        )
        return

    if not is_valid_repo(module):
        await reply_err(
            bot,
            ctx,
            f"`{module}` is not a valid module repository.",
        )
        return

    info, _ = get_module_info(module)
    commit_id = str(info.cur_commit.id)[:7] if info and info.cur_commit else "Unknown"
    remote_url = info.url if info else "Unknown"

    embed = await reply_embed(
        bot,
        "Unloading Module",
        f"{executor.mention} is unloading `{module}`.",
        Color.WARN,
    )
    embed.add_field(name="Current Commit", value=f"`{commit_id}`", inline=True)
    embed.add_field(name="Remote URL", value=remote_url, inline=True)
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])

    module_full_name = f"extensions.{module}.main"
    unload_success = True

    try:
        arc_client.unload_extension(module_full_name)
        logger.info("Unloaded extension '%s'", module_full_name)
    except Exception as e:
        unload_success = False
        logger.exception("Failed to unload extension '%s': %s", module_full_name, e)
        await reply_err(
            bot,
            ctx,
            f"Failed to unload extension `{module}`: {e}. Attempting cleanup.",
        )

    if unload_success:
        try:
            if arc_client.is_started:
                await arc_client.resync_commands()
                logger.info("Resynced commands after extension unload")
        except Exception as e:
            logger.exception("Failed to resync commands after unload: %s", e)

    delete_success = await asyncio.to_thread(delete_module, module)

    if delete_success:
        final_message = f"Unloaded module `{module}` and removed its directory."
        if not unload_success:
            final_message += " (Extension unload encountered issues, but cleanup completed.)"
        await reply_ok(bot, ctx, final_message)
        completion_embed = await reply_embed(
            bot,
            "Module Unloaded",
            f"`{module}` unloaded and deleted by {executor.mention}.",
        )
        await dm_role_members(embeds=[completion_embed])
    else:
        await reply_err(
            bot,
            ctx,
            f"Failed to delete directory for module `{module}`. Manual cleanup required.",
        )


# --- Update Module Command ---


@cmd_module.include()
@arc.slash_subcommand(
    name="update",
    description="Update a module to the latest version",
)
async def cmd_update_mod(
    ctx: arc.client.GatewayContext,
    module: arc.Option[
        str,
        arc.StrParams(
            name="module",
            description="Module name",
            autocomplete_with=mod_autocomplete,
        ),
    ],
) -> None:
    await defer(ctx)
    executor = ctx.user
    module_dir = EXTENSIONS_DIR / module
    backup_base = BACKUP_DIR / module
    module_full_name = f"extensions.{module}.main"

    logger.info(
        "User %s (%s) requested update of module: %s",
        executor.username,
        executor.id,
        module,
    )

    if not module_dir.is_dir():
        await reply_err(
            bot,
            ctx,
            f"Failed to locate module: directory `{module}` not found.",
        )
        return

    info, valid_info = get_module_info(module)
    if not valid_info or not info:
        await reply_err(
            bot,
            ctx,
            f"Failed to retrieve repository info for module `{module}`.",
        )
        return

    if info.cur_commit and info.rmt_commit and info.cur_commit.id == info.rmt_commit.id and info.mods == 0:
        await reply_ok(
            bot,
            ctx,
            f"Module `{module}` already up-to-date (no local modifications).",
        )
        return

    current_commit_id = str(info.cur_commit.id) if info.cur_commit else "Unknown"
    target_commit_id = str(info.rmt_commit.id) if info.rmt_commit else "Unknown"

    logger.info(
        "Initiated update for module %s: cur=%s target=%s mods=%d",
        module,
        current_commit_id,
        target_commit_id,
        info.mods,
    )

    embed = await reply_embed(
        bot,
        "Updating Module",
        f"{executor.mention} is updating `{module}`.",
        Color.WARN,
    )
    embed.url = info.url
    embed.add_field(name="Current Commit", value=f"`{current_commit_id}`", inline=True)
    embed.add_field(name="Target Commit", value=f"`{target_commit_id}`", inline=True)
    if info.mods > 0:
        embed.add_field(name="Warning", value="Local modifications detected!")
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, f"Backing up module `{module}`.", title=None)

    patch_path: str | None = None
    has_local_changes = False
    original_commit_id = info.cur_commit.id if info.cur_commit else None

    try:
        BACKUP_DIR.mkdir(exist_ok=True)
        if backup_base.exists():
            await asyncio.to_thread(
                shutil.rmtree,
                backup_base,
                ignore_errors=False,
                onerror=None,
            )

        await asyncio.to_thread(shutil.copytree, module_dir, backup_base, symlinks=True)
        logger.info("Created backup of module '%s' at '%s'", module, backup_base)

        repo = pygit2.Repository(str(module_dir))
        diff = repo.diff()
        if diff.patch:
            has_local_changes = True
            patch_path = str(backup_base / "local_changes.patch")
            try:
                pathlib.Path(patch_path).write_text(diff.patch, encoding="utf-8")
                logger.info(
                    "Saved local changes patch for '%s' to '%s'",
                    module,
                    patch_path,
                )
            except Exception as e:
                logger.exception("Failed to write patch file for '%s': %s", module, e)
                patch_path = None
                has_local_changes = False
    except Exception as e:
        logger.exception(
            "Failed to create backup for module '%s': %s",
            module,
            e,
        )
        await reply_err(
            bot,
            ctx,
            f"Failed to create backup: {e}. Update aborted.",
        )
        return

    async def restore_backup() -> bool:
        logger.warning(
            "Attempting to restore module '%s' from backup '%s'",
            module,
            backup_base,
        )
        if not backup_base.exists():
            logger.exception(
                "Failed to restore: backup directory '%s' missing",
                backup_base,
            )
            return False
        try:
            if module_dir.exists():
                await aioshutil.rmtree(module_dir)
            await aioshutil.copytree(backup_base, module_dir, symlinks=True)

            if original_commit_id:
                try:
                    repo = pygit2.Repository(str(module_dir))
                    from pygit2.enums import ResetMode

                    repo.reset(original_commit_id, ResetMode.HARD)
                    logger.info(
                        "Reset module '%s' to original commit %s",
                        module,
                        original_commit_id,
                    )
                except Exception as reset_err:
                    logger.exception(
                        "Failed to reset module '%s' after restore: %s",
                        module,
                        reset_err,
                    )
            logger.info("Restored module '%s' from backup", module)
            return True
        except Exception as err:
            logger.exception(
                "Failed to restore module '%s' from backup: %s",
                module,
                err,
            )
            return False

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, f"Pulling updates for module `{module}`.", title=None)

    pull_result = await asyncio.to_thread(pull_module, module)

    if pull_result != 0:
        error_reasons = {
            1: "Cannot update the main repo using this command.",
            2: "Failed to fetch or apply remote changes.",
            3: "Master branch not found or checkout failed.",
        }
        error_msg = error_reasons.get(pull_result, "Git pull failed with unknown error")
        logger.exception(
            "Failed to pull updates for module '%s': %s (%s)",
            module,
            error_msg,
            pull_result,
        )
        if await restore_backup():
            await reply_err(
                bot,
                ctx,
                f"Failed to update module: {error_msg}. Restored from backup.",
            )
        else:
            await reply_err(
                bot,
                ctx,
                f"Failed to update module: {error_msg}. CRITICAL: Backup restoration failed. Manual intervention required.",
            )
        return

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, f"Updating dependencies for `{module}`.", title=None)

    pip_success = await asyncio.to_thread(
        run_pip,
        str(module_dir / "requirements.txt"),
        install=True,
    )

    if not pip_success:
        logger.exception(
            "Failed to update dependencies for module '%s'",
            module,
        )
        if await restore_backup():
            await reply_err(
                bot,
                ctx,
                f"Failed to update dependencies for `{module}`. Restored from backup.",
            )
        else:
            await reply_err(
                bot,
                ctx,
                f"Failed to update dependencies for `{module}`. CRITICAL: Backup restoration failed. Manual intervention required.",
            )
        return

    with contextlib.suppress(Exception):
        await reply_ok(
            bot,
            ctx,
            f"Validating and reloading module `{module}`.",
            title=None,
        )

    reload_success = await load_module(bot, module, is_reload=True)

    if not reload_success:
        logger.exception("Failed to reload module '%s' after update", module)
        if await restore_backup():
            logger.info("Loading restored version of module '%s'", module)

            final_load_success = await load_module(bot, module, is_reload=False)

            if final_load_success:
                await reply_err(
                    bot,
                    ctx,
                    f"Failed to reload updated module `{module}`: previous version restored and loaded.",
                )
            else:
                await reply_err(
                    bot,
                    ctx,
                    f"Failed to reload updated module `{module}`: restored from backup but reload failed. Manual intervention required.",
                )
        else:
            await reply_err(
                bot,
                ctx,
                f"Failed to reload updated module `{module}`. CRITICAL: Backup restoration failed. Manual intervention required.",
            )
        with contextlib.suppress(Exception):
            if backup_base.exists():
                await aioshutil.rmtree(backup_base)
                logger.info(
                    "Cleaned up backup '%s' after failed reload",
                    backup_base,
                )
        return

    if has_local_changes and patch_path and pathlib.Path(patch_path).exists():
        logger.info("Reapplying local changes from '%s'", patch_path)
        try:
            process = await asyncio.create_subprocess_exec(
                "git",
                "apply",
                "--reject",
                "--whitespace=fix",
                patch_path,
                cwd=str(module_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            stdout_str = stdout.decode(errors="ignore").strip()
            stderr_str = stderr.decode(errors="ignore").strip()

            if process.returncode == 0:
                logger.info("Reapplied local changes for '%s'", module)
                if stdout_str:
                    logger.info("Git apply stdout: %s", stdout_str)
                if stderr_str:
                    logger.info("Git apply stderr: %s", stderr_str)
                await ctx.respond(
                    "Reapplied local changes.",
                    flags=hikari.MessageFlag.EPHEMERAL,
                )
            else:
                logger.exception(
                    "Failed to apply patch for '%s' (exit %d)",
                    module,
                    process.returncode,
                )
                if stdout_str:
                    logger.exception("Git apply stdout: %s", stdout_str)
                if stderr_str:
                    logger.exception("Git apply stderr: %s", stderr_str)
                await reply_err(
                    bot,
                    ctx,
                    f"Update completed but failed to reapply local changes. Changes preserved in `{patch_path}`.",
                    ephemeral=False,
                )
        except Exception as patch_err:
            logger.exception(
                "Failed to reapply patch for '%s': %s",
                module,
                patch_err,
            )
            await reply_err(
                bot,
                ctx,
                f"Update completed but failed to reapply local changes: {patch_err}",
            )

    try:
        changelog_content = "No changelog provided."
        if (changelog_path := module_dir / "CHANGELOG").is_file():
            with contextlib.suppress(Exception):
                changelog_content = changelog_path.read_text(
                    encoding="utf-8",
                    errors="ignore",
                )

        result_embed = await reply_embed(
            bot,
            "Module Updated",
            f"Updated module `{module}` to commit `{target_commit_id[:7]}`.",
        )

        max_field_len = 1000
        changelog_chunks = [
            changelog_content[i : i + max_field_len] for i in range(0, len(changelog_content), max_field_len)
        ]

        for i, chunk in enumerate(changelog_chunks):
            field_name = (
                "CHANGELOG" if len(changelog_chunks) == 1 else f"CHANGELOG (Part {i + 1}/{len(changelog_chunks)})"
            )
            result_embed.add_field(
                name=field_name,
                value=f"```md\n{chunk.strip() or 'No CHANGELOG available.'}\n```",
            )

        pages = [result_embed]
        navigator = nav.navigator.NavigatorView(
            pages=pages,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client)
        await ctx.respond_with_builder(builder)
        miru_client.start_view(navigator)

        completion_embed = await reply_embed(
            bot,
            "Module Updated",
            f"`{module}` updated by {executor.mention}.",
        )
        await dm_role_members(embeds=[completion_embed])

    except Exception as e:
        logger.exception("Failed to send update confirmation: %s", e)
        await reply_ok(
            bot,
            ctx,
            f"Updated `{module}` but failed to display details.",
        )

    with contextlib.suppress(Exception):
        if backup_base.exists():
            await aioshutil.rmtree(backup_base)
            logger.info("Cleaned up backup '%s'", backup_base)


# --- Module Info Command ---


@cmd_module.include()
@arc.slash_subcommand(name="info", description="Show information about a loaded module")
async def cmd_info_mod(
    ctx: arc.client.GatewayContext,
    module: arc.Option[
        str,
        arc.StrParams(
            name="module",
            description="Module name",
            autocomplete_with=mod_autocomplete,
        ),
    ],
) -> None:
    await defer(ctx)

    info, valid = get_module_info(module)
    if not valid or not info:
        await reply_err(
            bot,
            ctx,
            f"Failed to locate module `{module}`: not found or invalid repository. Use `/kernel module list`.",
        )
        return

    try:
        has_modifications = info.mods > 0
        embed_color = Color.WARN if has_modifications else Color.INFO
        result_embed = await reply_embed(
            bot,
            f"Module: `{module}`",
            f"Repository: {info.url}",
            embed_color,
        )
        result_embed.url = info.url

        current_commit_ts = info.cur_time_utc or "N/A"
        remote_commit_ts = info.rmt_time_utc or "N/A"
        current_commit_id = str(info.cur_commit.id)[:10] if info.cur_commit else "N/A"
        remote_commit_id = str(info.rmt_commit.id)[:10] if info.rmt_commit else "N/A"

        result_embed.add_field(
            name="Status",
            value=(f"{info.mods} local modification(s) detected" if has_modifications else "No local modifications"),
        )
        result_embed.add_field(
            name="Local Commit",
            value=f"ID: `{current_commit_id}`\nTime: `{current_commit_ts}`",
            inline=True,
        )
        result_embed.add_field(
            name="Remote Commit",
            value=f"ID: `{remote_commit_id}`\nTime: `{remote_commit_ts}`",
            inline=True,
        )

        changelog = info.changelog.strip() or "No changelog information available."
        max_field_len = 1000
        chunks = [changelog[i : i + max_field_len] for i in range(0, len(changelog), max_field_len)]
        for i, chunk in enumerate(chunks):
            field_name = "Recent Changes"
            if len(chunks) > 1:
                field_name += f" (Part {i + 1}/{len(chunks)})"
            result_embed.add_field(name=field_name, value=f"```md\n{chunk}\n```")

        pages = [result_embed]
        navigator = nav.navigator.NavigatorView(
            pages=pages,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client)
        await ctx.respond_with_builder(builder)
        miru_client.start_view(navigator)

    except Exception as e:
        logger.exception("Failed to send module info for '%s': %s", module, e)
        await reply_err(bot, ctx, f"Failed to display module information: {e}.")


# --- List Modules Command ---


def get_loadable_mods() -> list[str]:
    try:
        with os.scandir(EXTENSIONS_DIR) as entries:
            modules = [
                entry.name
                for entry in entries
                if entry.is_dir() and entry.name != "__pycache__" and is_valid_repo(entry.name)
            ]
        return sorted(modules, key=lambda x: x.casefold())
    except Exception:
        logger.exception("Failed to enumerate loadable modules")
        return []


@cmd_module.include()
@arc.slash_subcommand(name="list", description="List all loaded modules")
async def cmd_list_mods(ctx: arc.client.GatewayContext) -> None:
    await defer(ctx)

    modules_list = get_loadable_mods()

    if not modules_list:
        await reply_err(
            bot,
            ctx,
            "Failed to locate any modules: extensions directory empty. Use `/kernel module load` to add modules.",
        )
        return

    embed = await reply_embed(
        bot,
        "Module List",
        f"Discovered {len(modules_list)} loadable modules:",
    )

    field_count = 0
    max_fields = 25
    for module_name in modules_list:
        if field_count >= max_fields:
            embed.set_footer(text=f"Displaying first {max_fields} modules.")
            break
        try:
            info, valid = get_module_info(module_name)
            if valid and info:
                commit_id = str(info.cur_commit.id)[:7] if info.cur_commit else "N/A"
                display_name = (
                    module_name.split("__")[-1]
                    .replace("_s_", "/")
                    .replace("_u_", "_")
                    .replace("_d_", ".")
                    .replace("_h_", "-")
                )
                status = "WARNING" if info.mods > 0 else ""
                embed.add_field(
                    name=f"{status}{display_name}",
                    value=f"`{module_name}`\nCommit: `{commit_id}`",
                    inline=True,
                )
                field_count += 1
            else:
                embed.add_field(
                    name=module_name,
                    value="*Error fetching info*",
                    inline=True,
                )
                field_count += 1
        except Exception as e:
            logger.exception(
                "Failed to process module '%s' for list: %s",
                module_name,
                e,
            )
            embed.add_field(name=module_name, value="*Error*", inline=True)
            field_count += 1

    try:
        pages = [embed]
        navigator = nav.navigator.NavigatorView(
            pages=pages,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client)
        await ctx.respond_with_builder(builder)
        miru_client.start_view(navigator)
    except Exception as e:
        logger.exception("Failed to send module list: %s", e)
        await reply_err(bot, ctx, f"Failed to display module list: {e}.")


# --- Download Command ---


_dl_lock = asyncio.Lock()


@cmd_debug.include()
@arc.slash_subcommand(
    name="download",
    description="Download current running code as tarball",
)
async def cmd_download(ctx: arc.client.GatewayContext) -> None:
    if _dl_lock.locked():
        await ctx.respond(
            "Download already in progress.",
            flags=hikari.MessageFlag.EPHEMERAL,
        )
        return

    async with _dl_lock:
        await defer(ctx)
        logger.info(
            "User %s (%s) requested code download",
            ctx.user.username,
            ctx.user.id,
        )

        def _compress_code(filename: str, source_path: pathlib.Path) -> None:
            excluded_patterns = frozenset(
                {
                    ".git",
                    "venv",
                    "__pycache__",
                    "*.pyc",
                    "*.log",
                    ".env",
                    ".bak",
                    "flag",
                },
            )

            def tar_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
                path = pathlib.Path(tarinfo.name)
                for part in path.parts:
                    if part in excluded_patterns:
                        logger.info("Excluding '%s' from archive", tarinfo.name)
                        return None
                    for pattern in excluded_patterns:
                        if "*" in pattern and pathlib.PurePosixPath(part).match(
                            pattern,
                        ):
                            logger.info("Excluding '%s' from archive", tarinfo.name)
                            return None
                if pathlib.Path(tarinfo.name).name == pathlib.Path(filename).name:
                    return None
                return tarinfo

            tar_filename = filename.replace(".tar.zst", ".tar")
            with tarfile.open(tar_filename, "w") as tar:
                tar.add(source_path, arcname=".", filter=tar_filter)

            with open(tar_filename, "rb") as f_in, open(filename, "wb") as f_out:
                f_out.write(compression.zstd.compress(f_in.read(), level=6))

            pathlib.Path(tar_filename).unlink()
            logger.info("Compressed code to '%s'", filename)

        temp_file_path: str | None = None
        try:
            async with aiofiles.tempfile.NamedTemporaryFile(
                mode="wb",
                suffix=".tar.zst",
                prefix="Discord-Bot-Framework_",
                delete=False,
            ) as tmp:
                temp_name = tmp.name
                if isinstance(temp_name, int):
                    raise RuntimeError("Temporary file path is a file descriptor")
                temp_file_path = os.fsdecode(os.fspath(temp_name))
            if not temp_file_path:
                raise RuntimeError("Temporary file path is empty")

            await asyncio.to_thread(_compress_code, temp_file_path, BASE_DIR)

            file_size = pathlib.Path(temp_file_path).stat().st_size
            logger.info(
                "Sending archive '%s' (%d bytes)",
                temp_file_path,
                file_size,
            )
            await ctx.respond(
                "Code archive attached:",
                attachments=[
                    hikari.File(temp_file_path, filename="client_code.tar.zst"),
                ],
            )

        except Exception as e:
            logger.exception("Failed to complete code download")
            await reply_err(
                bot,
                ctx,
                f"Failed to complete download: {e}.",
            )
        finally:
            if temp_file_path and pathlib.Path(temp_file_path).exists():
                with contextlib.suppress(Exception):
                    pathlib.Path(temp_file_path).unlink()
                    logger.info(
                        "Cleaned up temporary file: %s",
                        temp_file_path,
                    )


# --- Debug Info Command ---


@cmd_debug.include()
@arc.slash_subcommand(
    name="info",
    description="Show debugging information and system status",
)
async def cmd_info(ctx: arc.client.GatewayContext) -> None:
    await defer(ctx)

    me = bot.get_me()
    system_info = {
        "python_version": sys.version.split()[0],
        "platform": sys.platform,
        "pid": os.getpid(),
        "cwd": str(BASE_DIR),
        "log_file": (str(LOG_FILE.relative_to(BASE_DIR)) if LOG_FILE.is_relative_to(BASE_DIR) else str(LOG_FILE)),
    }

    bot_info = {
        "user_id": me.id if me else "N/A",
        "username": me.username if me else "N/A",
        "guild_count": len(bot.cache.get_guilds_view()),
        "module_count": len(get_loadable_mods()),
        "latency": f"{bot.heartbeat_latency * 1000:.2f} ms" if bot.heartbeat_latency else "N/A",
    }

    embed = await reply_embed(
        bot,
        "System Status",
        "Runtime diagnostics and bot information.",
        Color.INFO,
    )

    embed.add_field(
        name="System",
        value=f"- Python: `{system_info['python_version']}`\n"
        f"- Platform: `{system_info['platform']}`\n"
        f"- PID: `{system_info['pid']}`\n"
        f"- Log: `{system_info['log_file']}`",
        inline=True,
    )
    embed.add_field(
        name="Bot",
        value=f"- User: `{bot_info['username']}` ({bot_info['user_id']})\n"
        f"- Guilds: `{bot_info['guild_count']}`\n"
        f"- Modules: `{bot_info['module_count']}`\n"
        f"- Latency: `{bot_info['latency']}`",
        inline=True,
    )

    try:
        log_dir = LOG_FILE.parent
        if log_dir.is_dir():
            log_files = sorted(
                (f for f in log_dir.glob("*.log*") if f.is_file()),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            )[:5]

            if log_files:
                log_list = "\n".join(f"- `{f.name}` ({f.stat().st_size // 1024} KB)" for f in log_files)
                embed.add_field(name="Recent Logs", value=log_list)
    except Exception as e:
        logger.exception("Failed to list log files: %s", e)

    try:
        pages = [embed]
        navigator = nav.navigator.NavigatorView(
            pages=pages,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client)
        await ctx.respond_with_builder(builder)
        miru_client.start_view(navigator)
    except Exception as e:
        logger.exception("Failed to send system status: %s", e)
        await reply_err(bot, ctx, f"Failed to display system status: {e}.")


# --- Kernel Info Command ---


@cmd_review.include()
@arc.slash_subcommand(
    name="info",
    description="Show information about the Kernel (main bot code)",
)
async def cmd_info_kernel(ctx: arc.client.GatewayContext) -> None:
    await defer(ctx)

    info = get_kernel_info()
    if not info:
        await reply_err(
            bot,
            ctx,
            "Failed to retrieve kernel repository information.",
        )
        return

    try:
        color = Color.WARN if info.mods > 0 else Color.INFO
        embed = await reply_embed(
            bot,
            "Kernel",
            f"Repository: {info.url}",
            color,
        )
        embed.url = info.url

        current_commit_ts = info.cur_time_utc or "N/A"
        remote_commit_ts = info.rmt_time_utc or "N/A"
        current_commit_id = str(info.cur_commit.id)[:10] if info.cur_commit else "N/A"
        remote_commit_id = str(info.rmt_commit.id)[:10] if info.rmt_commit else "N/A"

        embed.add_field(
            name="Status",
            value=(f"{info.mods} local modification(s) detected" if info.mods > 0 else "No local modifications"),
        )
        embed.add_field(
            name="Local Commit",
            value=f"ID: `{current_commit_id}`\nTime: `{current_commit_ts}`",
            inline=True,
        )
        embed.add_field(
            name="Remote Commit",
            value=f"ID: `{remote_commit_id}`\nTime: `{remote_commit_ts}`",
            inline=True,
        )

        pages = [embed]
        navigator = nav.navigator.NavigatorView(
            pages=pages,
            timeout=180,
            autodefer=True,
        )
        builder = await navigator.build_response_async(miru_client)
        await ctx.respond_with_builder(builder)
        miru_client.start_view(navigator)

    except Exception as e:
        logger.exception("Failed to send kernel information: %s", e)
        await reply_err(bot, ctx, f"Failed to display kernel information: {e}.")


# --- Update Kernel Command ---


@cmd_review.include()
@arc.slash_subcommand(
    name="update",
    description="Update the kernel to the latest version",
)
async def cmd_update_kernel(ctx: arc.client.GatewayContext) -> None:
    await defer(ctx)
    executor = ctx.user

    logger.info(
        "User %s (%s) initiated kernel update",
        executor.username,
        executor.id,
    )

    info = get_kernel_info()
    if not info:
        await reply_err(
            bot,
            ctx,
            "Failed to retrieve kernel repository information.",
        )
        return

    if info.cur_commit and info.rmt_commit and info.cur_commit.id == info.rmt_commit.id and info.mods == 0:
        await reply_ok(
            bot,
            ctx,
            "Kernel already up-to-date (no local modifications).",
        )
        return

    current_commit_id = str(info.cur_commit.id) if info.cur_commit else "Unknown"
    target_commit_id = str(info.rmt_commit.id) if info.rmt_commit else "Unknown"

    logger.info(
        "Initiated kernel update: cur=%s target=%s mods=%d",
        current_commit_id,
        target_commit_id,
        info.mods,
    )

    embed = await reply_embed(
        bot,
        "Updating Kernel",
        f"{executor.mention} is updating kernel.",
        Color.WARN,
    )
    embed.url = info.url
    embed.add_field(name="Current Commit", value=f"`{current_commit_id}`", inline=True)
    embed.add_field(name="Target Commit", value=f"`{target_commit_id}`", inline=True)
    if info.mods > 0:
        embed.add_field(
            name="Warning",
            value="Local modifications detected and will be overwritten.",
        )
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon=ctx.member.display_avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, "Pulling kernel updates.", title=None)

    pull_result = await asyncio.to_thread(pull_kernel)

    if pull_result != 0:
        error_reasons = {
            1: "Repository misidentified as kernel",
            2: "Failed to fetch or apply remote changes",
            3: "Master branch checkout failed",
        }
        error_msg = error_reasons.get(pull_result, "Git pull failed with")
        logger.exception(
            "Failed to pull kernel updates: %s (%s)",
            error_msg,
            pull_result,
        )
        await reply_err(
            bot,
            ctx,
            f"Failed to update kernel: {error_msg}.",
        )
        return

    with contextlib.suppress(Exception):
        await reply_ok(bot, ctx, "Updating kernel dependencies.", title=None)

    pip_success = await asyncio.to_thread(
        run_pip,
        str(BASE_DIR / "requirements.txt"),
        install=True,
    )

    if not pip_success:
        logger.exception("Failed to update kernel dependencies")
        await reply_err(
            bot,
            ctx,
            "Kernel updated but dependency update failed. Bot may be unstable. Check logs and requirements.txt.",
        )
    else:
        logger.info("Updated kernel dependencies")

    with contextlib.suppress(Exception):
        await reply_ok(
            bot,
            ctx,
            "Kernel update complete. Signaling restart now.",
            title=None,
        )

    result_embed = await reply_embed(
        bot,
        "Kernel Updated",
        f"Updated to `{target_commit_id[:7]}`. Restarting.",
    )
    await response(ctx, embeds=[result_embed])

    try:
        FLAG_DIR.mkdir(exist_ok=True)
        reboot_flag = FLAG_DIR / "restart"
        reboot_flag.write_text(
            f"Restart triggered post-kernel-update at {datetime.datetime.now(datetime.timezone.utc).isoformat()} by {executor.id}",
        )
        logger.info("Set restart flag at %s", reboot_flag)
        reboot_notice_embed = await reply_embed(
            bot,
            "Restarting",
            "Kernel updated. Bot restarting.",
        )
        await dm_role_members(embeds=[reboot_notice_embed])
    except Exception:
        logger.exception(
            "Failed to signal restart after kernel update",
        )
        await reply_err(
            bot,
            ctx,
            "Kernel updated but restart signaling failed. Manual restart required.",
        )


# --- Main Execution ---


async def main() -> None:
    def exception_handler(event_loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exception = context.get("exception")
        if exception:
            logger.exception(
                "Failed to handle unhandled exception in event loop: %s",
                exception,
                exc_info=exception,
            )
        else:
            logger.exception(
                "Failed to process event loop error: %s",
                context.get("message"),
            )

    async def shutdown_handler(signal: signal.Signals) -> None:
        logger.info("Received exit signal %s, shutting down gracefully", signal.name)
        shutdown_event.set()

        current_task = asyncio.current_task()
        pending_tasks: set[asyncio.Task] = {task for task in asyncio.all_tasks() if task is not current_task}

        with contextlib.suppress(Exception):
            if bot and bot.is_alive:
                logger.warning("Closing bot connection")
                await bot.close()
                logger.warning("Closed bot connection")

        if not pending_tasks:
            logger.warning("Found no outstanding tasks to cancel")
            return

        logger.info("Cancelling %d outstanding tasks", len(pending_tasks))
        for task in pending_tasks:
            task.cancel()

        try:
            completed_tasks, unfinished_tasks = await asyncio.wait(
                pending_tasks,
                return_when=asyncio.ALL_COMPLETED,
            )
            logger.info("Cancelled %d tasks", len(completed_tasks))
            if unfinished_tasks:
                logger.warning(
                    "Failed to cancel %d tasks within timeout",
                    len(unfinished_tasks),
                )
        except Exception:
            logger.exception("Failed to wait for task cancellation")

    shutdown_event = asyncio.Event()
    event_loop = asyncio.get_running_loop()
    event_loop.set_exception_handler(exception_handler)

    for signal_type in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(
            signal_type,
            lambda s=signal_type: asyncio.create_task(shutdown_handler(s)),
        )

    discovered_extensions: set[str] = set()
    try:
        with os.scandir(EXTENSIONS_DIR) as directory_entries:
            for entry in directory_entries:
                entry_name = entry.name
                if entry.is_file() and entry_name.endswith(".py") and not entry_name.startswith("_"):
                    discovered_extensions.add(f"extensions.{entry_name[:-3]}")
                elif (
                    entry.is_dir()
                    and entry_name != "__pycache__"
                    and (EXTENSIONS_DIR / entry_name / "main.py").is_file()
                ):
                    discovered_extensions.add(f"extensions.{entry_name}.main")
    except Exception:
        logger.exception("Failed to discover extensions")

    extension_modules = frozenset(discovered_extensions)
    logger.info("Discovered %d potential extensions", len(extension_modules))

    if extension_modules:
        logger.info("Loading extensions: %s", sorted(extension_modules))
        loaded_extensions: set[str] = set()
        failed_extensions: set[str] = set()
        for extension_module in sorted(extension_modules, key=str.casefold):
            module_parts = extension_module.rsplit(".", 2)
            module_name = module_parts[-2] if module_parts[-1] == "main" else module_parts[-1]
            try:
                if extension_module.endswith(".main"):
                    (loaded_extensions if await load_module(bot, module_name) else failed_extensions).add(
                        extension_module,
                    )
                else:
                    arc_client.load_extension(extension_module)
                    logger.info("Loaded extension '%s'", extension_module)
                    loaded_extensions.add(extension_module)
            except Exception:
                logger.exception("Failed to load extension '%s'", extension_module)
                failed_extensions.add(extension_module)
        logger.info(
            "Loaded %d extensions, failed to load %d",
            len(loaded_extensions),
            len(failed_extensions),
        )
        if failed_extensions:
            logger.warning("Failed to load extensions: %s", failed_extensions)

    try:
        await asyncio.gather(
            bot.start(),
            shutdown_event.wait(),
            return_exceptions=True,
        )
    except Exception:
        logger.exception("Failed to start bot client")


def entrypoint() -> None:
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Failed to execute main")
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
