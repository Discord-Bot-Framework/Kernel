from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import enum
import functools
import importlib
import logging
import os
import pathlib
import shutil
import signal
import sys
import tarfile
from importlib.metadata import distribution
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING, Final, TypeVar

import aiofiles
import aioshutil
import compression.zstd
import interactions
import pygit2
from dotenv import load_dotenv
from interactions.api.events import (
    AutocompleteError,
    CommandError,
    ComponentError,
    ModalError,
)
from interactions.client.errors import (
    AlreadyDeferred,
    AlreadyResponded,
    BadArgument,
    BadRequest,
    BotException,
    CommandCheckFailure,
    CommandException,
    CommandOnCooldown,
    DiscordError,
    EmptyMessageException,
    EphemeralEditException,
    EventLocationNotProvided,
    ExtensionException,
    ExtensionLoadException,
    ExtensionNotFound,
    Forbidden,
    ForeignWebhookException,
    GatewayNotFound,
    HTTPException,
    InteractionException,
    InteractionMissingAccess,
    LibraryException,
    LoginError,
    MaxConcurrencyReached,
    MessageException,
    NotFound,
    RateLimited,
    ThreadException,
    ThreadOutsideOfGuild,
    TooManyChanges,
    VoiceAlreadyConnected,
    VoiceConnectionTimeout,
    VoiceWebSocketClosed,
    WebSocketClosed,
    WebSocketRestart,
)
from interactions.ext.paginators import Paginator

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

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
logger.addHandler(log_handler)


T = TypeVar("T")


# --- Client ---


if not TOKEN:
    logger.critical("Failed to load TOKEN")
    sys.exit(1)

client = interactions.Client(
    token=TOKEN,
    activity=interactions.Activity(
        name="with interactions.py",
        type=interactions.ActivityType.COMPETING,
        created_at=interactions.Timestamp.now(datetime.timezone.utc),
    ),
    debug_scope=GUILD_ID,
    intents=interactions.Intents.ALL,
    disable_dm_commands=True,
    auto_defer=True,
)

os.environ.pop("TOKEN", None)
with contextlib.suppress(Exception):
    pathlib.Path(".env").unlink(missing_ok=True)


@interactions.listen()
async def on_login(event: interactions.api.events.internal.Login) -> None:
    logger.info("Detected app type: %s", type(event.client).__name__)
    logger.info("Detected app instance: %s", event.client)


@interactions.listen()
async def on_startup(event: interactions.api.events.internal.Startup) -> None:
    logger.info("Detected app type: %s", type(event.client).__name__)
    logger.info("Detected app instance: %s", event.client)
    try:
        await client.synchronise_interactions(delete_commands=True)
        if client.user:
            await client.change_presence(
                status=interactions.Status.ONLINE if client.user else interactions.Status.DO_NOT_DISTURB,
            )
            logger.info(
                "Authenticated as %s (%s)",
                client.user.username,
                client.user.id,
            )
        else:
            logger.exception("Failed to retrieve client user object")
    except Exception:
        logger.exception("Failed to complete startup sequence")


# --- Git ---


URL_TRANS_MAP = str.maketrans({"_": "_u_", "/": "_s_", ".": "_d_", "-": "_h_"})
MAIN_REPO_PATH: Final[str | None] = pygit2.discover_repository(str(BASE_DIR))


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
        f"{relevant_netloc.translate(URL_TRANS_MAP)}__{path_part.translate(URL_TRANS_MAP)}",
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
        logger.exception("Failed to clone repository: %s", url)
        shutil.rmtree(repo_path, ignore_errors=True)
        return "", False


def pull_repo(repo_path_str: str) -> int:
    try:
        repo = pygit2.Repository(repo_path_str)
        origin = repo.remotes["origin"]
    except Exception:
        logger.exception("Failed to pull repository: %s", repo_path_str)
        return 2
    with contextlib.suppress(Exception):
        origin.fetch()
    try:
        remote_ref = repo.lookup_reference("refs/remotes/origin/master")
        local_ref = repo.lookup_reference("refs/heads/master")
    except Exception:
        logger.exception("Failed to lookup reference: %s", repo_path_str)
        return 3
    remote_oid = remote_ref.target
    if local_ref.target == remote_oid:
        return 0
    try:
        repo.checkout_tree(repo.get(remote_oid))
        local_ref.set_target(remote_oid)
        repo.head.set_target(remote_oid)
    except Exception:
        logger.exception("Failed to checkout tree: %s", repo_path_str)
        return 2
    return 0


def pull_module(name: str) -> int:
    if not (repo_path := pygit2.discover_repository(str(EXTENSIONS_DIR / name))):
        return 2
    if repo_path == MAIN_REPO_PATH:
        return 1
    return pull_repo(repo_path)


def pull_kernel() -> int:
    return pull_repo(MAIN_REPO_PATH) if MAIN_REPO_PATH else 2


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
        logger.exception("Failed to get repo commits for: %s", repo_path_str)
        return None
    with contextlib.suppress(Exception):
        origin.fetch()
    try:
        remote_ref = repo.lookup_reference("refs/remotes/origin/master")
    except Exception:
        logger.exception("Failed to lookup remote reference for: %s", repo_path_str)
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


def get_ext_info(name: str) -> tuple[Repo | None, bool]:
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
        logger.exception("Failed to get origin URL for: %s", repo_path_str)
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
        logger.exception("Failed to get kernel repository URL")
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


def delete_module(name: str) -> bool:
    module_path = EXTENSIONS_DIR / name
    if not module_path.is_dir() or not is_valid_repo(name):
        return False
    try:
        shutil.rmtree(module_path)
        return True
    except Exception:
        logger.exception("Failed to delete module: %s", module_path)
        return False


async def load_module(
    client: interactions.Client,
    module_name: str,
    *,
    is_reload: bool = False,
) -> bool:
    module_full_name = f"extensions.{module_name}.main"
    module_disk_path = EXTENSIONS_DIR / module_name
    if not module_disk_path.is_dir():
        return False
    is_valid, error_msg, missing_deps = await check_local_ext(
        str(module_disk_path),
        module_name,
    )
    if not is_valid:
        if not is_reload:
            msg = f"Failed to load `{module_name}`:\n{error_msg}"
            if missing_deps:
                msg += f"\n\n**Missing:**\n- {'\n- '.join(missing_deps)}"
            await dm_role_members(
                embeds=[
                    await mk_embed(client, "Dependencies Failed", msg, Color.ERROR),
                ],
            )
        return False
    if not is_reload:
        try:
            client.load_extension(module_full_name)
            if client.is_ready:
                await client.synchronise_interactions(delete_commands=True)
            return True
        except Exception as e:
            logger.exception("Failed to load module: %s", module_full_name)
            await dm_role_members(
                embeds=[
                    await mk_embed(
                        client,
                        "Load Failed",
                        f"Failed to load `{module_name}`:\n```py\n{e!s}\n```",
                        Color.ERROR,
                    ),
                ],
            )
            await asyncio.to_thread(delete_module, module_name)
            return False
    try:
        client.reload_extension(module_full_name)
        if client.is_ready:
            await client.synchronise_interactions(delete_commands=True)
        logger.info("Reloaded module '%s'", module_full_name)
        return True
    except Exception as e:
        logger.exception("Failed to reload module '%s': %s", module_full_name, e)
        await dm_role_members(
            embeds=[
                await mk_embed(
                    client,
                    "Reload Failed",
                    f"Failed to reload `{module_name}`:\n```py\n{e!s}\n```\n"
                    f"The extension system attempted to restore the previous state.",
                    Color.ERROR,
                ),
            ],
        )
        return False


async def check_local_ext(
    module_path_str: str,
    module_name: str,
) -> tuple[bool, str, list[str]]:
    module_path = pathlib.Path(module_path_str)
    if not module_path.is_dir():
        return False, f"pathlib.Path not found: {module_path}", []
    valid_struct, struct_msg = await check_ext_exec(str(module_path), module_name)
    if not valid_struct:
        return False, struct_msg, []
    valid_deps, missing_deps = await asyncio.to_thread(check_ext_deps, str(module_path))
    return (True, "", []) if valid_deps else (False, "Missing dependencies.", missing_deps)


async def check_remote_ext(url: str) -> tuple[bool, str, list[str]]:
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
            logger.exception("Failed to clone repository: %s", parsed_url)
            return False, "Failed to clone", []
        valid_struct, struct_msg = await check_ext_exec(str(temp_path), module_name)
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
                logger.exception("Failed to read requirements.txt")
                error_msg = f"Failed to read requirements.txt: {e}"
        return True, error_msg, reqs_preview


def check_ext_deps(module_path_str: str) -> tuple[bool, list[str]]:
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
    missing: list[str] = []
    for req_str in requirements:
        from packaging.version import Version

        try:
            from packaging.requirements import Requirement

            req = Requirement(req_str)
            dist = distribution(req.name)
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


async def check_ext_exec(module_path_str: str, module_name: str) -> tuple[bool, str]:
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


# --- View ---


class Color(enum.IntEnum):
    ERROR = 0xE81123
    WARN = 0xFFB900
    INFO = 0x0078D7


async def send_or_edit_response(
    ctx: interactions.SlashContext | interactions.BaseContext,
    *,
    content: str = "",
    embeds: interactions.Embed | Sequence[interactions.Embed] | None = None,
    components: list[interactions.ActionRow] | None = None,
    ephemeral: bool = True,
) -> None:
    embed_list: Sequence[interactions.Embed] | None
    if embeds is None:
        embed_list = None
    elif isinstance(embeds, interactions.Embed):
        embed_list = [embeds]
    else:
        embed_list = embeds
    has_responded = getattr(ctx, "responded", False)
    has_deferred = getattr(ctx, "deferred", False)
    if has_responded or has_deferred:
        try:
            edit_method = getattr(ctx, "edit", None)
            if edit_method:
                await edit_method(
                    content=content or "",
                    embeds=embed_list,
                    components=components,
                )
            return
        except Exception:
            pass
    else:
        with contextlib.suppress(Exception):
            respond_method = getattr(ctx, "respond", None)
            if respond_method:
                await respond_method(
                    content=content,
                    embeds=embed_list,
                    components=components,
                    ephemeral=ephemeral,
                )


async def mk_embed(
    client: interactions.Client,
    title: str,
    description: str = "",
    color: Color = Color.INFO,
) -> interactions.Embed:
    embed = interactions.Embed(
        title=title,
        description=description,
        color=int(color),
        timestamp=interactions.Timestamp.now(datetime.timezone.utc),
    )

    if client.user:
        embed.set_author(name=client.user.username, icon_url=client.user.avatar_url)
        embed.set_footer(text=client.user.username, icon_url=client.user.avatar_url)

    return embed


async def reply_err(
    client: interactions.Client,
    ctx: interactions.SlashContext | interactions.BaseContext,
    message: str,
    *,
    ephemeral: bool = True,
) -> None:
    embed = await mk_embed(client, "Exception", message[:1900], Color.ERROR)
    await send_or_edit_response(
        ctx,
        embeds=embed,
        ephemeral=ephemeral,
    )


async def reply_ok(
    client: interactions.Client,
    ctx: interactions.SlashContext | interactions.BaseContext,
    message: str,
    *,
    ephemeral: bool = True,
) -> None:
    embed = await mk_embed(client, "Complete", message)
    await send_or_edit_response(
        ctx,
        embeds=embed,
        ephemeral=ephemeral,
    )


async def defer_safe(ctx: interactions.SlashContext) -> None:
    has_responded = getattr(ctx, "responded", False)
    has_deferred = getattr(ctx, "deferred", False)
    if has_responded or has_deferred:
        return
    try:
        await ctx.defer(ephemeral=True)
    except Exception:
        logger.exception("Failed to defer")
        with contextlib.suppress(Exception):
            await ctx.respond(
                "Failed to defer.",
                ephemeral=True,
            )


# --- Members ---


async def dm_role_members(
    ctx: interactions.SlashContext | None = None,
    msg: str | None = None,
    *,
    embeds: Sequence[interactions.Embed] | None = None,
    components: list[interactions.ActionRow] | None = None,
) -> list[interactions.Message]:
    if not (ROLE_ID and GUILD_ID):
        return []

    bot_user = client.user
    if not bot_user:
        return []

    try:
        guild: interactions.Guild | None
        if ctx and hasattr(ctx, "guild") and ctx.guild:
            guild = ctx.guild if isinstance(ctx.guild, interactions.Guild) else await client.fetch_guild(GUILD_ID)
        else:
            guild = await client.fetch_guild(GUILD_ID)

        if not guild:
            logger.exception(f"Failed to fetch guild {GUILD_ID}")
            return []

        role = await guild.fetch_role(int(ROLE_ID))
        if not role:
            logger.exception(f"Failed to fetch role {ROLE_ID} in guild {GUILD_ID}")
            return []

        members = [m for m in role.members if m.id != bot_user.id]
        if not members:
            return []

        processed_embeds = list(embeds) if embeds else None

        async def send_dm(member: interactions.Member) -> interactions.Message | None:
            try:
                return await member.send(
                    content=msg,
                    embeds=processed_embeds,
                    components=components,
                )
            except Exception:
                logger.debug(f"Failed to send DM to member {member.id}")
                return None

        tasks = [send_dm(member) for member in members]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        return [msg for msg in results if msg is not None]

    except Exception:
        logger.exception(
            f"Failed to process role members for guild {GUILD_ID}, role {ROLE_ID}",
        )
        return []


# --- Commands ---


async def is_privileged(ctx: interactions.SlashContext) -> bool:
    if ROLE_ID and isinstance(ctx.author, interactions.Member):
        has_role = ctx.author.has_role(interactions.Snowflake(ROLE_ID))
        if not has_role:
            logger.info(
                "Denied access to user %s: missing required role %s",
                ctx.author.id,
                ROLE_ID,
            )
            await reply_err(
                client,
                ctx,
                "Insufficient privileges.",
                ephemeral=True,
            )
            return False
        return True
    logger.warning(
        "Failed to verify permissions for user %s: ROLE_ID not set or ctx.author is not Member",
        ctx.author.id,
    )
    await reply_err(
        client,
        ctx,
        "Invalid configuration.",
        ephemeral=True,
    )
    return False


cmd_group = interactions.SlashCommand(
    name="framework",
    description="client Framework Commands",
    checks=[is_privileged],
    dm_permission=False,
    default_member_permissions=interactions.Permissions.ADMINISTRATOR,
    cooldown=interactions.models.internal.cooldowns.Cooldown(
        cooldown_bucket=interactions.models.internal.cooldowns.Buckets.GUILD,
        rate=2,
        interval=60,
    ),
    max_concurrency=interactions.models.internal.cooldowns.MaxConcurrency(
        concurrent=1,
        concurrency_bucket=interactions.models.internal.cooldowns.Buckets.GUILD,
    ),
)
cmd_module = cmd_group.group(name="module", description="Module Commands")
cmd_kernel = cmd_group.group(name="review", description="Kernel Commands")
cmd_debug = cmd_group.group(name="debug", description="Debug Commands")


@interactions.listen(CommandError, disable_default_listeners=True)
@interactions.listen(ComponentError, disable_default_listeners=True)
@interactions.listen(AutocompleteError, disable_default_listeners=True)
@interactions.listen(ModalError, disable_default_listeners=True)
async def on_error(event: CommandError) -> None:
    error = event.error
    ctx = event.ctx

    if isinstance(error, CommandCheckFailure):
        logger.warning(
            "Rejected command from user %s: %s (check: %s)",
            ctx.user.id,
            error,
            error.check.__name__ if hasattr(error.check, "__name__") else str(error.check),
        )
        await reply_err(
            client,
            ctx,
            "Failed to meet command requirements.",
            ephemeral=True,
        )
    elif isinstance(error, CommandOnCooldown):
        retry_after = error.cooldown.get_cooldown_time()
        logger.info(
            "Rate limited command for user %s: retrying in %.2fs",
            ctx.user.id,
            retry_after,
        )
        await reply_err(
            client,
            ctx,
            f"Failed to execute rate-limited command. Retry in {retry_after:.1f}s.",
            ephemeral=True,
        )
    elif isinstance(error, RateLimited):
        retry_after = getattr(error, "retry_after", 5.0)
        logger.info(
            "Exhausted rate limit for user %s: retrying in %.2fs (status: %s, route: %s)",
            ctx.user.id,
            retry_after,
            getattr(error, "status", "unknown"),
            getattr(error, "route", "unknown"),
        )
        await reply_err(
            client,
            ctx,
            f"Failed to execute command due to rate limit. Retry in {retry_after:.1f}s.",
            ephemeral=True,
        )
    elif isinstance(error, MaxConcurrencyReached):
        logger.info(
            "Blocked concurrent command execution by user %s (max: %s)",
            ctx.user.id,
            getattr(error, "max_conc", "unknown"),
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to concurrency limit.",
            ephemeral=True,
        )
    elif isinstance(error, BadArgument):
        logger.info(
            "Rejected invalid argument for command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            f"Failed to validate argument: {error}",
            ephemeral=True,
        )
    elif isinstance(error, CommandException):
        logger.error(
            "Encountered command exception in command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to command error.",
            ephemeral=True,
        )
    elif isinstance(error, (Forbidden, InteractionMissingAccess)):
        logger.warning(
            "Denied access to command for user %s: %s (scope: %s)",
            ctx.user.id,
            error,
            getattr(error, "scope", "unknown"),
        )
        await reply_err(
            client,
            ctx,
            "Insufficient permissions.",
            ephemeral=True,
        )
    elif isinstance(error, (HTTPException, BadRequest, NotFound)):
        logger.error(
            "Encountered HTTP error in command by user %s: %s (status: %s, code: %s, route: %s)",
            ctx.user.id,
            error,
            getattr(error, "status", "unknown"),
            getattr(error, "code", "unknown"),
            getattr(error, "route", "unknown"),
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to API error.",
            ephemeral=True,
        )
    elif isinstance(error, (AlreadyResponded, AlreadyDeferred, EphemeralEditException)):
        logger.warning(
            "Detected interaction state conflict in command by user %s: %s",
            ctx.user.id,
            error,
        )
    elif isinstance(
        error,
        (ExtensionException, ExtensionLoadException, ExtensionNotFound),
    ):
        logger.error(
            "Encountered extension error in command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to extension error.",
            ephemeral=True,
        )
    elif isinstance(error, (MessageException, EmptyMessageException)):
        logger.error(
            "Failed to send message for command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            "Failed to send message.",
            ephemeral=True,
        )
    elif isinstance(
        error,
        (VoiceAlreadyConnected, VoiceConnectionTimeout, VoiceWebSocketClosed),
    ):
        logger.error(
            "Encountered voice connection error in command by user %s: %s (code: %s)",
            ctx.user.id,
            error,
            getattr(error, "code", "unknown"),
        )
        await reply_err(
            client,
            ctx,
            "Failed to establish voice connection.",
            ephemeral=True,
        )
    elif isinstance(error, (WebSocketClosed, WebSocketRestart, GatewayNotFound)):
        logger.error(
            "Detected connection failure in command by user %s: %s (code: %s, resume: %s)",
            ctx.user.id,
            error,
            getattr(error, "code", "unknown"),
            getattr(error, "resume", "unknown"),
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to connection error.",
            ephemeral=True,
        )
    elif isinstance(error, (ThreadException, ThreadOutsideOfGuild)):
        logger.error(
            "Failed to perform thread operation in command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute thread operation.",
            ephemeral=True,
        )
    elif isinstance(error, DiscordError):
        logger.error(
            "Encountered Discord error in command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to Discord error.",
            ephemeral=True,
        )
    elif isinstance(error, (BotException, LibraryException, InteractionException)):
        logger.error(
            "Encountered bot error in command by user %s: %s",
            ctx.user.id,
            error,
        )
        await reply_err(
            client,
            ctx,
            "Failed to execute command due to bot error.",
            ephemeral=True,
        )
    elif isinstance(
        error,
        (TooManyChanges, ForeignWebhookException, EventLocationNotProvided, LoginError),
    ):
        logger.error("Failed to execute command by user %s: %s", ctx.user.id, error)
        await reply_err(
            client,
            ctx,
            "Failed to execute command.",
            ephemeral=True,
        )
    else:
        raise error


# --- Command Debug Export ---


@cmd_debug.subcommand(
    "export",
    sub_cmd_description="Export files/directories",
)
@interactions.slash_option(
    name="path",
    description="Relative path to export",
    required=True,
    opt_type=interactions.OptionType.STRING,
    autocomplete=True,
)
async def cmd_export(ctx: interactions.SlashContext, path: str) -> None:
    await defer_safe(ctx)

    try:
        target_path = BASE_DIR.joinpath(path).resolve()
    except Exception:
        logger.exception("Failed to resolve export path")
        await reply_err(
            client,
            ctx,
            "Failed to resolve path: invalid format.",
        )
        return

    if BASE_DIR not in target_path.parents and target_path != BASE_DIR:
        await reply_err(
            client,
            ctx,
            "Failed to access path: outside allowed directory.",
        )
        return

    if not target_path.exists():
        await reply_err(
            client,
            ctx,
            f"Failed to locate path: `{path}` does not exist.",
        )
        return

    if not (target_path.is_file() or target_path.is_dir()):
        await reply_err(
            client,
            ctx,
            f"Failed to identify path type: `{path}` is neither file nor directory.",
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
        except Exception as e:
            await reply_err(client, ctx, f"Failed to create archive: {e}")
            return

        if not archive_filename or not pathlib.Path(archive_filename).exists():
            await reply_err(client, ctx, "Failed to create output file.")
            return

        await ctx.respond(
            f"Exporting `{path}`:",
            attachments=[interactions.File(archive_filename)],
        )


@cmd_export.autocomplete("path")
async def cmd_export_path(
    ctx: interactions.AutocompleteContext,
) -> None:
    choices: list[interactions.SlashCommandChoice] = [
        interactions.SlashCommandChoice(name="All Files", value="all"),
    ]
    try:
        files = [f for f in os.listdir(BASE_DIR) if (BASE_DIR / f).is_file() and not f.startswith(".")]
        choices.extend(
            [interactions.SlashCommandChoice(name=file, value=file) for file in sorted(files)],
        )
    except Exception as e:
        logger.exception("Failed to autocomplete")
        choices = [interactions.SlashCommandChoice(name=f"Error: {e!s}", value="error")]

    await ctx.send(choices[:25])


# --- Command Debug Restart ---


@cmd_debug.subcommand("restart", sub_cmd_description="Restart the client")
async def cmd_reboot(ctx: interactions.SlashContext) -> None:
    await defer_safe(ctx)
    executor = ctx.author

    try:
        embed = await mk_embed(
            client,
            "Restart Initiated",
            f"{executor.mention} initiated client restart.",
            Color.WARN,
        )
        if ctx.member:
            embed.set_author(
                name=ctx.member.display_name,
                icon_url=ctx.member.avatar_url,
            )

        await dm_role_members(ctx, embeds=[embed])
        await reply_ok(client, ctx, "Initiating client restart sequence.")

        FLAG_DIR.mkdir(exist_ok=True)
        (FLAG_DIR / "restart").write_text(
            f"Restart at {datetime.datetime.now(datetime.timezone.utc).isoformat()} by {executor.id}",
        )
        await client.stop()
    except Exception as e:
        logger.exception("Failed to execute restart command: %s")
        await reply_err(client, ctx, f"Failed to restart client: {e}")


# --- Command Module Load ---


@cmd_module.subcommand(
    "load",
    sub_cmd_description="Load module from Git URL",
)
@interactions.slash_option(
    name="url",
    description="Git repo URL (e.g., https://github.com/user/repo.git)",
    required=True,
    opt_type=interactions.OptionType.STRING,
)
async def cmd_load_mod(ctx: interactions.SlashContext, url: str) -> None:
    await defer_safe(ctx)
    executor = ctx.user

    git_url, parsed_name, validated_url = parse_repo_url(url)
    if not validated_url or not parsed_name:
        await reply_err(
            client,
            ctx,
            "Failed to parse Git URL: invalid format.",
        )
        return

    if (EXTENSIONS_DIR / parsed_name).is_dir():
        await reply_err(
            client,
            ctx,
            f"Failed to load module: `{parsed_name}` already exists.",
        )
        return

    with contextlib.suppress(Exception):
        await ctx.edit(content=f"Validating `{parsed_name}`")

    remote_valid, remote_error_msg, _ = await check_remote_ext(git_url)
    if not remote_valid:
        await reply_err(client, ctx, f"Failed to validate module: {remote_error_msg}")
        return

    await dm_role_members(
        ctx,
        embeds=[
            await mk_embed(
                client,
                "Loading Module",
                f"{executor.mention} is loading `{parsed_name}`.",
            ),
        ],
    )

    with contextlib.suppress(Exception):
        await ctx.edit(content=f"Cloning `{parsed_name}`.")

    cloned_name, clone_success = await asyncio.to_thread(clone_repo, git_url)
    if not clone_success:
        await reply_err(client, ctx, f"Failed to clone repository: `{cloned_name}`.")
        return

    reqs_path = EXTENSIONS_DIR / cloned_name / "requirements.txt"
    if reqs_path.is_file():
        with contextlib.suppress(Exception):
            await ctx.edit(content="Installing dependencies.")
        pip_success = await asyncio.to_thread(run_pip, str(reqs_path), True)
        if not pip_success:
            await asyncio.to_thread(delete_module, cloned_name)
            await reply_err(
                client,
                ctx,
                "Failed to install dependencies: removed module.",
            )
            return

    with contextlib.suppress(Exception):
        await ctx.edit(content=f"Loading `{cloned_name}`")

    if await load_module(client, cloned_name):
        await reply_ok(client, ctx, f"Loading module `{cloned_name}`.")
        await dm_role_members(
            embeds=[
                await mk_embed(
                    client,
                    "Module Loaded",
                    f"`{cloned_name}` loaded by {executor.mention}.",
                ),
            ],
        )


# --- Module Option Decorator ---


def mod_opt() -> Callable:
    return interactions.slash_option(
        name="module",
        description="Module name",
        required=True,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )


# --- Command Module Unload ---


@cmd_module.subcommand("unload", sub_cmd_description="Unload and delete a module")
@mod_opt()
async def cmd_unload_mod(ctx: interactions.SlashContext, module: str) -> None:
    await defer_safe(ctx)
    executor = ctx.author

    logger.info(
        "User %s (%s) requested unload of module: %s",
        executor.username,
        executor.id,
        module,
    )

    module_path = EXTENSIONS_DIR / module
    if not module_path.is_dir():
        await reply_err(
            client,
            ctx,
            f"Failed to locate directory: `{module}` not found.",
        )
        return

    if not is_valid_repo(module):
        await reply_err(
            client,
            ctx,
            f"Failed to unload: `{module}` is not a valid module repository (or is the kernel).",
        )
        return

    info, _ = get_ext_info(module)
    commit_id = str(info.cur_commit.id)[:7] if info and info.cur_commit else "Unknown"
    remote_url = info.url if info else "Unknown"

    embed = await mk_embed(
        client,
        "Unloading Module",
        f"{executor.mention} is unloading `{module}`.",
        Color.WARN,
    )
    embed.add_field(name="Current Commit", value=f"`{commit_id}`", inline=True)
    embed.add_field(name="Remote URL", value=remote_url, inline=True)
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon_url=ctx.member.avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])

    module_full_name = f"extensions.{module}.main"
    unload_success = True

    try:
        client.unload_extension(module_full_name)
        logger.info("Unloaded extension '%s'", module_full_name)
    except Exception as e:
        unload_success = False
        logger.exception("Failed to unload extension '%s': %s", module_full_name, e)
        await reply_err(
            client,
            ctx,
            f"Failed to unload extension `{module}`: {e}. Attempting cleanup.",
        )

    if unload_success:
        try:
            if client.is_ready:
                await client.synchronise_interactions(delete_commands=True)
                logger.info("Resynced commands after extension unload")
        except Exception as e:
            logger.exception("Failed to resync commands after unload: %s", e)

    delete_success = await asyncio.to_thread(delete_module, module)

    if delete_success:
        final_message = f"Unloading module `{module}` and removing its directory."
        if not unload_success:
            final_message += " (Extension unload encountered issues, but finished cleanup.)"
        await reply_ok(client, ctx, final_message)
        completion_embed = await mk_embed(
            client,
            "Unloading Module",
            f"Unloading `{module}` and deleting by {executor.mention}.",
        )
        await dm_role_members(embeds=[completion_embed])
    else:
        await reply_err(
            client,
            ctx,
            f"Failed to delete directory for module `{module}`: cleanup required.",
        )


# --- Command Module Update ---


@cmd_module.subcommand(
    "update",
    sub_cmd_description="Update a module to the latest version",
)
@mod_opt()
async def cmd_update_mod(ctx: interactions.SlashContext, module: str) -> None:
    await defer_safe(ctx)
    executor = ctx.author
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
            client,
            ctx,
            f"Failed to locate directory: `{module}` not found.",
        )
        return

    info, valid_info = get_ext_info(module)
    if not valid_info or not info:
        await reply_err(
            client,
            ctx,
            f"Failed to retrieve repository info for module `{module}`.",
        )
        return

    if info.cur_commit and info.rmt_commit and info.cur_commit.id == info.rmt_commit.id and info.mods == 0:
        await reply_ok(
            client,
            ctx,
            f"Module `{module}` remains up-to-date.",
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

    embed = await mk_embed(
        client,
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
            icon_url=ctx.member.avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])

    with contextlib.suppress(Exception):
        await ctx.edit(content=f"Backing up module `{module}`.")

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
            client,
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
        await ctx.edit(
            content=f"Pulling updates for module `{module}`.",
        )

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
                client,
                ctx,
                f"Failed to update module: {error_msg}. Restored from backup.",
            )
        else:
            await reply_err(
                client,
                ctx,
                f"Failed to update module: {error_msg}. Backup restoration failed. Intervention required.",
            )
        return

    with contextlib.suppress(Exception):
        await ctx.edit(
            content=f"Updating dependencies for `{module}`.",
        )

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
                client,
                ctx,
                f"Failed to update dependencies for `{module}`. Restored from backup.",
            )
        else:
            await reply_err(
                client,
                ctx,
                f"Failed to update dependencies for `{module}`. Backup restoration failed. Intervention required.",
            )
        return

    with contextlib.suppress(Exception):
        await ctx.edit(
            content=f"Validating and reloading module `{module}`.",
        )

    reload_success = await load_module(client, module, is_reload=True)

    if not reload_success:
        logger.exception("Failed to reload module '%s' after update", module)
        if await restore_backup():
            logger.info("Loading restored version of module '%s'", module)

            if module_full_name in client.ext:
                logger.warning(
                    f"Extension '{module_full_name}' still loaded despite failed reload",
                )
                try:
                    client.unload_extension(module_full_name)
                    await client.synchronise_interactions(delete_commands=True)
                except Exception as unload_err:
                    logger.exception(
                        f"Failed to explicitly unload '{module_full_name}': {unload_err}",
                    )
            else:
                logger.debug(
                    f"Extension '{module_full_name}' not found in client.ext, no unload needed before loading restored version.",
                )

            final_load_success = await load_module(client, module, is_reload=False)

            if final_load_success:
                await reply_err(
                    client,
                    ctx,
                    f"Failed to reload updated module `{module}`: previous version restored and loaded.",
                )
            else:
                await reply_err(
                    client,
                    ctx,
                    f"Failed to reload updated module `{module}`: restored from backup but reload failed. Intervention required.",
                )
        else:
            await reply_err(
                client,
                ctx,
                f"Failed to reload updated module `{module}`. Backup restoration failed.",
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
                    ephemeral=True,
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
                    client,
                    ctx,
                    f"Update finished but failed to reapply local changes. Changes preserved in `{patch_path}`.",
                    ephemeral=False,
                )
        except Exception as patch_err:
            logger.exception(
                "Failed to reapply patch for '%s': %s",
                module,
                patch_err,
            )
            await reply_err(
                client,
                ctx,
                f"Update finished but failed to reapply local changes: {patch_err}",
            )

    try:
        changelog_content = "No changelog provided."
        if (changelog_path := module_dir / "CHANGELOG").is_file():
            with contextlib.suppress(Exception):
                changelog_content = changelog_path.read_text(
                    encoding="utf-8",
                    errors="ignore",
                )

        result_embed = await mk_embed(
            client,
            "Updating Module",
            f"Updating module `{module}` to commit `{target_commit_id[:7]}`.",
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

        paginator = Paginator.create_from_embeds(client, result_embed, timeout=180)
        paginator.show_callback_button = True
        paginator.show_select_menu = True
        paginator.wrong_user_message = "Requester controls pagination."
        await paginator.send(ctx)

        completion_embed = await mk_embed(
            client,
            "Module Loaded",
            f"Loaded `{module}` by {executor.mention}.",
        )
        await dm_role_members(embeds=[completion_embed])

    except Exception as e:
        logger.exception("Failed to send update confirmation: %s", e)
        await reply_ok(
            client,
            ctx,
            f"Updated `{module}` but failed to display details.",
        )

    with contextlib.suppress(Exception):
        if backup_base.exists():
            await aioshutil.rmtree(backup_base)
            logger.info("Cleaned up backup '%s'", backup_base)


# --- Command Module Info ---


@cmd_module.subcommand(
    "info",
    sub_cmd_description="Show information about a loaded module",
)
@mod_opt()
async def cmd_info_mod(ctx: interactions.SlashContext, module: str) -> None:
    await defer_safe(ctx)

    info, valid = get_ext_info(module)
    if not valid or not info:
        await reply_err(
            client,
            ctx,
            f"Not found or invalid `{module}`.",
        )
        return

    try:
        has_modifications = info.mods > 0
        embed_color = Color.WARN if has_modifications else Color.INFO
        result_embed = await mk_embed(
            client,
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

        paginator = Paginator.create_from_embeds(client, result_embed, timeout=180)
        paginator.show_callback_button = True
        paginator.show_select_menu = True
        paginator.wrong_user_message = "User who requested this info can control the pagination."
        await paginator.send(ctx)

    except Exception as e:
        logger.exception("Failed to send module info for '%s': %s", module, e)
        await reply_err(
            client,
            ctx,
            f"Failed to display module information for {module}: {e}.",
        )


# --- Command Module Autocomplete ---


@cmd_unload_mod.autocomplete("module")
@cmd_update_mod.autocomplete("module")
@cmd_info_mod.autocomplete("module")
async def cmd_module_name(
    ctx: interactions.AutocompleteContext,
) -> None:
    query = (ctx.input_text or "").lower() if ctx.input_text else ""
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

    await ctx.send(choices)


# --- Command Modules List ---


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


@cmd_module.subcommand("list", sub_cmd_description="List all loaded modules")
async def cmd_list_mods(ctx: interactions.SlashContext) -> None:
    await defer_safe(ctx)

    modules_list = get_loadable_mods()

    if not modules_list:
        await reply_err(
            client,
            ctx,
            "Failed to locate extensions directory: empty.",
        )
        return

    embed = await mk_embed(
        client,
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
            info, valid = get_ext_info(module_name)
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
        paginator = Paginator.create_from_embeds(client, embed, timeout=180)
        paginator.show_callback_button = True
        paginator.show_select_menu = True
        paginator.wrong_user_message = "User who requested this list can control the pagination."
        await paginator.send(ctx)
    except Exception as e:
        logger.exception("Failed to send module list: %s", e)
        await reply_err(
            client,
            ctx,
            f"Failed to display module list: {e}.",
        )


# --- Command Download ---


_dl_lock = asyncio.Lock()


@cmd_debug.subcommand(
    "download",
    sub_cmd_description="Download current running code as tarball",
)
async def cmd_download(ctx: interactions.SlashContext) -> None:
    if _dl_lock.locked():
        await ctx.respond(
            "Download already in progress. Retry later.",
            ephemeral=True,
        )
        return

    async with _dl_lock:
        await defer_safe(ctx)
        logger.info(
            "User %s (%s) requested code download",
            ctx.user.username,
            ctx.user.id,
        )

        def compress_code(filename: str, source_path: pathlib.Path) -> None:
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
                path_parts = set(pathlib.Path(tarinfo.name).parts)
                if any(pattern in path_parts for pattern in excluded_patterns):
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
                prefix="Discord-client-Framework_",
                delete=False,
            ) as tmp:
                temp_file_path_str: str = str(tmp.name)

            await asyncio.to_thread(compress_code, temp_file_path_str, BASE_DIR)

            file_size = pathlib.Path(temp_file_path_str).stat().st_size
            logger.info(
                "Sending archive '%s' (%d bytes)",
                temp_file_path_str,
                file_size,
            )
            await ctx.respond(
                "Attaching code archive:",
                attachments=[
                    interactions.File(
                        temp_file_path_str,
                        file_name="client_code.tar.zst",
                    ),
                ],
            )

        except Exception as e:
            logger.exception("Failed to complete code download")
            await reply_err(
                client,
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


# --- Command Debug Info ---


@cmd_debug.subcommand(
    "info",
    sub_cmd_description="Show information about status",
)
async def cmd_info(ctx: interactions.SlashContext) -> None:
    await defer_safe(ctx)

    me = client.user
    system_info = {
        "python_version": sys.version.split()[0],
        "platform": sys.platform,
        "pid": os.getpid(),
        "cwd": str(BASE_DIR),
        "log_file": (str(LOG_FILE.relative_to(BASE_DIR)) if LOG_FILE.is_relative_to(BASE_DIR) else str(LOG_FILE)),
    }

    client_info = {
        "user_id": me.id if me else "N/A",
        "username": me.username if me else "N/A",
        "guild_count": len(client.guilds),
        "module_count": len(get_loadable_mods()),
        "latency": (f"{client.latency * 1000:.2f} ms" if client.latency is not None else "N/A"),
    }

    embed = await mk_embed(
        client,
        "Status",
        "Runtime diagnostics and client information.",
        Color.INFO,
    )

    embed.add_field(
        name="System",
        value=f"- Python: `{system_info['python_version']}`\n"
        f"- Platform: `{system_info['platform']}`\n"
        f"- PID: `{system_info['pid']}`\n"
        f"- CWD: `{system_info['cwd']}`\n"
        f"- Log: `{system_info['log_file']}`",
        inline=True,
    )
    embed.add_field(
        name="client",
        value=f"- User: `{client_info['username']}` ({client_info['user_id']})\n"
        f"- Guilds: `{client_info['guild_count']}`\n"
        f"- Modules: `{client_info['module_count']}`\n"
        f"- Latency: `{client_info['latency']}`",
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
        paginator = Paginator.create_from_embeds(client, embed, timeout=180)
        paginator.show_callback_button = True
        paginator.show_select_menu = True
        paginator.wrong_user_message = "User who requested this info can control the pagination."
        await paginator.send(ctx)
    except Exception as e:
        logger.exception("Failed to send system status: %s", e)
        await reply_err(
            client,
            ctx,
            f"Failed to display system status: {e}.",
        )


# --- Command Kernel Info ---


@cmd_kernel.subcommand(
    "info",
    sub_cmd_description="Show information about the Kernel",
)
async def cmd_info_kernel(ctx: interactions.SlashContext) -> None:
    await defer_safe(ctx)

    info = get_kernel_info()
    if not info:
        await reply_err(
            client,
            ctx,
            "Failed to retrieve kernel repository information.",
        )
        return

    try:
        color = Color.WARN if info.mods > 0 else Color.INFO
        embed = await mk_embed(
            client,
            "Kernel",
            f"Repo: {info.url}",
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

        paginator = Paginator.create_from_embeds(client, embed, timeout=180)
        paginator.show_callback_button = True
        paginator.show_select_menu = True
        paginator.wrong_user_message = "User who requested this info can control the pagination."
        await paginator.send(ctx)

    except Exception as e:
        logger.exception("Failed to send kernel information")
        await reply_err(
            client,
            ctx,
            f"Failed to display kernel information: {e}.",
        )


# --- Command Kernel Update ---


@cmd_kernel.subcommand(
    "update",
    sub_cmd_description="Update the kernel to the latest version",
)
async def cmd_kernel_update(ctx: interactions.SlashContext) -> None:
    await defer_safe(ctx)
    executor = ctx.user

    logger.info(
        "User %s (%s) initiated kernel update",
        executor.username,
        executor.id,
    )

    info = get_kernel_info()
    if not info:
        await reply_err(
            client,
            ctx,
            "Failed to retrieve kernel repository information.",
        )
        return

    if info.cur_commit and info.rmt_commit and info.cur_commit.id == info.rmt_commit.id and info.mods == 0:
        await reply_ok(
            client,
            ctx,
            "Kernel remains up-to-date.",
        )
        return

    current_commit_id = str(info.cur_commit.id) if info.cur_commit else "Unknown"
    target_commit_id = str(info.rmt_commit.id) if info.rmt_commit else "Unknown"

    logger.info(
        "Initiating kernel update: cur=%s target=%s mods=%d",
        current_commit_id,
        target_commit_id,
        info.mods,
    )

    embed = await mk_embed(
        client,
        "Updating Kernel",
        f"{executor.mention} is updating kernel.",
        Color.WARN,
    )
    embed.url = info.url
    embed.add_field(name="Current Commit", value=f"`{current_commit_id}`", inline=True)
    embed.add_field(name="Target Commit", value=f"`{target_commit_id}`", inline=True)
    if info.mods > 0:
        embed.add_field(
            name="WARNING",
            value="Local modifications detected and will be overwritten.",
        )
    if ctx.member:
        embed.set_author(
            name=ctx.member.display_name,
            icon_url=ctx.member.avatar_url,
        )
    await dm_role_members(ctx, embeds=[embed])

    with contextlib.suppress(Exception):
        await ctx.edit(content="Pulling kernel updates.")

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
            client,
            ctx,
            f"Failed to update kernel: {error_msg}.",
        )
        return

    with contextlib.suppress(Exception):
        await ctx.edit(content="Updating kernel dependencies.")

    pip_success = await asyncio.to_thread(
        run_pip,
        str(BASE_DIR / "requirements.txt"),
        install=True,
    )

    if not pip_success:
        logger.exception("Failed to update kernel dependencies")
        await reply_err(
            client,
            ctx,
            "Kernel updated but failed to update dependencies.",
        )

    with contextlib.suppress(Exception):
        await ctx.edit(
            content="Kernel update finished.",
        )

    result_embed = await mk_embed(
        client,
        "Updating Kernel",
        f"Updating to `{target_commit_id[:7]}`. Restarting.",
    )
    await ctx.edit(embeds=[result_embed])

    try:
        FLAG_DIR.mkdir(exist_ok=True)
        reboot_flag = FLAG_DIR / "restart"
        reboot_flag.write_text(
            f"Restart triggered post-kernel-update at {datetime.datetime.now(datetime.timezone.utc).isoformat()} by {executor.id}",
        )
        logger.info("Set restart flag at %s", reboot_flag)
        reboot_notice_embed = await mk_embed(
            client,
            "Restarting",
            "Kernel updated. Restarting client.",
        )
        await dm_role_members(embeds=[reboot_notice_embed])
    except Exception:
        logger.exception(
            "Failed to signal restart after kernel update",
        )
        await reply_err(
            client,
            ctx,
            "Failed to restart client: restart signaling failed. Restart required.",
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
            if client and client._closed:
                logger.warning("Closing client connection")
                await client.stop()
                logger.warning("Closed client connection")

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
                    (loaded_extensions if await load_module(client, module_name) else failed_extensions).add(
                        extension_module,
                    )
                else:
                    client.load_extension(extension_module)
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
            client.astart(),
            shutdown_event.wait(),
            return_exceptions=True,
        )
    except Exception:
        logger.exception("Failed to start client client")


def entrypoint() -> None:
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Failed to execute main")
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
