from __future__ import annotations

import contextlib
import dataclasses
import datetime
import functools
import shutil
from typing import TYPE_CHECKING, Final
from urllib.parse import urlsplit

import pygit2

from src.git.constants import GIT_URL_TRANS_MAP, MAIN_REPO_PATH
from src.shared.constants import EXTENSIONS_DIR

if TYPE_CHECKING:
    import pathlib

_REF_NAMES: Final[tuple[str, ...]] = (
    "refs/remotes/origin/HEAD",
    "refs/remotes/origin/main",
    "refs/remotes/origin/master",
)


def _open_repo(repo_path_str: str) -> pygit2.Repository | None:
    try:
        return pygit2.Repository(repo_path_str)
    except Exception:
        return None


def _get_origin(repo: pygit2.Repository) -> pygit2.Remote | None:
    try:
        return repo.remotes["origin"]
    except Exception:
        return None


def _fetch_origin(origin: pygit2.Remote) -> None:
    with contextlib.suppress(Exception):
        origin.fetch()


def resolve_remote_ref(repo: pygit2.Repository) -> pygit2.Reference | None:
    for ref_name in _REF_NAMES:
        try:
            ref = repo.lookup_reference(ref_name)
        except KeyError:
            continue

        if ref_name.endswith("/HEAD"):
            target = ref.target
            if isinstance(target, str):
                with contextlib.suppress(Exception):
                    return repo.lookup_reference(target)
            continue

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
class RepoInfo:
    uncommitted_changes: int
    url: str
    local_commit: pygit2.Commit | None = None
    remote_commit: pygit2.Commit | None = None
    changelog: str = ""

    def __post_init__(self) -> None:
        if self.uncommitted_changes < 0:
            msg = f"uncommitted_changes must be non-negative, got {self.uncommitted_changes}"
            raise ValueError(msg)

    @staticmethod
    @functools.lru_cache(maxsize=1024)
    def _ts(commit_time: int, offset: int) -> float:
        return commit_time + offset * 60

    @classmethod
    def _format_time(cls, commit: pygit2.Commit | None) -> str | None:
        if commit is None:
            return None
        return datetime.datetime.fromtimestamp(
            cls._ts(commit.commit_time, commit.committer.offset),
            datetime.timezone.utc,
        ).isoformat()

    @property
    def local_commit_time_utc(self) -> str | None:
        return self._format_time(self.local_commit)

    @property
    def remote_commit_time_utc(self) -> str | None:
        return self._format_time(self.remote_commit)


def parse_repo_url(url: str) -> tuple[str, str, bool]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.path.endswith(".git")
        or "." not in parsed.netloc
    ):
        return url, "", False

    netloc_parts = parsed.netloc.split(".")
    relevant_netloc = ".".join(
        part for part in netloc_parts if part not in frozenset({"www", "com"})
    )
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
    _, repo_name, is_valid = parse_repo_url(url)
    if not is_valid:
        return "", False

    repo_path = EXTENSIONS_DIR / repo_name
    EXTENSIONS_DIR.mkdir(exist_ok=True)

    try:
        pygit2.clone_repository(url, str(repo_path))
    except Exception:
        shutil.rmtree(repo_path, ignore_errors=True)
        return "", False

    return repo_name, True


def pull_repo(repo_path_str: str) -> int:
    repo = _open_repo(repo_path_str)
    if repo is None:
        return 2

    origin = _get_origin(repo)
    if origin is None:
        return 2

    _fetch_origin(origin)

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
    module_path = EXTENSIONS_DIR / name
    if (module_path / "package.json").is_file():
        return True

    module_repo_path = pygit2.discover_repository(str(module_path))
    return bool(module_repo_path and module_repo_path != MAIN_REPO_PATH)


def _resolve_commits(
    repo: pygit2.Repository,
    remote_ref: pygit2.Reference,
) -> tuple[pygit2.Commit | None, pygit2.Commit | None]:
    remote_commit_obj = repo.get(remote_ref.target)
    head_commit_obj = repo.get(repo.head.target)

    remote_commit: pygit2.Commit | None = (
        remote_commit_obj if isinstance(remote_commit_obj, pygit2.Commit) else None
    )
    head_commit: pygit2.Commit | None = (
        head_commit_obj if isinstance(head_commit_obj, pygit2.Commit) else None
    )
    return head_commit, remote_commit


def get_repo_commits(
    repo_path_str: str,
) -> tuple[pygit2.Commit | None, pygit2.Commit | None, int] | None:
    repo = _open_repo(repo_path_str)
    if repo is None:
        return None

    origin = _get_origin(repo)
    if origin is None:
        return None

    _fetch_origin(origin)

    remote_ref = resolve_remote_ref(repo)
    if remote_ref is None:
        return None

    head_commit, remote_commit = _resolve_commits(repo, remote_ref)

    modifications = (
        -1
        if remote_commit is None or head_commit is None
        else repo.diff(head_commit.id, remote_commit.id).stats.files_changed
    )

    return head_commit, remote_commit, modifications


def _origin_url(repo_path_str: str) -> str | None:
    repo = _open_repo(repo_path_str)
    if repo is None:
        return None

    origin = _get_origin(repo)
    if origin is None:
        return None

    return origin.url


def _read_changelog(module_path: pathlib.Path) -> str:
    changelog_path = module_path / "CHANGELOG"
    if not changelog_path.is_file():
        return ""

    with contextlib.suppress(Exception):
        return changelog_path.read_text(encoding="utf-8", errors="ignore")
    return ""


def get_module_info(name: str) -> tuple[RepoInfo | None, bool]:
    module_path = EXTENSIONS_DIR / name
    repo_path_str = pygit2.discover_repository(str(module_path))
    if not repo_path_str or repo_path_str == MAIN_REPO_PATH:
        return None, False

    commits = get_repo_commits(repo_path_str)
    if commits is None:
        return None, False

    head_commit, remote_commit, modifications = commits
    origin_url = _origin_url(repo_path_str)
    if not origin_url:
        return None, False

    return RepoInfo(
        uncommitted_changes=modifications,
        url=origin_url,
        local_commit=head_commit,
        remote_commit=remote_commit,
        changelog=_read_changelog(module_path),
    ), True


def get_kernel_info() -> RepoInfo | None:
    if not MAIN_REPO_PATH:
        return None

    commits = get_repo_commits(MAIN_REPO_PATH)
    if commits is None:
        return None

    head_commit, remote_commit, modifications = commits
    origin_url = _origin_url(MAIN_REPO_PATH)
    if not origin_url:
        return None

    return RepoInfo(
        uncommitted_changes=modifications,
        url=origin_url,
        local_commit=head_commit,
        remote_commit=remote_commit,
    )
