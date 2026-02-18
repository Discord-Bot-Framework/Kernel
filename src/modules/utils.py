from __future__ import annotations

import ast
import asyncio
import functools
import importlib.metadata
import shutil
from collections.abc import Sequence
from pathlib import Path

import anyio
import pygit2
from aiofiles import tempfile

from src.container.types import ModuleType
from src.git.constants import MAIN_REPO_PATH
from src.git.utils import is_valid_repo, parse_repo_url, pull_repo
from src.shared.constants import EXTENSIONS_DIR

CheckResult = tuple[bool, str, Sequence[str]]


def pull_kernel() -> int:
    return pull_repo(MAIN_REPO_PATH) if MAIN_REPO_PATH else 2


def pull_module(name: str) -> int:
    repo_path = pygit2.discover_repository(str(EXTENSIONS_DIR / name))
    if not repo_path:
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


@functools.lru_cache(maxsize=256)
def detect_module_type(module_path: Path) -> ModuleType | None:
    if (module_path / "main.py").is_file():
        return ModuleType.PYTHON
    return None


def _strip_requirement_lines(content: str) -> list[str]:
    return [
        stripped
        for line in content.splitlines()
        if (stripped := line.split("#")[0].strip())
    ]


def _read_text_file(file_path: Path) -> str | None:
    try:
        return file_path.read_text(encoding="utf-8")
    except OSError:
        return None


async def check_local_module(
    module_path_str: str,
    module_name: str,
) -> CheckResult:
    module_path = anyio.Path(module_path_str)
    if not await module_path.is_dir():
        return False, f"Failed to find path: {module_path_str}", ()

    module_type = detect_module_type(Path(module_path_str))
    if module_type is None:
        return False, "Failed to detect module type", ()

    valid_struct, struct_msg = await _check_module_exec(
        module_path_str,
        module_name,
        module_type,
    )
    if not valid_struct:
        return False, struct_msg, ()

    valid_deps, missing_deps = await asyncio.to_thread(
        _check_module_deps,
        module_path_str,
        module_type,
    )
    if valid_deps:
        return True, "", ()
    return False, "Failed to resolve dependencies.", tuple(missing_deps)


def _extract_python_requirements(content: str) -> tuple[str, ...]:
    from packaging.requirements import Requirement

    dependencies: list[str] = []
    for requirement_line in _strip_requirement_lines(content):
        try:
            Requirement(requirement_line)
        except Exception:
            continue
        dependencies.append(requirement_line)
    return tuple(dependencies)


async def check_remote_module(url: str) -> CheckResult:
    parsed_url, module_name, is_valid_url = parse_repo_url(url)
    if not is_valid_url or not module_name:
        return False, f"Failed to validate Git URL: {url}", ()

    async with tempfile.TemporaryDirectory(
        prefix=f"{module_name}_remote_check_",
    ) as temp_dir:
        temp_path = Path(temp_dir)
        try:
            await asyncio.to_thread(pygit2.clone_repository, parsed_url, str(temp_path))
        except pygit2.GitError:
            return False, "Failed to clone repository", ()

        module_type = detect_module_type(temp_path)
        if module_type is None:
            return False, "Failed to detect module type", ()

        valid_struct, struct_msg = await _check_module_exec(
            str(temp_path),
            module_name,
            module_type,
        )
        if not valid_struct:
            return False, struct_msg, ()

        dep_file = temp_path / module_type.dependency_file
        if not dep_file.is_file():
            return True, "", ()

        content = _read_text_file(dep_file)
        if content is None:
            return True, "", ()

        return True, "", _extract_python_requirements(content)


def _check_module_deps(
    module_path_str: str,
    module_type: ModuleType,
) -> tuple[bool, tuple[str, ...]]:
    dep_file = Path(module_path_str) / module_type.dependency_file
    if not dep_file.is_file():
        return True, ()

    content = _read_text_file(dep_file)
    if content is None:
        return False, (f"Failed to read {module_type.dependency_file}",)
    return _check_python_deps(content)


def _check_python_deps(content: str) -> tuple[bool, tuple[str, ...]]:
    requirements = _strip_requirement_lines(content)

    if not requirements:
        return True, ()

    from packaging.requirements import Requirement
    from packaging.version import Version

    missing: list[str] = []
    for req_str in requirements:
        try:
            req = Requirement(req_str)
            dist = importlib.metadata.distribution(req.name)
        except importlib.metadata.PackageNotFoundError:
            missing.append(f"{req_str} (failed to install)")
            continue
        except Exception as e:
            missing.append(f"{req_str} (failed to parse: {e})")
            continue

        if req.specifier and not req.specifier.contains(
            Version(dist.version),
            prereleases=True,
        ):
            missing.append(
                f"{req_str} (installed {dist.version}, required {req.specifier})",
            )

    if missing:
        return False, tuple(missing)
    return True, ()


async def _check_module_exec(
    module_path_str: str,
    module_name: str,
    module_type: ModuleType,
) -> tuple[bool, str]:
    if module_type == ModuleType.PYTHON:
        return await _check_python_module_exec(module_path_str, module_name)
    return False, "Unknown module type"


async def _check_python_module_exec(
    module_path_str: str,
    module_name: str,
) -> tuple[bool, str]:
    main_file = Path(module_path_str) / "main.py"
    if not main_file.is_file():
        return False, f"Failed to locate main.py in `{module_name}`."

    try:
        code = main_file.read_text(encoding="utf-8")
    except OSError as e:
        return False, f"Failed to read main.py: {e}"

    try:
        tree = ast.parse(code, filename=str(main_file))
    except SyntaxError as e:
        pointer = " " * ((e.offset or 1) - 1) + "^"
        code_snippet = f"{e.text.strip()}\n{pointer}" if e.text else ""
        return (
            False,
            f"Failed to compile main.py (line {e.lineno}):\n```py\n{code_snippet}\n```",
        )

    try:
        compile(tree, str(main_file), "exec", dont_inherit=True)
    except Exception as e:
        return False, f"Failed to compile: {e}"

    return True, ""
