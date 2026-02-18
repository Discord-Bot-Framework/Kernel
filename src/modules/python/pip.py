from __future__ import annotations

import contextlib
import importlib
import pathlib
import typing

from src.shared.logger import logger

PipMain = typing.Callable[[list[str]], int]
_pip_main: PipMain | None = None
_pip_main_resolved = False


def _resolve_pip_main() -> PipMain | None:
    global _pip_main_resolved, _pip_main
    if _pip_main_resolved:
        return _pip_main
    _pip_main_resolved = True

    with contextlib.suppress(Exception):
        pip_internal = importlib.import_module("pip._internal.cli.main")
        pip_internal_main = getattr(pip_internal, "main", None)
        if callable(pip_internal_main):
            _pip_main = typing.cast("PipMain", pip_internal_main)
            return _pip_main

    with contextlib.suppress(Exception):
        pip_module = importlib.import_module("pip")
        pip_module_main = getattr(pip_module, "main", None)
        if callable(pip_module_main):
            _pip_main = typing.cast("PipMain", pip_module_main)
            return _pip_main

    logger.critical("Failed to import pip main function")
    return None


def run_pip(file_path: str, *, install: bool = True) -> bool:
    pip_main = _resolve_pip_main()
    if pip_main is None:
        logger.exception(
            "Failed to process requirements file: pip main function unavailable",
        )
        return False

    path = pathlib.Path(file_path).expanduser().resolve()
    if not path.is_file():
        logger.exception("Failed to locate requirements file: %s", path)
        return False

    operation = ("install", "-U") if install else ("uninstall", "-y")
    command = [*operation, "-r", str(path)]

    try:
        status_code = pip_main(command)
        if status_code == 0:
            logger.info(
                "Processed requirements file '%s' with pip %s",
                path,
                " ".join(operation),
            )
            return True
        logger.exception(
            "Failed to process requirements file '%s': pip exited with status %d",
            path,
            status_code,
        )
        return False
    except Exception:
        logger.exception("Failed to process requirements file '%s'", path)
        return False
