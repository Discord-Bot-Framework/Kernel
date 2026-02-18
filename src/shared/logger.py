from __future__ import annotations

import logging
import pathlib
import threading
from logging.handlers import RotatingFileHandler
from typing import Final


_LOG_FORMAT: Final[str] = (
    "[%(asctime)s.%(msecs)03d] "
    "[created=%(created)f] "
    "[relative=%(relativeCreated)d] "
    "[pid=%(process)d:%(processName)s] "
    "[tid=%(thread)d:%(threadName)s] "
    "[task=%(taskName)s] "
    "[%(levelname)-8s:%(levelno)d] "
    "[logger=%(name)s] "
    "[module=%(module)s] "
    "[file=%(filename)s] "
    "[path=%(pathname)s] "
    "[func=%(funcName)s] "
    "[line=%(lineno)d] "
    "%(message)s"
)


_DATE_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S%z"
_MAX_BYTES: Final[int] = 1024 * 1024
_BACKUP_COUNT: Final[int] = 1

_logger_lock = threading.RLock()
_handler_cache: dict[pathlib.Path, RotatingFileHandler] = {}
_formatter = logging.Formatter(_LOG_FORMAT, _DATE_FORMAT)


def _has_target_handler(
    logger_instance: logging.Logger,
    file_path: pathlib.Path,
) -> bool:
    file_str = str(file_path)
    return any(
        isinstance(h, RotatingFileHandler)
        and str(getattr(h, "baseFilename", "")) == file_str
        for h in logger_instance.handlers
    )


def _get_or_create_handler(file_path: pathlib.Path) -> RotatingFileHandler:
    with _logger_lock:
        handler = _handler_cache.get(file_path)
        if handler is not None:
            return handler

        file_path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            file_path,
            maxBytes=_MAX_BYTES,
            backupCount=_BACKUP_COUNT,
            encoding="utf-8",
            delay=True,
        )
        handler.setFormatter(_formatter)
        handler.setLevel(logging.DEBUG)
        _handler_cache[file_path] = handler
        return handler


def _get_logger(path: pathlib.Path, name: str | None = None) -> logging.Logger:
    file_path = pathlib.Path(path).resolve()
    logger_instance = logging.getLogger(name)
    logger_instance.setLevel(logging.DEBUG)
    logger_instance.propagate = False

    with _logger_lock:
        if not _has_target_handler(logger_instance, file_path):
            logger_instance.addHandler(_get_or_create_handler(file_path))

    return logger_instance


def get_module_logger(
    module_file: str,
    module_name: str,
    log_filename: str = "module.log",
) -> logging.Logger:
    module_dir = pathlib.Path(module_file).resolve().parent
    log_path = module_dir / log_filename
    return _get_logger(log_path, module_name)
