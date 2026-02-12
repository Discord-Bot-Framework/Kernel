from __future__ import annotations

import pathlib

from shared.constants import SOCKET_DIR


def get_uds_path(module_name: str) -> pathlib.Path:
    return SOCKET_DIR / f"{module_name}.sock"
