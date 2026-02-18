from __future__ import annotations

import enum

_ENTRY_FILES: dict[str, str] = {
    "python": "main.py",
}
_DEPENDENCY_FILES: dict[str, str] = {
    "python": "requirements.txt",
}


class ModuleType(enum.StrEnum):
    PYTHON = "python"

    @property
    def entry_file(self) -> str:
        return _ENTRY_FILES[self.value]

    @property
    def dependency_file(self) -> str:
        return _DEPENDENCY_FILES[self.value]
