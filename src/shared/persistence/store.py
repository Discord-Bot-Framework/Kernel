from __future__ import annotations

from typing import TYPE_CHECKING, Final, cast

import lmdb
import msgpack
from typing_extensions import Self

if TYPE_CHECKING:
    import pathlib

type Msgpack = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | list[Msgpack]
    | tuple[Msgpack, ...]
    | dict[str, Msgpack]
    | dict[bytes, Msgpack]
    | dict[int, Msgpack]
)

_MSGPACK_PACK_OPTS: Final = {"use_bin_type": True}
_MSGPACK_UNPACK_OPTS: Final = {"raw": False}


class Store:
    __slots__ = ("_dbs", "_env", "db_names", "map_size", "path")

    def __init__(
        self,
        path: pathlib.Path,
        db_names: tuple[str, ...],
        *,
        map_size: int,
    ) -> None:
        self.path: Final = path
        self.db_names: Final = db_names
        self.map_size: Final = map_size
        self._env: lmdb.Environment | None = None
        self._dbs: dict[str, lmdb._Database] = {}

    def open(self) -> None:
        if self._env is not None:
            return
        self.path.mkdir(parents=True, exist_ok=True)
        env = lmdb.open(
            str(self.path),
            map_size=self.map_size,
            max_dbs=len(self.db_names),
            create=True,
            subdir=True,
            lock=True,
            readahead=False,
            max_spare_txns=16,
        )
        self._env = env
        self._dbs = {name: env.open_db(name.encode()) for name in self.db_names}

    def close(self) -> None:
        if self._env is None:
            return
        self._env.close()
        self._env = None
        self._dbs.clear()

    @property
    def env(self) -> lmdb.Environment | None:
        return self._env

    def get_db(self, name: str) -> lmdb._Database | None:
        return self._dbs.get(name)

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def pack_msgpack(data: Msgpack) -> bytes:
    result = msgpack.packb(data, **_MSGPACK_PACK_OPTS)
    if result is None:
        msg = "Failed to pack msgpack data; packb returned None"
        raise TypeError(msg)
    return result


def unpack_msgpack(
    data: bytes,
    *,
    strict_map_key: bool = True,
) -> Msgpack:
    return msgpack.unpackb(data, strict_map_key=strict_map_key, **_MSGPACK_UNPACK_OPTS)


def unpack_msgpack_mapping(
    data: bytes,
    *,
    strict_map_key: bool = True,
) -> dict[str, Msgpack]:
    unpacked = unpack_msgpack(data, strict_map_key=strict_map_key)
    if not isinstance(unpacked, dict):
        msg = "Failed to unpack msgpack mapping; invalid payload type"
        raise TypeError(msg)
    return cast("dict[str, Msgpack]", unpacked)
