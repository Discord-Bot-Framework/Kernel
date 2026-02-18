from __future__ import annotations

from typing import TYPE_CHECKING

from src.shared.persistence.constants import MSGPACK_DECODE_ERRORS
from src.shared.persistence.store import (
    Msgpack,
    Store,
    pack_msgpack,
    unpack_msgpack_mapping,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    import lmdb


def list_mapping_records(
    env: lmdb.Environment | None,
    store: Store,
    db_name: str,
    *,
    strict_map_key: bool = True,
) -> list[dict[str, Msgpack]]:
    if env is None:
        return []
    db = store.get_db(db_name)
    if db is None:
        return []

    result: list[dict[str, Msgpack]] = []
    with env.begin(db=db) as txn:
        cursor = txn.cursor()
        for value in cursor.iternext(values=True):
            try:
                result.append(
                    unpack_msgpack_mapping(bytes(value), strict_map_key=strict_map_key)
                )
            except MSGPACK_DECODE_ERRORS:
                continue
    return result


def get_mapping_record(
    env: lmdb.Environment | None,
    store: Store,
    db_name: str,
    key: bytes,
    *,
    strict_map_key: bool = True,
) -> dict[str, Msgpack] | None:
    if env is None:
        return None
    db = store.get_db(db_name)
    if db is None:
        return None

    with env.begin(db=db) as txn:
        packed = txn.get(key)
    if packed is None:
        return None

    try:
        return unpack_msgpack_mapping(bytes(packed), strict_map_key=strict_map_key)
    except MSGPACK_DECODE_ERRORS:
        return None


def put_mapping_record(
    env: lmdb.Environment | None,
    store: Store,
    db_name: str,
    key: bytes,
    payload: Mapping[str, Msgpack],
) -> None:
    if env is None:
        return
    db = store.get_db(db_name)
    if db is None:
        return

    packed = pack_msgpack(dict(payload))
    with env.begin(write=True, db=db) as txn:
        txn.put(key, packed)


def delete_record(
    env: lmdb.Environment | None,
    store: Store,
    db_name: str,
    key: bytes,
) -> bool:
    if env is None:
        return False
    db = store.get_db(db_name)
    if db is None:
        return False

    with env.begin(write=True, db=db) as txn:
        return txn.delete(key)
