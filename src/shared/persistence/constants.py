from __future__ import annotations

from typing import Final

import msgpack

MSGPACK_DECODE_ERRORS: Final[tuple[type[BaseException], ...]] = (
    msgpack.ExtraData,
    msgpack.FormatError,
    ValueError,
    TypeError,
)
