from typing import Final

import msgpack

MSGPACK_DECODE_ERRORS: Final = (
    msgpack.ExtraData,
    msgpack.FormatError,
    ValueError,
    TypeError,
)
