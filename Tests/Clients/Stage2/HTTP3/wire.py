import asyncio

from aioquic.buffer import encode_uint_var


ENCODED_HEADER_LIMIT = 32768
FRAGMENT_VALUE = b"0123456789abcdef" * 1800


def oversized_headers_prefix():
    return encode_uint_var(1) + encode_uint_var(ENCODED_HEADER_LIMIT + 1)


async def fragmented_headers(protocol, stream, headers, end_stream):
    # The pinned aioquic encoder produces the real QPACK block and encoder updates.
    block = protocol.http._encode_headers(stream, headers)
    assert 16384 < len(block) <= ENCODED_HEADER_LIMIT, len(block)
    length = encode_uint_var(len(block))
    midpoint = len(block) // 2
    fragments = [encode_uint_var(1), length, block[:midpoint], block[midpoint:]]
    for index, fragment in enumerate(fragments):
        last = index == len(fragments) - 1
        protocol._quic.send_stream_data(stream, fragment, end_stream=end_stream and last)
        protocol.transmit()
        if not last:
            # ACKed PING barriers separate type, length and payload writes on the wire.
            await asyncio.wait_for(protocol.ping(), timeout=5)
    return len(block)
