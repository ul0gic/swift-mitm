import asyncio
import json
from pathlib import Path

import pylsqpack
from aioquic.asyncio.client import connect
from aioquic.h3.connection import encode_frame


OCTET_FIELDS = [(b"x-octet", b"\x80\xff"), (b"x-octet", b"\xc3\xa9"),
                (b"x-huffman", b"\x80\xff"), (b"x-octet", b"\xff\x80")]
REQUEST_BODY = b"octet-request"
RESPONSE_BODY = b"octet-response"


def field_hex(fields):
    return [[name.hex(), value.hex()] for name, value in fields]


def octet_headers(headers):
    block = bytearray(b"\x00\x00")
    for name, value in headers:
        assert len(name) < 135 and len(value) < 127
        block.extend(bytes([0x20 | min(len(name), 7)]))
        if len(name) >= 7:
            block.append(len(name) - 7)
        block.extend(name)
        if name == b"x-huffman":
            assert value == b"\x80\xff"
            # RFC7541 Appendix B: 128=(0xfffe6,20),255=(0x3ffffee,26); EOS-prefix padding.
            encoded = (((0xfffe6 << 26) | 0x3ffffee) << 2 | 3).to_bytes(6, "big")
            block.append(0x80 | len(encoded))
            block.extend(encoded)
        else:
            block.append(len(value))
            block.extend(value)
    # RFC9204 4.5.1/4.5.6 literal fixture must also decode with the independent maintained stack.
    _, decoded = pylsqpack.Decoder(0, 0).feed_header(0, bytes(block))
    assert decoded == headers, (field_hex(decoded), field_hex(headers))
    return encode_frame(1, bytes(block))


async def octet_fidelity(ready, authority, certificate, peer, client_type, configuration, complete):
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=client_type) as client:
        assert client.alpn == "h3"
        stream, response = client.raw_request()
        request_headers = [(b":method", b"POST"), (b":scheme", b"https"),
                           (b":authority", authority.encode("ascii")), (b":path", b"/octet-fidelity")] + OCTET_FIELDS
        client._quic.send_stream_data(stream, octet_headers(request_headers), end_stream=False)
        client._quic.send_stream_data(stream, encode_frame(0, REQUEST_BODY), end_stream=False)
        client._quic.send_stream_data(stream, octet_headers(OCTET_FIELDS), end_stream=True)
        client.transmit()
        assert await complete(response) == RESPONSE_BODY
        assert response.interim == [[(b":status", b"103")] + OCTET_FIELDS], field_hex(response.interim[0]) if response.interim else []
        assert response.headers == [(b":status", b"200")] + OCTET_FIELDS, field_hex(response.headers)
        assert response.trailers == OCTET_FIELDS, field_hex(response.trailers)
        async with asyncio.timeout(5):
            while True:
                stats = json.loads(Path(peer["stats"]).read_text())
                if stats["octetTrailers"]:
                    break
                await asyncio.sleep(0.02)
        assert stats["octetHeaders"] == field_hex(OCTET_FIELDS), stats
        assert stats["octetTrailers"] == field_hex(OCTET_FIELDS), stats
        assert stats["octetBody"] == REQUEST_BODY.hex(), stats
        assert await complete(client.request(authority, "/after-octets")[1]) == b"origin"
    return {"requestFields": stats["octetHeaders"], "requestTrailers": stats["octetTrailers"],
            "responseFields": field_hex(response.headers), "responseTrailers": field_hex(response.trailers),
            "interimFields": field_hex(response.interim[0]), "literalAndHuffmanValues": True,
            "sameConnectionProgress": True}
