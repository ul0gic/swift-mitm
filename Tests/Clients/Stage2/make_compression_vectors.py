import gzip
import pathlib
import sys
import zlib

output = pathlib.Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
plain = b"data: first\n\ndata: second\n\n"
inner = gzip.compress(plain, mtime=0)
split = len(inner) // 2
outer = gzip.compress(inner[:split], mtime=0) + gzip.compress(inner[split:], mtime=0)
(output / "plain.bin").write_bytes(plain)
(output / "double-gzip.bin").write_bytes(outer)
(output / "gzip.bin").write_bytes(inner)
(output / "deflate.bin").write_bytes(zlib.compress(plain))
message = bytes([8, 42])
compressed = gzip.compress(message, mtime=0)
grpc_frame = bytes([1]) + len(compressed).to_bytes(4, "big") + compressed
(output / "grpc-gzip.bin").write_bytes(grpc_frame + grpc_frame)
websocket_plain = b"HelloHelloHello"
compressor = zlib.compressobj(wbits=-15)
frames = bytearray()
for _ in range(2):
    compressed = compressor.compress(websocket_plain) + compressor.flush(zlib.Z_SYNC_FLUSH)
    payload = compressed[:-4]
    assert len(payload) < 126
    frames.extend(bytes([0xC1, len(payload)]) + payload)
(output / "websocket-context.bin").write_bytes(frames)
(output / "websocket-context-plain.bin").write_bytes(websocket_plain + websocket_plain)
