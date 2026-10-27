import gzip
from pathlib import Path
import sys

output = Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
for size in (64, 65):
    compressed = gzip.compress(b"a" * size, mtime=0)
    frame = bytes([1]) + len(compressed).to_bytes(4, "big") + compressed
    (output / f"semantic-grpc-{size}.bin").write_bytes(frame)
