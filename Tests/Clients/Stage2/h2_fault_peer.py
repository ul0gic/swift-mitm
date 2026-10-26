import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import signal
import sys
import threading
import time

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client_tls import create_identity

PREFIX = "/stage2.Fault/"
PAYLOAD = bytes(range(256)) * 512


def streaming(request, context):
    yield request + b"\x01"
    yield request + b"\x02"


def unary(request, context):
    return request


def server(ready_file):
    certificate, key = create_identity(ready_file.parent)
    instance = grpc.server(ThreadPoolExecutor(max_workers=4), compression=grpc.Compression.NoCompression)
    instance.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("stage2.Fault", {
        "Slow": grpc.unary_stream_rpc_method_handler(streaming),
        "Cancel": grpc.unary_stream_rpc_method_handler(streaming),
        "Fast": grpc.unary_unary_rpc_method_handler(unary),
    }),))
    port = instance.add_secure_port("127.0.0.1:0", grpc.ssl_server_credentials(((key.read_bytes(), certificate.read_bytes()),)))
    if not port:
        raise RuntimeError("gRPC fault peer could not bind")
    instance.start()
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    temporary = ready_file.with_suffix(".tmp")
    temporary.write_text(json.dumps({"port": port, "root": str(certificate)}))
    temporary.replace(ready_file)
    stopped.wait()
    instance.stop(0).wait()


def client(target, root):
    channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(root_certificates=root.read_bytes()))
    with channel, ThreadPoolExecutor(max_workers=1) as executor:
        fast = channel.unary_unary(PREFIX + "Fast")
        started = time.monotonic()
        slow = channel.unary_stream(PREFIX + "Slow")(PAYLOAD, timeout=20)
        slow.initial_metadata()
        head_elapsed = time.monotonic() - started
        assert head_elapsed >= 1.5, head_elapsed
        completed = executor.submit(list, slow)
        assert fast(b"sibling-before-completion", timeout=5) == b"sibling-before-completion"
        assert not completed.done(), "sibling was blocked until the throttled stream completed"
        result = completed.result(timeout=20)
        elapsed = time.monotonic() - started
        assert result == [PAYLOAD + b"\x01", PAYLOAD + b"\x02"]
        received_bytes = sum(map(len, result))
        minimum = 1.5 + (received_bytes - 1024) / 65536
        assert elapsed >= minimum, (elapsed, minimum)
        assert slow.code() == grpc.StatusCode.OK
        cancelling = channel.unary_stream(PREFIX + "Cancel")(PAYLOAD, timeout=20)
        cancelling.initial_metadata()
        assert fast(b"sibling-before-cancel", timeout=5) == b"sibling-before-cancel"
        assert cancelling.cancel(), "pending stream could not be cancelled"
        try:
            list(cancelling)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.CANCELLED, error.code()
        else:
            raise AssertionError("cancelled stream completed successfully")
        assert fast(b"sibling-after-cancel", timeout=5) == b"sibling-after-cancel"
    print(json.dumps({"grpcio": grpc.__version__, "responseBytes": received_bytes,
                      "headDelayMilliseconds": head_elapsed * 1000, "elapsedMilliseconds": elapsed * 1000,
                      "siblingBeforeCompletion": True, "cancelled": True, "siblingAfterCancel": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["server", "client"])
    parser.add_argument("--ready-file", type=Path)
    parser.add_argument("--target")
    parser.add_argument("--root", type=Path)
    arguments = parser.parse_args()
    if arguments.mode == "server":
        server(arguments.ready_file)
    else:
        client(arguments.target, arguments.root)
