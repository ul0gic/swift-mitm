import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import threading

import grpc

from client_tls import create_identity


def unary(request, context):
    context.set_trailing_metadata((("result-bin", b"\x00\xff"),))
    return request


def server_stream(request, context):
    yield request + b"\x01"
    yield request + b"\x02"


def client_stream(requests, context):
    return b"".join(requests)


def bidirectional(requests, context):
    for request in requests:
        yield request[::-1]


def denied(request, context):
    context.abort(grpc.StatusCode.PERMISSION_DENIED, "independent denial")


def server(ready_file):
    certificate, key = create_identity(ready_file.parent)
    instance = grpc.server(ThreadPoolExecutor(max_workers=4), compression=grpc.Compression.Gzip)
    handlers = {
        "Unary": grpc.unary_unary_rpc_method_handler(unary),
        "ServerStream": grpc.unary_stream_rpc_method_handler(server_stream),
        "ClientStream": grpc.stream_unary_rpc_method_handler(client_stream),
        "Bidi": grpc.stream_stream_rpc_method_handler(bidirectional),
        "Denied": grpc.unary_unary_rpc_method_handler(denied),
    }
    instance.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("independent.Transport", handlers),))
    port = instance.add_secure_port("127.0.0.1:0", grpc.ssl_server_credentials(((key.read_bytes(), certificate.read_bytes()),)))
    if not port:
        raise RuntimeError("gRPC peer could not bind")
    instance.start()
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    temporary = ready_file.with_suffix(".tmp")
    temporary.write_text(json.dumps({"port": port, "root": str(certificate)}))
    temporary.replace(ready_file)
    stopped.wait()
    instance.stop(0).wait()


def client(target, root):
    if not os.environ.get("https_proxy"):
        raise RuntimeError("explicit proxy is required")
    channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(root_certificates=root.read_bytes()))
    prefix = "/independent.Transport/"
    with channel:
        unary_call = channel.unary_unary(prefix + "Unary")
        result, call = unary_call.with_call(b"\x00\xffunary", timeout=10, compression=grpc.Compression.Gzip)
        assert result == b"\x00\xffunary"
        assert ("result-bin", b"\x00\xff") in call.trailing_metadata()
        assert list(channel.unary_stream(prefix + "ServerStream")(b"stream", timeout=10)) == [b"stream\x01", b"stream\x02"]
        assert channel.stream_unary(prefix + "ClientStream")(iter([b"one", b"two"]), timeout=10) == b"onetwo"
        assert list(channel.stream_stream(prefix + "Bidi")(iter([b"one", b"two"]), timeout=10)) == [b"eno", b"owt"]
        try:
            channel.unary_unary(prefix + "Denied")(b"", timeout=10)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.PERMISSION_DENIED
            assert error.details() == "independent denial"
        else:
            raise AssertionError("trailers-only error was lost")
    print(json.dumps({"grpcio": grpc.__version__, "scenarios": ["unary", "server", "client", "bidi", "trailers-only"], "trusted": True}))


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
