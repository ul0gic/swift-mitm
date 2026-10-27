import argparse
import base64
import hashlib
from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import secrets
import signal
import socket
import ssl
import sys
import threading
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client_tls import create_identity
from loopback_http import LoopbackHTTPServer


def exact(stream, count):
    result = bytearray()
    while len(result) < count:
        incoming = stream.read(count - len(result))
        if not incoming:
            raise RuntimeError("peer closed before the complete frame")
        result.extend(incoming)
    return bytes(result)


def frame(stream, masked):
    header = exact(stream, 2)
    if bool(header[1] & 128) != masked or header[1] & 127 > 125:
        raise RuntimeError("expected a small frame with the correct masking direction")
    mask = exact(stream, 4) if masked else None
    payload = exact(stream, header[1] & 127)
    if mask is not None:
        payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return bool(header[0] & 128), header[0] & 15, payload


def masked_frame(opcode, payload, final=True):
    mask = secrets.token_bytes(4)
    return bytes([(128 if final else 0) | opcode, 128 | len(payload)]) + mask + bytes(
        value ^ mask[index % 4] for index, value in enumerate(payload)
    )


class Peer(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    released = threading.Event()
    held_lock = threading.Lock()
    held_requests = 0

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for part in (b"data: \xe2", b"\x82\xac-first\n\n", b"data: second\n\n"):
                self.wfile.write(format(len(part), "x").encode() + b"\r\n" + part + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        elif path == "/debug":
            self.respond(json.dumps({"X-Lab": self.headers.get("X-Lab")}).encode(), "application/json")
        elif path == "/resource":
            self.respond(b"SwiftMITM local replay resource\n", "text/plain")
        elif path == "/held":
            with self.held_lock:
                type(self).held_requests += 1
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.flush()
            if not self.released.wait(30):
                raise RuntimeError("held response was not released")
            self.wfile.write(b"ok")
            self.wfile.flush()
        elif path == "/held-count":
            with self.held_lock:
                count = type(self).held_requests
            self.respond(str(count).encode(), "text/plain")
        elif path == "/release":
            self.released.set()
            self.respond(b"released", "text/plain")
        elif path == "/socket":
            self.websocket()
        else:
            self.send_error(404)

    def respond(self, body, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def websocket(self):
        key = self.headers["Sec-WebSocket-Key"]
        accept = base64.b64encode(hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
        ).digest()).decode()
        self.send_response(101)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        self.connection.settimeout(10)
        message = bytearray()
        while True:
            final, opcode, payload = frame(self.rfile, masked=True)
            if opcode == 9:
                self.wfile.write(bytes([0x8A, len(payload)]) + payload)
                self.wfile.flush()
                continue
            if opcode not in (0, 1):
                raise RuntimeError("unexpected client frame")
            message.extend(payload)
            if final:
                break
        if bytes(message) != b"client-text":
            raise RuntimeError("fragmented client message changed")
        self.wfile.write(b"\x01\x07server-\x89\x01p\x80\x04text")
        self.wfile.flush()
        while True:
            _, opcode, payload = frame(self.rfile, masked=True)
            if opcode == 10:
                continue
            if opcode != 8 or payload != b"\x03\xe8":
                raise RuntimeError("expected normal WebSocket close")
            self.wfile.write(b"\x88\x02" + payload)
            self.wfile.flush()
            self.close_connection = True
            return

    def log_message(self, *_):
        pass


def server(ready_file):
    ready_file.parent.mkdir(parents=True, exist_ok=True)
    print("origin-start: identity", file=sys.stderr, flush=True)
    certificate, key = create_identity(ready_file.parent)
    print("origin-start: bind", file=sys.stderr, flush=True)
    instance = LoopbackHTTPServer(("127.0.0.1", 0), Peer)
    instance.daemon_threads = True
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    print("origin-start: tls", file=sys.stderr, flush=True)
    context.load_cert_chain(certificate, key)
    instance.socket = context.wrap_socket(instance.socket, server_side=True)
    temporary = ready_file.with_suffix(".tmp")
    temporary.write_text(json.dumps({"port": instance.server_port, "root": str(certificate)}))
    temporary.replace(ready_file)
    print("origin-start: ready", file=sys.stderr, flush=True)
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=instance.shutdown).start())
    instance.serve_forever()
    instance.server_close()


def headers(stream):
    status = stream.readline(8192)
    fields = {}
    total = len(status)
    while True:
        line = stream.readline(8192)
        total += len(line)
        if not line or total > 32768:
            raise RuntimeError("invalid or excessive response head")
        if line == b"\r\n":
            return status, fields
        name, value = line.decode("ascii").split(":", 1)
        fields[name.lower()] = value.strip()


def websocket(target, proxy, root):
    destination = urlsplit("https://" + target)
    proxy_url = urlsplit(proxy)
    if destination.hostname not in ("localhost", "127.0.0.1") or proxy_url.hostname != "127.0.0.1":
        raise RuntimeError("this demonstration uses loopback peers only")
    with socket.create_connection((proxy_url.hostname, proxy_url.port), timeout=10) as connection:
        connection.sendall(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode())
        with connection.makefile("rb", buffering=0) as stream:
            status, _ = headers(stream)
        if status.split()[1] != b"200":
            raise RuntimeError("proxy rejected CONNECT")
        context = ssl.create_default_context(cafile=str(root))
        context.set_alpn_protocols(["http/1.1"])
        with context.wrap_socket(connection, server_hostname=destination.hostname) as secure:
            key = base64.b64encode(secrets.token_bytes(16)).decode()
            secure.sendall((f"GET /socket HTTP/1.1\r\nHost: {target}\r\nUpgrade: websocket\r\n"
                            f"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                            f"Sec-WebSocket-Key: {key}\r\n\r\n").encode())
            with secure.makefile("rb") as stream:
                status, fields = headers(stream)
                expected = base64.b64encode(hashlib.sha1(
                    (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
                ).digest()).decode()
                if status.split()[1] != b"101" or fields.get("sec-websocket-accept") != expected:
                    raise RuntimeError("invalid WebSocket handshake")
                secure.sendall(masked_frame(1, b"client-", final=False)
                               + masked_frame(9, b"p") + masked_frame(0, b"text"))
                message = bytearray()
                while True:
                    final, opcode, payload = frame(stream, masked=False)
                    if opcode == 9:
                        secure.sendall(masked_frame(10, payload))
                        continue
                    if opcode == 10:
                        continue
                    if opcode not in (0, 1):
                        raise RuntimeError("unexpected server frame")
                    message.extend(payload)
                    if final:
                        break
                if bytes(message) != b"server-text":
                    raise RuntimeError("fragmented server message changed")
                secure.sendall(masked_frame(8, b"\x03\xe8"))
                if frame(stream, masked=False) != (True, 8, b"\x03\xe8"):
                    raise RuntimeError("normal close was not preserved")
    print(json.dumps({"websocket": "passed", "fragmented": True, "trusted": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="mode", required=True)
    origin = commands.add_parser("server")
    origin.add_argument("--ready-file", type=Path, required=True)
    client = commands.add_parser("websocket")
    client.add_argument("--target", required=True)
    client.add_argument("--proxy", required=True)
    client.add_argument("--root", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.mode == "server":
        server(arguments.ready_file)
    else:
        websocket(arguments.target, arguments.proxy, arguments.root)
