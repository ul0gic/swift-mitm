import argparse
import base64
import hashlib
from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import signal
import ssl
import sys
import threading

from client_tls import create_identity
from loopback_http import LoopbackHTTPServer

PAGE = b'''<!doctype html><meta charset="utf-8"><body>pending<script>
let sseDone = false, socketDone = false;
window.__swiftmitmDone = false;
let finishing = false;
async function finish() {
  if (!sseDone || !socketDone || finishing) return;
  finishing = true;
  await fetch('/done');
  window.__swiftmitmDone = true;
  document.body.textContent = 'SwiftMITM browser passed';
  if (window.webkit) window.webkit.messageHandlers.result.postMessage('passed');
}
const events = new EventSource('/events');
const received = [];
events.onmessage = event => {
  received.push(event.data);
  if (received.length === 1) {
    if (event.data !== '\\u20ac-first') throw new Error('split UTF-8 lost');
    fetch('/sse-observed');
  } else {
    if (event.data !== 'second') throw new Error('SSE ordering lost');
    sseDone = true; finish();
  }
};
const socket = new WebSocket('wss://' + location.host + '/socket');
socket.onopen = () => socket.send('client-text');
socket.onmessage = event => {
  if (event.data !== 'server-text') throw new Error('WebSocket payload lost');
  socket.close(1000, 'done');
};
socket.onclose = event => {
  if (event.code !== 1000) throw new Error('WebSocket close lost');
  socketDone = true; finish();
};
</script>'''


class Peer(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    observed = threading.Event()
    done = threading.Event()
    events_ended = threading.Event()

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(PAGE)))
            self.end_headers()
            self.wfile.write(PAGE)
        elif self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.chunk(b": heartbeat\n\ndata: \xe2")
            self.chunk(b"\x82\xac-first\n\n")
            if not self.observed.wait(10):
                raise RuntimeError("browser did not observe SSE before response completion")
            self.chunk(b"data: second\n\n")
            self.done.wait(10)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            self.events_ended.set()
        elif self.path == "/socket":
            key = self.headers["Sec-WebSocket-Key"]
            accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            opcode, payload = self.frame()
            if opcode != 1 or payload != b"client-text":
                raise RuntimeError("wrong browser WebSocket input")
            self.wfile.write(b"\x81\x0bserver-text")
            self.wfile.flush()
            opcode, payload = self.frame()
            if opcode != 8:
                raise RuntimeError("browser did not close WebSocket")
            self.wfile.write(bytes([0x88, len(payload)]) + payload)
            self.wfile.flush()
            self.close_connection = True
        elif self.path in ("/sse-observed", "/done"):
            (self.observed if self.path == "/sse-observed" else self.done).set()
            if self.path == "/done" and not self.events_ended.wait(10):
                raise RuntimeError("SSE response did not complete after the client barrier")
            self.send_response(204)
            self.end_headers()
        else:
            self.send_error(404)

    def chunk(self, data):
        self.wfile.write(format(len(data), "x").encode() + b"\r\n" + data + b"\r\n")
        self.wfile.flush()

    def frame(self):
        head = self.rfile.read(2)
        if len(head) != 2 or not head[1] & 128 or head[1] & 127 > 125:
            raise RuntimeError("expected a small masked browser frame")
        mask = self.rfile.read(4)
        payload = self.rfile.read(head[1] & 127)
        return head[0] & 15, bytes(value ^ mask[index % 4] for index, value in enumerate(payload))

    def log_message(self, *_):
        pass


def server(ready_file, bind_address):
    print("origin-start: identity", file=sys.stderr, flush=True)
    certificate, key = create_identity(
        ready_file.parent, additional_ip=None if bind_address == "127.0.0.1" else bind_address
    )
    print("origin-start: bind", file=sys.stderr, flush=True)
    instance = LoopbackHTTPServer((bind_address, 0), Peer)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--bind-address", default="127.0.0.1")
    arguments = parser.parse_args()
    server(arguments.ready_file, arguments.bind_address)
