# Run the local interception lab

Run the delivered `InterceptionLab` executable against a temporary HTTPS origin to observe SSE, edit a request at a breakpoint, replay a GET, and export a redacted HAR. This recipe uses Python 3's standard library, curl, OpenSSL 3, macOS 26+, and Xcode 27.1 with Swift 6.4. Port 8080 must be free.

Run the setup from the repository root and keep the terminal open. The two generated identities are independent: clients trust the interception CA, while the proxy trusts the origin certificate. No command installs system trust or changes proxy settings.

## Generate temporary credentials and build

```sh
LAB_DIR="$(mktemp -d "${TMPDIR:-/tmp}/swiftmitm-lab.XXXXXX")"
umask 077
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$LAB_DIR/ca-key.pem"
openssl req -new -x509 -key "$LAB_DIR/ca-key.pem" -out "$LAB_DIR/ca-cert.pem" \
  -days 1 -subj '/CN=SwiftMITM local lab' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$LAB_DIR/origin-key.pem"
openssl req -new -x509 -key "$LAB_DIR/origin-key.pem" -out "$LAB_DIR/origin-cert.pem" \
  -days 1 -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,digitalSignature,keyCertSign,cRLSign' \
  -addext 'extendedKeyUsage=serverAuth'
swift build --package-path Examples/InterceptionLab -c release
LAB_BIN="$(swift build --package-path Examples/InterceptionLab -c release --show-bin-path)/InterceptionLab"
```

The files contain short-lived private keys and remain outside the repository. The self-signed origin certificate is a trust anchor only for this lab process.

## Start an HTTPS origin

Create and run this standard-library origin. It serves two SSE events, echoes the `X-Lab` header at `/debug`, and returns an identity-coded response at `/resource`.

```sh
cat > "$LAB_DIR/origin.py" <<'PY_ORIGIN'
import http.server
import json
import pathlib
import ssl
import sys
import time
import urllib.parse

root = pathlib.Path(sys.argv[1])

class Origin(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        if path == "/events":
            parts = [b"data: first\n\n", b"data: second\n\n"]
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(sum(map(len, parts))))
            self.end_headers()
            for part in parts:
                self.wfile.write(part)
                self.wfile.flush()
                time.sleep(0.2)
            return
        if path == "/debug":
            body = json.dumps({"X-Lab": self.headers.get("X-Lab")}).encode()
        elif path == "/resource":
            body = b'{"message":"replayed"}'
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Origin)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(root / "origin-cert.pem", root / "origin-key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
(root / "port").write_text(str(server.server_port))
server.serve_forever()
PY_ORIGIN
python3 "$LAB_DIR/origin.py" "$LAB_DIR" >"$LAB_DIR/origin.log" 2>&1 &
ORIGIN_PID=$!
python3 - "$LAB_DIR/port" <<'PY_READY'
import pathlib
import sys
import time

path = pathlib.Path(sys.argv[1])
for _ in range(100):
    if path.exists() and path.read_text().isdigit():
        break
    time.sleep(0.1)
else:
    raise SystemExit("Origin did not become ready; inspect origin.log")
PY_READY
```

The bounded readiness wait confirms the selected origin port. Start the proxy:

```sh
ORIGIN_PORT="$(cat "$LAB_DIR/port")"
"$LAB_BIN" serve "$LAB_DIR/ca-cert.pem" "$LAB_DIR/ca-key.pem" \
  --allow-internal --origin-ca "$LAB_DIR/origin-cert.pem" --edit-header X-Lab=reviewed \
  >"$LAB_DIR/inspection.log" 2>"$LAB_DIR/inspection.err" &
PROXY_PID=$!
python3 - "$LAB_DIR/inspection.log" <<'PY_READY'
import pathlib
import sys
import time

path = pathlib.Path(sys.argv[1])
for _ in range(100):
    if path.exists() and "listening http://127.0.0.1:8080" in path.read_text():
        break
    time.sleep(0.1)
else:
    raise SystemExit("Proxy did not become ready; inspect inspection.err")
PY_READY
```

The readiness wait requires `listening http://127.0.0.1:8080` in `$LAB_DIR/inspection.log` before continuing. If startup fails, inspect the error file before sending traffic. `--allow-internal` permits internal destinations generally; the listener remains loopback-only.

## Observe SSE and the edited request

```sh
curl --fail --max-time 20 --no-buffer --noproxy '' --proxy http://127.0.0.1:8080 \
  --cacert "$LAB_DIR/ca-cert.pem" "https://localhost:$ORIGIN_PORT/events"
curl --fail --max-time 20 --noproxy '' --proxy http://127.0.0.1:8080 \
  --cacert "$LAB_DIR/ca-cert.pem" "https://localhost:$ORIGIN_PORT/debug"
cat "$LAB_DIR/inspection.log"
```

The client receives `data: first` and `data: second`, then `{"X-Lab": "reviewed"}`. Independently inspect the transcript for two `sse dataBytes=` summaries per representation (four total), `paused GET /debug`, and `original`/`effective` inspection records. The breakpoint resumes automatically with the configured edit. A successful HTTP response alone does not establish semantic inspection.

## Replay and export HAR

```sh
"$LAB_BIN" replay "$LAB_DIR/ca-cert.pem" "$LAB_DIR/ca-key.pem" \
  "https://localhost:$ORIGIN_PORT/resource?token=lab-demo-token" "$LAB_DIR/replay.har" \
  --allow-internal --origin-ca "$LAB_DIR/origin-cert.pem"
python3 -m json.tool "$LAB_DIR/replay.har"
```

The command prints `replay status=200` and `HAR 1.2 written`. The HAR contains one exchange with measured timings and omitted bodies. Its `queryString` entry for `token` is `[REDACTED]`, and its URL must not contain `lab-demo-token`. The value is synthetic; application-specific secrets still need an application redaction policy. Missing timings or decoded content size are export errors, never invented values.

## Stop and clean up

```sh
kill -INT "$PROXY_PID"
wait "$PROXY_PID"
cat "$LAB_DIR/inspection.log"
kill -TERM "$ORIGIN_PID"
wait "$ORIGIN_PID" || test "$?" -eq 143
rm -rf "$LAB_DIR"
```

The proxy transcript should end with `inspectionComplete=true` and `stopped`. `deliveryLoss`, `inspectionComplete=false`, connection failures, or absent semantic records mean the run is incomplete. If interrupted earlier, stop the two recorded processes before deleting their credentials. Do not reuse this CA outside the lab.

## Other protocols

This origin implements HTTPS and SSE. To exercise gRPC, gRPC-Web, or WebSocket inspection, supply a TLS service and client you control, configure the explicit CONNECT proxy and client CA trust, and pass the origin's trust root through `--origin-ca`. Use HTTP/2 for native gRPC and a `wss` endpoint for WebSockets. Expect `grpc event` or `webSocketMessage decodedBytes=` summaries in the console. Descriptor-backed protobuf field decoding requires an embedding application that loads a schema. See [Embedding](Embedding.md).
