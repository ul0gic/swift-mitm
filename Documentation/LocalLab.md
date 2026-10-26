# Run the local interception lab

These recipes exercise the public `InterceptionLab` consumer against repository-owned peers. They require macOS 26, Swift 6.4, Python 3 and OpenSSL 3. Commands are source-reviewed but have not yet passed the candidate's GitHub Actions checks.

Run the setup from the repository root. Keep this terminal open; subsequent commands use its variables. The temporary directory contains short-lived private keys and is created outside the repository. No command installs a certificate in system trust or changes system proxy settings.

```sh
LAB_ROOT="$PWD"
LAB_DIR="$(mktemp -d "${TMPDIR:-/tmp}/swiftmitm-lab.XXXXXX")"
umask 077
python3 -m venv "$LAB_DIR/venv"
"$LAB_DIR/venv/bin/python" -m pip install cryptography==50.0.2 grpcio==1.85.0
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$LAB_DIR/ca-key.pem"
openssl req -new -x509 -key "$LAB_DIR/ca-key.pem" -out "$LAB_DIR/ca-cert.pem" \
  -days 1 -subj '/CN=SwiftMITM local lab' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'
swift build --package-path Examples/InterceptionLab -c release
LAB_BIN="$LAB_ROOT/Examples/InterceptionLab/.build/release/InterceptionLab"
```

The build produces `InterceptionLab`. The interception CA signs the certificates seen by clients; each independent origin generates a separate identity that the proxy explicitly trusts. `--allow-internal` enables internal destinations generally for this proxy process, so leave the listener bound to its default loopback address.

## SSE, WebSocket, a paused edit, replay and HAR

Start the independent HTTPS origin:

```sh
mkdir "$LAB_DIR/http"
"$LAB_DIR/venv/bin/python" Tests/Clients/Stage2/lab_peer.py server \
  --ready-file "$LAB_DIR/http/ready.json" >"$LAB_DIR/http-peer.log" 2>&1 &
HTTP_PID=$!
```

After `$LAB_DIR/http/ready.json` exists, start the proxy with the origin's generated trust root and one header edit:

```sh
HTTP_PORT="$("$LAB_DIR/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["port"])' "$LAB_DIR/http/ready.json")"
"$LAB_BIN" serve "$LAB_DIR/ca-cert.pem" "$LAB_DIR/ca-key.pem" \
  --allow-internal --origin-ca "$LAB_DIR/http/origin-ca.pem" --edit-header X-Lab=reviewed \
  >"$LAB_DIR/http-inspection.log" 2>"$LAB_DIR/http-inspection.err" &
PROXY_PID=$!
```

Wait for `listening http://127.0.0.1:8080` in `$LAB_DIR/http-inspection.log`. These clients use the proxy explicitly and verify its generated leaf certificates against the interception CA:

```sh
curl --fail --no-buffer --noproxy '' --proxy http://127.0.0.1:8080 \
  --cacert "$LAB_DIR/ca-cert.pem" "https://localhost:$HTTP_PORT/events"
"$LAB_DIR/venv/bin/python" Tests/Clients/Stage2/lab_peer.py websocket \
  --target "localhost:$HTTP_PORT" --proxy http://127.0.0.1:8080 --root "$LAB_DIR/ca-cert.pem"
curl --fail --noproxy '' --proxy http://127.0.0.1:8080 \
  --cacert "$LAB_DIR/ca-cert.pem" "https://localhost:$HTTP_PORT/debug"
cat "$LAB_DIR/http-inspection.log"
```

The SSE client receives two events. The WebSocket client validates a fragmented exchange, ping/pong and close. The `/debug` response echoes `X-Lab: reviewed` as JSON, showing the origin received the edit. Independently inspect the proxy transcript for `sse dataBytes=`, `webSocketMessage bytes=`, `paused GET /debug`, and separate `original`/`effective` inspection records. The breakpoint is resolved automatically with the configured edit; this is not an interactive prompt.

Replay a new GET to the same origin and export its metadata:

```sh
"$LAB_BIN" replay "$LAB_DIR/ca-cert.pem" "$LAB_DIR/ca-key.pem" \
  "https://localhost:$HTTP_PORT/resource?token=lab-demo-token" "$LAB_DIR/replay.har" \
  --allow-internal --origin-ca "$LAB_DIR/http/origin-ca.pem"
"$LAB_DIR/venv/bin/python" -m json.tool "$LAB_DIR/replay.har"
```

The command prints `replay status=200` and `HAR 1.2 written`. The HAR contains one exchange with measured timings and omitted bodies. Its `queryString` entry for `token` contains `[REDACTED]`, and its URL must not contain `lab-demo-token`. This synthetic value demonstrates query redaction without sending real credentials; it does not prove every redaction rule. An unavailable timing or decoded-size result is an export failure, not a fabricated HAR value.

Stop this proxy before starting the gRPC recipe, which uses the same listener port:

```sh
kill -INT "$PROXY_PID"
wait "$PROXY_PID"
kill -TERM "$HTTP_PID"
wait "$HTTP_PID"
```

The proxy transcript should end with `inspectionComplete=true` and a `stopped` record. `deliveryLoss`, `inspectionComplete=false`, connection failures or missing semantic records mean the demonstration is incomplete even if a client returned successfully.

## Native gRPC

Start the existing independent gRPC peer. It generates its own P-256 origin identity and writes its selected port to a ready file.

```sh
mkdir "$LAB_DIR/grpc"
"$LAB_DIR/venv/bin/python" Tests/Clients/grpc_peer.py server \
  --ready-file "$LAB_DIR/grpc/ready.json" >"$LAB_DIR/grpc-peer.log" 2>&1 &
GRPC_PID=$!
```

After `$LAB_DIR/grpc/ready.json` exists, start the proxy and read the origin port:

```sh
GRPC_PORT="$("$LAB_DIR/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["port"])' "$LAB_DIR/grpc/ready.json")"
"$LAB_BIN" serve "$LAB_DIR/ca-cert.pem" "$LAB_DIR/ca-key.pem" \
  --allow-internal --origin-ca "$LAB_DIR/grpc/origin-ca.pem" \
  >"$LAB_DIR/grpc-inspection.log" 2>"$LAB_DIR/grpc-inspection.err" &
PROXY_PID=$!
```

After the transcript contains `listening http://127.0.0.1:8080`, run the client. Clearing bypass variables makes the loopback target use the explicit proxy; the only client trust input is the generated interception CA.

```sh
env https_proxy=http://127.0.0.1:8080 grpc_proxy=http://127.0.0.1:8080 \
  no_proxy= NO_PROXY= no_grpc_proxy= \
  "$LAB_DIR/venv/bin/python" Tests/Clients/grpc_peer.py client \
  --target "localhost:$GRPC_PORT" --root "$LAB_DIR/ca-cert.pem"
cat "$LAB_DIR/grpc-inspection.log"
kill -INT "$PROXY_PID"
wait "$PROXY_PID"
kill -TERM "$GRPC_PID"
wait "$GRPC_PID"
```

The client checks unary, server-streaming, client-streaming, bidirectional and trailers-only calls and prints `trusted: true` in its JSON result. The proxy transcript must independently contain `grpc event` records; a successful RPC alone does not establish inspection. These generic RPCs carry opaque bytes, so this recipe proves framing/status observation, not protobuf field decoding. The console does not load a descriptor schema.

## Cleanup

Stop any remaining lab processes using the PIDs recorded above before deleting the generated directory:

```sh
rm -rf "$LAB_DIR"
```

Do not reuse this short-lived CA outside the lab. On a TLS error, check the two separate trust inputs: clients use `ca-cert.pem`; the proxy uses the origin's `origin-ca.pem`. Do not bypass certificate validation.
