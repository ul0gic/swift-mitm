import argparse
import http.client
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client_tls import create_identity


def wait_for(predicate, process, timeout=15):
    deadline = time.monotonic() + timeout
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError("process exited before readiness")
        if time.monotonic() >= deadline:
            raise TimeoutError("readiness deadline exceeded")
        time.sleep(0.02)


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def get(port, path, root, headers=None):
    context = ssl.create_default_context(cafile=str(root))
    context.set_alpn_protocols(["http/1.1"])
    connection = http.client.HTTPSConnection("127.0.0.1", 8080, timeout=15, context=context)
    connection.set_tunnel("localhost", port)
    try:
        connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        body = response.read(65536)
        assert response.status == 200, response.status
        return body
    finally:
        connection.close()


def run(binary):
    scripts = Path(__file__).resolve().parent
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy",
                 "grpc_proxy", "no_proxy", "NO_PROXY", "no_grpc_proxy"):
        environment.pop(name, None)
    processes = []
    handles = []
    phase = "setup"
    missing_marker = None
    with tempfile.TemporaryDirectory(prefix="swiftmitm-lab-") as temporary:
        directory = Path(temporary)
        try:
            proxy_directory = directory / "proxy"
            proxy_directory.mkdir()
            certificate, key = create_identity(proxy_directory)
            ready = {}
            for name, script in (("http", scripts / "lab_peer.py"), ("grpc", scripts.parent / "grpc_peer.py")):
                peer_directory = directory / name
                peer_directory.mkdir()
                ready_file = peer_directory / "ready.json"
                log = (directory / f"{name}.log").open("wb")
                handles.append(log)
                peer = subprocess.Popen([sys.executable, str(script), "server", "--ready-file", str(ready_file)],
                                        stdout=log, stderr=subprocess.STDOUT, env=environment)
                processes.append(peer)
                wait_for(ready_file.exists, peer)
                ready[name] = json.loads(ready_file.read_text())
            upstream_roots = directory / "upstream-roots.pem"
            upstream_roots.write_text("".join(Path(peer["root"]).read_text() for peer in ready.values()))
            serve_log = directory / "serve.log"
            log = serve_log.open("wb")
            handles.append(log)
            serve = subprocess.Popen([
                str(binary), "serve", str(certificate), str(key), "--allow-internal",
                "--origin-ca", str(upstream_roots), "--edit-header", "X-Lab=reviewed"
            ], stdout=log, stderr=subprocess.STDOUT, env=environment)
            processes.append(serve)
            wait_for(lambda: "listening http://127.0.0.1:8080" in serve_log.read_text(), serve)
            phase = "sse"
            events = get(ready["http"]["port"], "/events", certificate)
            assert events.decode() == "data: €-first\n\ndata: second\n\n", events
            phase = "paused-header-edit"
            debug = get(ready["http"]["port"], "/debug", certificate, {"X-Lab": "original"})
            assert json.loads(debug) == {"X-Lab": "reviewed"}, debug
            phase = "websocket"
            websocket = subprocess.run([
                sys.executable, str(scripts / "lab_peer.py"), "websocket",
                "--target", f"localhost:{ready['http']['port']}",
                "--proxy", "http://127.0.0.1:8080", "--root", str(certificate)
            ], env=environment, capture_output=True, text=True, timeout=20, check=True)
            assert json.loads(websocket.stdout) == {"websocket": "passed", "fragmented": True, "trusted": True}
            phase = "grpc"
            grpc_environment = environment | {
                "grpc_proxy": "http://127.0.0.1:8080", "https_proxy": "http://127.0.0.1:8080",
                "no_proxy": "", "NO_PROXY": "", "no_grpc_proxy": ""
            }
            grpc = subprocess.run([
                sys.executable, str(scripts.parent / "grpc_peer.py"), "client",
                "--target", f"localhost:{ready['grpc']['port']}", "--root", str(certificate)
            ], env=grpc_environment, capture_output=True, text=True, timeout=60, check=False)
            (directory / "grpc-client.log").write_text(
                f"returncode={grpc.returncode}\nstdout:\n{grpc.stdout[-960:]}\nstderr:\n{grpc.stderr[-960:]}\n",
                encoding="utf-8"
            )
            grpc.check_returncode()
            assert json.loads(grpc.stdout)["trusted"] is True
            phase = "serve-stop"
            serve.send_signal(signal.SIGINT)
            assert serve.wait(timeout=15) == 0
            observed = serve_log.read_text()
            phase = "serve-markers"
            for marker in ("sse dataBytes=", "grpc event", "webSocketMessage decodedBytes=", "paused GET /debug",
                           "inspectionComplete=true", '"type":"stopped"'):
                present = marker in observed
                missing_marker = None if present else marker
                assert present, {"missingMarker": missing_marker}
            phase = "delivery-integrity"
            assert "deliveryLoss" not in observed
            assert "lossDetected=true" not in observed
            phase = "replay"
            har = directory / "replay.har"
            replay = subprocess.run([
                str(binary), "replay", str(certificate), str(key),
                f"https://localhost:{ready['http']['port']}/resource?token=lab-demo-token",
                str(har), "--allow-internal", "--origin-ca", str(upstream_roots)
            ], env=environment, capture_output=True, text=True, timeout=30, check=True)
            assert "replay complete status=200" in replay.stdout
            phase = "redacted-har"
            document = json.loads(har.read_text())
            assert document["log"]["version"] == "1.2"
            assert len(document["log"]["entries"]) == 1
            entry = document["log"]["entries"][0]
            assert entry["request"]["method"] == "GET"
            assert entry["response"]["status"] == 200
            assert "lab-demo-token" not in har.read_text()
            assert {"name": "token", "value": "[REDACTED]"} in entry["request"]["queryString"]
            assert "text" not in entry["response"]["content"]
            assert entry["response"]["content"]["size"] == len(b"SwiftMITM local replay resource\n")
            for timing_phase in ("send", "wait", "receive"):
                assert entry["timings"][timing_phase] >= 0
            print(json.dumps({"sse": True, "grpc": True, "websocket": True, "pausedEdit": True,
                              "replay": True, "redactedHAR": True, "gracefulStop": True}))
        except Exception as error:
            for path in directory.glob("*.log"):
                print(f"{path.name}:\n{path.read_text(errors='replace')[-2048:]}", file=sys.stderr)
            if phase == "replay" and isinstance(error, subprocess.CalledProcessError):
                print(f"replay returncode={error.returncode}\nstdout:\n{(error.stdout or '')[:512]}\n"
                      f"stderr:\n{(error.stderr or '')[:4096]}", file=sys.stderr, flush=True)
            print(json.dumps({"labFailurePhase": phase, "exceptionType": type(error).__name__[:128],
                              "missingMarker": missing_marker}), file=sys.stderr, flush=True)
            raise
        finally:
            for process in reversed(processes):
                stop(process)
            for handle in handles:
                handle.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    run(parser.parse_args().binary)
